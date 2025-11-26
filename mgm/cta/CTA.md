CTA integration in EOS MGM
==========================

This document describes EOS MGM’s integration points with CTA (CERN Tape Archive), how to enable tape mode, how requests are prepared and evictions reported, and how the tape-aware garbage collector (TGC) works and is configured.

Contents
- Overview
- Enabling tape mode (mTapeEnabled)
- CTA report logs
- CTA request/eviction reporting
- Prepare flow (stage/abort)
- Evict flow
- Tape-aware garbage collection (TGC)
- Components in mgm/cta and mgm/cta/tgc
- Configuration knobs and CLI
- Operational notes

Overview
--------
When MGM is configured for tape (mTapeEnabled=true), a number of tape-aware features are enabled:
- Requests to stage data from, or evict data to, CTA are recognized and routed via MGM’s workflow engine (WFE) and bulk-request code paths.
- EOS produces structured CTA-related report records (written into the standard EOS report log stream) for auditing and monitoring.
- A Tape-aware Garbage Collector (TGC) coordinates disk-space reclamation in spaces with tape-backed files.

Enabling tape mode (mTapeEnabled)
---------------------------------
Tape mode is a runtime setting of the MGM. In code paths you will frequently see checks similar to:
- “isTapeEnabled()” on MGM interfaces; or
- “mTapeEnabled” in core MGM classes.

To enable tape mode, set the following directive in the MGM xrootd configuration (e.g. `/etc/xrd.cf.mgm` or `/etc/eos/config/mgm/...`):

```
mgmofs.tapeenabled 1
```

When tape is enabled:
- MGM accepts “prepare/stage” requests and forwards them into the workflow engine (WFE), and
- TGC subsystems and tape-specific policies are effective where configured.

CTA report logs
---------------
CTA-related events (prepare, WFE activity, evict, file create/delete for CTA) are recorded as EOS report log lines (key=value pairs). These are written through a small helper in `mgm/cta/Reporter.*`:
- `cta::Reporter` and the typed helpers:
  - `cta::ReporterPrepareReq`
  - `cta::ReporterPrepareWfe`
  - `cta::ReporterEvict`
  - `cta::ReporterFileCreation`
  - `cta::ReporterFileDeletion`
- Each helper accumulates “report parameters” (`cta::ReportParam`) and emits on destruction through the configured writer callback (defaults to MGM Iostat writer).

Report log compression and rotation:
- If EOS ZSTD-compressed reports are enabled, CTA records transparently go to the compressed, rotated segments. See the report logging environment variables:
  - `EOS_ZSTD_REPORTS=1` to enable compressed reports;
  - `EOS_ZSTD_REPORTS_ROTATION=<seconds>` (default 86400);
  - `EOS_ZSTD_REPORTS_LEVEL=<1..19>` (default 1).

CTA request/eviction reporting
------------------------------
The following MGM subsystems emit CTA report records:
- PrepareManager (`mgm/bulk-request/prepare/manager/PrepareManager.*`):
  - Upon receiving a “prepare” (stage) request, a `cta::ReporterPrepareReq` is initialized with the standard keys (log id, path, uid/gid, host, reqid, etc.).
  - The request id is augmented with a timestamp to ensure uniqueness.
- WFE (`mgm/WFE.cc`):
  - When tape is enabled and WFE processes prepare-related events, a `cta::ReporterPrepareWfe` is used to record progress or failure states.
  - File creation/deletion related to CTA metadata results in `cta::ReporterFileCreation` and `cta::ReporterFileDeletion` records.
- Admin Evict command (`mgm/proc/admin/EvictCmd.cc`):
  - When an evict request is processed, `cta::ReporterEvict` records the details and any errors encountered.

Prepare flow (stage/abort)
--------------------------
The **prepare** (stage) flow is handled in the bulk-request manager and the WFE:
1) Client issues a prepare (stage) request.
2) MGM validates input and identity mapping (IdMap) as usual.
3) `PrepareManager` formats the request, derives/augments the request id, and emits a CTA prepare-request record.
4) WFE receives the event (`sync::prepare` or `sync::abort_prepare`) and performs the workflow-specific actions, reporting WFE-side milestones to the CTA log.

Abort prepare requests go through a similar path but with the abort event.

Evict flow
----------
The **evict** path removes disk replicas for files that are already on tape:
1) The admin command path validates the file(s) and checks tape presence (`EOS_TAPE_MODE_T` bit on mode).
2) If permitted and applicable, MGM proceeds with eviction logic for the requested disk replicas.
3) `cta::ReporterEvict` records the eviction attempt, including any errors (e.g. “not on tape”, “no permission”, “fsid mismatch”).

Tape-aware garbage collection (TGC)
-----------------------------------
The TGC subsystem is under `mgm/cta/tgc/`. Its goals are to:
- Track per-space tape/disk state and determine safe candidates for garbage collection,
- Honor per-space policies (age, quotas/targets, budgets),
- Provide instrumentation (e.g., freed bytes histograms).

Key components (high level):
- `SmartSpaceStats`:
  - Periodically collects and maintains statistics at space level (age distributions, histograms, rates).
  - Uses `FreedBytesHistogram` to summarize GC effects and aging.
- `Lru`:
  - Provides least-recently-used ordering information to pick candidates when needed.
- `MultiSpaceTapeGc`:
  - Orchestrates GC across multiple spaces, reads policy knobs via `SpaceConfig`, and drives `TapeGc` runs.
- `RealTapeGcMgm` (and `ITapeGcMgm` interface):
  - MGM-specific facade with IO and control logic for GC operations from MGM’s point of view.
- `SpaceToTapeGcMap`, `SpaceStats`, `TapeGcStats`:
  - Structures that hold per-space GC configuration and counters/statistics.
- `AsyncUint64ShellCmd`:
  - Helper to run shell commands asynchronously and parse integer outputs where needed by GC metrics.
- `IClock` / `RealClock`, `DummyClock`:
  - Time abstractions to improve testability.

The TGC runs only when tape is enabled and the affected spaces are configured for GC. TGC is modular and policy-driven through the above classes; it does not enable itself automatically for every space without configuration.

TGC configuration (per-space)
-----------------------------
Configuration members are read from each EOS space’s configuration (via `FsView::SpaceView.GetConfigMember`). The following keys are supported:

- `tgc.qryperiodsecs` (default: 320)
  - Delay in seconds between TGC space-stat queries.
  - Must be > 0 and <= `TGC_MAX_QRY_PERIOD_SECS` (calculated from histogram size and maximum bin width).
  - Also controls the FreedBytesHistogram bin width (effectively `ceil(qryperiodsecs / nbBins)`).
  - Invalid values are ignored and an error is logged.

- `tgc.availbytes` (default: 0)
  - Target minimum available bytes for the space. Garbage collection will only proceed if current `availBytes < tgc.availbytes`.

- `tgc.totalbytes` (default: 1 Exabyte)
  - Minimum total bytes threshold before GC can begin. If current `totalBytes < tgc.totalbytes`, GC will not run. The very high default effectively disables this guard unless explicitly set.

- `tgc.freebytesscript` (default: empty)
  - Optional external script path that, when set, is invoked as `script <space-name>` and must print a single unsigned 64-bit integer (free bytes) to stdout.
  - The value is used asynchronously; when pending, TGC may keep the previous value if available, or fall back to internal stats on error. Errors are logged and do not stop TGC.

Notes:
- TGC periodically updates and uses a histogram of freed bytes; `tgc.qryperiodsecs` determines the histogram bin width and influences smoothing.
- TGC configuration is cached in-memory for up to `TGC_DEFAULT_MAX_CONFIG_CACHE_AGE_SECS` (10s) to reduce load.

Setting TGC parameters
~~~~~~~~~~~~~~~~~~~~~~
Use the EOS console to set space members. For example, to configure space “default”:

```
eos space config default tgc.qryperiodsecs=600
eos space config default tgc.availbytes=500000000000   # 500 GB
eos space config default tgc.totalbytes=10000000000000 # 10 TB
eos space config default tgc.freebytesscript=/usr/local/bin/eos-free-bytes
```

Components in mgm/cta and mgm/cta/tgc
--------------------------------------
- `mgm/cta/`:
  - `Reporter.*` – generic CTA report record emitters.
  - `Utils.*` – utility helpers used by CTA/TGC code (parsing, rounding, reading FDs, etc).
- `mgm/cta/tgc/`:
  - Core GC logic and helpers enumerated in the TGC section above.

Configuration knobs and CLI
---------------------------
Tape mode and WFE
- MGM tape mode (mTapeEnabled) must be enabled in configuration for tape workflows to be active.
- WFE is responsible for driving prepare/evict workflows; in the default space the knobs are read from space configs (e.g., `space.wfe=on` and related interval settings).

Report logs
- Optional compressed reports can be enabled system-wide:
  - `EOS_ZSTD_REPORTS=1`
  - `EOS_ZSTD_REPORTS_ROTATION=<seconds>` (default 86400)
  - `EOS_ZSTD_REPORTS_LEVEL=<1..19>` (default 1)

Evict CLI
- The `eos evict` admin command has additional options (e.g. `fsid`, `ignore-evict-counter`, `ignore-removal-on-fst`) with server-side validation (see `EvictCmd`).

TGC policies
- Per-space GC parameters are read by the TGC from `SpaceConfig` and related structures; these govern when GC runs and how aggressively it frees disk.
- The console “space” commands expose some of these knobs (see `console/commands/com_proto_space.cc` for the up-to-date list).

Operational notes
-----------------
- MGM-side tape interactions rely on standard EOS metadata flags:
  - Tape presence is identified with `EOS_TAPE_MODE_T` on file metadata.
- CTA report logs use the standard EOS report ingestion path, so any downstream collection that consumes EOS report logs will automatically see CTA events as well.
- CTA and TGC modules live under `mgm/cta/` and are only linked by the MGM plugin build; no FST-side code is affected by enabling tape on the MGM side.

Pointers to code
----------------
- CTA report writers: `mgm/cta/Reporter.*`
- CTA utilities: `mgm/cta/Utils.*`
- TGC core: `mgm/cta/tgc/*`
- Bulk-request prepare manager: `mgm/bulk-request/prepare/manager/PrepareManager.*`
- Workflow engine (WFE): `mgm/WFE.cc`
- Evict command: `mgm/proc/admin/EvictCmd.cc`
- Space console commands: `console/commands/com_proto_space.cc`


