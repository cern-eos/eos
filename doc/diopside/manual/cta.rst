.. index::
   pair: MGM; CTA

.. highlight:: rst

CTA integration (Tape Mode)
===========================

This chapter describes EOS MGM’s integration with CTA (CERN Tape Archive): enabling tape mode, CTA report logging, stage/abort flows via the Workflow Engine (WFE), eviction, and the tape-aware garbage collector (TGC).

Overview
--------
When the MGM is configured for tape (``mTapeEnabled=true``), EOS will:

- Accept and process tape-related workflows (stage/evict) through the WFE and bulk-request subsystems.
- Produce structured CTA report records into the standard EOS report log stream.
- Run the Tape-aware Garbage Collector (TGC) for tape-backed spaces when configured.

Enabling Tape Mode
------------------
Tape functionality is enabled at the MGM level (``mTapeEnabled``). In code, related decisions are guarded by tape checks (e.g. ``isTapeEnabled()`` or ``mTapeEnabled``).

In the MGM xrootd configuration file (for example ``/etc/xrd.cf.mgm`` or ``/etc/eos/config/mgm/...``) enable tape mode with:

.. code-block:: none

   mgmofs.tapeenabled 1

CTA Report Logs
---------------
CTA events are written as EOS report log entries (``key=value`` pairs) using helpers in ``mgm/cta/Reporter.*``:

- Reporter classes:

  - ``cta::Reporter`` (base)
  - ``cta::ReporterPrepareReq``
  - ``cta::ReporterPrepareWfe``
  - ``cta::ReporterEvict``
  - ``cta::ReporterFileCreation``
  - ``cta::ReporterFileDeletion``

Each reporter accumulates fields and emits a single line to the report log when it goes out of scope.

Compressed report segments
~~~~~~~~~~~~~~~~~~~~~~~~~~
If compressed reports are enabled, CTA records are automatically written to ZSTD-compressed, time-rotated files:

.. code-block:: bash

   EOS_ZSTD_REPORTS=1
   EOS_ZSTD_REPORTS_ROTATION=86400  # seconds, default 86400 (1 day)
   EOS_ZSTD_REPORTS_LEVEL=1         # compression level 1..19, default 1

Stage (Prepare) Flow
--------------------
The stage/abort flow involves the bulk-request manager and the WFE:

1. A client issues a "prepare" request.
2. ``PrepareManager`` validates inputs, maps identities (IdMap), and augments the request id (with a timestamp).
3. A ``cta::ReporterPrepareReq`` record is emitted with the request metadata (log id, path, uid/gid, host, reqid, etc.).
4. The WFE receives the event (``sync::prepare`` / ``sync::abort_prepare``) and performs the workflow actions, reporting its milestones via ``cta::ReporterPrepareWfe``.

Evict Flow
----------
Evict removes disk replicas for files that are tape-resident:

1. Admin command validates file(s) and verifies tape presence (``EOS_TAPE_MODE_T``).
2. If permitted, MGM proceeds with disk-replica eviction according to the request.
3. A ``cta::ReporterEvict`` record captures the operation and any errors (e.g. “not on tape”, permission issues).

Tape-aware Garbage Collection (TGC)
-----------------------------------
TGC modules live under ``mgm/cta/tgc`` and are activated only when tape is enabled and spaces are configured for GC. Major components:

- ``SmartSpaceStats``: collects/maintains per-space statistics (ages, histograms).
- ``FreedBytesHistogram``: summarizes GC effects and aging.
- ``Lru``: ordering for file selection when needed.
- ``MultiSpaceTapeGc``: orchestrates GC across spaces, reads policies from ``SpaceConfig`` and drives ``TapeGc``.
- ``RealTapeGcMgm`` / ``ITapeGcMgm``: MGM-specific facade for GC operations.
- ``SpaceToTapeGcMap``, ``SpaceStats``, ``TapeGcStats``: configuration and counters.
- ``AsyncUint64ShellCmd``: helper for metrics that require external commands.
- ``IClock`` / ``RealClock``: time abstractions to aid testing.

TGC configuration (per-space)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TGC is configured per space via space configuration members (read by MGM). The following keys are supported:

- ``tgc.qryperiodsecs`` (default: 320)

  - Delay in seconds between TGC space-stat queries.
  - Must be > 0 and <= ``TGC_MAX_QRY_PERIOD_SECS`` (derived from histogram dimensions).
  - Also drives the FreedBytesHistogram bin width: roughly ``ceil(qryperiodsecs / nbBins)``.

- ``tgc.availbytes`` (default: 0)

  - Target minimum available bytes. GC only proceeds if current ``availBytes < tgc.availbytes``.

- ``tgc.totalbytes`` (default: 1 Exabyte)

  - Minimum total bytes threshold before GC can begin. If current ``totalBytes < tgc.totalbytes``, GC does not run. The large default effectively disables this guard unless set.

- ``tgc.freebytesscript`` (default: empty)

  - Optional script path to override free-bytes with an external source. Invoked as ``<script> <space-name>`` and must print a single unsigned 64-bit integer (free bytes) on stdout. Used asynchronously with fallback to internal stats on error.

Configuration cache
^^^^^^^^^^^^^^^^^^^
- TGC caches space configuration in memory up to ``TGC_DEFAULT_MAX_CONFIG_CACHE_AGE_SECS`` (10 seconds) to reduce overhead.

Setting parameters
^^^^^^^^^^^^^^^^^^
Use the EOS console to set space members (example for ``default`` space):

.. code-block:: bash

   eos space config default tgc.qryperiodsecs=600
   eos space config default tgc.availbytes=500000000000     # 500 GB
   eos space config default tgc.totalbytes=10000000000000   # 10 TB
   eos space config default tgc.freebytesscript=/usr/local/bin/eos-free-bytes

Configuration and CLI
---------------------
Tape mode and WFE
~~~~~~~~~~~~~~~~~
- Tape mode must be enabled at MGM for workflows to be active.
- The WFE needs to be on (e.g., in the default space configuration), with appropriate intervals and thread settings.

Report logging
~~~~~~~~~~~~~~
- Optional compressed reports:

  - ``EOS_ZSTD_REPORTS=1``
  - ``EOS_ZSTD_REPORTS_ROTATION=<seconds>`` (default 86400)
  - ``EOS_ZSTD_REPORTS_LEVEL=<1..19>`` (default 1)

Evict CLI
~~~~~~~~~
- The admin “evict” command supports CTA-specific options (e.g., ``fsid``, ``ignore-evict-counter``, ``ignore-removal-on-fst``) with server-side validation.

TGC policies
~~~~~~~~~~~~
- Per-space GC policies are read by TGC from ``SpaceConfig`` and related structures, controlling when and how aggressively GC runs.
- See space-related console commands (``console/commands/com_proto_space.cc``) for available knobs.

Code Pointers
-------------
- CTA report helpers: ``mgm/cta/Reporter.*``
- CTA utilities: ``mgm/cta/Utils.*``
- TGC core: ``mgm/cta/tgc/*``
- Prepare manager: ``mgm/bulk-request/prepare/manager/PrepareManager.*``
- Workflow engine: ``mgm/WFE.cc``
- Evict command: ``mgm/proc/admin/EvictCmd.cc``
- Space console commands: ``console/commands/com_proto_space.cc``


