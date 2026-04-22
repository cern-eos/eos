# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

Out-of-source CMake build with Ninja (preferred) or Make. The existing build tree in this clone is `cmake-build-relwithdebinfo/` (Ninja, RelWithDebInfo).

```bash
git submodule update --init --recursive     # required — much of the code pulls submodules
mkdir build && cd build
cmake3 -GNinja ..                            # add -DCLIENT=1 for client-only build
ninja -j 4
```

Useful CMake options:
- `-DCLIENT=1` — build only client binaries (skips `mgm/`, `namespace/`, `auth_plugin/`, `archive/`, `unit_tests/`, `quarkdb/`).
- `-DASAN=1` / `-DTSAN=1` — AddressSanitizer / ThreadSanitizer build. Run ASan unit tests with `LSAN_OPTIONS=suppressions=misc/var/eos/test/LeakSanitizer.supp`.
- `-DUSE_SYSTEM_GTEST=ON` — use installed GoogleTest instead of the bundled submodule at `unit_tests/googletest`.
- `-DCOVERAGE=ON` — LCOV coverage instrumentation (see `cmake/EosCoverage.cmake`).
- `-DPACKAGEONLY=ON` — skip compile flag configuration (used for `srpm`/`rpm` targets).

Packaging targets (from build dir): `ninja dist` (source tarball), `ninja srpm`, `ninja rpm`.

## Testing

Unit tests are built as part of `all`. Four executables, all linking against EOS libraries statically, installed to `sbin/`:

```bash
ninja eos-unit-tests              # console + mgm + common + fusex
ninja eos-unit-tests-fst          # fst (built with -D_NOOFS=1)
ninja eos-unit-tests-with-qdb     # needs a running QuarkDB — see below
ninja eos-unit-tests-with-instance # needs a running EOS instance
```

All executables are GoogleTest binaries — filter with `--gtest_filter=SuiteName.TestName` / `SuiteName.*`, list with `--gtest_list_tests`.

The `-with-qdb` and `-with-instance` binaries require live services. QuarkDB tests connect via `EOS_QUARKDB_HOSTPORT` (default `localhost:9999`) and `EOS_QUARKDB_PASSWD` (see `namespace/ns_quarkdb/tests/README.md`).

Integration / instance tests live under `test/` as standalone executables and shell scripts (e.g. `eos-instance-test`, `eos-fsck-test`, `eos-drain-test`).

Unit-test sources are declared in `unit_tests/CMakeLists.txt` — add new tests to the appropriate `*_UT_SRCS` list there.

## Style

`.clang-format` and `.clang-tidy` at the repo root define the house style. Notable choices: `AccessModifierOffset: -2`, `AlwaysBreakAfterReturnType: AllDefinitions`, brace on next line after function definitions only. Run `clang-format -i <file>` before committing non-trivial reformatting.

## Architecture

EOS is a multi-PB disk storage system built on top of XRootD. The running system is a set of cooperating XRootD plugins plus a FUSE client and auxiliary services. Understanding which component owns which code path is the key to navigating the repo.

### Components / top-level directories

- **`mgm/`** — **MGM** (Meta Data Server) plugin. The head node: namespace redirector, scheduler, policy, quota, ACLs, config, CLI dispatch. Entry point `XrdMgmOfs.cc`; commands under `mgm/proc/` and `mgm/commandmap/`; subsystems in peer dirs (`balancer/`, `drain/`, `fsck/`, `convert/`, `groupbalancer/`, `groupdrainer/`, `tgc/` tape garbage collector, `placement/`, `scheduler/`, `FuseServer/`, `http/rest-api/`, `bulk-request/`, `wfe/` workflow engine, `qdbmaster/` HA master election). Links as `XrdEosMgm-Static`.
- **`fst/`** — **FST** (File Storage Target) plugin. Runs on every storage node: serves file I/O, checksums, scans, migrations. Entry points `XrdFstOfs*.cc` and `XrdFstOss*.cc`; layouts (`layout/`, including RAIN/erasure), I/O backends (`io/`), background work (`ScanDir`, `Verify`, `storage/`, `http/`).
- **`namespace/`** — Metadata namespace library. `namespace/ns_quarkdb/` is the production QuarkDB-backed implementation (containers/files in `ContainerMD`/`FileMD`, persistence via `qclient`). Loaded by MGM as `NsQuarkdbPlugin`. The old in-memory implementation is retained only as interfaces/utilities.
- **`fusex/`** — `eosxd` FUSE client. Bidirectional cache coherence with MGM's `FuseServer` via the protobuf protocol in `mgm/fusex.proto`. Submount, stats, and an alternative CFS daemon live here.
- **`common/`** — Shared primitives used by every component: logging (`Logging.hh` — `eos_static_info`/`eos_static_err`), RWMutex, threading (`AssistedThread`, `ThreadPool`, concurrency primitives), `FileSystem`, `Mapping`, `StringConversion`, config, `Fmd` (file-metadata record), symkeys, buffer manager, etc. Prefer these over stdlib re-implementations.
- **`console/`** — `eos` CLI client binary, plus per-command parsers in `console/commands/`. Shares the `MGM ↔ console` protobuf surface in `proto/`.
- **`proto/`** — Protobuf definitions consumed by multiple components (auth, console commands, CTA, FST, REST, audit). Generated sources are placed alongside the consumers at build time.
- **`client/`** — gRPC client library (`EosGrpcClient`); only built if gRPC is found.
- **`auth_plugin/`** — Optional XRootD authorization plugin that proxies auth decisions to the MGM over ZeroMQ.
- **`mq/`** — XRootD MQ plugin (legacy pub/sub) and the newer ZeroMQ-based messaging under `common/`.
- **`quarkdb/`** — QuarkDB server sources pulled in as a submodule; built as part of the tree when server components are enabled.
- **`test/`** / **`unit_tests/`** — End-to-end and integration executables / shell tests vs. GoogleTest-based unit tests.

### Cross-cutting notes

- **Protobuf everywhere.** MGM↔console, MGM↔FuseServer, audit records, CTA workflow, auth plugin, REST tape API, and most gRPC services are all defined in `proto/` or as `.proto` files next to the consumer (e.g. `mgm/fusex.proto`, `mgm/wfe.proto`). When changing cross-component surfaces, update the `.proto` first.
- **QuarkDB** is the persistent backing store for the namespace and for MGM HA state. `namespace/ns_quarkdb/qclient/` is the submodule client library shared by MGM and FST. Master election and config live in `mgm/qdbmaster/` and `mgm/config/`.
- **Audit logging** is structured JSON serialized from `proto/Audit.proto`, written ZSTD-compressed and rotated hourly (`EOS_AUDIT_ROTATION`). See `AUDIT.md` for the record schema and integration points.
- **Submodules** are load-bearing — `namespace/ns_quarkdb/qclient`, `common/xrootd-ssi-protobuf-interface`, `quarkdb`, `unit_tests/googletest`, `common/grpc-proto`, `proto/eos-protobuf-spec`, `fst/css_plugin`, `console/parser`. Running without `git submodule update --init --recursive` will fail at configure time.
- **Server vs. client-only.** Everything under `mgm/`, `namespace/`, `auth_plugin/`, `archive/`, `unit_tests/`, and `quarkdb/` is gated on `NOT CLIENT` in the top-level `CMakeLists.txt`. When editing build rules for those directories, they must stay inside that conditional block.
