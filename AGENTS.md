# Agent Notes for EOS

This file is a compact operating guide for AI coding agents working in the EOS
repository. It is not a substitute for reading the code around the change. Use
it to get oriented, avoid common footguns, and choose reasonable verification.

## Local Overrides

Agents should also read `AGENTS.local.md` from the repository root if it exists.
That file is for user-, machine-, or agent-specific preferences and must not be
committed. Local overrides may refine workflow details such as preferred build
directories, remote hosts, aliases, or reporting style.

If instructions conflict, prefer the more specific local instruction only when
it does not weaken repository safety, tests, review expectations, or production
operation rules from this file.

## First Principles

- Read the relevant source before editing. EOS has long-lived local patterns,
  especially in MGM, FST, console command handling, and protobuf plumbing.
- Keep patches scoped. Do not mix dashboard experiments, operational scripts,
  generated artifacts, and production code in one commit unless the user asks.
- Treat CERN EOS instances as production-like systems. Do not delete files,
  install packages, restart services, change policies, or trigger failovers
  unless the user explicitly authorizes that action.
- There may be unrelated untracked or modified files in the worktree. Ignore
  them unless the user explicitly asks you to handle them.

## Repository Map

- `mgm/`: Metadata Manager service, namespace control, scheduling, policies,
  admin commands, monitoring, and XRootD OFS plugin integration.
- `fst/`: File Storage server code and storage-side IO/reporting behavior.
- `common/`: Shared types, constants, utilities, protocol helpers, and common
  configuration keys.
- `console/`: EOS CLI implementation. Native commands live under
  `console/commands/native/`; legacy `com_*` sources are under
  `console/commands/coms/`.
- `proto/`: Protobuf definitions and the `eos-protobuf-spec` submodule.
- `fusex/`: FUSE client.
- `unit_tests/`: Unit tests, including MGM/FST focused tests.
- `test/`: Instance/integration scripts and dedicated test executables.
- `utils/`: Developer and operational helper scripts.
- `doc/`: Sphinx and Doxygen documentation sources.

## Build And Test

For server-side development, prefer an AlmaLinux 10 environment. Use AlmaLinux
9 as a fallback when AlmaLinux 10 is not available or when reproducing an
EL9-specific issue. For simple development and debugging, run MGM and FST on
the same host unless the test specifically needs a multi-host deployment.

Initialize submodules before a fresh build:

```bash
git submodule update --init --recursive
```

Typical Linux build:

```bash
mkdir -p build
cd build
cmake3 ..
make -j4
```

Ninja builds are supported:

```bash
mkdir -p build-with-ninja
cd build-with-ninja
cmake3 -GNinja ..
ninja -j4
```

Useful focused targets, when present in the local build tree:

```bash
make eos-unit-tests
make eos-unit-tests-fst
```

Run unit test binaries directly after building them. For ASAN test builds:

```bash
cmake3 ../ -DASAN=1
make eos-unit-tests eos-unit-tests-fst
```

Some local build trees are platform-specific and may not expose MGM/FST server
targets. Check available targets with:

```bash
cmake --build build --target help
```

Always run at least:

```bash
git diff --check
```

Install the repository pre-commit hooks so they run automatically on commits.
Prefer the distribution package when available; otherwise use Python tooling:

```bash
# Preferred on EL-style systems when available
dnf install pre-commit

# Portable fallback
python3 -m pip install --user pre-commit

pre-commit install
```

Commits run pre-commit hooks, including `clang-format-diff` on staged C/C++
changes. If the hook rewrites files, review and re-stage the result before
committing again.

## Coding Conventions

- Use existing EOS helper APIs and local abstractions before adding new ones.
- Prefer structured parsers/APIs over ad hoc string parsing.
- Keep comments sparse and useful; explain non-obvious intent, not mechanics.
- Follow the surrounding include order and formatting. Let the repository's
  `clang-format-diff` hook format changed C/C++ lines.
- Avoid broad refactors while fixing a narrow behavior.
- In C++ code, prefer modern, type-safe idioms where they fit the surrounding
  style: use `nullptr` instead of `NULL`, `const` for values and methods that
  should not mutate state, `override` for virtual overrides, and scoped enums
  where introducing a new enum is appropriate.
- Prefer RAII ownership types and existing EOS smart-pointer conventions over
  manual `new`/`delete`. Avoid raw owning pointers in new code.
- Avoid unnecessary copies in hot paths; pass larger objects by `const&` and use
  `std::move` only when ownership transfer is intended and clear.
- Keep error handling explicit. Do not swallow exceptions or return codes unless
  the surrounding API intentionally treats that failure as best-effort.

## MGM, FST, Console, And Protobuf Tips

- MGM operational logic often spans `mgm/shaping`, `mgm/proc/admin`,
  `mgm/monitoring`, and `common/Constants.hh`. Configuration changes usually
  need all relevant CLI, engine, persistence, monitoring, and tests updated.
- Be especially careful with MGM locks, in particular RW locks. Before changing
  code that takes `RWMutex`, `RWLock`, namespace, filesystem-view, or service
  locks, inspect the existing lock ordering and check for calls made while the
  lock is held. Do not introduce blocking IO, callbacks, logging paths that can
  re-enter locked code, or lock-order inversions. Deadlocks in MGM are
  production-critical, so explicitly reason about them in reviews.
- Prometheus metrics must be explicitly registered and emitted in
  `mgm/monitoring/PrometheusExporter.cc`. A value available in CLI output is
  not automatically available in `/metrics`.
- For new EOS monitoring metrics, prefer the built-in Prometheus exporter over
  `eos_exporter` when possible. Use `eos_exporter` only when the metric is
  already defined there or the change explicitly belongs to that exporter.
- Metric and dashboard-facing monitoring changes should normally include
  `MONIT` in the commit subject prefix, either alone or combined with the
  affected subsystem, for example `MGM/MONIT: ...`.
- Console protobuf commands generally require updates in native command code
  and the protobuf spec submodule. Check `proto/eos-protobuf-spec` status
  before assuming generated command fields exist.
- Any CLI change that adds, removes, or changes protobuf-backed command fields
  must also be reflected in the `eos-protobuf-spec` repository
  (`proto/eos-protobuf-spec`) so the same command works over gRPC.
- If a change touches both MGM and FST behavior, use a commit subject prefix
  that reflects both, e.g. `MGM/FST: ...`.
- If a change only affects monitoring or console output, prefer specific
  prefixes such as `MONIT:`, `CONSOLE:`, or combined prefixes when appropriate.

## Git Hygiene

- Inspect `git status --short --branch` before editing.
- Do not revert or clean files you did not create unless asked.
- Keep generated or experimental files out of commits unless they are part of
  the requested change.
- When folding into an existing commit, use `git commit --amend` or an
  interactive rebase carefully, then push with `--force-with-lease` only when
  rewriting the remote branch is expected.
- Do not bypass commit hooks by default. Commit without running hooks only when
  the hook itself is broken or unavailable; first try to understand the failure,
  patch the hook if appropriate, or record why the hook could not run.
- Prefer small commits with a single-line subject using the
  `SUBSYSTEM: short title` style, for example `MGM: Fix namespace lease check`.
  This keeps `git log --oneline` readable.
- A longer explanation is allowed when useful, but put it in the commit body
  after a blank line so it does not appear in `git log --oneline`. Prefer a
  single-message commit when the subject is enough.
- Use accurate subsystem prefixes:
  - `MGM: ...`
  - `FST: ...`
  - `CONSOLE: ...`
  - `MONIT: ...`
  - `PROTO: ...`
  - combined prefixes like `MGM/FST/CONSOLE: ...` when the patch spans them.
- When an AI coding assistant substantially helped implement, debug, or
  validate a change, add an `Assisted-by` trailer to the commit message. Match
  the existing style used in this repository, for example:

```text
Assisted-by: Codex:gpt-5.5
```

  Use the actual assistant/model when known. Do not add the trailer for purely
  manual changes or for commits where the assistant only performed trivial
  formatting or command execution.

## Operational Safety

- For remote hosts such as `eospilot`, `eoshomedev`, or build machines, start
  with read-only diagnostics unless the user explicitly asks for mutation.
- "Do not delete" means exactly that. It is fine to identify candidate files,
  sizes, owners, and commands the user can run; do not remove them.
- Be careful with `dnf`, service restarts, failovers, EOS policies, and traffic
  shaping settings. Confirm intent before making changes that affect a running
  instance.
- When running benchmarks, state where clients run from, what EOS app/tag is
  used, what limits/policies are set, and how cleanup will happen. Stop any
  background transfers before starting a new benchmark.
- If using `curl` to validate metrics, prefer direct exporter checks first:

```bash
curl -fsS --max-time 10 http://HOST:PORT/metrics | grep METRIC_NAME
```

## When You Are Unsure

- Prefer a small read-only check over guessing.
- Name the assumption in the response.
- If a command may mutate production state, ask first.
