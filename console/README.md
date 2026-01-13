## EOS Console Command Framework (Native)

All console commands are dispatched through `CommandRegistry`/`IConsoleCommand`. The legacy `commands[]` table is gone; registration happens only via per-file `Register*` functions.

### Directory layout (console/)
- `CommandFramework.*`        : Registry, interfaces, and core wiring
- `ConsoleMain.*`             : REPL and dispatch; calls `RegisterNativeConsoleCommands()`
- `commands/native/`          : Native command implementations
  - `*-proto-native.cc`       : Proto-based commands
  - `*-cmd-native.cc`         : Direct MGM (`mgm.cmd=...`) commands
  - `*-com-native.cc`         : Wrappers around legacy `com_*`
  - `*-alias.cc`              : Aliases forwarding to other commands
- `commands/helpers/`         : Shared helpers (e.g., `ICmdHelper`, FsHelper, TokenHelper)
- `commands/coms/`            : Legacy `com_*.cc` sources
  - `commands/coms/unused/`   : Legacy sources no longer linked/needed
- `commands/native/LegacySymbols.cc` : Minimal legacy symbols still required by some native wrappers
- `console/CMakeLists.txt`    : Build list; add new native files here

### Core interfaces
- `IConsoleCommand`: `name()`, `description()`, `requiresMgm(args)`, `run(args, CommandContext&)`, `printHelp()`.
- `CommandContext`: current CLI state plus callbacks `clientCommand(...)` and `outputResult(...)`. Prefer these over direct legacy globals.
- `CommandRegistry`: `reg(...)`, `find(name)`, `all()`. Newest registration wins, so native overrides any legacy adapters.

### Naming conventions for native files
- `*-proto-native.cc`  : builds/uses protobuf helpers (`SpaceProto_*`, etc.).
- `*-cmd-native.cc`    : builds MGM requests directly (`mgm.cmd=...`).
- `*-com-native.cc`    : wraps or delegates to legacy `com_*` implementations.
- `*-alias.cc`         : forwards to another command (e.g., `info-alias` → `file info`).

### Registering commands
- Each file exposes `RegisterXxx...()` and calls `CommandRegistry::instance().reg(...)`.
- `CommandFramework.cc` invokes all `Register*` once at startup (`RegisterNativeConsoleCommands()`).
- Add new files to `console/CMakeLists.txt` under `EosConsoleCommands-Objects`.

### Calling another console command
Use the registry to compose/forward behavior:
```c++
IConsoleCommand* other = CommandRegistry::instance().find("file");
if (!other) { fprintf(stderr, "error: 'file' command not available\n"); return EINVAL; }
std::vector<std::string> forwarded = {"info", path};
int rc = other->run(forwarded, ctx); // reuse the same CommandContext
```

### Creating an alias
- Create a small `*-alias.cc`.
- In `run(...)`, find the target command, prepend the subcommand, and call `run` on the target with the same `CommandContext`.
- Implement `printHelp()` with a `usage:` line (emit to `stderr`) noting it forwards to the target.

### Help / usage
- `printHelp()` should write to `stderr` and begin with `usage: <cmd> ...`.
- On invalid args or `--help`, call `printHelp()`, set `global_retc = EINVAL`, and return.

### MGM requirement
- Return `false` from `requiresMgm(...)` for local-only commands (e.g., `clear`, `whoami`); otherwise typically return `!wants_help(args.c_str())` so help does not ping MGM.

### Migrating legacy commands
- If a native command still calls `com_*`, keep the file named `*-com-native.cc`.
- If it builds `mgm.cmd=...`, use `*-cmd-native.cc`.
- If it uses protobuf helpers, use `*-proto-native.cc`.
- Move logic into `run(...)` using `CommandContext` callbacks; drop legacy dependencies once behavior matches.

