## EOS Console Command Framework

The console has been refactored to route all commands through a small, typed command framework. This enables incremental migration of C‑style `com_*` commands to modern C++ classes without changing existing behavior.

### Key Pieces

- `CommandFramework.hh/.cc`
  - `IConsoleCommand`: interface for all commands
    - `name()`, `description()`, `requiresMgm(args)`, `run(args, ctx)`, `printHelp()`
  - `CommandRegistry`: registers and finds commands; registry order prefers most recently registered (native overrides legacy)
  - `RegisterNativeConsoleCommands()`: calls per‑module registrars to add new native commands (and unified legacy adapters)

- `CommandContext`
  - Lightweight context passed into `run(...)` providing access to current CLI state and helpers
  - Fields: `serverUri`, `GlobalOptions*`, flags (`json`, `silent`, `interactive`, `timing`)
  - Callbacks to existing glue: `clientCommand(...)` and `outputResult(...)`

- Native command modules (kept small):
  - `commands/native/CoreNativeCommands.cc`: `help`, `?`, toggles (`json`, `silent`, `timing`), `quit`/`exit`/`.q`
  - `commands/native/PwdCdNativeCommands.cc`: `pwd`, `cd`
  - `commands/native/LsNativeCommand.cc`: `ls` (currently delegates to legacy impl)
  - `commands/native/VersionStatusNativeCommands.cc`: `version`, `status`
  - `commands/native/MkdirRmNativeCommands.cc`: `mkdir`, `rm`
  - `commands/native/InfoStatNativeCommands.cc`: `info` (delegates), `stat` (inline)
  - `commands/native/RemainingLegacyNativeCommands.cc`: centralized inclusion and registration of remaining legacy `com_*` commands as native adapters

### Dispatch Flow

1. `ConsoleMain.cc` initializes the registry once on first command:
   - `RegisterNativeConsoleCommands()` (register modern classes and unified legacy adapters)
2. Input is tokenized; the first token selects the command: `CommandRegistry::find(name)`
3. `ConsoleMain` builds a `CommandContext` from current globals and calls `icmd->run(args, ctx)`
4. `requiresMgm(args)` is used to decide whether to ping the MGM before execution

### Completion and Help

- `ConsoleCompletion.cc` uses the registry to generate command completions (no dependency on the legacy table)
- The native `help` command lists all commands in the registry; when invoked with a specific command it delegates to existing `com_help` for per‑command details until each command provides native help

### Migration Strategy

The framework allows gradual conversion of legacy commands:

1. Register legacy commands via adapters (no behavior change)
2. Create a native command (in a focused `commands/native/*.cc` file) with the same `name()` and register it. The registry prefers native over legacy automatically
3. Port logic from `commands/com_*.cc` into the native `run(...)`, using `CommandContext` to call `client_command(...)` and `output_result(...)`
4. Remove the legacy file once parity is achieved (and update `CMakeLists.txt`)

Legacy `commands[]` is no longer used for registration; remaining `com_*` are registered via adapters in `RemainingLegacyNativeCommands.cc` until fully ported.

### Adding a New Native Command

1. Create a small `.cc` file under `commands/native/`, e.g. `EchoNativeCommand.cc`:

```c++
#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

namespace {
class EchoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "echo"; }
  const char* description() const override { return "Echo arguments"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    for (size_t i = 0; i < args.size(); ++i) {
      if (i) fprintf(stdout, " ");
      fprintf(stdout, "%s", args[i].c_str());
    }
    fprintf(stdout, "\n");
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterEchoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<EchoCommand>());
}
```

2. Declare and call its registrar from `RegisterNativeConsoleCommands()` (or add it to a grouping module):

```c++
// CommandFramework.cc
extern void RegisterEchoNativeCommand();
RegisterEchoNativeCommand();
```

3. Add the new file to `eos/console/CMakeLists.txt` under `EosConsoleCommands-Objects`

### Notes

- Native command files are deliberately small to keep the codebase navigable
- Registry order ensures native commands supersede legacy ones safely
- When a command does not require an MGM, set `requiresMgm(...)` to return `false`
- Prefer `CommandContext` callbacks over reaching into globals directly, except for flags that are intentionally process‑wide (`json`, `silent`, etc.)
 - Basic wrappers fallback: consider adding new wrappers to `BasicWrappersNativeCommands.cc` if you need immediate routing without a full native port; as you port, replace wrappers with dedicated native modules and remove the legacy entries


