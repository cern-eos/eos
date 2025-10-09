// ----------------------------------------------------------------------
// File: CommandFramework.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
#include "common/Path.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sstream>
#include <cstdlib>

CommandRegistry& CommandRegistry::instance()
{
  static CommandRegistry inst;
  return inst;
}

void CommandRegistry::reg(std::unique_ptr<IConsoleCommand> cmd)
{
  mCommandsView.push_back(cmd.get());
  mCommands.emplace_back(std::move(cmd));
}

IConsoleCommand* CommandRegistry::find(const std::string& name) const
{
  // Prefer most recently registered (native overrides legacy)
  for (auto it = mCommandsView.rbegin(); it != mCommandsView.rend(); ++it) {
    if (name == (*it)->name()) return *it;
  }
  return nullptr;
}

int CFuncCommandAdapter::run(const std::vector<std::string>& args, CommandContext&)
{
  std::ostringstream oss;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i) oss << ' ';
    oss << args[i];
  }
  std::string joined = oss.str();
  return mFunc((char*)joined.c_str());
}

// As an initial step, reuse the existing commands[] list to populate the registry.
extern COMMAND commands[];

void RegisterAllConsoleCommands()
{
  for (int i = 0; commands[i].name; ++i) {
    // Determine requiresMgm similar to current logic by name heuristic
    std::string nm = commands[i].name;
    bool req = !(nm == "clear" || nm == "console" || nm == "cp" ||
                 nm == "exit" || nm == "help" || nm == "json" ||
                 nm == "pwd" || nm == "quit" || nm == "role" ||
                 nm == "silent" || nm == "timing" || nm == "?" ||
                 nm == ".q" || nm == "daemon" || nm == "scitoken");
    CommandRegistry::instance().reg(std::make_unique<CFuncCommandAdapter>(
      commands[i].name, commands[i].doc ? commands[i].doc : "", commands[i].func, req));
  }
}

// Default empty; concrete commands will be registered here as they are migrated
void RegisterNativeConsoleCommands()
{
  // Registration split across native modules for maintainability
  // Basic wrappers
  extern void RegisterBasicWrappersNativeCommands();
  RegisterBasicWrappersNativeCommands();
  // Core
  extern void RegisterCoreNativeCommands();
  RegisterCoreNativeCommands();
  // Pwd/Cd
  extern void RegisterPwdCdNativeCommands();
  RegisterPwdCdNativeCommands();
  // Ls
  extern void RegisterLsNativeCommand();
  RegisterLsNativeCommand();
  // Version/Status
  extern void RegisterVersionStatusNativeCommands();
  RegisterVersionStatusNativeCommands();
  // Mkdir/Rm
  extern void RegisterMkdirRmNativeCommands();
  RegisterMkdirRmNativeCommands();
  // Info/Stat
  extern void RegisterInfoStatNativeCommands();
  RegisterInfoStatNativeCommands();
}


