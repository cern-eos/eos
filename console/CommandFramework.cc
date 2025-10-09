// ----------------------------------------------------------------------
// File: CommandFramework.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <sstream>

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
  // Native help command overrides legacy
  class HelpCommand : public IConsoleCommand {
  public:
    const char* name() const override { return "help"; }
    const char* description() const override { return "Display this text"; }
    bool requiresMgm(const std::string&) const override { return false; }
    int run(const std::vector<std::string>& args, CommandContext&) override {
      if (!args.empty()) {
        // Defer to legacy help for per-command help
        extern int com_help(char*);
        std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
        std::string joined = oss.str();
        return com_help((char*)joined.c_str());
      }
      auto& all = CommandRegistry::instance().all();
      fprintf(stderr, "Available commands:\n");
      for (auto* c : all) {
        fprintf(stderr, "  %-16s %s\n", c->name(), c->description());
      }
      return 0;
    }
    void printHelp() const override {}
  };

  // Toggle command for simple flags
  class ToggleFlagCommand : public IConsoleCommand {
  public:
    enum Which { JSON, SILENT, TIMING };
    ToggleFlagCommand(const char* n, const char* d, Which w, bool value)
      : mName(n), mDesc(d), mWhich(w), mValue(value) {}
    const char* name() const override { return mName.c_str(); }
    const char* description() const override { return mDesc.c_str(); }
    bool requiresMgm(const std::string&) const override { return false; }
    int run(const std::vector<std::string>&, CommandContext&) override {
      // Use existing globals
      extern bool json; extern bool silent; extern bool timing; extern bool interactive; extern bool runpipe; extern bool global_highlighting;
      switch (mWhich) {
        case JSON:   json = mValue; interactive = false; global_highlighting = false; runpipe = false; break;
        case SILENT: silent = mValue; break;
        case TIMING: timing = mValue; break;
      }
      return 0;
    }
    void printHelp() const override {}
  private:
    std::string mName; std::string mDesc; Which mWhich; bool mValue;
  };

  // Quit command
  class QuitCommand : public IConsoleCommand {
  public:
    QuitCommand(const char* n) : mName(n) {}
    const char* name() const override { return mName.c_str(); }
    const char* description() const override { return (mName=="exit"||mName==".q")?"Exit from EOS console":"Exit from EOS console"; }
    bool requiresMgm(const std::string&) const override { return false; }
    int run(const std::vector<std::string>&, CommandContext&) override {
      extern int done; done = 1; return 0;
    }
    void printHelp() const override {}
  private:
    std::string mName;
  };

  CommandRegistry::instance().reg(std::make_unique<HelpCommand>());
  // Aliases for help
  class HelpAlias : public HelpCommand { public: const char* name() const override { return "?"; } };
  CommandRegistry::instance().reg(std::make_unique<HelpAlias>());

  // Toggles
  CommandRegistry::instance().reg(std::make_unique<ToggleFlagCommand>("json",   "Toggle JSON output flag for stdout", ToggleFlagCommand::JSON,   true));
  CommandRegistry::instance().reg(std::make_unique<ToggleFlagCommand>("silent", "Toggle silent flag for stdout",     ToggleFlagCommand::SILENT, true));
  CommandRegistry::instance().reg(std::make_unique<ToggleFlagCommand>("timing", "Toggle timing flag for execution time measurement", ToggleFlagCommand::TIMING, true));

  // Quit variants
  CommandRegistry::instance().reg(std::make_unique<QuitCommand>("quit"));
  CommandRegistry::instance().reg(std::make_unique<QuitCommand>("exit"));
  CommandRegistry::instance().reg(std::make_unique<QuitCommand>(".q"));

  // pwd
  class PwdCommand : public IConsoleCommand {
  public:
    const char* name() const override { return "pwd"; }
    const char* description() const override { return "Print working directory"; }
    bool requiresMgm(const std::string&) const override { return false; }
    int run(const std::vector<std::string>&, CommandContext&) override {
      extern XrdOucString gPwd; fprintf(stdout, "%s\n", gPwd.c_str()); return 0;
    }
    void printHelp() const override {}
  };
  CommandRegistry::instance().reg(std::make_unique<PwdCommand>());

  // cd
  class CdCommand : public IConsoleCommand {
  public:
    const char* name() const override { return "cd"; }
    const char* description() const override { return "Change directory"; }
    bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
    int run(const std::vector<std::string>& args, CommandContext&) override {
      // Reuse legacy implementation for correctness; join args and call com_cd
      extern int com_cd(char*);
      std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
      std::string joined = oss.str();
      return com_cd((char*)joined.c_str());
    }
    void printHelp() const override {}
  };
  CommandRegistry::instance().reg(std::make_unique<CdCommand>());

  // Override additional frequently used legacy commands with adapters to allow
  // future drop-in native replacements without depending on the static table order.
  auto legacyDoc = [](const char* nm) -> const char* {
    for (int i = 0; commands[i].name; ++i) {
      if (std::string(nm) == commands[i].name) {
        return commands[i].doc ? commands[i].doc : "";
      }
    }
    return "";
  };
  auto legacyFunc = [](const char* nm) -> COMMAND* {
    for (int i = 0; commands[i].name; ++i) {
      if (std::string(nm) == commands[i].name) {
        return &commands[i];
      }
    }
    return nullptr;
  };
  auto reqMgm = [](const std::string& nm) -> bool {
    return !(nm == "clear" || nm == "console" || nm == "cp" ||
             nm == "exit" || nm == "help" || nm == "json" ||
             nm == "pwd" || nm == "quit" || nm == "role" ||
             nm == "silent" || nm == "timing" || nm == "?" ||
             nm == ".q" || nm == "daemon" || nm == "scitoken");
  };

  const char* names[] = {"status","ls","find","info","stat","mkdir","rmdir","rm","mv","ln","cp","version","whoami","who","file","map","report","quota"};
  for (const char* nm : names) {
    if (auto* cmd = legacyFunc(nm)) {
      CommandRegistry::instance().reg(std::make_unique<CFuncCommandAdapter>(
        cmd->name, legacyDoc(nm), cmd->func, reqMgm(nm)));
    }
  }

  // Native 'ls' delegating to legacy behavior for full parity; will be replaced later
  class LsCommand : public IConsoleCommand {
  public:
    const char* name() const override { return "ls"; }
    const char* description() const override { return "List a directory"; }
    bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
    int run(const std::vector<std::string>& args, CommandContext&) override {
      extern int com_ls(char*);
      std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
      std::string joined = oss.str();
      return com_ls((char*)joined.c_str());
    }
    void printHelp() const override {}
  };
  CommandRegistry::instance().reg(std::make_unique<LsCommand>());

  // Native 'version' overriding legacy for cleaner parsing and client version append
  class VersionCommand : public IConsoleCommand {
  public:
    const char* name() const override { return "version"; }
    const char* description() const override { return "Verbose client/server version"; }
    bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
    int run(const std::vector<std::string>& args, CommandContext&) override {
      // Reuse legacy for server call logic to ensure parity
      extern int com_version(char*);
      std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
      std::string joined = oss.str();
      return com_version((char*)joined.c_str());
    }
    void printHelp() const override {}
  };
  CommandRegistry::instance().reg(std::make_unique<VersionCommand>());
}


