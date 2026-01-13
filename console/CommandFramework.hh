// ----------------------------------------------------------------------
// File: CommandFramework.hh
// Purpose: Lightweight command registry and adapters for console commands
// ----------------------------------------------------------------------

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "console/ConsoleMain.hh"

// Context passed to commands to access existing facilities without global coupling
struct CommandContext {
  std::string serverUri;
  GlobalOptions* globalOpts;
  bool json;
  bool silent;
  bool interactive;
  bool timing;
  std::string userRole;
  std::string groupRole;

  // Thin wrappers to existing functions
  XrdOucEnv* (*clientCommand)(XrdOucString& in, bool isAdmin, std::string* reply);
  int (*outputResult)(XrdOucEnv* result, bool highlighting);
};

class IConsoleCommand {
public:
  virtual ~IConsoleCommand() = default;
  virtual const char* name() const = 0;
  virtual const char* description() const = 0;
  virtual bool requiresMgm(const std::string& args) const { return !wants_help(args.c_str()); }
  virtual int run(const std::vector<std::string>& args, CommandContext& ctx) = 0;
  virtual void printHelp() const = 0;
};

class CommandRegistry {
public:
  static CommandRegistry& instance();
  void reg(std::unique_ptr<IConsoleCommand> cmd);
  IConsoleCommand* find(const std::string& name) const;
  const std::vector<IConsoleCommand*>& all() const { return mCommandsView; }

private:
  std::vector<std::unique_ptr<IConsoleCommand>> mCommands;
  std::vector<IConsoleCommand*> mCommandsView;
};

// Adapter to integrate legacy C-style commands (int func(char*))
class CFuncCommandAdapter : public IConsoleCommand {
public:
  using CFunc = int(*)(char*);
  CFuncCommandAdapter(const char* n, const char* d, CFunc f, bool reqMgm)
    : mName(n), mDesc(d), mFunc(f), mRequiresMgm(reqMgm) {}

  const char* name() const override { return mName.c_str(); }
  const char* description() const override { return mDesc.c_str(); }
  bool requiresMgm(const std::string& args) const override { return mRequiresMgm && !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override;
  void printHelp() const override {}

private:
  std::string mName;
  std::string mDesc;
  CFunc mFunc;
  bool mRequiresMgm;
};

// Register native (class-based) commands that supersede legacy ones
void RegisterNativeConsoleCommands();


