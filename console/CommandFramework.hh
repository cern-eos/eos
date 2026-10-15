// ----------------------------------------------------------------------
// File: CommandFramework.hh
// Purpose: Lightweight command registry and adapters for console commands
// ----------------------------------------------------------------------

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "console/CommandCompletion.hh"
#include "console/ConsoleMain.hh"

// Context passed to commands to access existing facilities without global coupling
struct CommandContext {
  GlobalOptions* globalOpts;
  bool json;
  bool silent;

  // Thin wrappers to existing functions
  XrdOucEnv* (*clientCommand)(XrdOucString& in, bool isAdmin, std::string* reply);
  int (*outputResult)(XrdOucEnv* result, bool highlighting);
};

class IConsoleCommand {
public:
  virtual ~IConsoleCommand() = default;
  virtual const char* name() const = 0;
  virtual const char* description() const = 0;
  virtual std::string
  helpText() const
  {
    return {};
  }
  virtual bool requiresMgm(const std::string& args) const { return !wants_help(args.c_str()); }
  virtual bool requiresMgmArguments(const std::vector<std::string>& args) const;
  virtual CommandCompletionResult completion(const std::vector<std::string>& args) const;
  virtual std::vector<std::string> complete(const std::vector<std::string>& args) const;
  virtual int run(const std::vector<std::string>& args, CommandContext& ctx) = 0;
  virtual void printHelp() const = 0;
  virtual void
  printHelp(const std::vector<std::string>&) const
  {
    printHelp();
  }
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

// Register native (class-based) commands that supersede legacy ones
void RegisterNativeConsoleCommands();
void EnsureNativeCommandRegistryInitialized();
