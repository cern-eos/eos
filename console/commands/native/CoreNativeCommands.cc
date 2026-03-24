// ----------------------------------------------------------------------
// File: CoreNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
class HelpCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "help";
  }
  const char*
  description() const override
  {
    return "Display this text";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    if (args.empty()) {
      auto& all = CommandRegistry::instance().all();
      fprintf(stderr, "Available commands:\n");
      // Make a copy and sort by command name
      std::vector<IConsoleCommand*> sorted(all.begin(), all.end());
      std::sort(sorted.begin(), sorted.end(),
                [](const IConsoleCommand* a, const IConsoleCommand* b) {
                  return std::string(a->name()) < std::string(b->name());
                });
      for (auto* c : sorted) {
        fprintf(stderr, "  %-16s %s\n", c->name(), c->description());
      }
      return 0;
    }
    // Print detailed help for a specific command
    IConsoleCommand* cmd = CommandRegistry::instance().find(args[0].c_str());
    if (!cmd) {
      fprintf(stderr, "error: unknown command '%s'\n", args[0].c_str());
      return (global_retc = EINVAL);
    }
    cmd->printHelp();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "usage: help [command]\n");
  }
};

class ToggleFlagCommand : public IConsoleCommand {
public:
  enum Which { JSON, SILENT, TIMING };
  ToggleFlagCommand(const char* n, const char* d, Which w)
      : mName(n), mDesc(d), mWhich(w)
  {
  }
  const char*
  name() const override
  {
    return mName.c_str();
  }
  const char*
  description() const override
  {
    return mDesc.c_str();
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>&, CommandContext&) override
  {
    switch (mWhich) {
    case JSON:
      ::json = (!::json);
      gGlobalOpts.mJsonFormat = ::json;
      if (::json) {
        ::interactive = false;
        ::global_highlighting = false;
      }
      if (!::silent) {
        fprintf(stderr, "json=%d\n", ::json);
      }
      break;
    case SILENT:
      ::silent = (!::silent);
      break;
    case TIMING:
      ::timing = (!::timing);
      break;
    }
    return 0;
  }
  void
  printHelp() const override
  {
  }

private:
  std::string mName;
  std::string mDesc;
  Which mWhich;
};

class QuitCommand : public IConsoleCommand {
public:
  QuitCommand(const char* n) : mName(n) {}
  const char*
  name() const override
  {
    return mName.c_str();
  }
  const char*
  description() const override
  {
    return "Exit from EOS console";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>&, CommandContext&) override
  {
    ::done = 1;
    return 0;
  }
  void
  printHelp() const override
  {
  }

private:
  std::string mName;
};
} // namespace

void
RegisterCoreNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<HelpCommand>());
  class HelpAlias : public HelpCommand {
  public:
    const char*
    name() const override
    {
      return "?";
    }
  };
  CommandRegistry::instance().reg(std::make_unique<HelpAlias>());
  CommandRegistry::instance().reg(std::make_unique<ToggleFlagCommand>(
      "json", "Toggle JSON output flag for stdout", ToggleFlagCommand::JSON));
  CommandRegistry::instance().reg(std::make_unique<ToggleFlagCommand>(
      "silent", "Toggle silent flag for stdout", ToggleFlagCommand::SILENT));
  CommandRegistry::instance().reg(std::make_unique<ToggleFlagCommand>(
      "timing", "Toggle timing flag for execution time measurement",
      ToggleFlagCommand::TIMING));
  CommandRegistry::instance().reg(std::make_unique<QuitCommand>("quit"));
  CommandRegistry::instance().reg(std::make_unique<QuitCommand>("exit"));
  CommandRegistry::instance().reg(std::make_unique<QuitCommand>(".q"));
}
