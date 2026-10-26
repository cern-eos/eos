// ----------------------------------------------------------------------
// File: CoreNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/ConsoleHelp.hh"
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
    if (!args.empty() && (args.front() == "--help" || args.front() == "-h" ||
                          args.front() == "--help-all")) {
      printHelp();
      global_retc = 0;
      return 0;
    }
    if (args.empty()) {
      auto& all = CommandRegistry::instance().all();
      // Make a copy and sort by command name
      std::vector<IConsoleCommand*> sorted(all.begin(), all.end());
      std::sort(sorted.begin(), sorted.end(),
                [](const IConsoleCommand* a, const IConsoleCommand* b) {
                  return std::string(a->name()) < std::string(b->name());
                });
      if (eos::console::HelpWidth(STDERR_FILENO)) {
        eos::console::HelpPage page;
        page.blocks.push_back(
            {eos::console::HelpBlockKind::Section, "Available commands:", {}});
        for (const auto* command : sorted) {
          page.blocks.push_back({eos::console::HelpBlockKind::Definition,
                                 eos::console::HelpCommandText(command->name()),
                                 command->description()});
        }
        eos::console::PrintHelpPage(page);
        return 0;
      }
      fprintf(stderr, "%s\n",
              eos::console::Colorize(
                  "Available commands:", eos::console::TextStyle::Heading, STDERR_FILENO)
                  .c_str());
      for (auto* c : sorted) {
        const std::string name = c->name();
        fprintf(
            stderr, "  %s%s %s\n",
            eos::console::Colorize(name, eos::console::TextStyle::Command, STDERR_FILENO)
                .c_str(),
            std::string(name.size() < 16 ? 16 - name.size() : 0, ' ').c_str(),
            c->description());
      }
      return 0;
    }
    // Print detailed help for a specific command
    IConsoleCommand* cmd = CommandRegistry::instance().find(args[0].c_str());
    if (!cmd) {
      fprintf(
          stderr, "%s unknown command '%s'\n",
          eos::console::Colorize("error:", eos::console::TextStyle::Error, STDERR_FILENO)
              .c_str(),
          args[0].c_str());
      return (global_retc = EINVAL);
    }
    cmd->printHelp(std::vector<std::string>(args.begin() + 1, args.end()));
    return 0;
  }
  void
  printHelp() const override
  {
    eos::console::PrintLegacyHelp(
        "Usage: help [<command> [<operation>...]]\n"
        "List all commands, or show help for the selected command and operation.\n"
        "\nEXAMPLES:\n"
        "  # List available commands\n"
        "  eos help\n"
        "\n"
        "  # Read filesystem listing help\n"
        "  eos help fs ls\n");
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
      eos::console::SetJsonColorOutput(::json);
      eos::console::SetJsonHelpOutput(::json);
      if (::json) {
        ::interactive = false;
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
    void
    printHelp() const override
    {
      eos::console::PrintLegacyHelp(
          "Usage: ? [<command> [<operation>...]]\n"
          "Alias for help: list commands or read help for a selected operation.\n"
          "\nEXAMPLES:\n"
          "  # Read filesystem listing help from a shell\n"
          "  eos '?' fs ls\n");
    }
  };
  CommandRegistry::instance().reg(std::make_unique<HelpAlias>());
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<ToggleFlagCommand>("json", "Toggle JSON output flag for stdout",
                                          ToggleFlagCommand::JSON),
      {{},
       eos::console::ShortHelpPolicy::Always,
       {},
       "Usage: json\nToggle JSON output in the interactive shell.\n"}));
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<ToggleFlagCommand>("silent", "Toggle silent flag for stdout",
                                          ToggleFlagCommand::SILENT),
      {{},
       eos::console::ShortHelpPolicy::Always,
       {},
       "Usage: silent\nToggle silent output in the interactive shell.\n"}));
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<ToggleFlagCommand>(
          "timing", "Toggle timing flag for execution time measurement",
          ToggleFlagCommand::TIMING),
      {{},
       eos::console::ShortHelpPolicy::Always,
       {},
       "Usage: timing\nToggle command timing in the interactive shell.\n"}));
  CommandRegistry::instance().reg(
      eos::console::WithCommandHelp(std::make_unique<QuitCommand>("quit"),
                                    {{},
                                     eos::console::ShortHelpPolicy::Always,
                                     {},
                                     "Usage: quit\nExit the interactive EOS shell.\n"}));
  CommandRegistry::instance().reg(
      eos::console::WithCommandHelp(std::make_unique<QuitCommand>("exit"),
                                    {{},
                                     eos::console::ShortHelpPolicy::Always,
                                     {},
                                     "Usage: exit\nExit the interactive EOS shell.\n"}));
  CommandRegistry::instance().reg(
      eos::console::WithCommandHelp(std::make_unique<QuitCommand>(".q"),
                                    {{},
                                     eos::console::ShortHelpPolicy::Always,
                                     {},
                                     "Usage: .q\nExit the interactive EOS shell.\n"}));
}
