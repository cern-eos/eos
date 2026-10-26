// ----------------------------------------------------------------------
// File: health-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/commands/HealthCommand.hh"
#include <memory>
#include <sstream>

namespace {
class HealthConsoleCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "health";
  }
  const char*
  description() const override
  {
    return "Cluster health check";
  }
  std::string
  helpText() const override
  {
    return "Usage: eos health [OPTION] [SECTION]\n\n"
           "Options available:\n"
           "  --help    Print help\n"
           "  -m        Turn on monitoring mode\n"
           "  -a        Display all information, not just critical\n\n"
           "Sections available:\n"
           "  all         Display all sections (default value)\n"
           "  nodes       Display only information about nodes\n"
           "  drain       Display drain health information\n"
           "  placement   Display placement contention health information\n";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = 0;
      return 0;
    }
    ::HealthCommand cmd(joined.c_str());
    cmd.Execute();
    global_retc = 0;
    return 0;
  }
  void
  printHelp() const override
  {
    ::HealthCommand("").PrintHelp();
  }
};
} // namespace

void
RegisterHealthNativeCommand()
{
  eos::console::CommandHelpSpec help;
  help.topics = {
      {{"all"}, "  all ", "Show every health section."},
      {{"nodes"}, "  nodes ", "Check storage node health."},
      {{"drain"}, "  drain ", "Check drain health."},
      {{"placement"}, "  placement ", "Check placement contention."},
  };
  help.common = "OPTIONS:\n"
                "  --help : Print help.\n"
                "  -m : Turn on monitoring mode.\n"
                "  -a : Display all information, not just critical.\n\n"
                "With no section, display all health sections.\n";
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<HealthConsoleCommand>(), std::move(help)));
}
