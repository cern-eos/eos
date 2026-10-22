// ----------------------------------------------------------------------
// File: status-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCommandHelp.hh"
#include <memory>

namespace {
class StatusCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "status";
  }
  const char*
  description() const override
  {
    return "Display status information on an MGM";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>&, CommandContext&) override
  {
    (void)!system("eos-status");
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterStatusNativeCommand()
{
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<StatusCommand>(),
      {{},
       eos::console::ShortHelpPolicy::Always,
       {},
       "Usage: status\nDisplay MGM status using the eos-status utility.\n"}));
}
