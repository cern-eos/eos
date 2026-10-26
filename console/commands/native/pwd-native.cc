// ----------------------------------------------------------------------
// File: pwd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCommandHelp.hh"
#include <memory>

namespace {
class PwdCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "pwd";
  }
  const char*
  description() const override
  {
    return "Print working directory";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>&, CommandContext&) override
  {
    fprintf(stdout, "%s\n", ::gPwd.c_str());
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterPwdNativeCommand()
{
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<PwdCommand>(),
      {{},
       eos::console::ShortHelpPolicy::Always,
       {},
       "Usage: pwd\nPrint the current EOS working directory.\n"}));
}
