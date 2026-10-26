// ----------------------------------------------------------------------
// File: license-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCommandHelp.hh"
#include <memory>
#include <sstream>

extern const char* license;

namespace {
class LicenseCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "license";
  }
  const char*
  description() const override
  {
    return "Show EOS license information";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>&, CommandContext&) override
  {
    fprintf(stdout, "%s", license);
    global_retc = 0;
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterLicenseNativeCommand()
{
  CommandRegistry::instance().reg(eos::console::WithCommandHelp(
      std::make_unique<LicenseCommand>(),
      {{},
       eos::console::ShortHelpPolicy::Always,
       {},
       "Usage: license\nDisplay the EOS software license.\n"}));
}
