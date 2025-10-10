// ----------------------------------------------------------------------
// File: license-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_license(char*);

namespace {
class LicenseCommand : public IConsoleCommand {
public:
  const char* name() const override { return "license"; }
  const char* description() const override { return "Show EOS license information"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_license((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterLicenseNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LicenseCommand>());
}


