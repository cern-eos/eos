// ----------------------------------------------------------------------
// File: whoami-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_whoami(char*);

namespace {
class WhoamiCommand : public IConsoleCommand {
public:
  const char* name() const override { return "whoami"; }
  const char* description() const override { return "Determine how we are mapped on server side"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_whoami((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterWhoamiNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<WhoamiCommand>());
}


