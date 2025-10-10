// ----------------------------------------------------------------------
// File: role-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_role(char*);

namespace {
class RoleCommand : public IConsoleCommand {
public:
  const char* name() const override { return "role"; }
  const char* description() const override { return "Switch role or show roles"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_role((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterRoleNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RoleCommand>());
}


