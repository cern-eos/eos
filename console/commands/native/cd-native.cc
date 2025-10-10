// ----------------------------------------------------------------------
// File: cd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_cd(char*);

namespace {
class CdCommand : public IConsoleCommand {
public:
  const char* name() const override { return "cd"; }
  const char* description() const override { return "Change directory"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return ::com_cd((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterCdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CdCommand>());
}


