// ----------------------------------------------------------------------
// File: mv-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_mv(char*);

namespace {
class MvCommand : public IConsoleCommand {
public:
  const char* name() const override { return "mv"; }
  const char* description() const override { return "Rename file or directory"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_mv((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterMvNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MvCommand>());
}


