// ----------------------------------------------------------------------
// File: cp-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

extern int com_cp(char*);

namespace {
class CpCommand : public IConsoleCommand {
public:
  const char* name() const override { return "cp"; }
  const char* description() const override { return "Copy files"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    // For now, delegate to legacy com_cp due to complexity of protocol handling
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_cp((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterCpNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CpCommand>());
}


