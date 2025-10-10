// ----------------------------------------------------------------------
// File: console-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_console(char*);

namespace {
class ConsoleCommand : public IConsoleCommand {
public:
  const char* name() const override { return "console"; }
  const char* description() const override { return "Run Error Console"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_console((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterConsoleNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ConsoleCommand>());
}


