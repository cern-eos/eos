// ----------------------------------------------------------------------
// File: motd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_motd(char*);

namespace {
class MotdCommand : public IConsoleCommand {
public:
  const char* name() const override { return "motd"; }
  const char* description() const override { return "Message of the day"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_motd((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterMotdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MotdCommand>());
}


