// ----------------------------------------------------------------------
// File: daemon-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

extern int com_daemon(char*);

namespace {
class DaemonCommand : public IConsoleCommand {
public:
  const char* name() const override { return "daemon"; }
  const char* description() const override { return "Run EOS daemon control"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str(); return com_daemon((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterDaemonNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DaemonCommand>());
}


