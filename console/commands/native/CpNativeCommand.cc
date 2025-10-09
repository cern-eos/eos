// ----------------------------------------------------------------------
// File: CpNativeCommand.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

// use legacy implementation while migrating logic
extern int com_cp(char*);

namespace {
class CpCommand : public IConsoleCommand {
public:
  const char* name() const override { return "cp"; }
  const char* description() const override { return "Cp command"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; }
    std::string joined = oss.str();
    return com_cp((char*)joined.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterCpNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CpCommand>());
}


