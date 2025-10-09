// ----------------------------------------------------------------------
// File: LsNativeCommand.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>
// legacy command symbols
extern int com_ls(char*);

namespace {
class LsCommand : public IConsoleCommand {
public:
  const char* name() const override { return "ls"; }
  const char* description() const override { return "List a directory"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override { std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; } std::string joined = oss.str(); return com_ls((char*)joined.c_str()); }
  void printHelp() const override {}
};
}

void RegisterLsNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LsCommand>());
}


