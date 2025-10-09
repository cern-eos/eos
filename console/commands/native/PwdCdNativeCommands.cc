// ----------------------------------------------------------------------
// File: PwdCdNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>
// legacy command symbols
extern int com_cd(char*);

namespace {
class PwdCommand : public IConsoleCommand {
public:
  const char* name() const override { return "pwd"; }
  const char* description() const override { return "Print working directory"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>&, CommandContext&) override { fprintf(stdout, "%s\n", ::gPwd.c_str()); return 0; }
  void printHelp() const override {}
};

class CdCommand : public IConsoleCommand {
public:
  const char* name() const override { return "cd"; }
  const char* description() const override { return "Change directory"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override { std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<' '; oss<<args[i]; } std::string joined = oss.str(); return ::com_cd((char*)joined.c_str()); }
  void printHelp() const override {}
};
}

void RegisterPwdCdNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<PwdCommand>());
  CommandRegistry::instance().reg(std::make_unique<CdCommand>());
}


