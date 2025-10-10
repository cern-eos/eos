// ----------------------------------------------------------------------
// File: mv-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class MvCommand : public IConsoleCommand {
public:
  const char* name() const override { return "mv"; }
  const char* description() const override { return "Rename file or directory"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.size() < 2) { fprintf(stdout, "usage: mv <src> <dst>\n"); global_retc = EINVAL; return 0; }
    XrdOucString in = "mgm.cmd=file&mgm.subcmd=rename";
    XrdOucString src = abspath(args[0].c_str()); XrdOucString dst = abspath(args[1].c_str());
    in += "&mgm.path="; in += src; in += "&mgm.file.target="; in += dst;
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterMvNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MvCommand>());
}


