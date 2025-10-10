// ----------------------------------------------------------------------
// File: whoami-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class WhoamiCommand : public IConsoleCommand {
public:
  const char* name() const override { return "whoami"; }
  const char* description() const override { return "Determine how we are mapped on server side"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    XrdOucString in = "mgm.cmd=whoami";
    if (!args.empty()) { in += "&authz="; in += args[0].c_str(); }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterWhoamiNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<WhoamiCommand>());
}


