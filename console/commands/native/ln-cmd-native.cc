// ----------------------------------------------------------------------
// File: ln-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class LnCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "ln";
  }
  const char*
  description() const override
  {
    return "Create a symbolic link";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.size() < 2) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=file&mgm.subcmd=symlink";
    XrdOucString target = abspath(args[0].c_str());
    XrdOucString link = abspath(args[1].c_str());
    in += "&mgm.path=";
    in += target;
    in += "&mgm.file.target=";
    in += link;
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: ln <target> <link>\n");
  }
};
} // namespace

void
RegisterLnNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LnCommand>());
}
