// ----------------------------------------------------------------------
// File: rmdir-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class RmdirCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "rmdir";
  }
  const char*
  description() const override
  {
    return "Remove a directory";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty() || wants_help(args[0].c_str())) {
      fprintf(stderr, "Usage: rmdir <path>\n");
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=rmdir&mgm.path=";
    XrdOucString p = abspath(args[0].c_str());
    in += p;
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterRmdirNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RmdirCommand>());
}
