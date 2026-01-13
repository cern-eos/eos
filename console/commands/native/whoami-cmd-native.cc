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
  const char*
  name() const override
  {
    return "whoami";
  }
  const char*
  description() const override
  {
    return "Determine how we are mapped on server side";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (!args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=whoami";
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: whoami\n");
  }
};
} // namespace

void
RegisterWhoamiNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<WhoamiCommand>());
}
