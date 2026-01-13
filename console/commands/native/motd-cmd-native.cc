// ----------------------------------------------------------------------
// File: motd-native.cc
// ----------------------------------------------------------------------

#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include <fcntl.h>
#include <memory>
#include <sstream>
#include <unistd.h>

namespace {
class MotdCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "motd";
  }
  const char*
  description() const override
  {
    return "Message of the day";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (!args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=motd";
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: motd\n");
  }
};
} // namespace

void
RegisterMotdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MotdCommand>());
}
