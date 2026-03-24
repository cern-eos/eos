// ----------------------------------------------------------------------
// File: motd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sstream>

namespace {
std::string MakeMotdHelp()
{
  return "Usage: motd\n\n"
         "Display the message of the day.\n";
}

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
      std::ostringstream oss;
      for (size_t i = 0; i < args.size(); ++i) {
        if (i)
          oss << ' ';
        oss << args[i];
      }
      if (wants_help(oss.str().c_str())) {
        printHelp();
        return 0;
      }
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
    fprintf(stderr, "%s", MakeMotdHelp().c_str());
  }
};
} // namespace

void
RegisterMotdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MotdCommand>());
}
