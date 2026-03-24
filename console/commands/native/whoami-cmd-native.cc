// ----------------------------------------------------------------------
// File: whoami-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <XrdOuc/XrdOucString.hh>
#include <sstream>

namespace {
std::string MakeWhoamiHelp()
{
  return "Usage: whoami\n\n"
         "Determine how the current user is mapped on the server side.\n";
}

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
      std::ostringstream oss;
      for (size_t i = 0; i < args.size(); ++i) {
        if (i)
          oss << ' ';
        oss << args[i];
      }
      if (wants_help(oss.str().c_str())) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
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
    fprintf(stderr, "%s", MakeWhoamiHelp().c_str());
  }
};
} // namespace

void
RegisterWhoamiNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<WhoamiCommand>());
}
