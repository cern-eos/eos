// ----------------------------------------------------------------------
// File: reconnect-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class ReconnectCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "reconnect";
  }
  const char*
  description() const override
  {
    return "Reconnect to MGM";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    std::string proto = args.empty() ? std::string() : args[0];
    if (!proto.empty() && proto != "gsi" && proto != "krb5" &&
        proto != "unix" && proto != "sss") {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (!proto.empty()) {
      fprintf(stdout, "# reconnecting to %s with <%s> authentication\n",
              serveruri.c_str(), proto.c_str());
      setenv("XrdSecPROTOCOL", proto.c_str(), 1);
    } else {
      fprintf(stdout, "# reconnecting to %s\n", serveruri.c_str());
    }
    XrdOucString path = serveruri;
    path += "//proc/admin/"; // nudge connection
    global_retc = 0;
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: reconnect [gsi,krb5,unix,sss] : reconnect to the "
                    "management node [using the specified protocol]\n");
  }
};
} // namespace

void
RegisterReconnectNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ReconnectCommand>());
}
