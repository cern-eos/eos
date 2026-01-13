// ----------------------------------------------------------------------
// File: version-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class VersionCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "version";
  }
  const char*
  description() const override
  {
    return "Verbose client/server version";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    // Manual option parsing to reject unknown flags and honor -h/--help
    bool want_features = false;
    bool want_monitoring = false;
    for (const auto& a : args) {
      if (a == "-f" || a == "--features") {
        want_features = true;
      } else if (a == "-m" || a == "--monitoring") {
        want_monitoring = true;
      } else if (a == "-h" || a == "--help") {
        printHelp();
        global_retc = EINVAL;
        return 0;
      } else {
        // Unknown argument
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
    }
    XrdOucString in = "mgm.cmd=version";
    std::string opts;
    if (want_features)
      opts += "f";
    if (want_monitoring)
      opts += "m";
    if (!opts.empty()) {
      in += "&mgm.option=";
      in += opts.c_str();
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    if (opts.find('m') == std::string::npos && !ctx.json) {
      fprintf(stdout, "EOS_CLIENT_VERSION=%s EOS_CLIENT_RELEASE=%s\n", VERSION,
              RELEASE);
    }
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: version [-f] [-m]                                :  print EOS version number\n"
            "        -f                                              :  print the list of supported features\n"
            "        -m                                              :  print in monitoring format\n");
  }
};
} // namespace

void
RegisterVersionNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<VersionCommand>());
}
