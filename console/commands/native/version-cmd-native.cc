// ----------------------------------------------------------------------
// File: version-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeVersionHelp()
{
  return "Usage: version [-f] [-m]\n\n"
         "Print EOS version number.\n\n"
         "Options:\n"
         "  -f, --features   print the list of supported features\n"
         "  -m, --monitoring print in monitoring format\n";
}

void ConfigureVersionApp(CLI::App& app, bool& opt_f, bool& opt_m)
{
  app.name("version");
  app.description("Verbose client/server version");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeVersionHelp();
      }));
  app.add_flag("-f,--features", opt_f, "print supported features");
  app.add_flag("-m,--monitoring", opt_m, "print in monitoring format");
}

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
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    bool opt_f = false;
    bool opt_m = false;
    ConfigureVersionApp(app, opt_f, opt_m);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=version";
    std::string opts;
    if (opt_f)
      opts += "f";
    if (opt_m)
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
    fprintf(stderr, "%s", MakeVersionHelp().c_str());
  }
};
} // namespace

void
RegisterVersionNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<VersionCommand>());
}
