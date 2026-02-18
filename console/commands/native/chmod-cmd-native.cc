// ----------------------------------------------------------------------
// File: chmod-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeChmodHelp()
{
  std::ostringstream oss;
  oss << "Usage: chmod [-r] <mode> <path>                             : set "
         "mode for <path> (-r recursive)\n";
  oss << "                 <mode> can be only numerical like 755, 644, 700\n";
  return oss.str();
}

void ConfigureChmodApp(CLI::App& app,
                      bool& opt_r,
                      std::string& mode,
                      std::string& path)
{
  app.name("chmod");
  app.description("Mode Interface");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeChmodHelp();
      }));
  app.add_flag("-r", opt_r, "recursive");
  app.add_option("mode", mode, "mode (e.g. 755, 644, 700)")->required();
  app.add_option("path", path, "path")->required();
}

class ChmodCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "chmod";
  }
  const char*
  description() const override
  {
    return "Mode Interface";
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
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    bool opt_r = false;
    std::string mode;
    std::string path;
    ConfigureChmodApp(app, opt_r, mode, path);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=chmod";
    if (opt_r)
      in += "&mgm.option=r";
    XrdOucString ap = abspath(path.c_str());
    in += "&mgm.path=";
    in += ap;
    in += "&mgm.chmod.mode=";
    in += mode.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    bool opt_r = false;
    std::string mode;
    std::string path;
    ConfigureChmodApp(app, opt_r, mode, path);
    const std::string help = app.help();
    fprintf(stderr, "%s", help.c_str());
  }
};
} // namespace

void
RegisterChmodNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChmodCommand>());
}
