// ----------------------------------------------------------------------
// File: mkdir-native.cc
// ----------------------------------------------------------------------

#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeMkdirHelp()
{
  return "Usage: mkdir [-p] <path>\n\n"
         "Create directory <path>. With -p, create parent directories as needed.\n\n"
         "Options:\n"
         "  -p  create parent directories as needed\n";
}

void ConfigureMkdirApp(CLI::App& app, bool& opt_p, std::string& path)
{
  app.name("mkdir");
  app.description("Create a directory");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeMkdirHelp();
      }));
  app.add_flag("-p", opt_p, "create parent directories as needed");
  app.add_option("path", path, "directory path")->required();
}

class MkdirCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "mkdir";
  }
  const char*
  description() const override
  {
    return "Create a directory";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return true;
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
    bool opt_p = false;
    std::string path;
    ConfigureMkdirApp(app, opt_p, path);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=mkdir";
    if (opt_p)
      in += "&mgm.option=p";
    XrdOucString ap = abspath(path.c_str());
    XrdOucString esc =
        eos::common::StringConversion::curl_escaped(ap.c_str()).c_str();
    in += "&mgm.path=";
    in += esc;
    in += "&eos.encodepath=1";
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeMkdirHelp().c_str());
  }
};
} // namespace

void
RegisterMkdirNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MkdirCommand>());
}
