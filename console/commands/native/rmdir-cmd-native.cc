// ----------------------------------------------------------------------
// File: rmdir-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeRmdirHelp()
{
  return "Usage: rmdir <path>\n\n"
         "Remove the empty directory <path>.\n";
}

void ConfigureRmdirApp(CLI::App& app, std::string& path)
{
  app.name("rmdir");
  app.description("Remove a directory");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRmdirHelp();
      }));
  app.add_option("path", path, "directory path")->required();
}

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
    std::string path;
    ConfigureRmdirApp(app, path);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=rmdir";
    XrdOucString p = abspath(path.c_str());
    XrdOucString esc =
        eos::common::StringConversion::curl_escaped(p.c_str()).c_str();
    in += "&mgm.path=";
    in += esc;
    in += "&eos.encodepath=1";
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeRmdirHelp().c_str());
  }
};
} // namespace

void
RegisterRmdirNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RmdirCommand>());
}
