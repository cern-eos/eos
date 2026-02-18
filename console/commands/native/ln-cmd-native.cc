// ----------------------------------------------------------------------
// File: ln-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeLnHelp()
{
  return "Usage: ln <link> <target>\n"
         "Create a symbolic link from <link> to <target>.\n";
}

void ConfigureLnApp(CLI::App& app, std::string& link, std::string& target)
{
  app.name("ln");
  app.description("Create a symbolic link");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeLnHelp();
      }));
  app.add_option("link", link, "link path")->required();
  app.add_option("target", target, "target path")->required();
}

class LnCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "ln";
  }
  const char*
  description() const override
  {
    return "Create a symbolic link";
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
    if (args.size() < 2 || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    IConsoleCommand* fileCmd = CommandRegistry::instance().find("file");
    if (!fileCmd) {
      fprintf(stderr, "error: 'file' command not available\n");
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    std::string link;
    std::string target;
    ConfigureLnApp(app, link, target);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::vector<std::string> fargs = {"symlink", link, target};
    return fileCmd->run(fargs, ctx);
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeLnHelp().c_str());
  }
};
} // namespace

void
RegisterLnNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LnCommand>());
}
