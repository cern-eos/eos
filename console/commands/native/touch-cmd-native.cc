// ----------------------------------------------------------------------
// File: touch-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeTouchHelp()
{
  return "Usage: touch [-a] [-n] [-0] <path> [linkpath|size [hexchecksum]]\n"
         "       touch -l <path> [lifetime [audience=user|app]]\n"
         "       touch -u <path>\n\n"
         "Touch a file. Delegates to 'file touch'.\n\n"
         "Options:\n"
         "  -a  absorb\n"
         "  -n  no layout\n"
         "  -0  truncate\n"
         "  -l  lock\n"
         "  -u  unlock\n";
}

void ConfigureTouchApp(CLI::App& app)
{
  app.name("touch");
  app.description("Touch a file");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeTouchHelp();
      }));
}

class TouchCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "touch";
  }
  const char*
  description() const override
  {
    return "Touch a file";
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
    IConsoleCommand* fileCmd = CommandRegistry::instance().find("file");
    if (!fileCmd) {
      fprintf(stderr, "error: 'file' command not available\n");
      global_retc = EINVAL;
      return 0;
    }
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    ConfigureTouchApp(app);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::vector<std::string> remaining = app.remaining();
    std::reverse(remaining.begin(), remaining.end());
    if (remaining.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::vector<std::string> fargs;
    fargs.reserve(remaining.size() + 1);
    fargs.push_back("touch");
    fargs.insert(fargs.end(), remaining.begin(), remaining.end());
    return fileCmd->run(fargs, ctx);
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeTouchHelp().c_str());
  }
};
} // namespace

void
RegisterTouchNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TouchCommand>());
}
