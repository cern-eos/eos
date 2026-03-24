// ----------------------------------------------------------------------
// File: clear-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeClearHelp()
{
  return "Usage: clear\n"
         "'[eos] clear' is equivalent to the interactive shell "
         "command to clear the screen.\n";
}

void ConfigureClearApp(CLI::App& app)
{
  app.name("clear");
  app.description("Clear the terminal");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeClearHelp();
      }));
}

class ClearCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "clear";
  }
  const char*
  description() const override
  {
    return "Clear the terminal";
  }
  bool
  requiresMgm(const std::string&) const override
  {
    return false;
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    if (!args.empty()) {
      std::ostringstream oss;
      for (size_t i = 0; i < args.size(); ++i) {
        if (i)
          oss << ' ';
        oss << args[i];
      }
      std::string joined = oss.str();
      if (wants_help(joined.c_str())) {
        printHelp();
        return 0;
      }
    }

    CLI::App app;
    ConfigureClearApp(app);
    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      return 0;
    }

    int rc = system("clear");
    return rc;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeClearHelp().c_str());
  }
};
} // namespace

void
RegisterClearNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ClearCommand>());
}
