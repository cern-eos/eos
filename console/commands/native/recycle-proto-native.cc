// ----------------------------------------------------------------------
// File: recycle-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include "console/commands/helpers/RecycleHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeRecycleHelp()
{
  return "Usage: recycle [ls|purge|restore|config|project] [OPTIONS]\n\n"
         "  [-m]              print status of recycle bin\n"
         "  ls [<date> [<limit>]] [-m] [-n] [--all] [--uid] [--rid <val>]\n"
         "    list files in the recycle bin\n"
         "  purge [--all] [--uid] [--rid <val>] <date> | -k <key>\n"
         "    purge files by date or by key\n"
         "  restore [-p] [-f|--force-original-name] [-r|--restore-versions] <key>\n"
         "    undo deletion identified by recycle key\n"
         "  project --path <path> [--acl <val>]\n"
         "    setup recycle id for given top level directory\n"
         "  config <key> <value>\n"
         "    configure recycle policy (--dump, --add-bin, --remove-bin, "
         "--enable, etc.)\n";
}

void ConfigureRecycleApp(CLI::App& app)
{
  app.name("recycle");
  app.description("Recycle Bin Functionality");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeRecycleHelp();
      }));
}

class RecycleProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "recycle";
  }
  const char*
  description() const override
  {
    return "Recycle Bin Functionality";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
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

    RecycleHelper recycle(gGlobalOpts);
    if (!recycle.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    global_retc = recycle.Execute(true, true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureRecycleApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterRecycleProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RecycleProtoCommand>());
}