// ----------------------------------------------------------------------
// File: fs-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include "console/commands/helpers/FsHelper.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeFsHelp()
{
  return "Usage: fs add|boot|clone|compare|config|dropdeletion|dropghosts|"
         "dropfiles|dumpmd|ls|mv|rm|status [OPTIONS]\n\n"
         "  fs add [-m|--manual <fsid>] <uuid> <node-queue>|<host>[:<port>] "
         "<mountpoint> [<space_info> [<status> [<sharedfs>]]]\n"
         "  fs boot <fsid>|<uuid>|<node-queue>|* [--syncdisk|--syncmgm]\n"
         "  fs clone <sourceid> <targetid>\n"
         "  fs compare <sourceid> <targetid>\n"
         "  fs config <fsid> <key>=<value>\n"
         "  fs dropdeletion <fsid>\n"
         "  fs dropghosts <fsid> [--fxid fid1 [fid2] ...]\n"
         "  fs dropfiles <fsid> [-f]\n"
         "  fs dumpmd <fsid> [--count] [--fid|--fxid|--path] [--size] [-m|-s]\n"
         "  fs ls [-m|-l|-e|--io|--fsck|[-d|--drain]|-D|-F] [-s] [-b|--brief] "
         "[[matchlist]]\n"
         "  fs mv [--force] <src> <dst>\n"
         "  fs rm <fsid>|<mnt>|<node-queue> <mnt>|<hostname> <mnt>\n"
         "  fs status [-r] [-l] <identifier>\n";
}

void ConfigureFsApp(CLI::App& app)
{
  app.name("fs");
  app.description("File System configuration");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeFsHelp();
      }));
}

class FsProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fs";
  }
  const char*
  description() const override
  {
    return "File System configuration";
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
    FsHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // Confirmation is handled inside FsHelper where applicable
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureFsApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterFsProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FsProtoCommand>());
}
