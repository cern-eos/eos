// ----------------------------------------------------------------------
// File: fsck-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <memory>
#include <sstream>

namespace {
std::string MakeFsckHelp()
{
  return "Usage: fsck stat|config|report|repair|clean_orphans [OPTIONS]\n\n"
         "  stat [-m]           print consistency check summary\n"
         "  config <key> <val>  configure fsck options\n"
         "  report [-a] [-h] [-i] [-l] [-j|--json] [--error <tag>...]\n"
         "  repair --fxid <val> [--fsid <val>] [--error <err>] [--async]\n"
         "  clean_orphans [--fsid <val>] [--force-qdb-cleanup]\n";
}

void ConfigureFsckApp(CLI::App& app)
{
  app.name("fsck");
  app.description("File System Consistency Checking");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeFsckHelp();
      }));
}

class FsckProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fsck";
  }
  const char*
  description() const override
  {
    return "File System Consistency Checking";
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
    // Build mgm fsck commands from args where feasible; otherwise fallback
    if (args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    const std::string& sub = args[0];
    XrdOucString in = "mgm.cmd=fsck";
    if (sub == "stat") {
      in += "&mgm.subcmd=stat";
      if (std::find(args.begin() + 1, args.end(), "-m") != args.end())
        in += "&mgm.option=m";
    } else if (sub == "config") {
      if (args.size() < 3) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=config&mgm.key=";
      in += args[1].c_str();
      in += "&mgm.value=";
      in += args[2].c_str();
    } else if (sub == "report") {
      in += "&mgm.subcmd=report";
      for (size_t i = 1; i < args.size(); ++i) {
        const auto& a = args[i];
        if (a == "-a")
          in += "&mgm.option=a";
        else if (a == "-h")
          in += "&mgm.option=h";
        else if (a == "-i")
          in += "&mgm.option=i";
        else if (a == "-l")
          in += "&mgm.option=l";
        else if (a == "-j" || a == "--json")
          in += "&mgm.option=j";
        else if (a == "--error" && i + 1 < args.size()) {
          in += "&mgm.error=";
          in += args[++i].c_str();
        }
      }
    } else if (sub == "repair") {
      in += "&mgm.subcmd=repair";
      for (size_t i = 1; i < args.size(); ++i) {
        const auto& a = args[i];
        if (a == "--fxid" && i + 1 < args.size()) {
          in += "&mgm.fxid=";
          in += args[++i].c_str();
        } else if (a == "--fsid" && i + 1 < args.size()) {
          in += "&mgm.fsid=";
          in += args[++i].c_str();
        } else if (a == "--error" && i + 1 < args.size()) {
          in += "&mgm.error=";
          in += args[++i].c_str();
        } else if (a == "--async") {
          in += "&mgm.async=1";
        }
      }
    } else if (sub == "clean_orphans") {
      in += "&mgm.subcmd=clean_orphans";
      for (size_t i = 1; i < args.size(); ++i) {
        const auto& a = args[i];
        if (a == "--fsid" && i + 1 < args.size()) {
          in += "&mgm.fsid=";
          in += args[++i].c_str();
        } else if (a == "--force-qdb-cleanup") {
          in += "&mgm.forceqdb=1";
        }
      }
    } else {
      fprintf(stderr, "error: unsupported fsck subcommand\n");
      global_retc = EINVAL;
      return 0;
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureFsckApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterFsckProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FsckProtoCommand>());
}
