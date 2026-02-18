// ----------------------------------------------------------------------
// File: archive-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeArchiveHelp()
{
  std::ostringstream oss;
  oss << "Usage: archive <subcmd> [args...]\n\n"
      << "Subcommands:\n"
      << "  create <path>                          create "
         "archive file\n"
      << "  put [--retry] <path>                   copy files from EOS to "
         "archive location\n"
      << "  get [--retry] <path>                   recall archive back to "
         "EOS\n"
      << "  purge [--retry] <path>                 purge files on disk\n"
      << "  transfers [all|put|get|purge|job_uuid] show status of running "
         "jobs\n"
      << "  list [<path>]                          show status of archived "
         "directories in subtree\n"
      << "  kill <job_uuid>                         kill transfer\n"
      << "  delete <path>                           delete files from tape, "
         "keeping the ones on disk\n"
      << "  help [--help|-h]                       display help message\n";
  return oss.str();
}

void ConfigureArchiveApp(CLI::App& app,
                         std::string& subcmd,
                         bool& retry,
                         std::string& arg)
{
  app.name("archive");
  app.description("Archive Interface");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeArchiveHelp();
      }));
  app.add_option("subcmd", subcmd,
                 "create|put|get|purge|delete|transfers|list|kill|help")
      ->required();
  app.add_flag("--retry", retry, "retry on failure");
  app.add_option("arg", arg, "path, job_uuid, or option");
}

class ArchiveCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "archive";
  }
  const char*
  description() const override
  {
    return "Archive Interface";
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
    std::string subcmd;
    bool retry = false;
    std::string arg;
    ConfigureArchiveApp(app, subcmd, retry, arg);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    std::ostringstream in_cmd;
    in_cmd << "mgm.cmd=archive&mgm.subcmd=" << subcmd;

    if (subcmd == "create") {
      std::string p = arg.empty() ? gPwd.c_str() : arg;
      XrdOucString ap = abspath(p.c_str());
      in_cmd << "&mgm.archive.path=" << ap.c_str();
    } else if (subcmd == "put" || subcmd == "get" || subcmd == "purge" ||
               subcmd == "delete") {
      std::string p = arg.empty() ? gPwd.c_str() : arg;
      if (retry)
        in_cmd << "&mgm.archive.option=r";
      XrdOucString ap = abspath(p.c_str());
      in_cmd << "&mgm.archive.path=" << ap.c_str();
    } else if (subcmd == "transfers") {
      if (arg.empty())
        in_cmd << "&mgm.archive.option=all";
      else
        in_cmd << "&mgm.archive.option=" << arg;
    } else if (subcmd == "list") {
      if (arg.empty())
        in_cmd << "&mgm.archive.path=/";
      else if (arg == "./" || arg == ".") {
        XrdOucString ap = abspath(gPwd.c_str());
        in_cmd << "&mgm.archive.path=" << ap.c_str();
      } else
        in_cmd << "&mgm.archive.path=" << arg;
    } else if (subcmd == "kill") {
      if (arg.empty()) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in_cmd << "&mgm.archive.option=" << arg;
    } else if (subcmd == "help") {
      printHelp();
      return 0;
    } else {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = in_cmd.str().c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    std::string subcmd;
    bool retry = false;
    std::string arg;
    ConfigureArchiveApp(app, subcmd, retry, arg);
    const std::string help = app.help();
    fprintf(stderr, "%s", help.c_str());
  }
};
} // namespace

void
RegisterArchiveNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ArchiveCommand>());
}
