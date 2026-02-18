// ----------------------------------------------------------------------
// File: find-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include "console/commands/helpers/NewfindHelper.hh"
#include <algorithm>
#include <memory>
#include <sstream>

namespace {
std::string MakeFindHelp()
{
  return "Usage: find [OPTIONS] <path>\n\n"
         "Find files and directories. OPTIONS can be filters, actions, or output modifiers.\n\n"
         "Filters: [--maxdepth <n>] [--name <pattern>] [-f] [-d] [-0] [-g] "
         "[-uid <n>] [-nuid <n>] [-gid <n>] [-ngid <n>] [-flag <n>] [-nflag <n>] "
         "[--ctime|--mtime +<n>|-<n>] [-x <key>=<val>] [--faultyacl] [--stripediff]\n\n"
         "Actions: [-b] [--layoutstripes <n>] [--purge <n>] [--fileinfo] "
         "[--format formatlist] [--cache] [--du]\n\n"
         "Output: [--xurl] [-p <key>] [--nrep] [--nunlink] [--size] [--online] "
         "[--hosts] [--partition] [--fid] [--fs] [--checksum] [--ctime] [--mtime] "
         "[--uid] [--gid]\n\n"
         "<path> can be: file:... (local), root:... (XRootD), as3:... (S3), or EOS path.\n";
}

void ConfigureFindApp(CLI::App& app)
{
  app.name("find");
  app.description("Find files/directories");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeFindHelp();
      }));
}

class FindProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "find";
  }
  const char*
  description() const override
  {
    return "Find files/directories";
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
    // Reuse the same helper as the original newfind implementation
    NewfindHelper finder(*ctx.globalOpts);
    // Special schemes handled locally
    if (joined.find("root://") != std::string::npos) {
      std::string path = joined.substr(joined.rfind("root://"));
      path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
      global_retc = finder.FindXroot(path);
      return 0;
    } else if (joined.find("file:") != std::string::npos) {
      std::string path = joined.substr(joined.rfind("file:"));
      path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
      global_retc = finder.FindXroot(path);
      return 0;
    } else if (joined.find("as3:") != std::string::npos) {
      std::string path = joined.substr(joined.rfind("as3:"));
      path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
      global_retc = finder.FindAs3(path);
      return 0;
    }
    if (!finder.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = finder.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureFindApp(app);
    fprintf(stdout, "%s", app.help().c_str());
  }
};

// Provide 'newfind' alias to the same implementation as 'find'
class NewfindAliasCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "newfind";
  }
  const char*
  description() const override
  {
    return "Find files/directories (new)";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    IConsoleCommand* findCmd = CommandRegistry::instance().find("find");
    if (!findCmd) {
      fprintf(stderr, "error: 'find' command not available\n");
      global_retc = EINVAL;
      return 0;
    }
    return findCmd->run(args, ctx);
  }
  void
  printHelp() const override
  {
    // Delegate to 'find' help
    IConsoleCommand* findCmd = CommandRegistry::instance().find("find");
    if (findCmd)
      findCmd->printHelp();
  }
};
} // namespace

void
RegisterFindProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FindProtoCommand>());
  CommandRegistry::instance().reg(std::make_unique<NewfindAliasCommand>());
}
