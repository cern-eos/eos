// ----------------------------------------------------------------------
// File: member-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <XrdOuc/XrdOucString.hh>
#include <algorithm>
#include <iomanip>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeMemberHelp()
{
  std::ostringstream oss;
  oss << "Usage: member [--update] <egroup>\n"
      << "   show the (cached) information about egroup membership for the\n"
      << "   current user running the command. If the check is required for\n"
      << "   a different user then please use the \"eos -r <uid> <gid>\"\n"
      << "   command to switch to a different role.\n"
      << " Options:\n"
      << "    --update : Refresh cached egroup information\n";
  return oss.str();
}

void ConfigureMemberApp(CLI::App& app, bool& opt_update, std::string& egroup)
{
  app.name("member");
  app.description("Member management");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeMemberHelp();
      }));
  app.add_flag("--update", opt_update, "refresh cached egroup information");
  app.add_option("egroup", egroup, "egroup name")->required();
}

class MemberCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "member";
  }
  const char*
  description() const override
  {
    return "Member management";
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
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    bool opt_update = false;
    std::string egroup;
    ConfigureMemberApp(app, opt_update, egroup);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=member";
    in += "&mgm.egroup=";
    in += egroup.c_str();
    if (opt_update) {
      in += "&mgm.egroupupdate=true";
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    CLI::App app;
    bool update = false;
    std::string egroup;
    ConfigureMemberApp(app, update, egroup);
    return CompleteCliArguments(app, args);
  }
  std::string
  helpText() const override
  {
    return MakeMemberHelp();
  }
  void
  printHelp() const override
  {
    CLI::App app;
    bool update = false;
    std::string egroup;
    ConfigureMemberApp(app, update, egroup);
    eos::console::PrintCliHelp(app, CLI::AppFormatMode::Normal);
  }
};
} // namespace

void
RegisterMemberNativeCommand()
{
  CommandRegistry::instance().reg(
      eos::console::WithCommandHelp(std::make_unique<MemberCommand>(), {}));
}
