// ----------------------------------------------------------------------
// File: member-native.cc
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
std::string MakeMemberHelp()
{
  return "Usage: member [--update] <egroup>\n\n"
         "Show the (cached) information about egroup membership for the "
         "current user.\n"
         "If the check is required for a different user then use "
         "\"eos -r <uid> <gid>\" to switch to a different role.\n\n"
         "Options:\n"
         "  --update  refresh cached egroup information\n";
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
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeMemberHelp().c_str());
  }
};
} // namespace

void
RegisterMemberNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MemberCommand>());
}
