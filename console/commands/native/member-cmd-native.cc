// ----------------------------------------------------------------------
// File: member-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
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
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    bool update = false;
    std::string egroup;
    eos::common::StringTokenizer tok(joined.c_str());
    tok.GetLine();
    const char* option = nullptr;
    do {
      option = tok.GetToken();
      if (!option || !strlen(option))
        break;
      std::string soption = option;
      if (soption == "--help" || soption == "-h") {
        printHelp();
        global_retc = 0;
        return 0;
      }
      if (soption == "--update") {
        update = true;
        continue;
      }
      if (egroup.empty()) {
        egroup = soption;
      } else {
        fprintf(stderr, "error: command accepts only one egroup argument\n");
        global_retc = EINVAL;
        return 0;
      }
    } while (option && strlen(option));

    if (egroup.empty()) {
      fprintf(stderr, "error: no egroup argument given\n");
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=member";
    in += "&mgm.egroup=";
    in += egroup.c_str();
    if (update) {
      in += "&mgm.egroupupdate=true";
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: member [--update] <egroup>\n"
        "   show the (cached) information about egroup membership for the\n"
        "   current user running the command. If the check is required for\n"
        "   a different user then please use the \"eos -r <uid> <gid>\"\n"
        "   command to switch to a different role.\n"
        " Options:\n"
        "    --update : Refresh cached egroup information\n");
  }
};
} // namespace

void
RegisterMemberNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<MemberCommand>());
}
