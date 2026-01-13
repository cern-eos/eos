// ----------------------------------------------------------------------
// File: cd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class CdCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "cd";
  }
  const char*
  description() const override
  {
    return "Change directory";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (!args.empty() && wants_help(args[0].c_str())) {
      fprintf(stderr, "Usage: cd <path> | cd - | cd ~\n");
      global_retc = EINVAL;
      return 0;
    }

    static XrdOucString opwd = "/";
    static XrdOucString oopwd = "/";

    XrdOucString lsminuss;
    XrdOucString newpath;
    XrdOucString oldpwd;

    XrdOucString arg =
        args.empty() ? XrdOucString("") : XrdOucString(args[0].c_str());

    if (arg == "-") {
      oopwd = opwd;
      arg = (char*)opwd.c_str();
    }

    opwd = gPwd;
    newpath = abspath(arg.c_str());
    oldpwd = gPwd;

    if ((arg == "") || (arg == "~")) {
      if (getenv("EOS_HOME")) {
        newpath = abspath(getenv("EOS_HOME"));
      } else {
        fprintf(stderr,
                "warning: there is no home directory defined via EOS_HOME\n");
        newpath = opwd;
      }
    }

    gPwd = newpath;
    if ((!gPwd.endswith("/")) && (!gPwd.endswith("/\""))) {
      gPwd += "/";
    }

    while (gPwd.replace("/./", "/")) {
    }

    int dppos = 0;
    while ((dppos = gPwd.find("/../")) != STR_NPOS) {
      if (dppos == 0) {
        gPwd = oldpwd;
        break;
      }
      int rpos = gPwd.rfind("/", dppos - 1);
      if (rpos != STR_NPOS) {
        gPwd.erase(rpos, dppos - rpos + 3);
      } else {
        gPwd = oldpwd;
        break;
      }
    }

    if ((!gPwd.endswith("/")) && (!gPwd.endswith("/\""))) {
      gPwd += "/";
    }

    lsminuss = "mgm.cmd=cd&mgm.path=";
    lsminuss += gPwd;
    lsminuss += "&mgm.option=s";
    global_retc =
        ctx.outputResult(ctx.clientCommand(lsminuss, false, nullptr), true);
    if (global_retc) {
      gPwd = oldpwd;
    }
    return 0;
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterCdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CdCommand>());
}
