// ----------------------------------------------------------------------
// File: cd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include <fcntl.h>
#include <memory>
#include <unistd.h>
#include <vector>

namespace {
struct CdOptions {
  CLI::App app{"Change directory", "cd"};
  std::string path;

  CdOptions()
  {
    app.add_option("path", path, "Namespace path (- for previous, ~ for home)")
        ->check(eos::console::NonEmptyArgument);
    app.footer("With no path, use EOS_HOME if it is defined.");
  }
};

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
    CdOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    static XrdOucString previousPwd = "/";

    XrdOucString lsminuss;
    XrdOucString newpath;
    XrdOucString oldpwd;

    XrdOucString arg = options.path.c_str();

    if (arg == "-") {
      arg = previousPwd;
    }

    newpath = abspath(arg.c_str());
    oldpwd = gPwd;

    if ((arg == "") || (arg == "~")) {
      if (getenv("EOS_HOME")) {
        newpath = abspath(getenv("EOS_HOME"));
      } else {
        fprintf(stderr,
                "warning: there is no home directory defined via EOS_HOME\n");
        newpath = gPwd;
      }
    }

    gPwd = newpath;
    if (!gPwd.endswith("/")) {
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

    if (!gPwd.endswith("/")) {
      gPwd += "/";
    }

    lsminuss = "mgm.cmd=cd&mgm.path=";
    lsminuss += eos::common::StringConversion::curl_escaped(gPwd.c_str()).c_str();
    lsminuss += "&eos.encodepath=1&mgm.option=s";
    global_retc =
        ctx.outputResult(ctx.clientCommand(lsminuss, false, nullptr), true);
    if (global_retc) {
      gPwd = oldpwd;
    } else {
      previousPwd = oldpwd;
    }
    if (!global_retc && pwdfile.length()) {
      // store the last used directory
      int cfd = open(pwdfile.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRWXU);

      if (cfd >= 0) {
        if ((::write(cfd, gPwd.c_str(), gPwd.length())) != gPwd.length()) {
          fprintf(stderr, "warning: unable to store CWD to %s [errno=%d]\n",
                  pwdfile.c_str(), errno);
        }

        close(cfd);
      } else {
        fprintf(stderr, "warning: unable to store CWD to %s\n", pwdfile.c_str());
      }
    }
    return 0;
  }
  std::string
  helpText() const override
  {
    return CdOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterCdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CdCommand>());
}
