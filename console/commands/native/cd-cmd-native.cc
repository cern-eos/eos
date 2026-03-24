// ----------------------------------------------------------------------
// File: cd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeCdHelp()
{
  return "Usage: cd <path> | cd - | cd ~\n";
}

void ConfigureCdApp(CLI::App& app, std::string& path)
{
  app.name("cd");
  app.description("Change directory");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeCdHelp();
      }));
  app.add_option("path", path, "path (- for previous, ~ for home)")
      ->default_val("");
}

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
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      fprintf(stderr, "%s", MakeCdHelp().c_str());
      global_retc = EINVAL;
      return 0;
    }

    std::string path;
    if (!args.empty()) {
      CLI::App app;
      ConfigureCdApp(app, path);
      std::vector<std::string> cli_args = args;
      std::reverse(cli_args.begin(), cli_args.end());
      try {
        app.parse(cli_args);
      } catch (const CLI::ParseError&) {
        fprintf(stderr, "%s", MakeCdHelp().c_str());
        global_retc = EINVAL;
        return 0;
      }
    }

    static XrdOucString opwd = "/";
    static XrdOucString oopwd = "/";

    XrdOucString lsminuss;
    XrdOucString newpath;
    XrdOucString oldpwd;

    XrdOucString arg = path.c_str();

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
    fprintf(stderr, "%s", MakeCdHelp().c_str());
  }
};
} // namespace

void
RegisterCdNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<CdCommand>());
}
