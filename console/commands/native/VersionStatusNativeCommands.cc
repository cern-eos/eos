// ----------------------------------------------------------------------
// File: VersionStatusNativeCommands.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class VersionCommand : public IConsoleCommand {
public:
  const char* name() const override { return "version"; }
  const char* description() const override { return "Verbose client/server version"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (!args.empty() && (args[0] == "--help" || args[0] == "-h")) {
      fprintf(stdout,
              "usage: version [-f] [-m]                                             :  print EOS version number\n");
      fprintf(stdout,
              "                -f                                                   -  print the list of supported features\n");
      fprintf(stdout,
              "                -m                                                   -  print in monitoring format\n");
      global_retc = EINVAL;
      return 0;
    }

    XrdOucString in = "mgm.cmd=version";
    XrdOucString options = "";
    for (const auto& a : args) {
      if (a == "-f") options += "f";
      else if (a == "-m") options += "m";
      else if (!a.empty()) { fprintf(stdout, "usage: version [-f] [-m]\n"); global_retc = EINVAL; return 0; }
    }
    if (options.length()) { in += "&mgm.option="; in += options; }

    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    if ((options.find("m") == STR_NPOS) && !ctx.json) {
      fprintf(stdout, "EOS_CLIENT_VERSION=%s EOS_CLIENT_RELEASE=%s\n", VERSION, RELEASE);
    }
    return 0;
  }
  void printHelp() const override {}
};

class StatusCommand : public IConsoleCommand {
public:
  const char* name() const override { return "status"; }
  const char* description() const override { return "Display status information on an MGM"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>&, CommandContext&) override { (void)!system("eos-status"); return 0; }
  void printHelp() const override {}
};
}

void RegisterVersionStatusNativeCommands()
{
  CommandRegistry::instance().reg(std::make_unique<VersionCommand>());
  CommandRegistry::instance().reg(std::make_unique<StatusCommand>());
}


