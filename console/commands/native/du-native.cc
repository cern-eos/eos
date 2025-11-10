// ----------------------------------------------------------------------
// File: du-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

 

namespace {
class DuCommand : public IConsoleCommand {
public:
  const char* name() const override { return "du"; }
  const char* description() const override { return "Get du output"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    // Parse du flags and translate to proto find
    ConsoleArgParser p; p.addOption({"", 'a', false, false, "", "print files", ""}); p.addOption({"", 'h', false, false, "", "human readable", ""}); p.addOption({"", 's', false, false, "", "summary only", ""}); p.addOption({"si", '\0', false, false, "", "si units", ""});
    auto r = p.parse(args);
    if (r.has("help")) { printHelp(); global_retc = EINVAL; return 0; }
    std::vector<std::string> pos = r.positionals; if (pos.empty()) { printHelp(); global_retc = EINVAL; return 0; }
    std::string path = abspath(pos[0].c_str());
    std::string cmd = "--du";
    if (!r.has("a")) cmd += " -d";
    if (r.has("si")) cmd += " --du-si";
    if (r.has("h")) cmd += " --du-h";
    if (r.has("s")) cmd += " --maxdepth 0";
    cmd += " "; cmd += path;
    XrdOucString in = "mgm.cmd=find&mgm.find.arg="; in += cmd.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void printHelp() const override {
    fprintf(stdout,
            " usage:\n"
            "du [-a][-h][-s][--si] path\n"
            "'[eos] du ...' print unix like 'du' information showing subtreesize for directories\n"
            "\n"
            "Options:\n"
            "\n"
            "-a   : print also for files\n"
            "-h   : print human readable in units of 1000\n"
            "-s   : print only the summary\n"
            "--si : print in si units\n");
  }
};
}

void RegisterDuNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DuCommand>());
}


