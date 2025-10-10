// ----------------------------------------------------------------------
// File: du-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

extern int com_proto_find(char*);

namespace {
class DuCommand : public IConsoleCommand {
public:
  const char* name() const override { return "du"; }
  const char* description() const override { return "Get du output"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext&) override {
    // Parse du flags and translate to proto find
    ConsoleArgParser p; p.addOption({"", 'a', false, false, "", "print files", ""}); p.addOption({"", 'h', false, false, "", "human readable", ""}); p.addOption({"", 's', false, false, "", "summary only", ""}); p.addOption({"si", '\0', false, false, "", "si units", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals; if (pos.empty()) { fprintf(stderr, "usage: du [-a][-h][-s][--si] path\n"); global_retc = EINVAL; return 0; }
    std::string path = abspath(pos[0].c_str());
    std::string cmd = "--du";
    if (!r.has("a")) cmd += " -d";
    if (r.has("si")) cmd += " --du-si";
    if (r.has("h")) cmd += " --du-h";
    if (r.has("s")) cmd += " --maxdepth 0";
    cmd += " "; cmd += path;
    return com_proto_find((char*)cmd.c_str());
  }
  void printHelp() const override {}
};
}

void RegisterDuNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DuCommand>());
}


