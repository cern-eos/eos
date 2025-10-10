// ----------------------------------------------------------------------
// File: who-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class WhoCommand : public IConsoleCommand {
public:
  const char* name() const override { return "who"; }
  const char* description() const override { return "Statistics about connected users"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    ConsoleArgParser p; p.addOption({"", 'c', false, false, "", "by client host", ""}); p.addOption({"", 'n', false, false, "", "numeric ids", ""}); p.addOption({"", 'z', false, false, "", "auth protocols", ""}); p.addOption({"", 'a', false, false, "", "all", ""}); p.addOption({"", 'm', false, false, "", "monitor format", ""}); p.addOption({"", 's', false, false, "", "summary", ""});
    auto r = p.parse(args);
    XrdOucString in = "mgm.cmd=who";
    std::string opts; if (r.has("c")) opts += 'c'; if (r.has("n")) opts += 'n'; if (r.has("z")) opts += 'z'; if (r.has("a")) opts += 'a'; if (r.has("s")) opts += 's'; if (r.has("m")) opts += 'm';
    if (!opts.empty()) { in += "&mgm.option="; in += opts.c_str(); }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterWhoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<WhoCommand>());
}


