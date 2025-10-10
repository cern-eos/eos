// ----------------------------------------------------------------------
// File: chown-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class ChownCommand : public IConsoleCommand {
public:
  const char* name() const override { return "chown"; }
  const char* description() const override { return "Chown Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    // Usage: chown [-r] [-h|--nodereference] <owner>[:<group>] <path>
    ConsoleArgParser p; p.addOption({"", 'r', false, false, "", "recursive", ""}); p.addOption({"nodereference", 'h', false, false, "", "no dereference", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string& owner = pos[0]; const std::string& path = pos[1];
    XrdOucString in = "mgm.cmd=chown";
    XrdOucString opt;
    if (r.has("r")) opt += 'r';
    if (r.has("nodereference") || r.has("h")) opt += 'h';
    if (opt.length()) { in += "&mgm.chown.option="; in += opt; }
    XrdOucString ap = abspath(path.c_str()); in += "&mgm.path="; in += ap;
    in += "&mgm.chown.owner="; in += owner.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterChownNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChownCommand>());
}


