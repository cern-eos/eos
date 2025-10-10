// ----------------------------------------------------------------------
// File: chmod-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
class ChmodCommand : public IConsoleCommand {
public:
  const char* name() const override { return "chmod"; }
  const char* description() const override { return "Mode Interface"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    // usage: chmod [-r] <mode> <path>
    ConsoleArgParser p; p.addOption({"", 'r', false, false, "", "recursive", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.size() < 2) { printHelp(); global_retc = EINVAL; return 0; }
    const std::string& mode = pos[0]; const std::string& path = pos[1];
    XrdOucString in = "mgm.cmd=chmod";
    if (r.has("r")) in += "&mgm.option=r";
    XrdOucString ap = abspath(path.c_str()); in += "&mgm.path="; in += ap;
    in += "&mgm.chmod.mode="; in += mode.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterChmodNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChmodCommand>());
}


