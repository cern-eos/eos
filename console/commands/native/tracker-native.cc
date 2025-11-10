// ----------------------------------------------------------------------
// File: tracker-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class TrackerCommand : public IConsoleCommand {
public:
  const char* name() const override { return "tracker"; }
  const char* description() const override { return "Tracker management"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    // The legacy implementation simply forwards to 'space tracker'; preserve that behavior natively.
    std::ostringstream oss; for (size_t i=0;i<args.size();++i){ if(i)oss<<';'; oss<<args[i]; }
    XrdOucString in = "mgm.cmd=space&mgm.space.arg=tracker";
    if (!args.empty()) { in += ";"; in += oss.str().c_str(); }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterTrackerNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TrackerCommand>());
}


