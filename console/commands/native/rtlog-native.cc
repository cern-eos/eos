// ----------------------------------------------------------------------
// File: rtlog-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>
#include <string.h>

namespace {
class RtlogCommand : public IConsoleCommand {
public:
  const char* name() const override { return "rtlog"; }
  const char* description() const override { return "Real-time logging"; }
  bool requiresMgm(const std::string&) const override { return false; }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    if (args.empty()) { fprintf(stdout, "usage: rtlog [<queue>|*|.] [<sec>] [<tag>=err] [filter]\n"); global_retc = EINVAL; return 0; }
    XrdOucString queue = args.size()>0 ? args[0].c_str() : "";
    XrdOucString lines = args.size()>1 ? args[1].c_str() : "";
    XrdOucString tag = args.size()>2 ? args[2].c_str() : "";
    XrdOucString filter = args.size()>3 ? args[3].c_str() : "";
    if ((queue != ".") && (queue != "*") && (!queue.beginswith("/eos/"))) { filter = tag; tag = lines; lines = queue; queue = "."; }
    XrdOucString in = "mgm.cmd=rtlog&mgm.rtlog.queue="; in += queue;
    in += "&mgm.rtlog.lines="; in += (lines.length()?lines.c_str():"10");
    in += "&mgm.rtlog.tag="; in += (tag.length()?tag.c_str():"err");
    if (filter.length()) { in += "&mgm.rtlog.filter="; in += filter; }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterRtlogNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RtlogCommand>());
}


