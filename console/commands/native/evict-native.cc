// ----------------------------------------------------------------------
// File: evict-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class EvictCommand : public IConsoleCommand {
public:
  const char* name() const override { return "evict"; }
  const char* description() const override { return "Evict disk replicas of a file if it has tape replicas"; }
  bool requiresMgm(const std::string& args) const override { return !wants_help(args.c_str()); }
  int run(const std::vector<std::string>& args, CommandContext& ctx) override {
    ConsoleArgParser p; p.addOption({"ignore-evict-counter", '\0', false, false, "", "ignore evict counter", ""}); p.addOption({"ignore-removal-on-fst", '\0', false, false, "", "ns-only", ""}); p.addOption({"fsid", '\0', true, false, "<fsid>", "single fsid", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.empty()) { fprintf(stdout, "Usage: evict [--fsid <fsid>] [--ignore-removal-on-fst] [--ignore-evict-counter] <path|fid:...> ...\n"); global_retc = EINVAL; return 0; }
    XrdOucString in = "mgm.cmd=evict"; // using proto interface
    // Build protobuf-like request via client_command wrapper
    // Fallback to legacy: use com_evict style mgm.cmd if available
    // Here, we map to mgm.cmd=evict helper expected by backend
    if (r.has("ignore-evict-counter")) in += "&mgm.evict.ignoreevictcounter=1";
    if (r.has("ignore-removal-on-fst")) in += "&mgm.evict.ignoreremovalonfst=1";
    if (r.has("fsid")) { in += "&mgm.evict.fsid="; in += r.value("fsid").c_str(); }
    for (const auto& a : pos) {
      XrdOucString path = a.c_str();
      unsigned long long fid = 0ull;
      if (Path2FileDenominator(path, fid)) { in += "&mgm.evict.fid="; in += std::to_string(fid).c_str(); }
      else { XrdOucString ap = abspath(path.c_str()); in += "&mgm.evict.path="; in += ap; }
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void printHelp() const override {}
};
}

void RegisterEvictNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<EvictCommand>());
}


