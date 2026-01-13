// ----------------------------------------------------------------------
// File: evict-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class EvictCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "evict";
  }
  const char*
  description() const override
  {
    return "Evict disk replicas of a file if it has tape replicas";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    ConsoleArgParser p;
    p.addOption({"ignore-evict-counter", '\0', false, false, "",
                 "ignore evict counter", ""});
    p.addOption(
        {"ignore-removal-on-fst", '\0', false, false, "", "ns-only", ""});
    p.addOption({"fsid", '\0', true, false, "<fsid>", "single fsid", ""});
    auto r = p.parse(args);
    std::vector<std::string> pos = r.positionals;
    if (pos.empty()) {
      fprintf(stdout, "Usage: evict [--fsid <fsid>] [--ignore-removal-on-fst] "
                      "[--ignore-evict-counter] <path|fid:...> ...\n");
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=evict"; // using proto interface
    // Build protobuf-like request via client_command wrapper
    // Fallback to legacy: use com_evict style mgm.cmd if available
    // Here, we map to mgm.cmd=evict helper expected by backend
    if (r.has("ignore-evict-counter"))
      in += "&mgm.evict.ignoreevictcounter=1";
    if (r.has("ignore-removal-on-fst"))
      in += "&mgm.evict.ignoreremovalonfst=1";
    if (r.has("fsid")) {
      in += "&mgm.evict.fsid=";
      in += r.value("fsid").c_str();
    }
    for (const auto& a : pos) {
      XrdOucString path = a.c_str();
      unsigned long long fid = 0ull;
      if (Path2FileDenominator(path, fid)) {
        in += "&mgm.evict.fid=";
        in += std::to_string(fid).c_str();
      } else {
        XrdOucString ap = abspath(path.c_str());
        in += "&mgm.evict.path=";
        in += ap;
      }
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    std::ostringstream oss;
    oss << "Usage: evict [--fsid <fsid>] [--ignore-removal-on-fst] "
           "[--ignore-evict-counter] <path>|fid:<fid-dec>]|fxid:<fid-hex> "
           "[<path>|fid:<fid-dec>]|fxid:<fid-hex>] ...\n"
        << "    Removes disk replicas of the given files, separated by space\n"
        << std::endl
        << "  Optional arguments:\n"
        << "    --ignore-evict-counter  : Force eviction by bypassing evict "
           "counter\n"
        << "    --fsid <fsid>           : Evict disk copy only from a single "
           "fsid\n"
        << "    --ignore-removal-on-fst : Ignore file removal on fst, "
           "namespace-only operation\n"
        << std::endl
        << "    This command requires 'write' and 'p' acl flag permission\n"
        << std::endl;
    std::cerr << oss.str() << std::endl;
  }
};
} // namespace

void
RegisterEvictNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<EvictCommand>());
}
