// ----------------------------------------------------------------------
// File: evict-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
std::string MakeEvictHelp()
{
  std::ostringstream oss;
  oss << "Usage: evict [--fsid <fsid>] [--ignore-removal-on-fst] "
         "[--ignore-evict-counter] <path>|fid:<fid-dec>|fxid:<fid-hex> "
         "[<path>|fid:<fid-dec>|fxid:<fid-hex>] ...\n"
      << "    Removes disk replicas of the given files, separated by space\n\n"
      << "Options:\n"
      << "    --ignore-evict-counter  : Force eviction by bypassing evict "
         "counter\n"
      << "    --fsid <fsid>           : Evict disk copy only from a single "
         "fsid\n"
      << "    --ignore-removal-on-fst : Ignore file removal on fst, "
         "namespace-only operation\n\n"
      << "    This command requires 'write' and 'p' acl flag permission\n";
  return oss.str();
}

void ConfigureEvictApp(CLI::App& app,
                       bool& opt_ignore_evict_counter,
                       bool& opt_ignore_removal_on_fst,
                       std::string& fsid,
                       std::vector<std::string>& paths)
{
  app.name("evict");
  app.set_help_flag("");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeEvictHelp();
      }));
  app.add_flag("--ignore-evict-counter", opt_ignore_evict_counter,
               "ignore evict counter");
  app.add_flag("--ignore-removal-on-fst", opt_ignore_removal_on_fst, "ns-only");
  app.add_option("--fsid", fsid, "single fsid");
  app.add_option("paths", paths);
}

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
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    CLI::App app;
    app.allow_extras();
    bool opt_ignore_evict_counter = false;
    bool opt_ignore_removal_on_fst = false;
    std::string fsid;
    std::vector<std::string> pos;
    ConfigureEvictApp(app, opt_ignore_evict_counter, opt_ignore_removal_on_fst,
                      fsid, pos);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (pos.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    XrdOucString in = "mgm.cmd=evict"; // using proto interface
    // Build protobuf-like request via client_command wrapper
    // Fallback to legacy: use com_evict style mgm.cmd if available
    // Here, we map to mgm.cmd=evict helper expected by backend
    if (opt_ignore_evict_counter)
      in += "&mgm.evict.ignoreevictcounter=1";
    if (opt_ignore_removal_on_fst)
      in += "&mgm.evict.ignoreremovalonfst=1";
    if (!fsid.empty()) {
      in += "&mgm.evict.fsid=";
      in += fsid.c_str();
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
    CLI::App app;
    bool opt_ignore_evict_counter = false;
    bool opt_ignore_removal_on_fst = false;
    std::string fsid;
    std::vector<std::string> pos;
    ConfigureEvictApp(app, opt_ignore_evict_counter, opt_ignore_removal_on_fst,
                      fsid, pos);
    const std::string help = app.help();
    std::cerr << help << std::endl;
  }
};
} // namespace

void
RegisterEvictNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<EvictCommand>());
}
