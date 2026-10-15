// ----------------------------------------------------------------------
// File: evict-native.cc
// ----------------------------------------------------------------------

#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <stdexcept>

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
         "fsid (requires --ignore-evict-counter)\n"
      << "    --ignore-removal-on-fst : Ignore file removal on fst, "
         "namespace-only operation (requires --fsid)\n\n"
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

class EvictHelper : public ICmdHelper {
public:
  explicit EvictHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
  }
  bool
  ParseCommand(const char* arg) override
  {
    eos::common::StringTokenizer tokenizer(arg);
    tokenizer.GetLine();
    std::vector<std::string> args;
    while (const char* token = tokenizer.GetTokenUnquoted(false)) {
      args.emplace_back(token);
    }
    return ParseArguments(args);
  }
  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    mReq.clear_evict();
    CLI::App app;
    app.allow_extras(false);
    bool opt_ignore_evict_counter = false;
    bool opt_ignore_removal_on_fst = false;
    std::string fsid;
    std::vector<std::string> pos;
    ConfigureEvictApp(app, opt_ignore_evict_counter, opt_ignore_removal_on_fst, fsid,
                      pos);

    std::vector<std::string> cli_args = args;
    std::reverse(cli_args.begin(), cli_args.end());
    try {
      app.parse(cli_args);
    } catch (const CLI::ParseError&) {
      return false;
    }
    if (pos.empty() ||
        std::any_of(pos.begin(), pos.end(),
                    [](const std::string& path) { return path.empty(); }) ||
        (app.get_option("--fsid")->count() && fsid.empty())) {
      return false;
    }
    if ((!fsid.empty() && !opt_ignore_evict_counter) ||
        (fsid.empty() && opt_ignore_removal_on_fst)) {
      return false;
    }

    auto* evict = mReq.mutable_evict();
    evict->set_ignoreevictcounter(opt_ignore_evict_counter);
    evict->set_ignoreremovalonfst(opt_ignore_removal_on_fst);
    if (!fsid.empty()) {
      try {
        if (!std::all_of(fsid.begin(), fsid.end(),
                         [](char c) { return c >= '0' && c <= '9'; })) {
          throw std::invalid_argument("invalid fsid");
        }
        const auto value = std::stoull(fsid);
        evict->mutable_evictsinglereplica()->set_fsid(value);
      } catch (const std::exception&) {
        return false;
      }
    }
    for (const auto& a : pos) {
      XrdOucString path = a.c_str();
      unsigned long long fid = 0ull;
      auto* file = evict->add_file();
      if (Path2FileDenominator(path, fid)) {
        file->set_fid(fid);
      } else {
        file->set_path(abspath(path.c_str()));
      }
    }
    return true;
  }
};

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

    EvictHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
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
