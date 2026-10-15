// ----------------------------------------------------------------------
// File: evict-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include "console/commands/helpers/PathIdentifier.hh"
#include <CLI/CLI.hpp>
#include <charconv>
#include <memory>
#include <sstream>

namespace {
std::string
MakeEvictHelp()
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

void
ConfigureEvictApp(CLI::App& app, bool& ignore_counter, bool& ignore_removal,
                  uint64_t& fsid, std::vector<std::string>& paths)
{
  app.name("evict");
  app.set_help_flag("-h,--help");
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) { return MakeEvictHelp(); }));
  auto* counter =
      app.add_flag("--ignore-evict-counter", ignore_counter, "ignore evict counter")
          ->disable_flag_override();
  auto* filesystem =
      app.add_option("--fsid", fsid, "single filesystem id")
          ->transform(CLI::Validator{
              [](std::string& value) {
                uint64_t parsed = 0;
                const auto result =
                    std::from_chars(value.data(), value.data() + value.size(), parsed);
                if (value.empty() || result.ec != std::errc{} ||
                    result.ptr != value.data() + value.size()) {
                  return std::string("expected an unsigned decimal filesystem id");
                }
                value = std::to_string(parsed);
                return std::string{};
              },
              "FSID"})
          ->needs(counter);
  app.add_flag("--ignore-removal-on-fst", ignore_removal, "namespace-only eviction")
      ->needs(filesystem);
  app.add_option("paths", paths, "file paths or file identifiers")
      ->required()
      ->check(eos::console::NonEmptyArgument)
      ->check(CLI::Validator{
          [](const std::string& value) {
            const auto identifier = eos::console::ParsePathIdentifier(value);
            return identifier.type == eos::console::IdentifierType::Path ||
                           identifier.type == eos::console::IdentifierType::File
                       ? std::string{}
                       : std::string(
                             "expected a path, fid:<decimal> or fxid:<hexadecimal>");
          },
          "FILE"});
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
    std::vector<std::string> arguments;
    return eos::console::TokenizeArguments(arg, arguments) && ParseArguments(arguments);
  }

  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    mReq.clear_evict();
    CLI::App app;
    bool ignore_counter = false, ignore_removal = false;
    uint64_t fsid = 0;
    std::vector<std::string> paths;
    ConfigureEvictApp(app, ignore_counter, ignore_removal, fsid, paths);
    if (!eos::console::ParseCliArguments(app, args)) {
      return false;
    }

    auto* evict = mReq.mutable_evict();
    evict->set_ignoreevictcounter(ignore_counter);
    evict->set_ignoreremovalonfst(ignore_removal);
    if (*app.get_option("--fsid")) {
      evict->mutable_evictsinglereplica()->set_fsid(fsid);
    }
    for (const auto& path : paths) {
      auto* file = evict->add_file();
      const auto identifier = eos::console::ParsePathIdentifier(path);
      if (identifier.type == eos::console::IdentifierType::File) {
        file->set_fid(identifier.value);
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
    EvictHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    CLI::App app;
    bool counter = false, removal = false;
    uint64_t fsid = 0;
    std::vector<std::string> paths;
    ConfigureEvictApp(app, counter, removal, fsid, paths);
    return CompleteCliArguments(app, args, {},
                                {{app.get_option("paths"), CompletionPath::Eos}});
  }
  void
  printHelp() const override
  {
    CLI::App app;
    bool opt_ignore_evict_counter = false;
    bool opt_ignore_removal_on_fst = false;
    uint64_t fsid = 0;
    std::vector<std::string> pos;
    ConfigureEvictApp(app, opt_ignore_evict_counter, opt_ignore_removal_on_fst, fsid,
                      pos);
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
