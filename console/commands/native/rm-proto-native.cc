// ----------------------------------------------------------------------
// File: rm-proto-native.cc
// ----------------------------------------------------------------------

#include "common/Path.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include "console/commands/helpers/PathIdentifier.hh"
#include <memory>
#include <vector>

namespace {
struct RmOptions {
  CLI::App app{"Remove files or directories", "rm"};
  bool recursive = false;
  bool force = false;
  bool bypassRecycle = false;
  bool noConfirmation = false;
  bool noWorkflow = false;
  bool noGlobbing = false;
  std::vector<std::string> paths;

  RmOptions()
  {
    app.set_help_flag("-h,--help");
    auto* recursiveOption = app.add_flag("-r", recursive, "Remove recursively");
    app.add_flag("-f", force, "Compatibility flag for -rf and -fr")
        ->needs(recursiveOption)
        ->group("");
    app.add_flag("-F,--no-recycle-bin", bypassRecycle,
                 "Bypass recycling policies (root only)");
    app.add_flag("--no-confirmation", noConfirmation,
                 "Skip recursive deletion confirmation");
    app.add_flag("-n,--no-workflow", noWorkflow, "Do not run deletion workflows");
    app.add_flag("--no-globbing", noGlobbing, "Disable path globbing");
    app.add_option("paths", paths, "Paths or fid:/fxid:/cid:/cxid: identifiers")
        ->required()
        ->expected(1, -1)
        ->check(eos::console::NonEmptyArgument)
        ->check(CLI::Validator(
            [](const std::string& path) {
              const auto parsed = eos::console::ParsePathIdentifier(path);
              if (parsed.type == eos::console::IdentifierType::Invalid ||
                  parsed.type == eos::console::IdentifierType::Inode ||
                  path.compare(0, 4, "pid:") == 0 || path.compare(0, 5, "pxid:") == 0) {
                return std::string(
                    "expected a path or a complete uint64 file/container identifier");
              }
              return std::string{};
            },
            "PATH_OR_ID"));
  }
};

class RmHelper : public ICmdHelper {
public:
  explicit RmHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
  }

  bool
  ParseCommand(const char* arguments) override
  {
    std::vector<std::string> args;
    RmOptions options;
    if (!eos::console::TokenizeArguments(arguments, args) ||
        !eos::console::ParseCliArguments(options.app, args) ||
        options.paths.size() != 1) {
      return false;
    }
    Configure(options, options.paths.front());
    return true;
  }

  void
  Configure(const RmOptions& options, const std::string& path)
  {
    auto* rm = mReq.mutable_rm();
    rm->set_recursive(options.recursive);
    rm->set_bypassrecycle(options.bypassRecycle);
    rm->set_noworkflow(options.noWorkflow);
    rm->set_noglobbing(options.noGlobbing);
    const auto parsed = eos::console::ParsePathIdentifier(path);
    if (parsed.type == eos::console::IdentifierType::File) {
      rm->set_fileid(parsed.value);
      rm->set_recursive(false);
    } else if (parsed.type == eos::console::IdentifierType::Container) {
      rm->set_containerid(parsed.value);
    } else {
      const std::string absolute = abspath(path.c_str());
      rm->set_path(absolute);
      eos::common::Path namespacePath(absolute.c_str());
      mNeedsConfirmation = options.recursive && namespacePath.GetSubPathSize() < 4 &&
                           !options.noConfirmation;
    }
  }
};

class RmProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "rm";
  }
  const char*
  description() const override
  {
    return "Remove files or directories";
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    RmOptions options;
    return CompleteCliArguments(options.app, args, {},
                                {{options.app.get_option("paths"), CompletionPath::Eos}});
  }
  std::string
  helpText() const override
  {
    return RmOptions{}.app.help();
  }
  bool
  requiresMgm(const std::string& arguments) const override
  {
    const auto args = eos::console::TokenizeArguments(arguments.c_str());
    const auto delimiter = std::find(args.begin(), args.end(), "--");
    return std::find(args.begin(), delimiter, "--help") == delimiter &&
           std::find(args.begin(), delimiter, "-h") == delimiter;
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    RmOptions options;
    // Validate every operand before the first request: a malformed batch must
    // never delete an earlier valid operand as a side effect of parsing.
    if (!eos::console::ParseCliArguments(options.app, args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    int retc = 0;
    for (const auto& path : options.paths) {
      RmHelper rm(*ctx.globalOpts);
      rm.Configure(options, path);
      if (rm.NeedsConfirmation() && !rm.ConfirmOperation()) {
        retc = EINTR;
        continue;
      }
      if (const int rc = rm.Execute(true, true); rc != 0) {
        retc = rc;
      }
    }
    global_retc = retc;
    return retc;
  }
  void
  printHelp() const override
  {
    std::cerr << helpText();
  }
};
} // namespace

void
RegisterRmProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RmProtoCommand>());
}
