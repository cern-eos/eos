// ----------------------------------------------------------------------
// File: du-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include "console/commands/helpers/NewfindHelper.hh"
#include <memory>

namespace {
struct DuOptions {
  CLI::App app{"Print subtree size for directories", "du"};
  bool files = false, readable = false, summary = false, si = false;
  std::string path;

  DuOptions()
  {
    // -h is the established human-readable option for du.
    app.set_help_flag("--help", "Print this help message and exit");
    app.add_flag("-a", files, "Print files as well as directories");
    app.add_flag("-h", readable, "Print human-readable sizes");
    app.add_flag("-s", summary, "Print only the summary");
    app.add_flag("--si", si, "Print sizes in SI units");
    app.add_option("path", path, "Namespace path or file:/root:/as3: URI")
        ->required()
        ->check(eos::console::NonEmptyArgument);
    app.footer("file:, root: and as3: use find-compatible traversal (options ignored).");
  }
};

bool
IsLocalDuPath(const std::string& path)
{
  return path.rfind("file:", 0) == 0 || path.rfind("root:", 0) == 0 ||
         path.rfind("as3:", 0) == 0;
}

class DuHelper : public ICmdHelper {
public:
  explicit DuHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
  }
  bool
  ParseCommand(const char* arg) override
  {
    std::vector<std::string> args;
    return eos::console::TokenizeArguments(arg, args) && ParseArguments(args);
  }
  bool
  ParseArguments(const std::vector<std::string>& args)
  {
    DuOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      return false;
    }
    Configure(options);
    return true;
  }
  void
  Configure(const DuOptions& options)
  {
    std::string path = abspath(options.path.c_str());
    if (path.back() != '/' && path.back() != ':') {
      path += '/';
    }
    auto* find = mReq.mutable_find();
    find->set_du(true);
    find->set_directories(!options.files);
    find->set_dusi(options.si);
    find->set_dureadable(options.readable);
    if (options.summary) {
      find->set_maxdepth(0);
    }
    find->set_path(path);
  }
};

class DuCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "du";
  }
  const char*
  description() const override
  {
    return "Get du output";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    std::vector<std::string> arguments;
    return eos::console::TokenizeArguments(args.c_str(), arguments) &&
           requiresMgmArguments(arguments);
  }
  bool
  requiresMgmArguments(const std::vector<std::string>& args) const override
  {
    DuOptions options;
    std::vector<std::string> reversed(args.rbegin(), args.rend());
    try {
      options.app.parse(reversed);
    } catch (const CLI::ParseError&) {
      return false;
    }
    return !IsLocalDuPath(options.path);
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    DuOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (IsLocalDuPath(options.path)) {
      NewfindHelper finder(*ctx.globalOpts);
      global_retc = options.path.rfind("as3:", 0) == 0 ? finder.FindAs3(options.path)
                                                       : finder.FindXroot(options.path);
      return 0;
    }
    DuHelper helper(*ctx.globalOpts);
    helper.Configure(options);
    global_retc = helper.Execute();
    return 0;
  }
  std::string
  helpText() const override
  {
    return DuOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterDuNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DuCommand>());
}
