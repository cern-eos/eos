// ----------------------------------------------------------------------
// File: df-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>

namespace {
struct DfOptions {
  CLI::App app{"Print unix-like filesystem capacity information (base 1024)", "df"};
  bool monitoring = false, human = false, bytes = false;
  std::string path;

  DfOptions()
  {
    auto* monitor = app.add_flag("-m", monitoring, "Print monitoring output");
    auto* readable =
        app.add_flag("-H", human, "Print human-readable values in units of 1000");
    auto* raw = app.add_flag("-b", bytes, "Print raw byte/number values");
    monitor->excludes(readable)->excludes(raw);
    readable->excludes(raw);
    app.add_option("path", path, "Absolute namespace path")
        ->check(CLI::Validator(
            [](const std::string& value) {
              return !value.empty() && value.front() == '/'
                         ? std::string{}
                         : std::string("path must be absolute and non-empty");
            },
            "ABSOLUTE_PATH"));
  }
};

class DfHelper : public ICmdHelper {
public:
  explicit DfHelper(const GlobalOptions& opts)
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
    DfOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      return false;
    }
    auto* df = mReq.mutable_df();
    df->set_monitoring(options.monitoring);
    df->set_si(!options.human && !options.bytes);
    df->set_readable(!options.monitoring && !options.bytes);
    df->set_path(options.path);
    return true;
  }
};

class DfProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "df";
  }
  const char*
  description() const override
  {
    return "Get df output";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    DfHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  std::string
  helpText() const override
  {
    return DfOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterDfProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<DfProtoCommand>());
}
