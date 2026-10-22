// ----------------------------------------------------------------------
// File: chmod-cmd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include <memory>

namespace {
struct ChmodOptions {
  CLI::App app{"Set file or directory permissions", "chmod"};
  bool recursive = false;
  std::string mode, path;

  ChmodOptions()
  {
    app.add_flag("-r", recursive, "Change permissions recursively");
    app.add_option("mode", mode, "Octal mode from 0 to 7777 (for example 755 or 2777)")
        ->required()
        ->check(CLI::Validator(
            [](const std::string& value) {
              const auto first = value.find_first_not_of('0');
              if (value.empty() ||
                  value.find_first_not_of("01234567") != std::string::npos ||
                  (first != std::string::npos && value.size() - first > 4)) {
                return std::string("mode must be an octal number from 0 to 7777");
              }
              return std::string{};
            },
            "OCTAL_MODE"));
    app.add_option("path", path, "Namespace path")
        ->required()
        ->check(eos::console::NonEmptyArgument);
  }
};

class ChmodCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "chmod";
  }
  const char*
  description() const override
  {
    return "Mode Interface";
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    ChmodOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // The MGM expects octal digits without a leading zero, rather than the
    // decimal representation of the parsed permission bits.
    const auto first = options.mode.find_first_not_of('0');
    const std::string mode =
        first == std::string::npos ? "0" : options.mode.substr(first);
    XrdOucString in = "mgm.cmd=chmod";
    if (options.recursive) {
      in += "&mgm.option=r";
    }
    in += "&mgm.path=";
    in += eos::common::StringConversion::curl_escaped(abspath(options.path.c_str()))
              .c_str();
    in += "&eos.encodepath=1&mgm.chmod.mode=";
    in += mode.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    ChmodOptions options;
    return CompleteCliArguments(options.app, args, {},
                                {{options.app.get_option("path"), CompletionPath::Eos}});
  }
  std::string
  helpText() const override
  {
    return ChmodOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterChmodNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChmodCommand>());
}
