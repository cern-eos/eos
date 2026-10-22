// ----------------------------------------------------------------------
// File: chown-cmd-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleMain.hh"
#include <charconv>
#include <cstdint>
#include <memory>

namespace {
std::string
ValidateOwnerPart(const std::string& value)
{
  if (value.empty()) {
    return "owner/group must not be empty";
  }
  for (unsigned char character : value) {
    if (character <= ' ' || character == 127 ||
        std::string("&=?#%:").find(character) != std::string::npos) {
      return "owner/group contains an unsupported character";
    }
  }
  if (value.size() > 1 && (value.front() == '-' || value.front() == '+') &&
      value.find_first_not_of("0123456789", 1) == std::string::npos) {
    return "numeric owner/group must be unsigned";
  }
  if (value.find_first_not_of("0123456789") == std::string::npos) {
    uint32_t id = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), id);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()) {
      return "numeric owner/group must fit a 32-bit ID";
    }
  }
  return {};
}

std::string
CanonicalOwnerPart(const std::string& value)
{
  if (!value.empty() && value.find_first_not_of("0123456789") == std::string::npos) {
    const auto first = value.find_first_not_of('0');
    return first == std::string::npos ? "0" : value.substr(first);
  }
  return value;
}

struct ChownOptions {
  CLI::App app{"Change file or directory ownership", "chown"};
  bool recursive = false, noDereference = false;
  std::string owner, path;

  ChownOptions()
  {
    app.set_help_flag("--help", "Print this help message and exit");
    app.add_flag("-r", recursive, "Change ownership recursively");
    app.add_flag("-h,--nodereference", noDereference, "Do not follow symbolic links");
    app.add_option("owner", owner, "User name/ID, user:group, or :group")
        ->required()
        ->check(CLI::Validator(
            [](const std::string& value) {
              const auto colon = value.find(':');
              if (colon == std::string::npos) {
                return ValidateOwnerPart(value);
              }
              if (colon != 0) {
                const auto error = ValidateOwnerPart(value.substr(0, colon));
                if (!error.empty()) {
                  return error;
                }
              }
              return ValidateOwnerPart(value.substr(colon + 1));
            },
            "OWNER[:GROUP]"));
    app.add_option("path", path, "Namespace path")
        ->required()
        ->check(eos::console::NonEmptyArgument);
    app.footer("Names are resolved by the MGM; use :group to change only the group.");
  }
};

class ChownCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "chown";
  }
  const char*
  description() const override
  {
    return "Chown Interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    const auto arguments = eos::console::TokenizeArguments(args.c_str());
    const auto delimiter = std::find(arguments.begin(), arguments.end(), "--");
    return std::find(arguments.begin(), delimiter, "--help") == delimiter;
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    ChownOptions options;
    if (!eos::console::ParseCliArguments(options.app, args)) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    const auto colon = options.owner.find(':');
    const std::string owner =
        colon == std::string::npos
            ? CanonicalOwnerPart(options.owner)
            : CanonicalOwnerPart(options.owner.substr(0, colon)) + ":" +
                  CanonicalOwnerPart(options.owner.substr(colon + 1));
    std::string flags;
    if (options.recursive) {
      flags += 'r';
    }
    if (options.noDereference) {
      flags += 'h';
    }
    XrdOucString in = "mgm.cmd=chown";
    if (!flags.empty()) {
      in += "&mgm.chown.option=";
      in += flags.c_str();
    }
    in += "&mgm.path=";
    in += eos::common::StringConversion::curl_escaped(abspath(options.path.c_str()))
              .c_str();
    in += "&eos.encodepath=1&mgm.chown.owner=";
    in += owner.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    ChownOptions options;
    return CompleteCliArguments(options.app, args, {},
                                {{options.app.get_option("path"), CompletionPath::Eos}});
  }
  std::string
  helpText() const override
  {
    return ChownOptions{}.app.help();
  }
  void
  printHelp() const override
  {
    ChownOptions options;
    eos::console::PrintCliHelp(options.app, CLI::AppFormatMode::Normal);
  }
};
} // namespace

void
RegisterChownNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChownCommand>());
}
