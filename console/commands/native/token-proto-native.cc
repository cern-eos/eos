// ----------------------------------------------------------------------
// File: token-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/commands/helpers/TokenHelper.hh"
#include <CLI/CLI.hpp>
#include <memory>
#include <sstream>

namespace {
std::string MakeTokenHelp()
{
  return "Usage: token --token <token> | --path <path> --expires <expires> "
         "[--permission <perm>] [--owner <owner>] [--group <group>] [--tree] "
         "[--origin <origin> ...]\n\n"
         "  --token <token>   dump token JSON (independent of validity)\n"
         "  --path <path>     namespace restriction (directory or file)\n"
         "  --permission <perm>  e.g. 'rx' 'rwx' 'rwx!d' 'rwxq'\n"
         "  --owner <owner>   identify bearer as user\n"
         "  --group <group>   identify bearer with group\n"
         "  --tree            subtree token for whole tree under path\n"
         "  --origin <origin> restrict usage (regexp:hostname:username:protocol)\n";
}

void ConfigureTokenApp(CLI::App& app)
{
  app.name("token");
  app.description("Token interface");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeTokenHelp();
      }));
}

class TokenProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "token";
  }
  const char*
  description() const override
  {
    return "Token interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    (void)ctx;
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    TokenHelper token(gGlobalOpts);
    if (!token.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = token.Execute(true, true);
    return global_retc;
  }
  void
  printHelp() const override
  {
    CLI::App app;
    ConfigureTokenApp(app);
    std::cerr << app.help();
  }
};
} // namespace

void
RegisterTokenProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TokenProtoCommand>());
}
