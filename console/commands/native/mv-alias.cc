// ----------------------------------------------------------------------
// File: mv-alias.cc
// Purpose: Provide 'mv' alias to 'file rename'
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include <memory>
#include <vector>

namespace {
class MvAliasCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "mv";
  }
  const char*
  description() const override
  {
    return "Alias for 'file rename'";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.size() < 2) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    IConsoleCommand* fileCmd = CommandRegistry::instance().find("file");
    if (!fileCmd) {
      fprintf(stderr, "error: 'file' command not available\n");
      global_retc = EINVAL;
      return 0;
    }
    std::vector<std::string> forwarded;
    forwarded.reserve(args.size() + 1);
    forwarded.emplace_back("rename");
    forwarded.insert(forwarded.end(), args.begin(), args.end());
    return fileCmd->run(forwarded, ctx);
  }
  std::string
  helpText() const override
  {
    return "Usage: mv <src> <dst>\n\nRename a file or directory. Alias for eos file "
           "rename.\n";
  }
  void
  printHelp() const override
  {
    eos::console::PrintLegacyHelp("Usage: mv <src> <dst>\n");
  }
};
} // namespace

// Keep legacy registration symbol name expected by CommandFramework
void
RegisterMvNativeCommand()
{
  CommandRegistry::instance().reg(
      eos::console::WithCommandHelp(std::make_unique<MvAliasCommand>()));
}

// Backward-compatible alias (if ever referenced elsewhere)
void
RegisterMvAliasCommand()
{
  RegisterMvNativeCommand();
}
