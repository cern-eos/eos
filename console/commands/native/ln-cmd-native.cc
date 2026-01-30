// ----------------------------------------------------------------------
// File: ln-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
class LnCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "ln";
  }
  const char*
  description() const override
  {
    return "Create a symbolic link";
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
    IConsoleCommand* fileCmd = CommandRegistry::instance().find("file");
    if (!fileCmd) {
      fprintf(stderr, "error: 'file' command not available\n");
      global_retc = EINVAL;
      return 0;
    }
    if (args.size() < 2) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (wants_help(joined.c_str())) {
      fileCmd->printHelp();
      global_retc = EINVAL;
      return 0;
    }
    std::vector<std::string> fargs;
    fargs.reserve(args.size() + 1);
    fargs.push_back("symlink");
    fargs.insert(fargs.end(), args.begin(), args.end());
    return fileCmd->run(fargs, ctx);
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "Usage: ln <link> <target>\n");
  }
};
} // namespace

void
RegisterLnNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<LnCommand>());
}
