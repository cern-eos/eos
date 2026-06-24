// ----------------------------------------------------------------------
// File: touch-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>
#include <vector>

namespace {
std::string MakeTouchHelp()
{
  return "Usage: touch [-a] [-n] [-0] <path> [linkpath|size [hexchecksum]]\n"
         "       touch -l <path> [lifetime [audience=user|app]]\n"
         "       touch -u <path>\n\n"
         "Touch a file. Delegates to 'file touch'.\n\n"
         "Options:\n"
         "  -a  absorb\n"
         "  -n  no layout\n"
         "  -0  truncate\n"
         "  -l  lock\n"
         "  -u  unlock\n";
}

class TouchCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "touch";
  }
  const char*
  description() const override
  {
    return "Touch a file";
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
    if (args.empty() || wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    // This command has no flags of its own to parse - it just delegates
    // everything to 'file touch'
    std::vector<std::string> fargs;
    fargs.reserve(args.size() + 1);
    fargs.push_back("touch");
    fargs.insert(fargs.end(), args.begin(), args.end());
    return fileCmd->run(fargs, ctx);
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeTouchHelp().c_str());
  }
};
} // namespace

void
RegisterTouchNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TouchCommand>());
}
