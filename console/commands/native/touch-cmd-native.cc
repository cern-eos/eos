// ----------------------------------------------------------------------
// File: touch-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include <memory>
#include <sstream>

namespace {
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
      fileCmd->printHelp();
      global_retc = EINVAL;
      return 0;
    }
    std::vector<std::string> fargs;
    fargs.reserve(args.size() + 1);
    fargs.push_back("touch");
    fargs.insert(fargs.end(), args.begin(), args.end());
    return fileCmd->run(fargs, ctx);
  }
  void
  printHelp() const override
  {
  }
};
} // namespace

void
RegisterTouchNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<TouchCommand>());
}
