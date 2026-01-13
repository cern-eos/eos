// ----------------------------------------------------------------------
// File: chmod-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleArgParser.hh"
#include <memory>
#include <sstream>

namespace {
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
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty() || wants_help(args[0].c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    // parse optional -r
    size_t idx = 0;
    bool recursive = false;
    if (idx < args.size() && args[idx].rfind("-", 0) == 0) {
      if (args[idx] == "-r") {
        recursive = true;
        ++idx;
      } else {
        printHelp(); global_retc = EINVAL; return 0;
      }
    }

    if (idx + 1 >= args.size()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }

    const std::string& mode = args[idx];
    const std::string& path = args[idx + 1];

    XrdOucString in = "mgm.cmd=chmod";
    if (recursive)
      in += "&mgm.option=r";
    XrdOucString ap = abspath(path.c_str());
    in += "&mgm.path=";
    in += ap;
    in += "&mgm.chmod.mode=";
    in += mode.c_str();
    global_retc = ctx.outputResult(ctx.clientCommand(in, false, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr,
            "Usage: chmod [-r] <mode> <path>                             : set mode for <path> (-r recursive)\n");
    fprintf(stderr,
            "                 <mode> can be only numerical like 755, 644, 700\n");
  }
};
} // namespace

void
RegisterChmodNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<ChmodCommand>());
}
