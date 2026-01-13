// ----------------------------------------------------------------------
// File: rm-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

namespace {
class RmProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "rm";
  }
  const char*
  description() const override
  {
    return "Remove a file";
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
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    IConsoleCommand* rmNative = CommandRegistry::instance().find("rm");
    if (rmNative) {
      return rmNative->run(args, ctx);
    }
    fprintf(stderr, "error: native 'rm' command not available\n");
    global_retc = EINVAL;
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: rm [-r|-rf|-rF|-n] [--no-recycle-bin|-F] [--no-confirmation] "
        "[--no-workflow] [--no-globbing] "
        "[<path>|fid:<fid-dec>|fxid:<fid-hex>|cid:<cid-dec>|cxid:<cid-hex>]\n"
        "            -r | -rf : remove files/directories recursively\n"
        "                     - the 'f' option is a convenience option with no "
        "additional functionality!\n"
        "                     - the recursive flag is automatically removed it "
        "the target is a file!\n\n"
        " --no-recycle-bin|-F : remove bypassing recycling policies\n"
        "                     - you have to take the root role to use this "
        "flag!\n\n"
        "            -rF | Fr : remove files/directories recursively bypassing "
        "recycling policies\n"
        "                     - you have to take the root role to use this "
        "flag!\n"
        "                     - the recursive flag is automatically removed it "
        "the target is a file!\n"
        " --no-workflow | -n  : don't run a workflow when deleting!\n"
        " --no-confirmation : don't ask for confirmation if recursive "
        "deletions is running in directory level < 4\n"
        " --no-globbing     : disables path globbing feature (e.g: delete a "
        "file containing '[]' characters)\n");
  }
};
} // namespace

void
RegisterRmProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<RmProtoCommand>());
}
