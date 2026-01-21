// ----------------------------------------------------------------------
// File: rm-proto-native.cc
// ----------------------------------------------------------------------

#include "common/Path.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

namespace {
class RmHelper : public ICmdHelper {
public:
  RmHelper(const GlobalOptions& opts) : ICmdHelper(opts) { mIsAdmin = false; }
  ~RmHelper() override = default;

  bool ParseCommand(const char* arg) override
  {
    XrdOucString option;
    eos::console::RmProto* rm = mReq.mutable_rm();
    eos::common::StringTokenizer tokenizer(arg);
    bool noconfirmation = false;

    tokenizer.GetLine();

    while ((option = tokenizer.GetToken(false)).length() > 0 &&
           (option.beginswith("-"))) {
      if ((option == "-r") || (option == "-rf") || (option == "-fr")) {
        rm->set_recursive(true);
      } else if ((option == "-F") || (option == "--no-recycle-bin")) {
        rm->set_bypassrecycle(true);
      } else if (option == "-rF" || option == "-Fr") {
        rm->set_recursive(true);
        rm->set_bypassrecycle(true);
      } else if (option == "--no-confirmation") {
        noconfirmation = true;
      } else if (option == "--no-workflow" || option == "-n") {
        rm->set_noworkflow(true);
      } else if (option == "--no-globbing") {
        rm->set_noglobbing(true);
      } else {
        return false;
      }
    }

    auto path = option;

    do {
      XrdOucString param = tokenizer.GetToken();

      if (param.length()) {
        path += " ";
        path += param;
      } else {
        break;
      }
    } while (true);

    // remove escaped blanks
    while (path.replace("\\ ", " ")) {
    }

    if (path.length() == 0) {
      return false;
    }

    auto id = 0ull;

    if (Path2FileDenominator(path, id)) {
      rm->set_fileid(id);
      rm->set_recursive(false); // disable recursive option for files
      path = "";
    } else {
      if (Path2ContainerDenominator(path, id)) {
        rm->set_containerid(id);
        path = "";
      } else {
        path = abspath(path.c_str());
        rm->set_path(path.c_str());
      }
    }

    eos::common::Path cPath(path.c_str());

    if (path.length()) {
      mNeedsConfirmation = rm->recursive() &&
                           (cPath.GetSubPathSize() < 4) &&
                           !noconfirmation;
    }

    return true;
  }
};

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
    RmHelper rm(gGlobalOpts);
    if (!rm.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (rm.NeedsConfirmation() && !rm.ConfirmOperation()) {
      global_retc = EINTR;
      return 0;
    }
    global_retc = rm.Execute(true, true);
    return global_retc;
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
