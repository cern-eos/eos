// ----------------------------------------------------------------------
// File: fsck-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/FsckHelper.hh"
#include <iomanip>
#include <memory>
#include <sstream>

namespace {
std::string MakeFsckHelp()
{
  std::ostringstream oss;
  oss << "Usage: fsck [stat|config|report|repair|clean_orphans]\n"
      << "    control and display file system check information\n"
      << std::endl
      << "  fsck stat [-m]\n"
      << "    print summary of consistency checks\n"
      << "    -m         : print in monitoring format\n"
      << std::endl
      << "  fsck config <key> <value>\n"
      << "    configure the fsck with the following possible options:\n"
      << "    collect-interval-min : collection interval in minutes [default 30]\n"
      << "    collect              : control error collection thread - on/off\n"
      << "    repair               : control error repair thread - on/off\n"
      << "    best-effort          : control best-effort repair mode - on/off\n"
      << "    repair-category      : specify error types that the repair thread will handle\n"
      << "                           e.g all, m_cx_diff, m_mem_sz_diff, d_cx_diff, d_mem_sz_diff,\n"
      << "                           unreg_n, rep_diff_n, rep_missing_n, blockxs_err, stripe_err\n"
      << "    max-queued-jobs      : maximum number of queued jobs\n"
      << "    max-thread-pool-size : maximum number of threads in the fsck pool\n"
      << "    show-dark-files      : on/off [default off] - might affect instance performance\n"
      << "    show-offline         : on/off [default off] - might affect instance performance\n"
      << "    show-no-replica      : on/off [default off] - might affect instance performance\n"
      << std::endl
      << "  fsck report [-a] [-i] [-l] [-j|--json] [--error <tag1> <tag2> ...]\n"
      << "    report consistency check results, with the following options\n"
      << "    -a         : break down statistics per file system\n"
      << "    -i         : display file identifiers\n"
      << "    -l         : display logical file name\n"
      << "    -j|--json  : display in JSON output format\n"
      << "    --error    : display information about the following error tags\n"
      << std::endl
      << "  fsck repair --fxid <val> [--fsid <val>] [--error <err_type>] [--async]\n"
      << "    repair the given file if there are any errors\n"
      << "    --fxid  : hexadecimal file identifier\n"
      << "    --fsid  : file system id used for collecting info\n"
      << "    --error : error type for given file system id e.g. m_cx_diff unreg_n etc\n"
      << "    --async : job queued and ran by the repair thread if enabled\n"
      << std::endl
      << "  fsck clean_orphans [--fsid <val>] [--force-qdb-cleanup]\n"
      << "    clean orphans by removing the entries from disk and local\n"
      << "    database for all file systems or only for the given fsid.\n"
      << "    This operation is synchronous but the fsck output will be\n"
      << "    updated once the inconsistencies are refreshed.\n"
      << "    --force-qdb-cleanup : force remove orphan entries from qdb\n"
      << std::endl;
  return oss.str();
}

class FsckProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "fsck";
  }
  const char*
  description() const override
  {
    return "File System Consistency Checking";
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
      // Quote args with spaces so FsckHelper's tokenizer preserves them
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    if (args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    FsckHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", MakeFsckHelp().c_str());
  }
};
} // namespace

void
RegisterFsckProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FsckProtoCommand>());
}
