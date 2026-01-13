// ----------------------------------------------------------------------
// File: fsck-proto-native.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include <memory>
#include <sstream>

namespace {
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
      oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    // Build mgm fsck commands from args where feasible; otherwise fallback
    if (args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    const std::string& sub = args[0];
    XrdOucString in = "mgm.cmd=fsck";
    if (sub == "stat") {
      in += "&mgm.subcmd=stat";
      if (std::find(args.begin() + 1, args.end(), "-m") != args.end())
        in += "&mgm.option=m";
    } else if (sub == "config") {
      if (args.size() < 3) {
        printHelp();
        global_retc = EINVAL;
        return 0;
      }
      in += "&mgm.subcmd=config&mgm.key=";
      in += args[1].c_str();
      in += "&mgm.value=";
      in += args[2].c_str();
    } else if (sub == "report") {
      in += "&mgm.subcmd=report";
      for (size_t i = 1; i < args.size(); ++i) {
        const auto& a = args[i];
        if (a == "-a")
          in += "&mgm.option=a";
        else if (a == "-h")
          in += "&mgm.option=h";
        else if (a == "-i")
          in += "&mgm.option=i";
        else if (a == "-l")
          in += "&mgm.option=l";
        else if (a == "-j" || a == "--json")
          in += "&mgm.option=j";
        else if (a == "--error" && i + 1 < args.size()) {
          in += "&mgm.error=";
          in += args[++i].c_str();
        }
      }
    } else if (sub == "repair") {
      in += "&mgm.subcmd=repair";
      for (size_t i = 1; i < args.size(); ++i) {
        const auto& a = args[i];
        if (a == "--fxid" && i + 1 < args.size()) {
          in += "&mgm.fxid=";
          in += args[++i].c_str();
        } else if (a == "--fsid" && i + 1 < args.size()) {
          in += "&mgm.fsid=";
          in += args[++i].c_str();
        } else if (a == "--error" && i + 1 < args.size()) {
          in += "&mgm.error=";
          in += args[++i].c_str();
        } else if (a == "--async") {
          in += "&mgm.async=1";
        }
      }
    } else if (sub == "clean_orphans") {
      in += "&mgm.subcmd=clean_orphans";
      for (size_t i = 1; i < args.size(); ++i) {
        const auto& a = args[i];
        if (a == "--fsid" && i + 1 < args.size()) {
          in += "&mgm.fsid=";
          in += args[++i].c_str();
        } else if (a == "--force-qdb-cleanup") {
          in += "&mgm.forceqdb=1";
        }
      }
    } else {
      fprintf(stderr, "error: unsupported fsck subcommand\n");
      global_retc = EINVAL;
      return 0;
    }
    global_retc = ctx.outputResult(ctx.clientCommand(in, true, nullptr), true);
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(
        stderr,
        "Usage: fsck [stat|config|report|repair]\n"
        "    control and display file system check information\n\n"
        "  fsck stat [-m]\n"
        "    print summary of consistency checks\n"
        "    -m         : print in monitoring format\n\n"
        "  fsck config <key> <value>\n"
        "    configure the fsck with the following possible options:\n"
        "    collect-interval-min : collection interval in minutes [default "
        "30]\n"
        "    collect              : control error collection thread - on/off\n"
        "    repair               : control error repair thread - on/off\n"
        "    best-effort          : control best-effort repair mode - on/off\n"
        "    repair-category      : specify error types that the repair thread "
        "will handle\n"
        "                           e.g all, m_cx_diff, m_mem_sz_diff, "
        "d_cx_diff, d_mem_sz_diff,\n"
        "                           unreg_n, rep_diff_n, rep_missing_n, "
        "blockxs_err\n"
        "    max-queued-jobs      : maximum number of queued jobs\n"
        "    max-thread-pool-size : maximum number of threads in the fsck "
        "pool\n"
        "    show-dark-files      : on/off [default off] - might affect "
        "instance performance\n"
        "    show-offline         : on/off [default off] - might affect "
        "instance performance\n"
        "    show-no-replica      : on/off [default off] - might affect "
        "instance performance\n\n"
        "  fsck report [-a] [-h] [-i] [-l] [-j|--json] [--error <tag1> <tag2> "
        "...]\n"
        "    report consistency check results, with the following options\n"
        "    -a         : break down statistics per file system\n"
        "    -i         : display file identifiers\n"
        "    -l         : display logical file name\n"
        "    -j|--json  : display in JSON output format\n"
        "    --error    : display information about the following error "
        "tags\n\n"
        "  fsck repair --fxid <val> [--fsid <val>] [--error <err_type>] "
        "[--async]\n"
        "    repair the given file if there are any errors\n"
        "    --fxid  : hexadecimal file identifier\n"
        "    --fsid  : file system id used for collecting info\n"
        "    --error : error type for given file system id e.g. m_cx_diff "
        "unreg_n etc\n"
        "    --async : job queued and ran by the repair thread if enabled\n\n"
        "  fsck clean_orphans [--fsid <val>] [--force-qdb-cleanup]\n"
        "     clean orphans by removing the entries from disk and local\n "
        "     database for all file systems or only for the given fsid.\n"
        "     This operation is synchronous but the fsck output will be\n"
        "     updated once the inconsistencies are refreshed.\n"
        "     --force-qdb-cleanup : force remove orphan entries from qdb\n");
  }
};
} // namespace

void
RegisterFsckProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<FsckProtoCommand>());
}
