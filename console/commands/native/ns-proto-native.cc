// ----------------------------------------------------------------------
// File: ns-proto-native.cc
// ----------------------------------------------------------------------

#include "common/ParseUtils.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <algorithm>
#include <memory>
#include <sstream>
#include <utility>

// Native helper implementing the protobuf request (ported from legacy)
class NsNativeHelper : public ICmdHelper {
public:
  NsNativeHelper(const GlobalOptions& opts) : ICmdHelper(opts)
  {
    mIsAdmin = true;
  }
  ~NsNativeHelper() override = default;
  bool ParseCommand(const char* arg);
};

// Parse command line input (ported logic)
bool
NsNativeHelper::ParseCommand(const char* arg)
{
  const char* option;
  std::string soption;
  eos::console::NsProto* ns = mReq.mutable_ns();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  option = tokenizer.GetToken();
  std::string cmd = (option ? option : "");

  if (cmd == "stat") {
    eos::console::NsProto_StatProto* stat = ns->mutable_stat();
    if (!(option = tokenizer.GetToken())) {
      stat->set_monitor(false);
    } else {
      while (true) {
        soption = option;
        if (soption == "-a") {
          stat->set_groupids(true);
        } else if (soption == "-x") {
          stat->set_apps(true);
        } else if (soption == "-m") {
          stat->set_monitor(true);
        } else if (soption == "-n") {
          stat->set_numericids(true);
        } else if (soption == "--reset") {
          stat->set_reset(true);
        } else {
          return false;
        }
        if (!(option = tokenizer.GetToken())) {
          break;
        }
      }
    }
  } else if (cmd == "mutex") {
    using eos::console::NsProto_MutexProto;
    NsProto_MutexProto* mutex = ns->mutable_mutex();
    if (!(option = tokenizer.GetToken())) {
      mutex->set_list(true);
    } else {
      while (true) {
        soption = option;
        if (soption == "--toggletime") {
          mutex->set_toggle_timing(true);
        } else if (soption == "--toggleorder") {
          mutex->set_toggle_order(true);
        } else if (soption == "--toggledeadlock") {
          mutex->set_toggle_deadlock(true);
        } else if (soption == "--smplrate1") {
          mutex->set_sample_rate1(true);
        } else if (soption == "--smplrate10") {
          mutex->set_sample_rate10(true);
        } else if (soption == "--smplrate100") {
          mutex->set_sample_rate100(true);
        } else if (soption == "--setblockedtime") {
          option = tokenizer.GetToken();
          if (option) {
            mutex->set_blockedtime(std::stoul(option));
          } else {
            return false;
          }
        } else {
          return false;
        }
        if (!(option = tokenizer.GetToken())) {
          break;
        }
      }
    }
  } else if (cmd == "compact") {
    using eos::console::NsProto_CompactProto;
    NsProto_CompactProto* compact = ns->mutable_compact();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    soption = option;
    if (soption == "off") {
      compact->set_on(false);
    } else if (soption == "on") {
      compact->set_on(true);
      if ((option = tokenizer.GetToken())) {
        soption = option;
        int64_t delay = 0;
        try {
          delay = std::stol(soption);
        } catch (...) {
          return false;
        }
        compact->set_delay(delay);
        if ((option = tokenizer.GetToken())) {
          soption = option;
          int64_t interval = 0;
          try {
            interval = std::stol(soption);
          } catch (...) {
            return false;
          }
          compact->set_interval(interval);
          if ((option = tokenizer.GetToken())) {
            soption = option;
            if (soption == "files")
              compact->set_type(NsProto_CompactProto::FILES);
            else if (soption == "directories")
              compact->set_type(NsProto_CompactProto::DIRS);
            else if (soption == "all")
              compact->set_type(NsProto_CompactProto::ALL);
            else if (soption == "files-repair")
              compact->set_type(NsProto_CompactProto::FILES_REPAIR);
            else if (soption == "directories-repair")
              compact->set_type(NsProto_CompactProto::DIRS_REPAIR);
            else if (soption == "all-repair")
              compact->set_type(NsProto_CompactProto::ALL_REPAIR);
            else {
              return false;
            }
          }
        }
      }
    } else {
      return false;
    }
  } else if (cmd == "master") {
    using eos::console::NsProto_MasterProto;
    NsProto_MasterProto* master = ns->mutable_master();
    if (!(option = tokenizer.GetToken())) {
      master->set_op(NsProto_MasterProto::LOG);
    } else {
      soption = option;
      if (soption == "--log") {
        master->set_op(NsProto_MasterProto::LOG);
      } else if (soption == "--log-clear") {
        master->set_op(NsProto_MasterProto::LOG_CLEAR);
      } else if (soption == "--enable") {
        master->set_op(NsProto_MasterProto::ENABLE);
      } else if (soption == "--disable") {
        master->set_op(NsProto_MasterProto::DISABLE);
      } else {
        master->set_host(soption);
      }
    }
  } else if (cmd == "recompute_tree_size") {
    using eos::console::NsProto_TreeSizeProto;
    NsProto_TreeSizeProto* tree = ns->mutable_tree();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    while (true) {
      int pos = 0;
      soption = option;
      if (soption == "--depth") {
        if (!(option = tokenizer.GetToken())) {
          return false;
        }
        soption = option;
        try {
          tree->set_depth(std::stoul(soption));
        } catch (...) {
          return false;
        }
      } else if ((soption.find("cid:") == 0)) {
        pos = soption.find(':') + 1;
        tree->mutable_container()->set_cid(soption.substr(pos));
      } else if (soption.find("cxid:") == 0) {
        pos = soption.find(':') + 1;
        tree->mutable_container()->set_cxid(soption.substr(pos));
      } else {
        tree->mutable_container()->set_path(soption);
      }
      if (!(option = tokenizer.GetToken())) {
        break;
      }
    }
  } else if (cmd == "recompute_quotanode") {
    using eos::console::NsProto_QuotaSizeProto;
    NsProto_QuotaSizeProto* quota = ns->mutable_quota();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    while (true) {
      int pos = 0;
      soption = option;
      if ((soption.find("cid:") == 0)) {
        pos = soption.find(':') + 1;
        quota->mutable_container()->set_cid(soption.substr(pos));
      } else if (soption.find("cxid:") == 0) {
        pos = soption.find(':') + 1;
        quota->mutable_container()->set_cxid(soption.substr(pos));
      } else {
        quota->mutable_container()->set_path(soption);
      }
      if (!(option = tokenizer.GetToken())) {
        break;
      }
    }
  } else if (cmd == "update_quotanode") {
    using eos::console::NsProto_QuotaSizeProto;
    NsProto_QuotaSizeProto* quota = ns->mutable_quota();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    int npar = 0;
    while (true) {
      int pos = 0;
      soption = option;
      if ((soption.find("cid:") == 0)) {
        pos = soption.find(':') + 1;
        quota->mutable_container()->set_cid(soption.substr(pos));
      } else if (soption.find("cxid:") == 0) {
        pos = soption.find(':') + 1;
        quota->mutable_container()->set_cxid(soption.substr(pos));
      } else if (soption.find("uid:") == 0) {
        pos = soption.find(':') + 1;
        quota->set_uid(soption.substr(pos));
      } else if (soption.find("gid:") == 0) {
        pos = soption.find(':') + 1;
        quota->set_gid(soption.substr(pos));
      } else if (soption.find("bytes:") == 0) {
        pos = soption.find(':') + 1;
        quota->set_used_bytes(strtoul(soption.substr(pos).c_str(), 0, 10));
        npar++;
      } else if (soption.find("physicalbytes:") == 0) {
        pos = soption.find(':') + 1;
        quota->set_physical_bytes(strtoul(soption.substr(pos).c_str(), 0, 10));
        npar++;
      } else if (soption.find("inodes:") == 0) {
        pos = soption.find(':') + 1;
        quota->set_used_inodes(strtoul(soption.substr(pos).c_str(), 0, 10));
        npar++;
      } else {
        quota->mutable_container()->set_path(soption);
      }
      if (!(option = tokenizer.GetToken())) {
        break;
      }
    }
    if (npar && (npar != 3)) {
      return false;
    }
  } else if (cmd == "cache") {
    eos::console::NsProto_CacheProto* cache = ns->mutable_cache();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    soption = option;
    if (soption == "set") {
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      soption = option;
      if (soption == "-f") {
        cache->set_op(eos::console::NsProto_CacheProto::SET_FILE);
      } else if (soption == "-d") {
        cache->set_op(eos::console::NsProto_CacheProto::SET_DIR);
      } else {
        return false;
      }
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      uint64_t max_num = 0ull, max_size = 0ull;
      try {
        max_num = std::stoull(option);
      } catch (...) {
        return false;
      }
      if ((option = tokenizer.GetToken())) {
        try {
          max_size =
              eos::common::StringConversion::GetDataSizeFromString(option);
        } catch (...) {
          return false;
        }
      }
      cache->set_max_num(max_num);
      cache->set_max_size(max_size);
    } else if (soption == "drop") {
      if (!(option = tokenizer.GetToken())) {
        cache->set_op(eos::console::NsProto_CacheProto::DROP_ALL);
      } else {
        soption = option;
        if (soption == "-f") {
          cache->set_op(eos::console::NsProto_CacheProto::DROP_FILE);
        } else if (soption == "-d") {
          cache->set_op(eos::console::NsProto_CacheProto::DROP_DIR);
        } else {
          return false;
        }
      }
    } else if (soption == "drop-single-file") {
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      uint64_t target;
      try {
        target = std::stoull(option);
      } catch (...) {
        return false;
      }
      cache->set_op(eos::console::NsProto_CacheProto::DROP_SINGLE_FILE);
      cache->set_single_to_drop(target);
    } else if (soption == "drop-single-container") {
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      uint64_t target;
      try {
        target = std::stoull(option);
      } catch (...) {
        return false;
      }
      cache->set_op(eos::console::NsProto_CacheProto::DROP_SINGLE_CONTAINER);
      cache->set_single_to_drop(target);
    } else {
      return false;
    }
  } else if (cmd == "drain") {
    using eos::console::NsProto_DrainProto;
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    NsProto_DrainProto* drain = ns->mutable_drain();
    soption = option;
    if (soption == "list") {
      drain->set_op(eos::console::NsProto_DrainProto::LIST);
    } else if (soption == "set") {
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      soption = option;
      size_t pos = soption.find("=");
      if ((pos == std::string::npos) || (pos == soption.length() - 1)) {
        return false;
      }
      drain->set_op(eos::console::NsProto_DrainProto::SET);
      drain->set_key(soption.substr(0, pos));
      drain->set_value(soption.substr(pos + 1));
    } else {
      return false;
    }
  } else if (cmd == "reserve-ids") {
    using eos::console::NsProto_ReserveIdsProto;
    NsProto_ReserveIdsProto* reserve = ns->mutable_reserve();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    int64_t fileID = 0;
    if (!eos::common::ParseInt64(option, fileID) || fileID < 0) {
      return false;
    }
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    int64_t containerID = 0;
    if (!eos::common::ParseInt64(option, containerID) || containerID < 0) {
      return false;
    }
    reserve->set_fileid(fileID);
    reserve->set_containerid(containerID);
  } else if (cmd == "benchmark") {
    using eos::console::NsProto_BenchmarkProto;
    NsProto_BenchmarkProto* benchmark = ns->mutable_benchmark();
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    int64_t n_threads = 0;
    int64_t n_subdirs = 0;
    int64_t n_subfiles = 0;
    if (!eos::common::ParseInt64(option, n_threads) || n_threads < 0) {
      return false;
    }
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    if (!eos::common::ParseInt64(option, n_subdirs) || n_subdirs < 0) {
      return false;
    }
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    if (!eos::common::ParseInt64(option, n_subfiles) || n_subfiles < 0) {
      return false;
    }
    if ((option = tokenizer.GetToken())) {
      benchmark->set_prefix(option);
    }
    benchmark->set_threads(n_threads);
    benchmark->set_subdirs(n_subdirs);
    benchmark->set_subfiles(n_subfiles);
  } else if (cmd == "tracker") {
    eos::console::NsProto_TrackerProto* tracker = ns->mutable_tracker();
    tracker->set_op(eos::console::NsProto_TrackerProto::NONE);
    while ((option = tokenizer.GetToken())) {
      soption = option;
      if (soption == "list") {
        if (tracker->op() != eos::console::NsProto_TrackerProto::NONE) {
          std::cerr << "error: only one operation per command" << std::endl;
          return false;
        } else {
          tracker->set_op(eos::console::NsProto_TrackerProto::LIST);
        }
      } else if (soption == "clear") {
        if (tracker->op() != eos::console::NsProto_TrackerProto::NONE) {
          std::cerr << "error: only one operation per command" << std::endl;
          return false;
        } else {
          tracker->set_op(eos::console::NsProto_TrackerProto::CLEAR);
        }
      } else if (soption == "--name") {
        if (!(option = tokenizer.GetToken())) {
          return false;
        }
        tracker->set_name(option);
      } else {
        return false;
      }
    }
    if (tracker->op() == eos::console::NsProto_TrackerProto::NONE) {
      std::cerr << "error: no operation specified" << std::endl;
      return false;
    }
  } else if (cmd == "behaviour") {
    eos::console::NsProto_BehaviourProto* behaviour = ns->mutable_behaviour();
    behaviour->set_op(eos::console::NsProto_BehaviourProto::NONE);
    if (!(option = tokenizer.GetToken())) {
      return false;
    }
    soption = option;
    if (soption == "list") {
      behaviour->set_op(eos::console::NsProto_BehaviourProto::LIST);
    } else if (soption == "set") {
      behaviour->set_op(eos::console::NsProto_BehaviourProto::SET);
      while ((option = tokenizer.GetToken())) {
        soption = option;
        if (behaviour->name().empty()) {
          if (soption == "all") {
            std::cerr << "error: \"all\" is a reserved keyword" << std::endl;
            return false;
          }
          behaviour->set_name(soption);
        } else {
          behaviour->set_value(soption);
          break;
        }
      }
      if (behaviour->name().empty() || behaviour->value().empty()) {
        return false;
      }
    } else if (soption == "get") {
      behaviour->set_op(eos::console::NsProto_BehaviourProto::GET);
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      soption = option;
      behaviour->set_name(soption);
    } else if (soption == "clear") {
      behaviour->set_op(eos::console::NsProto_BehaviourProto::CLEAR);
      if (!(option = tokenizer.GetToken())) {
        return false;
      }
      soption = option;
      behaviour->set_name(soption);
    } else {
      std::cerr << "error: unknown behaviour subcommand" << std::endl;
      return false;
    }
  } else if (cmd == "") {
    eos::console::NsProto_StatProto* stat = ns->mutable_stat();
    stat->set_summary(true);
  } else {
    return false;
  }
  return true;
}

namespace {
std::string MakeNsHelp()
{
  std::ostringstream oss;
  oss << "Usage: ns [stat|mutex|compact|master|recompute_tree_size|"
         "recompute_quotanode|update_quotanode|cache|drain|reserve-ids|"
         "benchmark|tracker|behaviour]"
      << std::endl
      << "    print or configure basic namespace parameters" << std::endl
      << "  ns stat [-a] [-x] [-m] [-n] [--reset]" << std::endl
      << "    print namespace statistics" << std::endl
      << "    -a      : break down by uid/gid" << std::endl
      << "    -x      : break down by application" << std::endl
      << "    -m      : display in monitoring format <key>=<value>" << std::endl
      << "    -n      : display numerical uid/gid(s)" << std::endl
      << "    --reset : reset namespace counters" << std::endl
      << std::endl
      << "  ns mutex [<option>]" << std::endl
      << "    manage mutex monitoring. Option can be:" << std::endl
      << "    --toggletime     : toggle the timing" << std::endl
      << "    --toggleorder    : toggle the order" << std::endl
      << "    --toggledeadlock : toggle deadlock check" << std::endl
      << "    --smplrate1      : set timing sample rate at 1% (default, no "
         "slow-down)"
      << std::endl
      << "    --smplrate10     : set timing sample rate at 10% (medium "
         "slow-down)"
      << std::endl
      << "    --smplrate100    : set timing sample rate at 100% (severe "
         "slow-down)"
      << std::endl
      << "    --setblockedtime <ms>" << std::endl
      << "                     : set minimum time when a mutex lock lasting "
         "longer than <ms> \n"
      << "                       is reported in the log file [default=10000]\n"
      << std::endl
      << "  ns compact off|on <delay> [<interval>] [<type>]\n"
      << "    enable online compaction after <delay> seconds\n"
      << "    <interval> : if >0 then compaction is repeated automatically \n"
      << "                 after so many seconds\n"
      << "    <type>     : can be 'files', 'directories' or 'all'. By default  "
         "only the file\n"
      << "                 changelog is compacted. The repair flag can be "
         "indicated by using:\n"
      << "                 'files-repair', 'directories-repair' or "
         "'all-repair'\n"
      << std::endl
      << "  ns master [<option>]" << std::endl
      << "    master/slave operations. Option can be:" << std::endl
      << "    <master_hostname> : set hostname of MGM master RW daemon" << std::endl
      << "    --log             : show master log" << std::endl
      << "    --log-clear       : clean master log" << std::endl
      << "    --enable          : enable the slave/master supervisor thread "
         "modifying stall/"
      << std::endl
      << "                        redirectorion rules" << std::endl
      << "    --disable         : disable supervisor thread" << std::endl
      << std::endl
      << "  ns recompute_tree_size "
         "<path>|cid:<decimal_id>|cxid:<hex_id> [--depth <val>]"
      << std::endl
      << "    recompute the tree size of a directory and all its subdirectories"
      << std::endl
      << "    --depth : maximum depth for recomputation, default 0 i.e no "
         "limit"
      << std::endl
      << std::endl
      << "  ns recompute_quotanode <path>|cid:<decimal_id>|cxid:<hex_id>" << std::endl
      << "    recompute the specified quotanode" << std::endl
      << std::endl
      << "  ns update_quotanode "
         "<path>|cid:<decimal_id>|cxid:<hex_id> "
         "uid:<uid>|gid:<gid> bytes:<bytes> physicalbytes:<bytes> "
         "inodes:<inodes>\n"
      << "    update quota node with the specified (and unchecked) values\n"
      << "    Note: for project quotas the uid values needs to be specified\n"
      << "    since the accounting is done by accumulating the individual\n"
      << "    quotas of the users registered with the quota node.\n"
      << std::endl
      << "  ns cache set|drop [-d|-f] [<max_num>] [<max_size>K|M|G...]" << std::endl
      << "    set the max number of entries or the max size of the cache. Use "
         "the"
      << std::endl
      << "    ns stat command to see the current values." << std::endl
      << "    set        : update cache size for files or directories" << std::endl
      << "    drop       : drop cached file and/or directory entries" << std::endl
      << "    -d         : control the directory cache" << std::endl
      << "    -f         : control the file cache" << std::endl
      << "    <max_num>  : max number of entries" << std::endl
      << "    <max_size> : max size of the cache - not implemented yet" << std::endl
      << std::endl
      << "  ns cache drop-single-file <id of file to drop>\n"
      << "    force refresh of the given FileMD by dropping it from the "
         "cache\n"
      << std::endl
      << "  ns cache drop-single-container <id of container to drop>\n"
      << "    force refresh of the given ContainerMD by dropping it from the "
         "cache\n"
      << std::endl
      << "  ns drain list|set [<key>=<value>]                                 \n"
      << "    list : list the global drain configuration parameters           \n"
      << "    set  : set one of the following drain configuration parameters  \n"
      << "           max-thread-pool-size : max number of threads in drain pool\n"
      << "                                  [default 100, minimum 5]          \n"
      << "           max-fs-per-node      : max number of file systems per node that\n"
      << "                                  can be drained in parallel [default 5]\n"
      << std::endl
      << "  ns reserve-ids <file id> <container id>\n"
      << "    blacklist file and container IDs below the given threshold. The namespace\n"
      << "    will not allocate any file or container with IDs less than, or equal to "
         "the\n"
      << "    given blacklist thresholds.\n"
      << std::endl
      << "  ns benchmark <n-threads> <n-subdirs> <n-subfiles> [prefix=/benchmark]\n"
      << "     run metadata benchmark inside the MGM - results are printed into the MGM "
         "logfile and the shell\n"
      << "                n-threads  : number of parallel threads running a benchmark in "
         "the MGM\n"
      << "                n-subdirs  : directories created by each threads\n"
      << "                n-subfiles : number of files created in each sub-directory\n"
      << "                prefix     : absolute directory where to write the benchmarkf "
         "iles - default is /benchmark\n"
      << std::endl
      << "     example: eos ns benchmark 100 10 10\n"
      << std::endl
      << " ns tracker list|clear --name tracker_type\n"
      << "     list or clear the different file identifier trackers\n"
      << "     tracker_type : one of the following: drain, balance, fsck, convert, all\n"
      << std::endl
      << " ns behaviour list|set|clear\n"
      << "     modify the behaviour of internal mechanisms for the manager node\n"
      << "     list                    : list all the behaviour changes enforced\n"
      << "     set <behaviour> <value> : enforce given behavior\n"
      << "     get <behaviour>         : get behaviour configuration\n"
      << "     clear <behaviour>|all   : remove enforced behavior\n"
      << std::endl
      << "     The following behaviours are supported:\n"
      << "       rain_min_fsid_entry : for RAIN files the entry server will "
         "deterministically\n"
      << "         be the file system with the lowest fsid from the list of stripes\n"
      << "         Accepted values: \"on\" or \"off\" [default off]\n"
      << "       disable_access_log : do not log the INFO line for each access check\n"
      << "         Accepted values: \"on\" or \"off\" [default off]\n"
      << std::endl
      << "     Behaviours are saved in the MGM configuration.\n"
      << std::endl;
  return oss.str();
}

struct NsHelpTopic {
  std::string name;
  std::string summary;
  std::string arguments;
  std::vector<std::pair<std::string, std::string>> definitions;
  std::vector<std::string> notes;
  std::vector<std::string> examples;
  std::vector<NsHelpTopic> children;
};

const NsHelpTopic&
NamespaceHelp()
{
  static const NsHelpTopic help{
      "ns",
      "Print or configure namespace parameters.",
      "[<operation>]",
      {},
      {"With no operation, print a namespace summary."},
      {},
      {{"stat",
        "Print namespace statistics.",
        "[-a] [-x] [-m] [-n] [--reset]",
        {{"-a", "Break down statistics by user and group."},
         {"-x", "Break down statistics by application."},
         {"-m", "Use monitoring format: <key>=<value>."},
         {"-n", "Display numerical user and group IDs."},
         {"--reset", "Reset namespace counters."}},
        {},
        {"eos ns stat -m"},
        {}},
       {"mutex",
        "Inspect or configure mutex monitoring.",
        "[<option>...]",
        {{"--toggletime", "Toggle timing."},
         {"--toggleorder", "Toggle lock order monitoring."},
         {"--toggledeadlock", "Toggle deadlock checks."},
         {"--smplrate1", "Sample 1% of timings (default; no slowdown)."},
         {"--smplrate10", "Sample 10% of timings (medium slowdown)."},
         {"--smplrate100", "Sample all timings (severe slowdown)."},
         {"--setblockedtime <ms>", "Log locks held longer than this threshold, in "
                                   "milliseconds (default: 10000)."}},
        {"With no option, list the current monitoring configuration. Monitoring requires "
         "an MGM built with instrumented RW mutex support."},
        {"eos ns mutex"},
        {}},
       {"compact",
        "Legacy namespace compaction interface (unsupported).",
        "<operation>",
        {},
        {"The current MGM does not support this legacy operation and returns ENOTSUP."},
        {},
        {{"off",
          "Disable online compaction (unsupported).",
          "",
          {},
          {"The current MGM returns ENOTSUP."},
          {},
          {}},
         {"on",
          "Enable online compaction (unsupported).",
          "[<delay> [<interval> [<type>]]]",
          {{"<delay>", "Delay before compaction, in seconds."},
           {"<interval>",
            "Repeat compaction after this many seconds when greater than zero."},
           {"<type>", "files, directories, all, files-repair, directories-repair or "
                      "all-repair. The default compacts files."}},
          {"The current MGM does not support this legacy operation and returns ENOTSUP."},
          {},
          {}}}},
       {"master",
        "Inspect or configure master and slave supervision.",
        "[<hostname>|<option>]",
        {{"<hostname>", "Set the hostname of the read-write MGM master."},
         {"--log", "Show the master log (the default operation)."},
         {"--log-clear", "Clear the master log."},
         {"--enable",
          "Deprecated supervisor operation; the current MGM returns ENOTSUP."},
         {"--disable",
          "Deprecated supervisor operation; the current MGM returns ENOTSUP."}},
        {},
        {"eos ns master --log"},
        {}},
       {"recompute_tree_size",
        "Recompute a directory's tree size, including its subdirectories.",
        "<path>|cid:<decimal_id>|cxid:<hex_id> [--depth <value>]",
        {{"<path>|cid:<decimal_id>|cxid:<hex_id>",
          "Identify the directory by path, decimal container ID or hexadecimal container "
          "ID."},
         {"--depth <value>", "Maximum recursion depth; zero means unlimited (default)."}},
        {},
        {},
        {}},
       {"recompute_quotanode",
        "Recompute the specified quota node.",
        "<path>|cid:<decimal_id>|cxid:<hex_id>",
        {{"<path>|cid:<decimal_id>|cxid:<hex_id>",
          "Identify the quota node by path, decimal container ID or hexadecimal "
          "container ID."}},
        {},
        {},
        {}},
       {"update_quotanode",
        "Update a quota node using the supplied accounting values.",
        "<path>|cid:<decimal_id>|cxid:<hex_id> uid:<uid>|gid:<gid> bytes:<bytes> "
        "physicalbytes:<bytes> inodes:<inodes>",
        {{"<path>|cid:<decimal_id>|cxid:<hex_id>", "Identify the quota node."},
         {"uid:<uid>|gid:<gid>",
          "Identify the user or group whose accounting is updated."},
         {"bytes:<bytes>", "Logical bytes used."},
         {"physicalbytes:<bytes>", "Physical bytes used."},
         {"inodes:<inodes>", "Inodes used."}},
        {"Values are applied without checking their consistency. Supply all three "
         "accounting values together.",
         "Project quotas require a user ID because accounting accumulates the quotas of "
         "users registered with the quota node."},
        {},
        {}},
       {"cache",
        "Inspect or change namespace metadata caches.",
        "<operation>",
        {},
        {"Use 'eos ns stat' to inspect the current cache values."},
        {},
        {{"set",
          "Set a metadata cache's entry limit.",
          "-f|-d <max_num> [<max_size>]",
          {{"-f", "Control the file metadata cache."},
           {"-d", "Control the directory metadata cache."},
           {"<max_num>",
            "Maximum number of cache entries. The MGM applies values greater than 100."},
           {"<max_size>", "Optional size with a K, M or G suffix; the size limit is not "
                          "implemented yet."}},
          {"Choose exactly one of -f and -d. Use 'eos ns stat' to inspect current "
           "values."},
          {"eos ns cache set -f 1000000"},
          {}},
         {"drop",
          "Drop cached namespace metadata.",
          "[-f|-d]",
          {{"-f", "Drop file metadata entries."},
           {"-d", "Drop directory metadata entries."}},
          {"Without -f or -d, drop both caches."},
          {},
          {}},
         {"drop-single-file",
          "Drop one cached file to force a metadata refresh.",
          "<file_id>",
          {{"<file_id>", "Decimal ID of the file to drop from the cache."}},
          {},
          {},
          {}},
         {"drop-single-container",
          "Drop one cached directory to force a metadata refresh.",
          "<container_id>",
          {{"<container_id>", "Decimal ID of the container to drop from the cache."}},
          {},
          {},
          {}}}},
       {"drain",
        "Inspect or configure global drain parameters.",
        "<operation>",
        {},
        {},
        {},
        {{"list",
          "List global drain configuration parameters.",
          "",
          {},
          {},
          {"eos ns drain list"},
          {}},
         {"set",
          "Set one global drain configuration parameter.",
          "<key>=<value>",
          {{"max-thread-pool-size=<value>",
            "Maximum threads in the drain pool (default: 100; minimum: 5)."},
           {"max-fs-per-node=<value>",
            "Maximum file systems per node drained in parallel (default: 5)."}},
          {},
          {},
          {}}}},
       {"reserve-ids",
        "Reserve file and container IDs up to the given thresholds.",
        "<file_id> <container_id>",
        {{"<file_id>", "Nonnegative file ID threshold."},
         {"<container_id>", "Nonnegative container ID threshold."}},
        {"The namespace will not allocate IDs less than or equal to these thresholds."},
        {},
        {}},
       {"benchmark",
        "Run a metadata benchmark inside the MGM.",
        "<threads> <subdirs> <subfiles> [<prefix>]",
        {{"<threads>", "Number of parallel benchmark threads (capped at 1024)."},
         {"<subdirs>", "Directories created by each thread."},
         {"<subfiles>", "Files created in each subdirectory."},
         {"<prefix>",
          "Parent directory of benchmark/; the default writes to /benchmark."}},
        {"Results are printed to the MGM log and the console."},
        {},
        {}},
       {"tracker",
        "List or clear file identifier trackers.",
        "<operation> --name <type>",
        {{"--name <type>", "Select drain, balance, fsck, convert or all."}},
        {"Without --name, no tracker is selected."},
        {},
        {{"list",
          "List tracked file identifiers.",
          "--name <type>",
          {{"--name <type>", "Select drain, balance, fsck, convert or all."}},
          {"Without --name, no tracker is selected."},
          {"eos ns tracker list --name all"},
          {}},
         {"clear",
          "Clear tracked file identifiers.",
          "--name <type>",
          {{"--name <type>", "Select drain, balance, fsck, convert or all."}},
          {"Without --name, no tracker is selected."},
          {},
          {}}}},
       {"behaviour",
        "Inspect or change manager behaviour settings.",
        "<operation>",
        {},
        {"Settings are saved in the MGM configuration.",
         "rain_min_fsid_entry selects the lowest file system ID as the entry server for "
         "RAIN files. disable_access_log suppresses INFO lines for access checks. Both "
         "accept on or off and default to off."},
        {},
        {{"list",
          "List enforced behaviour changes.",
          "",
          {},
          {},
          {"eos ns behaviour list"},
          {}},
         {"set",
          "Set a manager behaviour.",
          "<name> <on|off>",
          {{"<name>", "rain_min_fsid_entry or disable_access_log."},
           {"<on|off>", "Enable or disable the behaviour."}},
          {},
          {},
          {}},
         {"get",
          "Get a manager behaviour's configuration.",
          "<name>",
          {{"<name>", "rain_min_fsid_entry or disable_access_log."}},
          {},
          {"eos ns behaviour get disable_access_log"},
          {}},
         {"clear",
          "Remove enforced behaviour settings.",
          "<name>|all",
          {{"<name>|all",
            "Select rain_min_fsid_entry, disable_access_log or all settings."}},
          {},
          {},
          {}}}}}};
  return help;
}

void
AppendNamespaceHelp(eos::console::HelpPage& page, const NsHelpTopic& topic,
                    const std::string& path, bool all)
{
  using eos::console::HelpBlockKind;
  page.blocks.push_back(
      {HelpBlockKind::Section, all && path != "eos ns" ? path : "USAGE", {}});
  page.blocks.push_back({HelpBlockKind::Usage,
                         path + (topic.arguments.empty() ? "" : " " + topic.arguments),
                         {}});
  page.blocks.push_back({HelpBlockKind::Paragraph, topic.summary, {}});
  if (!topic.children.empty()) {
    page.blocks.push_back({HelpBlockKind::Section, "OPERATIONS", {}});
    for (const auto& child : topic.children) {
      page.blocks.push_back({HelpBlockKind::Definition, child.name, child.summary});
    }
  }
  if (!topic.definitions.empty()) {
    page.blocks.push_back({HelpBlockKind::Section, "ARGUMENTS AND OPTIONS", {}});
    for (const auto& definition : topic.definitions) {
      page.blocks.push_back(
          {HelpBlockKind::Definition, definition.first, definition.second});
    }
  }
  if (!topic.notes.empty()) {
    page.blocks.push_back({HelpBlockKind::Section, "DETAILS", {}});
    for (const auto& note : topic.notes) {
      page.blocks.push_back({HelpBlockKind::Paragraph, note, {}});
    }
  }
  if (!topic.examples.empty()) {
    page.blocks.push_back({HelpBlockKind::Section, "EXAMPLES", {}});
    for (const auto& example : topic.examples) {
      page.blocks.push_back({HelpBlockKind::Example, example, {}});
    }
  }
  if (all) {
    for (const auto& child : topic.children) {
      AppendNamespaceHelp(page, child, path + " " + child.name, true);
    }
  } else {
    page.blocks.push_back({HelpBlockKind::Section, "LEARN MORE", {}});
    if (!topic.children.empty()) {
      page.blocks.push_back(
          {HelpBlockKind::Paragraph,
           "Run '" + path + " <operation> --help' for operation details.",
           {}});
    }
    page.blocks.push_back({HelpBlockKind::Paragraph,
                           "Run 'eos ns --help-all' for the complete namespace manual.",
                           {}});
  }
}

void
PrintNamespaceHelp(const std::vector<std::string>& args)
{
  if (!eos::console::HelpWidth(fileno(stderr))) {
    eos::console::PrintLegacyHelp(MakeNsHelp());
    return;
  }
  const auto* topic = &NamespaceHelp();
  std::string path = "eos ns";
  bool all = false;
  for (const auto& arg : args) {
    if (arg == "--") {
      break;
    }
    if (arg == "--help-all") {
      all = true;
      break;
    }
  }
  if (!all) {
    for (const auto& arg : args) {
      if (arg == "--") {
        break;
      }
      const auto child =
          std::find_if(topic->children.begin(), topic->children.end(),
                       [&arg](const NsHelpTopic& entry) { return entry.name == arg; });
      if (child == topic->children.end()) {
        break;
      }
      topic = &*child;
      path += " " + topic->name;
    }
  }
  eos::console::HelpPage page;
  AppendNamespaceHelp(page, *topic, path, all);
  eos::console::PrintHelpPage(page);
}

bool
WantsNamespaceHelp(const std::vector<std::string>& args)
{
  for (const auto& argument : args) {
    if (argument == "--") {
      break;
    }
    if (argument == "--help" || argument == "--help-all" || argument == "-h") {
      return true;
    }
  }
  return false;
}

class NsProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "ns";
  }
  const char*
  description() const override
  {
    return "Namespace Interface";
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !WantsNamespaceHelp(eos::console::TokenizeArguments(args.c_str()));
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
    if (WantsNamespaceHelp(args)) {
      printHelp(args);
      global_retc = EINVAL;
      return 0;
    }
    NsNativeHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp(args);
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    PrintNamespaceHelp({});
  }
  void
  printHelp(const std::vector<std::string>& args) const override
  {
    PrintNamespaceHelp(args);
  }
};
} // namespace

void
RegisterNsProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<NsProtoCommand>());
}
