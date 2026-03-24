// ----------------------------------------------------------------------
// File: ns-proto-native.cc
// ----------------------------------------------------------------------

#include "common/ParseUtils.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/CommandFramework.hh"
#include <CLI/CLI.hpp>
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <memory>
#include <sstream>

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
  return "Usage: ns stat|mutex|compact|master|cache|drain|reserve-ids|"
         "benchmark|tracker|behaviour|recompute_tree_size|recompute_quotanode|"
         "update_quotanode [OPTIONS]\n\n"
         "  stat [-a] [-m] [-n] [--reset]     namespace statistics\n"
         "  mutex [--toggletime|--toggleorder|--toggledeadlock|--smplrate*|"
         "--setblockedtime <ms>]\n"
         "  compact off|on <delay> [<interval>] [<type>]\n"
         "  master [<hostname>|--log|--log-clear|--enable|--disable]\n"
         "  cache set|drop [-d|-f] [<max_num>] [<max_size>]\n"
         "  drain list|set [<key>=<value>]\n"
         "  reserve-ids <file_id> <container_id>\n"
         "  benchmark <n-threads> <n-subdirs> <n-subfiles> [prefix]\n"
         "  tracker list|clear --name <tracker_type>\n"
         "  behaviour list|set|get|clear [<behaviour> [<value>]]\n"
         "  recompute_tree_size|recompute_quotanode|update_quotanode <path>|"
         "cid:<id>|cxid:<id> [options]\n";
}

void ConfigureNsApp(CLI::App& app)
{
  app.name("ns");
  app.description("Namespace Interface");
  app.set_help_flag("");
  app.allow_extras();
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) {
        return MakeNsHelp();
      }));
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
    NsNativeHelper helper(*ctx.globalOpts);
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
    CLI::App app;
    ConfigureNsApp(app);
    fprintf(stderr, "%s", app.help().c_str());
  }
};
} // namespace

void
RegisterNsProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<NsProtoCommand>());
}
