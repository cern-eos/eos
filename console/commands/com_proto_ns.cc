//------------------------------------------------------------------------------
//! @file com_ns.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include "common/ParseUtils.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

void com_ns_help();

//------------------------------------------------------------------------------
//! Class NsHelper
//------------------------------------------------------------------------------
class NsHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  NsHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {
    mIsAdmin = true;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~NsHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
NsHelper::ParseCommand(const char* arg)
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
    } else {
      soption = option;

      if (soption == "off") {
        compact->set_on(false);
      } else if (soption == "on") {
        compact->set_on(true);

        if ((option = tokenizer.GetToken())) {
          soption = option;
          int64_t delay  = 0;

          try {
            delay = std::stol(soption);
          } catch (std::exception& e) {
            return false;
          }

          compact->set_delay(delay);

          if ((option = tokenizer.GetToken())) {
            soption = option;
            int64_t interval = 0;

            try {
              interval = std::stol(soption);
            } catch (std::exception& e) {
              return false;
            }

            compact->set_interval(interval);

            if ((option = tokenizer.GetToken())) {
              soption = option;

              if (soption == "files") {
                compact->set_type(NsProto_CompactProto::FILES);
              } else if (soption == "directories") {
                compact->set_type(NsProto_CompactProto::DIRS);
              } else if (soption == "all") {
                compact->set_type(NsProto_CompactProto::ALL);
              } else if (soption == "files-repair") {
                compact->set_type(NsProto_CompactProto::FILES_REPAIR);
              } else if (soption == "directories-repair") {
                compact->set_type(NsProto_CompactProto::DIRS_REPAIR);
              } else if (soption == "all-repair") {
                compact->set_type(NsProto_CompactProto::ALL_REPAIR);
              } else {
                return false;
              }
            }
          }
        }
      } else {
        return false;
      }
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
    } else {
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
          } catch (const std::exception& e) {
            return false;
          }
        } else if ((soption.find("cid:") == 0)) {
          pos = soption.find(':') + 1;
          tree->mutable_container()->set_cid(soption.substr(pos));
        } else if (soption.find("cxid:") == 0) {
          pos = soption.find(':') + 1;
          tree->mutable_container()->set_cxid(soption.substr(pos));
        } else { // this should be a plain path
          tree->mutable_container()->set_path(soption);
        }

        if (!(option = tokenizer.GetToken())) {
          break;
        }
      }
    }
  } else if (cmd == "recompute_quotanode") {
    using eos::console::NsProto_QuotaSizeProto;
    NsProto_QuotaSizeProto* quota = ns->mutable_quota();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      while (true) {
        int pos = 0;
        soption = option;

        if ((soption.find("cid:") == 0)) {
          pos = soption.find(':') + 1;
          quota->mutable_container()->set_cid(soption.substr(pos));
        } else if (soption.find("cxid:") == 0) {
          pos = soption.find(':') + 1;
          quota->mutable_container()->set_cxid(soption.substr(pos));
        } else { // this should be a plain path
          quota->mutable_container()->set_path(soption);
        }

        if (!(option = tokenizer.GetToken())) {
          break;
        }
      }
    }
  } else if (cmd == "cache") {
    eos::console::NsProto_CacheProto* cache = ns->mutable_cache();

    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    soption = option;

    if (soption == "set")  {
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
      } catch (const std::exception& e) {
        return false;
      }

      if ((option = tokenizer.GetToken())) {
        try {
          max_size = eos::common::StringConversion::GetDataSizeFromString(option);
        } catch (const std::exception& e) {
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
      } catch (const std::exception& e) {
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
      } catch (const std::exception& e) {
        return false;
      }

      cache->set_op(eos::console::NsProto_CacheProto::DROP_SINGLE_CONTAINER);
      cache->set_single_to_drop(target);
    } else {
      return false;
    }
  } else if (cmd == "max_drain_threads") {
    using eos::console::NsProto_DrainSizeProto;
    NsProto_DrainSizeProto* drain_sz = ns->mutable_drain();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;
      uint64_t max_num_threads {0ull};

      try {
        max_num_threads = std::stoull(soption);

        if (max_num_threads < 4) {
          max_num_threads = 4;
        }
      } catch (const std::exception& e) {
        return false;
      }

      drain_sz->set_max_num(max_num_threads);
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

    // ---
    if (!(option = tokenizer.GetToken())) {
      return false;
    }

    int64_t containerID = 0;

    if (!eos::common::ParseInt64(option, containerID) || containerID < 0) {
      return false;
    }

    reserve->set_fileid(fileID);
    reserve->set_containerid(containerID);
  } else if (cmd == "") {
    eos::console::NsProto_StatProto* stat = ns->mutable_stat();
    stat->set_summary(true);
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Ns command entrypoint
//------------------------------------------------------------------------------
int com_ns(char* arg)
{
  if (wants_help(arg)) {
    com_ns_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  NsHelper ns(gGlobalOpts);

  if (!ns.ParseCommand(arg)) {
    com_ns_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = ns.Execute();

  if (global_retc) {
    std::cerr << ns.GetError();
  }

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_ns_help()
{
  std::ostringstream oss;
  oss << "Usage: ns [stat|mutex|compact|master|cache]" << std::endl
      << "    print or configure basic namespace parameters" << std::endl
      << "  ns stat [-a] [-m] [-n] [--reset]" << std::endl
      << "    print namespace statistics" << std::endl
      << "    -a      : break down by uid/gid" << std::endl
      << "    -m      : display in monitoring format <key>=<value>" << std::endl
      << "    -n      : display numerical uid/gid(s)" << std::endl
      << "    --reset : reset namespace counters" << std::endl
      << std::endl
      << "  ns mutex [<option>]" << std::endl
      << "    manage mutex monitoring. Option can be:" << std::endl
      << "    --toggletime     : toggle the timing" << std::endl
      << "    --toggleorder    : toggle the order" << std::endl
      << "    --toggledeadlock : toggle deadlock check" << std::endl
      << "    --smplrate1      : set timing sample rate at 1% (default, no slow-down)"
      << std::endl
      << "    --smplrate10     : set timing sample rate at 10% (medium slow-down)"
      << std::endl
      << "    --smplrate100    : set timing sample rate at 100% (severe slow-down)"
      << std::endl
      << "    --setblockedtime <ms>" << std::endl
      << "                     : set minimum time when a mutex lock lasting longer than <ms> is reported in the log file [default=10000" << std::endl
      << std::endl
      << "  ns compact off|on <delay> [<interval>] [<type>]" << std::endl
      << "    enable online compaction after <delay> seconds" << std::endl
      << "    <interval> : if >0 then compaction is repeated automatically " <<
      std::endl
      << "                 after so many seconds" << std::endl
      << "    <type>     : can be 'files', 'directories' or 'all'. By default  only the file"
      << std::endl
      << "                 changelog is compacted. The repair flag can be indicated by using"
      << std::endl
      << "                 'files-repair', 'directories-repair' or 'all-repair'. "
      << std::endl
      << std::endl
      << "  ns master [<option>]" << std::endl
      << "    master/slave operations. Option can be:" << std::endl
      << "    <master_hostname> : set hostname of MGM master RW daemon" << std::endl
      << "    --log             : show master log" << std::endl
      << "    --log-clear       : clean master log" << std::endl
      << "    --enable          : enable the slave/master supervisor thread modifying stall/"
      << std::endl
      << "                        redirectorion rules" << std::endl
      << "    --disable         : disable supervisor thread"
      << std::endl
      << std::endl
      << "  ns recompute_tree_size <path>|cid:<decimal_id>|cxid:<hex_id> [--depth <val>]"
      << std::endl
      << "    recompute the tree size of a directory and all its subdirectories"
      << std::endl
      << "    --depth : maximum depth for recomputation, default 0 i.e no limit"
      << std::endl
      << std::endl
      << "  ns recompute_quotanode <path>|cid:<decimal_id>|cxid:<hex_id>"
      << std::endl
      << "    recompute the specified quotanode"
      << std::endl
      << std::endl
      << "  ns cache set|drop [-d|-f] [<max_num>] [<max_size>K|M|G...]" << std::endl
      << "    set the max number of entries or the max size of the cache. Use the" <<
      std::endl
      << "    ns stat command to see the current values." << std::endl
      << "    set        : update cache size for files or directories" << std::endl
      << "    drop       : drop cached file and/or directory entries"
      << std::endl
      << "    -d         : control the directory cache" << std::endl
      << "    -f         : control the file cache" << std::endl
      << "    <max_num>  : max number of entries" << std::endl
      << "    <max_size> : max size of the cache - not implemented yet"
      << std::endl
      << std::endl
      << "  ns cache drop-single-file <id of file to drop>" << std::endl
      << "    force refresh of the given FileMD by dropping it from the cache"
      << std::endl
      << std::endl
      << "  ns cache drop-single-container <id of container to drop>" << std::endl
      << "    force refresh of the given ContainerMD by dropping it from the cache"
      << std::endl
      << std::endl
      << "  ns max_drain_threads <num>" << std::endl
      << "    set the max number of threads in the drain pool, default 400, minimum 4"
      << std::endl
      << std::endl
      << "  ns reserve-ids <file id> <container id>" << std::endl
      << "    blacklist file and container IDs below the given threshold. The namespace"
      << std::endl
      << "    will not allocate any file or container with IDs less than, or equal to the"
      << std::endl
      << "    given blacklist thresholds." << std::endl
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
