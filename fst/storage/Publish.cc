// ----------------------------------------------------------------------
// File: Publish.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Config.hh"
#include "fst/storage/FileSystem.hh"
#include "qclient/Formatting.hh"
#include "common/Utils.hh"
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "common/Timing.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/IntervalStopwatch.hh"
#include "common/SymKeys.hh"
#include <XrdVersion.hh>
#include <optional>
#include <sys/sysinfo.h>

#ifdef PROCPS3
  #include <proc/readproc.h>
#else
  #include <libproc2/pids.h>
#endif

XrdVERSIONINFOREF(XrdgetProtocol);

EOSFSTNAMESPACE_BEGIN

constexpr std::chrono::minutes Storage::sConsistencyTimeout;

//------------------------------------------------------------------------------
// Serialize hot files vector into std::string
// Return " " if given an empty vector, instead of "".
//
// This is to keep the entry in the hash, even if no opened files exist.
//------------------------------------------------------------------------------
static std::string HotFilesToString(
  const std::vector<eos::fst::OpenFileTracker::HotEntry>& entries)
{
  if (entries.size() == 0u) {
    return " ";
  }

  std::ostringstream ss;

  for (size_t i = 0; i < entries.size(); i++) {
    ss << entries[i].uses;
    ss << ":";
    ss << eos::common::FileId::Fid2Hex(entries[i].fid);
    ss << " ";
  }

  return ss.str();
}


//------------------------------------------------------------------------------
// Get uptime information in a more pretty format
//------------------------------------------------------------------------------
static void
GetUptime(std::map<std::string, std::string>& output)
{
  static float f_load = 1.f / (1 << SI_LOAD_SHIFT);
  static auto GetUptimePretty = [](const struct sysinfo & input) -> std::string {
    std::stringstream oss;
    std::time_t now_t = eos::common::Timing::GetNowInSec();
    struct tm now_tm_parts;
    (void) localtime_r(&now_t, &now_tm_parts);
    char str[9];

    if (strftime(str, 9, "%H:%M:%S", &now_tm_parts))
    {
      oss << str << " up ";
      int t_mins = (input.uptime % 3600) / 60;
      int t_hours = (input.uptime % 86400) / 3600;
      int t_days = input.uptime / 86400;

      if (t_days) {
        oss << t_days << " days, ";
      }

      oss << t_hours << ":" << t_mins << ", load average: ";
      oss.setf(std::ios::fixed);
      oss.precision(2);
      oss << input.loads[0] * f_load << ", "
          << input.loads[1] * f_load << ", "
          << input.loads[2] * f_load;
      return oss.str();
    }

    return "N/A";
  };
  struct sysinfo info;

  if (sysinfo(&info) == 0) {
    try {
      output["stat.sys.uptime_sec"] = std::to_string(info.uptime);
      output["stat.sys.load_avg_1m"] = std::to_string(info.loads[0] * f_load);
      output["stat.sys.load_avg_5m"] = std::to_string(info.loads[1] * f_load);
      output["stat.sys.load_avg_15m"] = std::to_string(info.loads[2] * f_load);
      output["stat.sys.uptime"] = GetUptimePretty(info);
      return;
    } catch (...) {
      // any error will populate the data with N/A entries
    }
  }

  output["stat.sys.uptime_sec"] = "N/A";
  output["stat.sys.load_avg_1m"] = "N/A";
  output["stat.sys.load_avg_5m"] = "N/A";
  output["stat.sys.load_avg_15m"] = "N/A";
  output["stat.sys.uptime"] = "N/A";
}

//------------------------------------------------------------------------------
// Retrieve xrootd version
//------------------------------------------------------------------------------
static std::string GetXrootdVersion()
{
  static std::string s_xrootd_version = "";

  if (!s_xrootd_version.empty()) {
    return s_xrootd_version;
  }

  XrdOucString v = XrdVERSIONINFOVAR(XrdgetProtocol).vStr;
  int pos = v.find(" ");

  if (pos != STR_NPOS) {
    v.erasefromstart(pos + 1);
  }

  s_xrootd_version = v.c_str();
  return s_xrootd_version;
}

//------------------------------------------------------------------------------
// Retrieve eos version
//------------------------------------------------------------------------------
static std::string GetEosVersion()
{
  static std::string s_eos_version = SSTR(VERSION << "-" << RELEASE).c_str();
  return s_eos_version;
}

//------------------------------------------------------------------------------
// Retrieve FST network interface
//------------------------------------------------------------------------------
static std::string GetNetworkInterface()
{
  static std::string s_net_interface = "";

  if (!s_net_interface.empty()) {
    return s_net_interface;
  }

  const char* ptr = getenv("EOS_FST_NETWORK_INTERFACE");

  if (ptr) {
    // Use value set in the environment
    s_net_interface = ptr;
  } else {
    // Use blindly the default
    s_net_interface = "eth0";
  }

  return s_net_interface;
}

//------------------------------------------------------------------------------
// Get network transfer RX/TX errors and dropped packet counters
//------------------------------------------------------------------------------
static void
GetNetworkCounters(std::map<std::string, std::string>& output)
{
  static const std::set<std::string> set_keys {"rx_errors", "rx_dropped",
    "tx_errors", "tx_dropped"};
  static std::map<std::string, std::string> map_key_paths;

  // Build set of files to query for the above counters depending on the
  // network interface name
  if (map_key_paths.empty()) {
    struct stat info;

    for (const auto& key : set_keys) {
      std::string fn_path = SSTR("/sys/class/net/" << GetNetworkInterface()
                                 << "/statistics/" << key);

      if (::stat(fn_path.c_str(), &info)) {
        map_key_paths[key] = "";
      } else {
        map_key_paths[key] = fn_path;
      }
    }
  }

  static const int max_read = 32;
  static char data[32] = "";
  std::map<std::string, std::string> map_counters;

  for (const auto& pair : map_key_paths) {
    map_counters[pair.first] = "N/A";
    FILE* fnetcounters = fopen(pair.second.c_str(), "r");

    if (fnetcounters) {
      data[0] = '\0';
      int nbytes = fread((void*)data, sizeof(char), max_read, fnetcounters);

      if (nbytes > 1) {
        data[nbytes - 1] = '\0';
        map_counters[pair.first] = data;
      }

      (void) fclose(fnetcounters);
    }
  }

  for (const auto& pair : map_counters) {
    const std::string key = "stat.net." + pair.first;
    output[key] = pair.second;
  }
}

//------------------------------------------------------------------------------
// Retrieve network interface speed as bytes/second
//------------------------------------------------------------------------------
static uint64_t GetNetSpeed()
{
  static uint64_t s_net_speed = 0ull;

  if (s_net_speed) {
    return s_net_speed;
  }

  const char* ptr = getenv("EOS_FST_NETWORK_SPEED");

  if (ptr) {
    const std::string sval = ptr;

    try {
      s_net_speed = std::stoull(sval);

      if (s_net_speed) {
        return s_net_speed;
      }
    } catch (...) {
      eos_static_err("msg=\"EOS_FST_NETWORK_SPEED not a numeric value\" "
                     "val=\"%s\"", sval.c_str());
    }
  }

  // Default value set to 1Gb/s
  s_net_speed = 1000000000;
  // Read network speed from the sys interface
  const std::string net_interface = GetNetworkInterface();
  const std::string fn_path = SSTR("/sys/class/net/" << net_interface
                                   << "/speed");
  FILE* fnetspeed = fopen(fn_path.c_str(), "r");

  if (fnetspeed) {
    const int max_read = 32;
    char data[32] = "";
    int nbytes = fread((void*)data, sizeof(char), max_read, fnetspeed);

    if (nbytes > 1) {
      data[nbytes] = '\0';

      try {
        const std::string sval = data;
        s_net_speed = std::stoull(sval);
        // We get Mb/s as number, convert to bytes/s
        s_net_speed *= 1000000;
      } catch (...) {
        eos_static_err("msg=\"network speed not a numeric value\" fn=\"%s\"",
                       fn_path.c_str());
      }
    }

    (void) fclose(fnetspeed);
  }

  eos_static_info("msg=\"network speed\" interface=\"%s\" speed=%.02f GB/s",
                  net_interface.c_str(), 1.0 * s_net_speed / 1000000000.0);
  return s_net_speed;
}

//------------------------------------------------------------------------------
// Retrieve number of TCP sockets in the system
//------------------------------------------------------------------------------
static std::string GetNumOfTcpSockets()
{
  static auto ReadTpcSocketsInUse =
  [](const std::string & fn, const std::string & search_tag) -> uint64_t {
    uint64_t num_sockets = 0ull;
    std::ifstream file(fn.c_str());

    if (file.is_open())
    {
      std::string line;

      while (std::getline(file, line)) {
        if (line.find(search_tag) == 0) {
          line.erase(0, search_tag.length());

          try {
            num_sockets = std::stoull(line.substr(0, line.find(' ')));
          } catch (...) {
            // if any error we report 0
          }

          break;
        }
      }
    }

    return num_sockets;
  };
  static const std::string tcp4_fn = "/proc/net/sockstat";
  static const std::string tcp6_fn = "/proc/net/sockstat6";
  static const std::string tcp4_tag = "TCP: inuse ";
  static const std::string tcp6_tag = "TCP6: inuse ";
  uint64_t num_tcp4_sockets = ReadTpcSocketsInUse(tcp4_fn, tcp4_tag);
  uint64_t num_tcp6_sockets = ReadTpcSocketsInUse(tcp6_fn, tcp6_tag);
  return std::to_string(num_tcp4_sockets + num_tcp6_sockets);
}

//------------------------------------------------------------------------------
// Get size of subtree by using the system "du -sb" command
//------------------------------------------------------------------------------
static std::string GetSubtreeSize(const std::string& path)
{
  std::string fn_pattern = "/tmp/fst.subtree.XXXXXX";
  const std::string tmp_name = eos::common::MakeTemporaryFile(fn_pattern);
  const std::string command = SSTR("du -sb " << path << " | cut -f1 > "
                                   << tmp_name);
  eos::common::ShellCmd cmd(command.c_str());
  eos::common::cmd_status rc = cmd.wait(5);

  if (rc.exit_code) {
    eos_static_err("msg=\"failed to compute subtree size\" path=%s",
                   path.c_str());
  }

  std::string retval;
  eos::common::StringConversion::LoadFileIntoString(tmp_name.c_str(), retval);
  (void) unlink(tmp_name.c_str());
  return retval;
}

//------------------------------------------------------------------------------
// Get number of kworker processes on the machine - a high number might indicate
// a problem with the machine and might require a reboot.
//------------------------------------------------------------------------------
static uint32_t GetNumOfKworkerProcs()
{
  uint32_t count = 0u;

#ifdef PROCPS3
  if (proc_t** procs = readproctab(PROC_FILLSTAT)) {
    for (int i = 0; procs[i]; ++i) {
      if (procs[i]->cmd) {
        eos_static_debug("msg=\"process cmd line\" cmd=\"%s\"", procs[i]->cmd);

        if (strstr(procs[i]->cmd, "kworker") == procs[i]->cmd) {
          ++count;
        }
      }

      freeproc(procs[i]);
    }

    free(procs);
  }
#else
  struct pids_info *info = nullptr;
  enum pids_item items[] = { PIDS_CMD };

  procps_pids_new(&info, items, 1);

  while (struct pids_stack *stack = procps_pids_get(info, PIDS_FETCH_TASKS_ONLY)) {
    char *cmd = PIDS_VAL(0, str, stack, info);
    if (strstr(cmd, "kworker") == cmd)
      ++count;
  }

  procps_pids_unref(&info);
#endif

  eos_static_debug("msg=\"current number of kworker processes\" count=%i", count);
  return count;
}

//------------------------------------------------------------------------------
// Overwrite statfs statistics for testing environment
//------------------------------------------------------------------------------
static void OverwriteTestingStatfs(const std::string& path,
                                   std::map<std::string, std::string>& output)
{
  static std::optional<bool> do_overwrite;

  if (!do_overwrite.has_value()) {
    const char* ptr = getenv("EOS_FST_TESTING");

    if (ptr == nullptr) {
      do_overwrite.emplace(false);
    } else {
      do_overwrite.emplace(true);
    }
  }

  if (!do_overwrite.value()) {
    return;
  }

  eos_static_info("msg=\"overwrite statfs values\" path=%s", path.c_str());
  static uint64_t subtree_max_size = 0ull;

  if (subtree_max_size == 0ull) {
    subtree_max_size = 10ull * 1024 * 1024 * 1024; // 10GB
    const char* ptr = getenv("EOS_FST_SUBTREE_MAX_SIZE");

    if (ptr) {
      if (!eos::common::StringToNumeric(std::string(ptr), subtree_max_size,
                                        subtree_max_size)) {
        eos_static_err("msg=\"failed convertion\" data=\"%s\"", ptr);
      }
    }
  }

  uint64_t bsize = 4096;
  (void) eos::common::StringToNumeric(output["stat.statfs.bsize"], bsize, bsize);
  uint64_t used_bytes {0ull};
  const std::string sused_bytes = GetSubtreeSize(path);
  (void) eos::common::StringToNumeric(sused_bytes, used_bytes);
  double filled = 100.0 - ((double) 100.0 * (subtree_max_size - used_bytes) /
                           subtree_max_size);
  output["stat.statfs.filled"] = std::to_string(filled);
  output["stat.statfs.usedbytes"] = std::to_string(used_bytes);
  output["stat.statfs.freebytes"] = std::to_string(subtree_max_size - used_bytes);
  output["stat.statfs.capacity"] = std::to_string(subtree_max_size);
}

//------------------------------------------------------------------------------
// Get statistics about this FST, used for publishing
//------------------------------------------------------------------------------
std::map<std::string, std::string>
Storage::GetFstStatistics(const std::string& tmpfile,
                          unsigned long long netspeed)
{
  eos::common::LinuxStat::linux_stat_t osstat;

  if (!eos::common::LinuxStat::GetStat(osstat)) {
    eos_crit("failed to get the memory usage information");
  }

  std::map<std::string, std::string> output;
  // Kernel version
  output["stat.sys.kernel"] = gConfig.KernelVersion.c_str();
  // Virtual memory size
  output["stat.sys.vsize"] = SSTR(osstat.vsize);
  // rss usage
  output["stat.sys.rss"] = SSTR(osstat.rss);
  // number of active threads on this machine
  output["stat.sys.threads"] = SSTR(osstat.threads);
  // eos version
  output["stat.sys.eos.version"] = GetEosVersion();
  // xrootd version
  output["stat.sys.xrootd.version"] = GetXrootdVersion();
  // adler32 of keytab
  output["stat.sys.keytab"] = gConfig.KeyTabAdler.c_str();
  // machine uptime
  GetUptime(output);
  // active TCP sockets
  output["stat.sys.sockets"] = GetNumOfTcpSockets();
  // number of kworker processes
  output["stat.sys.kworkers"] = std::to_string(GetNumOfKworkerProcs());
  // Collect network RX/TX errors and dropped packets
  GetNetworkCounters(output);
  // startup time of the FST daemon
  output["stat.sys.eos.start"] = gConfig.StartDate.c_str();
  // FST geotag
  output["stat.geotag"] = gOFS.GetGeoTag();
  // http port
  output["http.port"] = SSTR(gOFS.mHttpdPort);
  // debug level
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  output["debug.state"] = eos::common::StringConversion::ToLower
                          (g_logging.GetPriorityString
                           (g_logging.gPriorityLevel)).c_str();
  // net info
  output["stat.net.ethratemib"] = SSTR(netspeed / (8 * 1024 * 1024));
  output["stat.net.inratemib"] = SSTR(
                                   mFstLoad.GetNetRate(GetNetworkInterface().c_str(),
                                       "rxbytes") / 1024.0 / 1024.0);
  output["stat.net.outratemib"] = SSTR(
                                    mFstLoad.GetNetRate(GetNetworkInterface().c_str(),
                                        "txbytes") / 1024.0 / 1024.0);
  // publish timestamp
  output["stat.publishtimestamp"] = SSTR(
                                      eos::common::getEpochInMilliseconds().count());
  return output;
}

//------------------------------------------------------------------------------
// Insert statfs info into the map
//------------------------------------------------------------------------------
static void InsertStatfs(struct statfs* statfs,
                         std::map<std::string, std::string>& output)
{
  output["stat.statfs.type"] = std::to_string(statfs->f_type);
  output["stat.statfs.bsize"] = std::to_string(statfs->f_bsize);
  output["stat.statfs.blocks"] = std::to_string(statfs->f_blocks);
  output["stat.statfs.bfree"] = std::to_string(statfs->f_bfree);
  output["stat.statfs.bavail"] = std::to_string(statfs->f_bavail);
  output["stat.statfs.files"] = std::to_string(statfs->f_files);
  output["stat.statfs.ffree"] = std::to_string(statfs->f_ffree);
#ifdef __APPLE__
  output["stat.statfs.namelen"] = std::to_string(MNAMELEN);
#else
  output["stat.statfs.namelen"] = std::to_string(statfs->f_namelen);
#endif
  output["stat.statfs.freebytes"] = std::to_string(statfs->f_bfree *
                                    statfs->f_bsize);
  output["stat.statfs.usedbytes"] = std::to_string((statfs->f_blocks -
                                    statfs->f_bfree) * statfs->f_bsize);
  output["stat.statfs.filled"] = std::to_string(
                                   (double) 100.0 * ((double)(statfs->f_blocks - statfs->f_bfree) / (double)(
                                         1 + statfs->f_blocks)));
  output["stat.statfs.capacity"] = std::to_string(statfs->f_blocks *
                                   statfs->f_bsize);
  output["stat.statfs.fused"] = std::to_string(statfs->f_files - statfs->f_ffree);
}

//------------------------------------------------------------------------------
// Get statistics about this FileSystem, used for publishing
//------------------------------------------------------------------------------
std::map<std::string, std::string>
Storage::GetFsStatistics(FileSystem* fs)
{
  if (!fs) {
    eos_static_crit("asked to publish statistics for a null filesystem");
    return {};
  }

  eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();

  if (!fsid) {
    // during the boot phase we can find a filesystem without ID
    eos_static_warning("asked to publish statistics for filesystem with fsid=0");
    return {};
  }

  std::map<std::string, std::string> output;
  // Publish statfs
  std::unique_ptr<eos::common::Statfs> statfs = fs->GetStatfs();

  if (statfs) {
    InsertStatfs(statfs->GetStatfs(), output);
    OverwriteTestingStatfs(fs->GetPath(), output);
  }

  // Publish stat.disk.*
  double readratemb;
  double writeratemb;
  double diskload;
  std::map<std::string, std::string> iostats;

  if (fs->GetFileIOStats(iostats)) {
    readratemb = strtod(iostats["read-mb-second"].c_str(), 0);
    writeratemb = strtod(iostats["write-mb-second"].c_str(), 0);
    diskload = strtod(iostats["load"].c_str(), 0);
  } else {
    readratemb = mFstLoad.GetDiskRate(fs->GetPath().c_str(),
                                      "readSectors") * 512.0 / 1000000.0;
    writeratemb = mFstLoad.GetDiskRate(fs->GetPath().c_str(),
                                       "writeSectors") * 512.0 / 1000000.0;
    diskload = mFstLoad.GetDiskRate(fs->GetPath().c_str(),
                                    "millisIO") / 1000.0;
  }

  output["stat.disk.readratemb"] = std::to_string(readratemb);
  output["stat.disk.writeratemb"] = std::to_string(writeratemb);
  output["stat.disk.load"] = std::to_string(diskload);
  // Publish stat.health.*
  std::map<std::string, std::string> health;

  if (!fs->GetHealthInfo(health)) {
    health = mFstHealth.getDiskHealth(fs->GetPath());
  }

  output["stat.health"] = (health.count("summary") ? health["summary"].c_str() :
                           "N/A");
  // set some reasonable defaults if information is not available
  output["stat.health.indicator"] = (health.count("indicator") ?
                                     health["indicator"] : "N/A");
  output["stat.health.drives_total"] = (health.count("drives_total") ?
                                        health["drives_total"] : "1");
  output["stat.health.drives_failed"] = (health.count("drives_failed") ?
                                         health["drives_failed"] : "0");
  output["stat.health.redundancy_factor"] = (health.count("redundancy_factor") ?
      health["redundancy_factor"] : "1");
  {
    // don't publish smart info too often, it is few kb per filesystem!
    time_t now = time(NULL);
    static std::map<FileSystem*, time_t> smartPublishing;
    static XrdSysMutex smartPublishingMutex;
    bool publish = false;
    {
      XrdSysMutexHelper scope_lock(smartPublishingMutex);

      if (!smartPublishing[fs] ||
          (smartPublishing[fs] < now)) {
        smartPublishing[fs] = now + 3600;
        publish = true;
      }
    }

    if (publish) {
      // compress the json smart info
      eos::common::SymKey::ZBase64(health["attributes"],
                                   output["stat.health.z64smart"]);
    }
  }
  // Publish generic statistics, related to free space and current load
  long long r_open = (long long) gOFS.openedForReading.getOpenOnFilesystem(fsid);
  long long w_open = (long long) gOFS.openedForWriting.getOpenOnFilesystem(fsid);
  output["stat.ropen"] = std::to_string(r_open);
  output["stat.wopen"] = std::to_string(w_open);

  if (auto kv = output.find("stat.statfs.fused");
      kv != output.end()) {
    // FIXME: Actually subtract the statfs of the .eosorphans, also count
    // checksums & scrub files!
    output["stat.usedfiles"] = kv->second;
  }

  output["stat.boot"] = fs->GetStatusAsString(fs->GetStatus());
  output["stat.geotag"] = gOFS.GetGeoTag();
  output["stat.publishtimestamp"] = std::to_string(
                                      eos::common::getEpochInMilliseconds().count());
  output["stat.disk.iops"] = std::to_string(fs->getIOPS());
  output["stat.disk.bw"] = std::to_string(fs->getSeqBandwidth()); // in MB
  output["stat.http.port"] = std::to_string(gOFS.mHttpdPort);

  // FST alias
  if (gConfig.HostAlias.length()) {
    output["stat.alias.host"] = gConfig.HostAlias.c_str();
  }

  // FST port alias
  if (gConfig.PortAlias.length()) {
    output["stat.alias.port"] = gConfig.PortAlias.c_str();
  }

  // debug level
  output["stat.ropen.hotfiles"] = HotFilesToString(
                                    gOFS.openedForReading.getHotFiles(fsid, 10));
  output["stat.wopen.hotfiles"] = HotFilesToString(
                                    gOFS.openedForWriting.getHotFiles(fsid, 10));
  return output;
}

//------------------------------------------------------------------------------
// Publish statistics about the given filesystem
//------------------------------------------------------------------------------
bool Storage::PublishFsStatistics(FileSystem* fs)
{
  if (!fs) {
    eos_static_crit("%s", "msg=\"asked to publish statistics for a null fs\"");
    return false;
  }

  eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();

  if (!fsid) {
    // during the boot phase we can find a filesystem without ID
    eos_static_warning("%s", "msg=\"asked to publish statistics for fsid=0\"");
    return false;
  }

  common::FileSystemUpdateBatch batch;
  std::map<std::string, std::string> fsStats = GetFsStatistics(fs);

  for (auto it = fsStats.begin(); it != fsStats.end(); it++) {
    batch.setStringTransient(it->first, it->second);
  }

  CheckFilesystemFullness(fs, fsid);
  return fs->applyBatch(batch);
}

//------------------------------------------------------------------------------
// Publish
//------------------------------------------------------------------------------
void
Storage::Publish(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"publisher activated\"");
  std::string fn_pattern = "/tmp/fst.publish.XXXXXX";
  const std::string tmp_name = eos::common::MakeTemporaryFile(fn_pattern);

  if (tmp_name.empty()) {
    return;
  }

  // The following line acts as a barrier that prevents progress
  // until the config queue becomes known
  gConfig.getFstNodeConfigQueue("Publish");

  while (!assistant.terminationRequested()) {
    std::chrono::milliseconds randomizedReportInterval =
      gConfig.getRandomizedPublishInterval();
    common::IntervalStopwatch stopwatch(randomizedReportInterval);
    {
      // Publish with a MuxTransaction all file system changes
      eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);

      if (!gOFS.ObjectManager.OpenMuxTransaction()) {
        eos_static_err("%s", "msg=\"cannot open mux transaction\"");
      } else {
        std::map<eos::fst::FileSystem*, std::future<bool>> map_futures;

        // Copy out statfs info in parallel to speed-up things
        for (const auto& elem : mFsMap) {
          auto fs = elem.second;

          if (!fs) {
            continue;
          }

          try {
            map_futures.emplace(fs, std::async(std::launch::async,
                                               &Storage::PublishFsStatistics,
                                               this, fs));
          } catch (const std::system_error& e) {
            eos_static_err("msg=\"exception while collecting fs statistics\" "
                           "fsid=%lu msg=\"%s\"", elem.first, e.what());
          }
        }

        for (auto& elem : map_futures) {
          if (elem.second.get() == false) {
            eos_static_err("msg=\"failed to publish fs stats\" fspath=%s",
                           elem.first->GetPath().c_str());
          }
        }

        auto fstStats = GetFstStatistics(tmp_name, GetNetSpeed());
        // Set node status values
        common::SharedHashLocator locator = gConfig.getNodeHashLocator("Publish");

        if (!locator.empty()) {
          mq::SharedHashWrapper::Batch batch;

          for (auto it = fstStats.begin(); it != fstStats.end(); it++) {
            batch.SetTransient(it->first, it->second);
          }

          mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), locator, true, false);
          hash.set(batch);
        }

        gOFS.ObjectManager.CloseMuxTransaction();
      }
    }
    std::chrono::milliseconds sleepTime = stopwatch.timeRemainingInCycle();

    if (sleepTime == std::chrono::milliseconds(0)) {
      eos_static_warning("msg=\"publisher cycle exceeded %d millisec - took %d "
                         "millisec", randomizedReportInterval.count(),
                         stopwatch.timeIntoCycle());
    } else {
      assistant.wait_for(sleepTime);
    }
  }

  (void) unlink(tmp_name.c_str());
}

EOSFSTNAMESPACE_END
