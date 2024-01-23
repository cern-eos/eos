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
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "common/Timing.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/IntervalStopwatch.hh"
#include "common/SymKeys.hh"
#include "XrdVersion.hh"

XrdVERSIONINFOREF(XrdgetProtocol);

EOSFSTNAMESPACE_BEGIN

constexpr std::chrono::minutes Storage::sConsistencyTimeout;


//------------------------------------------------------------------------------
// Open random temporary file in /tmp/
//
// @return string temporary file path, if open failed return empty string
//------------------------------------------------------------------------------
std::string MakeTemporaryFile()
{
  char tmp_name[] = "/tmp/fst.publish.XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    eos_static_crit("%s", "msg=\"failed to create temporary file!\"");
    return "";
  }

  (void) close(tmp_fd);
  return tmp_name;
}

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
// Retrieve uptime
//------------------------------------------------------------------------------
static std::string GetUptime(const std::string& tmpname)
{
  eos::common::ShellCmd cmd(SSTR("uptime | tr -d \"\n\"| tr -s \" \" > " <<
                                 tmpname));
  eos::common::cmd_status rc = cmd.wait(5);

  if (rc.exit_code) {
    eos_static_err("%s", "msg=\"retrieve uptime call failed\"");
    return "N/A";
  }

  std::string retval;
  eos::common::StringConversion::LoadFileIntoString(tmpname.c_str(), retval);
  return retval;
}


//------------------------------------------------------------------------------
// Get uptime information in a more pretty format
//------------------------------------------------------------------------------
static
std::map<std::string, std::string>
GetUptimePrettyFormat(const std::string tmpname)
{
  using eos::common::StringTokenizer;
  std::map<std::string, std::string> map_info;
  {
    // Get uptime information in seconds
    eos::common::ShellCmd cmd(SSTR("cat /proc/uptime | sed 's/\\..*//' > " <<
                                   tmpname));
    eos::common::cmd_status rc = cmd.wait(5);

    if (rc.exit_code) {
      eos_static_err("%s", "msg=\"failed to retrieve uptime\"");
      return map_info;
    }

    std::string uptime_sec;
    eos::common::StringConversion::LoadFileIntoString(tmpname.c_str(), uptime_sec);
    eos::common::trim(uptime_sec);
    map_info["sys.stat.uptime_sec"] = uptime_sec;
  }
  {
    // Get load information
    eos::common::ShellCmd cmd(SSTR("cat /proc/loadavg > " << tmpname));
    eos::common::cmd_status rc = cmd.wait(2);

    if (rc.exit_code) {
      eos_static_err("%s", "msg=\"failed to retrieve loadavg\"");
      return map_info;
    }

    std::string load_info;
    eos::common::StringConversion::LoadFileIntoString(tmpname.c_str(), load_info);
    std::list<std::string_view> load_tokens =
      StringTokenizer::split<std::list<std::string_view>>(load_info, ' ');

    if (load_tokens.size() >= 3) {
      map_info["sys.stat.load_avg_1m"] = load_tokens.front();
      load_tokens.pop_front();
      map_info["sys.stat.load_avg_5m"] = load_tokens.front();
      load_tokens.pop_front();
      map_info["sys.stat.load_avg_15m"] = load_tokens.front();
      load_tokens.pop_front();
    }
  }
  return map_info;
}

//------------------------------------------------------------------------------
// Retrieve xrootd version
//------------------------------------------------------------------------------
static std::string GetXrootdVersion()
{
  static std::string s_xrootd_version = "";

  if (s_xrootd_version.empty()) {
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
      int nbytes = fread((void*)data, max_read, sizeof(char), fnetcounters);

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
    int nbytes = fread((void*)data, max_read, sizeof(char), fnetspeed);

    if (nbytes > 1) {
      data[nbytes - 1] = '\0';

      try {
        const std::string sval = ptr;
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
static std::string GetNumOfTcpSockets(const std::string& tmpname)
{
  std::string command = SSTR("cat /proc/net/tcp | wc -l | tr -d \"\n\" > " <<
                             tmpname);
  eos::common::ShellCmd cmd(command.c_str());
  eos::common::cmd_status rc = cmd.wait(5);

  if (rc.exit_code) {
    eos_static_err("%s", "msg=\"retrieve #socket call failed\"");
  }

  std::string retval;
  eos::common::StringConversion::LoadFileIntoString(tmpname.c_str(), retval);
  return retval;
}



//------------------------------------------------------------------------------
// Get size of subtree by using the system "du -sb" command
//------------------------------------------------------------------------------
static std::string GetSubtreeSize(const std::string& path)
{
  const std::string tmp_name = MakeTemporaryFile();
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
// Overwrite statfs statistics for testing environment
//------------------------------------------------------------------------------
static void OverwriteTestingStatfs(const std::string& path,
                                   std::map<std::string, std::string>& output)
{
  const char* ptr = getenv("EOS_FST_TESTING");

  if (ptr == nullptr) {
    return;
  }

  eos_static_info("msg=\"overwrite statfs values\" path=%s", path.c_str());
  uint64_t subtree_max_size = 10ull * 1024 * 1024 * 1024; // 10GB
  ptr = getenv("EOS_FST_SUBTREE_MAX_SIZE");

  if (ptr) {
    if (!eos::common::StringToNumeric(std::string(ptr), subtree_max_size,
                                      subtree_max_size)) {
      eos_static_err("msg=\"failed convertion\" data=\"%s\"", ptr);
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
  output["stat.sys.uptime"] = GetUptime(tmpfile);
  auto uptime_info = GetUptimePrettyFormat(tmpfile);
  output.insert(uptime_info.begin(), uptime_info.end());
  // active TCP sockets
  output["stat.sys.sockets"] = GetNumOfTcpSockets(tmpfile);
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
  std::string tmp_name = MakeTemporaryFile();

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
