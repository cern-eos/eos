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
#include "fst/txqueue/TransferQueue.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "qclient/Formatting.hh"
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "common/Timing.hh"
#include "common/IntervalStopwatch.hh"
#include "XrdVersion.hh"

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
// Retrieve net speed
//------------------------------------------------------------------------------
static uint64_t GetNetSpeed(const std::string& tmpname)
{
  if (getenv("EOS_FST_NETWORK_SPEED")) {
    return strtoull(getenv("EOS_FST_NETWORK_SPEED"), nullptr, 10);
  }

  std::string getNetspeedCommand = SSTR(
                                     "ip route list | sed -ne '/^default/s/.*dev //p' | cut -d ' ' -f1 |"
                                     " xargs -i ethtool {} 2>&1 | grep Speed | cut -d ' ' -f2 | cut -d 'M' -f1 > "
                                     << tmpname);
  eos::common::ShellCmd scmd1(getNetspeedCommand.c_str());
  eos::common::cmd_status rc = scmd1.wait(5);
  unsigned long long netspeed = 1000000000;

  if (rc.exit_code) {
    eos_static_err("%s", "msg=\"ip route list call failed to get netspeed\"");
    return netspeed;
  }

  FILE* fnetspeed = fopen(tmpname.c_str(), "r");

  if (fnetspeed) {
    if ((fscanf(fnetspeed, "%llu", &netspeed)) == 1) {
      // we get MB as a number => convert into bytes
      netspeed *= 1000000;
      eos_static_info("ethtool:networkspeed=%.02f GB/s",
                      1.0 * netspeed / 1000000000.0);
    }

    fclose(fnetspeed);
  }

  return netspeed;
}

//------------------------------------------------------------------------------
// Retrieve uptime
//------------------------------------------------------------------------------
static std::string GetUptime(const std::string& tmpname)
{
  eos::common::ShellCmd cmd(SSTR("uptime | tr -d \"\n\" > " << tmpname));
  eos::common::cmd_status rc = cmd.wait(5);

  if (rc.exit_code) {
    eos_static_err("retrieve uptime call failed");
    return "N/A";
  }

  std::string retval;
  eos::common::StringConversion::LoadFileIntoString(tmpname.c_str(), retval);
  return retval;
}

//------------------------------------------------------------------------------
// Retrieve xrootd version
//------------------------------------------------------------------------------
static std::string GetXrootdVersion()
{
  XrdOucString v = XrdVERSIONINFOVAR(XrdgetProtocol).vStr;
  int pos = v.find(" ");

  if (pos != STR_NPOS) {
    v.erasefromstart(pos + 1);
  }

  return v.c_str();
}

//------------------------------------------------------------------------------
// Retrieve eos version
//------------------------------------------------------------------------------
static std::string GetEosVersion()
{
  return SSTR(VERSION << "-" << RELEASE);
}

//------------------------------------------------------------------------------
// Retrieve FST network interface
//------------------------------------------------------------------------------
static std::string GetNetworkInterface()
{
  if (getenv("EOS_FST_NETWORK_INTERFACE")) {
    return getenv("EOS_FST_NETWORK_INTERFACE");
  }

  return "eth0";
}

//------------------------------------------------------------------------------
// Retrieve number of TCP sockets in the system
// TODO: Change return value to integer..
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
  // active TCP sockets
  output["stat.sys.sockets"] = GetNumOfTcpSockets(tmpfile);
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
// open random temporary file in /tmp/
// Return value: string containing the temporary file. If opening it was
// not possible, return empty string.
//------------------------------------------------------------------------------
std::string makeTemporaryFile()
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
// Insert statfs info into the map
//------------------------------------------------------------------------------
static void insertStatfs(struct statfs* statfs,
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

  // Publish inconsistency statistics
  if (fs->GetStatus() == eos::common::BootStatus::kBooted) {
    output = fs->CollectInconsistencyStats("stat.fsck.");
  }

  // Publish statfs
  std::unique_ptr<eos::common::Statfs> statfs = fs->GetStatfs();

  if (statfs) {
    insertStatfs(statfs->GetStatfs(), output);
  }

  // Publish stat.disk.*
  double readratemb;
  double writeratemb;
  double diskload;
  std::map<std::string, std::string> iostats;

  if (fs->getFileIOStats(iostats)) {
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

  if (!fs->getHealth(health)) {
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
  // Publish generic statistics, related to free space and current load
  long long r_open = (long long) gOFS.openedForReading.getOpenOnFilesystem(fsid);
  long long w_open = (long long) gOFS.openedForWriting.getOpenOnFilesystem(fsid);
  output["stat.ropen"] = std::to_string(r_open);
  output["stat.wopen"] = std::to_string(w_open);
  output["stat.usedfiles"] = std::to_string(gFmdDbMapHandler.GetNumFiles(fsid));
  output["stat.boot"] = fs->GetStatusAsString(fs->GetStatus());
  output["stat.geotag"] = gOFS.GetGeoTag();
  output["stat.publishtimestamp"] = std::to_string(
                                      eos::common::getEpochInMilliseconds().count());
  output["stat.balancer.running"] = std::to_string(
                                      fs->GetBalanceQueue()->GetRunningAndQueued());
  output["stat.disk.iops"] = std::to_string(fs->getIOPS());
  output["stat.disk.bw"] = std::to_string(fs->getSeqBandwidth()); // in MB
  output["stat.http.port"] = std::to_string(gOFS.mHttpdPort);
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
Storage::Publish(ThreadAssistant& assistant)
{
  eos_static_info("%s", "msg=\"publisher activated\"");
  // Get our network speed
  std::string tmp_name = makeTemporaryFile();

  if (tmp_name.empty()) {
    return;
  }

  unsigned long long netspeed = GetNetSpeed(tmp_name);
  eos_static_info("msg=\"publish networkspeed=%.02f GB/s\"",
                  1.0 * netspeed / 1000000000.0);
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
        // Copy out statfs info - this could be done in parallel to speed-up

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

        auto fstStats = GetFstStatistics(tmp_name, netspeed);
        // Set node status values
        common::SharedHashLocator locator = gConfig.getNodeHashLocator("Publish");

        if (!locator.empty()) {
          mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), locator, true, false);

          for (auto it = fstStats.begin(); it != fstStats.end(); it++) {
            hash.set(it->first, it->second);
          }
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
