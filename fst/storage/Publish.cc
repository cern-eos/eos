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
#include "fst/txqueue/TransferQueue.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/FmdDbMap.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "qclient/Formatting.hh"
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "common/Timing.hh"
#include "common/IntervalStopwatch.hh"
#include "XrdVersion.hh"

XrdVERSIONINFOREF(XrdgetProtocol);

EOSFSTNAMESPACE_BEGIN

constexpr std::chrono::seconds Storage::sConsistencyTimeout;

//------------------------------------------------------------------------------
// Serialize hot files vector into std::string
// Return " " if given an empty vector, instead of "".
//
// This is to keep the entry in the hash, even if no opened files exist.
//------------------------------------------------------------------------------
static std::string hotFilesToString(
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
static uint64_t getNetspeed(const std::string& tmpname)
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
    eos_static_err("ip route list call failed to get netspeed");
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
static std::string getUptime(const std::string& tmpname)
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
static std::string getXrootdVersion()
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
static std::string getEosVersion()
{
  return SSTR(VERSION << "-" << RELEASE);
}

//------------------------------------------------------------------------------
// Retrieve node geotag - must be maximum 8 characters
//------------------------------------------------------------------------------
static std::string getGeotag()
{
  if (getenv("EOS_GEOTAG")) {
    return getenv("EOS_GEOTAG");
  }

  return "dfgeotag";
}

//------------------------------------------------------------------------------
// Retrieve FST network interface
//------------------------------------------------------------------------------
static std::string getNetworkInterface()
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
static std::string getNumberOfTCPSockets(const std::string& tmpname)
{
  std::string command = SSTR("cat /proc/net/tcp | wc -l | tr -d \"\n\" > " <<
                             tmpname);
  eos::common::ShellCmd cmd(command.c_str());
  eos::common::cmd_status rc = cmd.wait(5);

  if (rc.exit_code) {
    eos_static_err("retrieve #socket call failed");
  }

  std::string retval;
  eos::common::StringConversion::LoadFileIntoString(tmpname.c_str(), retval);
  return retval;
}

//------------------------------------------------------------------------------
// Get statistics about this FST, used for publishing
//------------------------------------------------------------------------------
std::map<std::string, std::string>
Storage::getFSTStatistics(const std::string& tmpfile,
                          unsigned long long netspeed)
{
  eos::common::LinuxStat::linux_stat_t osstat;

  if (!eos::common::LinuxStat::GetStat(osstat)) {
    eos_crit("failed to get the memory usage information");
  }

  std::map<std::string, std::string> output;
  // Kernel version
  output["stat.sys.kernel"] = eos::fst::Config::gConfig.KernelVersion.c_str();
  // Virtual memory size
  output["stat.sys.vsize"] = SSTR(osstat.vsize);
  // rss usage
  output["stat.sys.rss"] = SSTR(osstat.rss);
  // number of active threads on this machine
  output["stat.sys.threads"] = SSTR(osstat.threads);
  // eos version
  output["stat.sys.eos.version"] = getEosVersion();
  // xrootd version
  output["stat.sys.xrootd.version"] = getXrootdVersion();
  // adler32 of keytab
  output["stat.sys.keytab"] = eos::fst::Config::gConfig.KeyTabAdler.c_str();
  // machine uptime
  output["stat.sys.uptime"] = getUptime(tmpfile);
  // active TCP sockets
  output["stat.sys.sockets"] = getNumberOfTCPSockets(tmpfile);
  // startup time of the FST daemon
  output["stat.sys.eos.start"] = eos::fst::Config::gConfig.StartDate.c_str();
  // FST geotag
  output["stat.geotag"] = getGeotag();
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
                                   mFstLoad.GetNetRate(getNetworkInterface().c_str(),
                                       "rxbytes") / 1024.0 / 1024.0);
  output["stat.net.outratemib"] = SSTR(
                                    mFstLoad.GetNetRate(getNetworkInterface().c_str(),
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
    eos_static_crit("failed to create temporary file!");
    return "";
  }

  (void) close(tmp_fd);
  return tmp_name;
}

//------------------------------------------------------------------------------
// Insert statfs info into the map
//------------------------------------------------------------------------------
static void insertStatfs(struct statfs* statfs,
  std::map<std::string, std::string> &output) {

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

  output["stat.statfs.freebytes"] = std::to_string(statfs->f_bfree * statfs->f_bsize);
  output["stat.statfs.usedbytes"] = std::to_string( (statfs->f_blocks - statfs->f_bfree) * statfs->f_bsize );
  output["stat.statfs.filled"] = std::to_string(
    (double) 100.0 * ((double) (statfs->f_blocks - statfs->f_bfree) / (double) (1 + statfs->f_blocks)));
  output["stat.statfs.capacity"] = std::to_string(statfs->f_blocks * statfs->f_bsize);
  output["stat.statfs.fused"] = std::to_string( (statfs->f_files - statfs->f_ffree) * statfs->f_bsize);
}

//------------------------------------------------------------------------------
// Get statistics about this FileSystem, used for publishing
//------------------------------------------------------------------------------
std::map<std::string, std::string>
Storage::getFsStatistics(FileSystem *fs, bool publishInconsistencyStats) {
  if(!fs) {
    eos_static_crit("asked to publish statistics for a null filesystem");
    return {};
  }

  eos::common::FileSystem::fsid_t fsid = fs->GetId();

  if(!fsid) {
    // during the boot phase we can find a filesystem without ID
    eos_static_warning("asked to publish statistics for filesystem with fsid=0");
    return {};
  }

  std::map<std::string, std::string> output;

  //----------------------------------------------------------------------------
  // Publish inconsistency statistics?
  //----------------------------------------------------------------------------
  if(publishInconsistencyStats && fs->GetStatus() == eos::common::BootStatus::kBooted) {
    XrdSysMutexHelper ISLock(fs->InconsistencyStatsMutex);
    gFmdDbMapHandler.GetInconsistencyStatistics(
      fsid,
      *fs->GetInconsistencyStats(),
      *fs->GetInconsistencySets()
    );

    for (auto it = fs->GetInconsistencyStats()->begin(); it != fs->GetInconsistencyStats()->end(); it++) {
      std::string sname = SSTR("stat.fsck." << it->first);
      output[sname] = std::to_string(it->second);
    }
  }

  //----------------------------------------------------------------------------
  // Publish statfs
  //----------------------------------------------------------------------------
  std::unique_ptr<eos::common::Statfs> statfs = fs->GetStatfs();

  if(statfs) {
    insertStatfs(statfs->GetStatfs(), output);
  }

  //----------------------------------------------------------------------------
  // Publish stat.disk.*
  //----------------------------------------------------------------------------
  double readratemb;
  double writeratemb;
  double diskload;

  std::map<std::string, std::string> iostats;

  if(fs->getFileIOStats(iostats)) {
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

  //----------------------------------------------------------------------------
  // Publish stat.health.*
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> health;

  if (!fs->getHealth(health)) {
    health = mFstHealth.getDiskHealth(fs->GetPath());
  }

  output["stat.health"] = (health.count("summary") ? health["summary"].c_str() : "N/A");
  output["stat.health.indicator"] = health["indicator"];
  output["stat.health.drives_total"] = health["drives_total"];
  output["stat.health.drives_failed"] = health["drives_failed"];
  output["stat.health.redundancy_factor"] = health["redundancy_factor"];

  //----------------------------------------------------------------------------
  // Publish generic statistics, related to free space and current load
  //----------------------------------------------------------------------------
  long long r_open = (long long) gOFS.openedForReading.getOpenOnFilesystem(fsid);
  long long w_open = (long long) gOFS.openedForWriting.getOpenOnFilesystem(fsid);

  output["stat.ropen"] = std::to_string(r_open);
  output["stat.wopen"] = std::to_string(w_open);
  output["stat.usedfiles"] = std::to_string(gFmdDbMapHandler.GetNumFiles(fsid));
  output["stat.boot"] = fs->GetStatusAsString(fs->GetStatus());
  output["stat.geotag"] = getGeotag();
  output["stat.publishtimestamp"] = std::to_string(
    eos::common::getEpochInMilliseconds().count());
  output["stat.drainer.running"] = std::to_string(
    fs->GetDrainQueue()->GetRunningAndQueued());
  output["stat.balancer.running"] = std::to_string(
    fs->GetBalanceQueue()->GetRunningAndQueued());
  output["stat.disk.iops"] = std::to_string(fs->getIOPS());
  output["stat.disk.bw"] = std::to_string(fs->getSeqBandwidth()); // in MB

  output["stat.http.port"] = std::to_string(gOFS.mHttpdPort);
  output["stat.ropen.hotfiles"] = hotFilesToString(gOFS.openedForReading.getHotFiles(fsid, 10));
  output["stat.wopen.hotfiles"] = hotFilesToString(gOFS.openedForWriting.getHotFiles(fsid, 10));

  return output;
}

//------------------------------------------------------------------------------
// Publish statistics about the given filesystem
//------------------------------------------------------------------------------
bool Storage::publishFsStatistics(FileSystem *fs, bool publishInconsistencyStats) {
  if(!fs) {
    eos_static_crit("asked to publish statistics for a null filesystem");
    return {};
  }

  eos::common::FileSystem::fsid_t fsid = fs->GetId();

  if(!fsid) {
    // during the boot phase we can find a filesystem without ID
    eos_static_warning("asked to publish statistics for filesystem with fsid=0");
    return {};
  }

  bool success = true;
  std::map<std::string, std::string> fsStats = getFsStatistics(fs, publishInconsistencyStats);

  for(auto it = fsStats.begin(); it != fsStats.end(); it++) {
    success &= fs->SetString(it->first.c_str(), it->second.c_str());
  }

  CheckFilesystemFullness(fs, fsid);
  return success;
}

//------------------------------------------------------------------------------
// Publish
//------------------------------------------------------------------------------
void
Storage::Publish(ThreadAssistant& assistant)
{
  eos_static_info("Publisher activated ...");
  // Get our network speed
  std::string tmp_name = makeTemporaryFile();

  if (tmp_name.empty()) {
    return;
  }

  unsigned long long netspeed = getNetspeed(tmp_name);
  eos_static_info("publishing:networkspeed=%.02f GB/s",
                  1.0 * netspeed / 1000000000.0);
  // The following line acts as a barrier that prevents progress
  // until the config queue becomes known.
  eos::fst::Config::gConfig.getFstNodeConfigQueue("Publish");
  common::IntervalStopwatch consistencyStatsStopwatch(sConsistencyTimeout);

  while (!assistant.terminationRequested()) {
    //--------------------------------------------------------------------------
    // Should we publish consistency stats during this cycle?
    //--------------------------------------------------------------------------
    bool publishConsistencyStats = consistencyStatsStopwatch.restartIfExpired();

    std::string publish_uptime = getUptime(tmp_name);
    std::string publish_sockets = getNumberOfTCPSockets(tmp_name);

    std::chrono::milliseconds randomizedReportInterval = eos::fst::Config::gConfig.getRandomizedPublishInterval();
    common::IntervalStopwatch stopwatch(randomizedReportInterval);

    {
      // run through our defined filesystems and publish with a MuxTransaction all changes
      eos::common::RWMutexReadLock lock(mFsMutex);

      if (!gOFS.ObjectManager.OpenMuxTransaction()) {
        eos_static_err("cannot open mux transaction");
      } else {
        // copy out statfs info
        for (size_t i = 0; i < mFsVect.size(); i++) {
          bool success = publishFsStatistics(mFsVect[i], publishConsistencyStats);
          if(!success && mFsVect[i]) {
            eos_static_err("cannot set net parameters on filesystem %s",
                           mFsVect[i]->GetPath().c_str());
          }
        }

        {
          std::map<std::string, std::string> fstStats = getFSTStatistics(tmp_name,
              netspeed);
          // set node status values
          gOFS.ObjectManager.HashMutex.LockRead();
          // we received a new symkey
          XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(
                                    Config::gConfig.getFstNodeConfigQueue("Publish").c_str(),
                                    "hash");

          if (hash) {
            for (auto it = fstStats.begin(); it != fstStats.end(); it++) {
              hash->Set(it->first.c_str(), it->second.c_str());
            }
          }

          gOFS.ObjectManager.HashMutex.UnLockRead();
        }

        gOFS.ObjectManager.CloseMuxTransaction();
      }
    }

    std::chrono::milliseconds sleepTime = stopwatch.timeRemainingInCycle();

    if (sleepTime == std::chrono::milliseconds(0)) {
      eos_static_warning("Publisher cycle exceeded %d milliseconds - took %d milliseconds",
                         randomizedReportInterval.count(), stopwatch.timeIntoCycle());
    } else {
      assistant.wait_for(sleepTime);
    }
  }

  (void) unlink(tmp_name.c_str());
}

//------------------------------------------------------------------------------
// Publish statistics about this FST node and filesystems.
//
// Channels used:
// - fst-stats:<my hostport> for FST statistics
// - fs-stats:<id>
//------------------------------------------------------------------------------
void Storage::QdbPublish(const QdbContactDetails& cd,
                                  ThreadAssistant& assistant)
{
  //----------------------------------------------------------------------------
  // Fetch a qclient object, decide on which channel to use
  //----------------------------------------------------------------------------
  std::unique_ptr<qclient::QClient> qcl = std::make_unique<qclient::QClient>(cd.members, cd.constructOptions());
  std::string channel = SSTR("fst-stats:" <<
                             eos::fst::Config::gConfig.FstHostPort);
  //----------------------------------------------------------------------------
  // Setup required variables..
  //----------------------------------------------------------------------------
  std::string tmp_name = makeTemporaryFile();
  unsigned long long netspeed = getNetspeed(tmp_name);

  if (tmp_name.empty()) {
    return;
  }

  //----------------------------------------------------------------------------
  // Main loop
  //----------------------------------------------------------------------------
  common::IntervalStopwatch consistencyStatsStopwatch(sConsistencyTimeout);
  while (!assistant.terminationRequested()) {
    //--------------------------------------------------------------------------
    // Should we publish consistency stats during this cycle?
    //--------------------------------------------------------------------------
    bool publishConsistencyStats = consistencyStatsStopwatch.restartIfExpired();

    //--------------------------------------------------------------------------
    // Publish FST stats
    //--------------------------------------------------------------------------
    std::map<std::string, std::string> fstStats = getFSTStatistics(tmp_name,
        netspeed);
    qcl->exec("publish", channel, qclient::Formatting::serialize(fstStats));

    //--------------------------------------------------------------------------
    // Publish individual fs stats
    //--------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(mFsMutex);

    for (size_t i = 0; i < mFsVect.size(); i++) {
      std::map<std::string, std::string> fsStats = getFsStatistics(mFsVect[i],
        publishConsistencyStats);

      eos::common::FileSystem::fsid_t fsid = mFsVect[i]->GetId();
      std::string fsChannel = SSTR("fs-" << fsid);
      qcl->exec("publish", fsChannel, qclient::Formatting::serialize(fsStats));
    }

    lock.Release();

    //--------------------------------------------------------------------------
    // Sleep until next cycle
    //--------------------------------------------------------------------------
    assistant.wait_for(eos::fst::Config::gConfig.getRandomizedPublishInterval());

  }

  //----------------------------------------------------------------------------
  // Cleanup temporary file
  //----------------------------------------------------------------------------
  (void) unlink(tmp_name.c_str());
}

EOSFSTNAMESPACE_END
