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
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "common/Timing.hh"
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
  const std::vector<eos::fst::OpenFileTracker::HotEntry> &entries) {

  if(entries.size() == 0u) {
    return " ";
  }

  std::ostringstream ss;
  for(size_t i = 0; i < entries.size(); i++) {
    XrdOucString hexfid;
    eos::common::FileId::Fid2Hex(entries[i].fid, hexfid);

    ss << entries[i].uses;
    ss << ":";
    ss << hexfid.c_str();
    ss << " ";
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Retrieve net speed
//------------------------------------------------------------------------------
static uint64_t getNetspeed(const std::string& tmpname) {
  if(getenv("EOS_FST_NETWORK_SPEED")) {
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
static std::string getUptime(const std::string &tmpname) {
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
static std::string getXrootdVersion() {
  XrdOucString v = XrdVERSIONINFOVAR(XrdgetProtocol).vStr;
  int pos = v.find(" ");

  if (pos != STR_NPOS) {
    v.erasefromstart(pos + 1);
  }

  return v.c_str();
}

//------------------------------------------------------------------------------
// Retrieve number of TCP sockets in the system
// TODO: Change return value to integer..
//------------------------------------------------------------------------------
static std::string getNumberOfTCPSockets(const std::string &tmpname) {
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
// Publish
//------------------------------------------------------------------------------
void
Storage::Publish(ThreadAssistant &assistant)
{
  eos_static_info("Publisher activated ...");
  // Get our network speed
  char tmp_name[] = "/tmp/fst.publish.XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    eos_static_err("failed to create temporary file for ip route command");
    return;
  }

  (void) close(tmp_fd);


  std::string eosVersion = SSTR(VERSION << "-" << RELEASE);
  std::string xrootdVersion = getXrootdVersion();

  XrdOucString lNodeGeoTag = (getenv("EOS_GEOTAG") ?
                              getenv("EOS_GEOTAG") : "geotagdefault");
  XrdOucString lEthernetDev = (getenv("EOS_FST_NETWORK_INTERFACE") ?
                               getenv("EOS_FST_NETWORK_INTERFACE") : "eth0");

  unsigned long long netspeed = getNetspeed(tmp_name);
  eos_static_info("publishing:networkspeed=%.02f GB/s",
                  1.0 * netspeed / 1000000000.0);
  // The following line acts as a barrier that prevents progress
  // until the config queue becomes known.
  eos::fst::Config::gConfig.getFstNodeConfigQueue("Publish");
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  std::chrono::steady_clock::time_point next_consistency_stats;
  std::chrono::steady_clock::time_point last_consistency_stats;

  while (true) {
    std::string publish_uptime = getUptime(tmp_name);
    std::string publish_sockets = getNumberOfTCPSockets(tmp_name);

    std::chrono::steady_clock::time_point cycleStart = std::chrono::steady_clock::now();

    std::chrono::milliseconds randomizedReportInterval =
      eos::fst::Config::gConfig.getRandomizedPublishInterval();

    eos::common::LinuxStat::linux_stat_t osstat;

    if (!eos::common::LinuxStat::GetStat(osstat)) {
      eos_err("failed to get the memory usage information");
    }

    {
      // run through our defined filesystems and publish with a MuxTransaction all changes
      eos::common::RWMutexReadLock lock(mFsMutex);

      if (!gOFS.ObjectManager.OpenMuxTransaction()) {
        eos_static_err("cannot open mux transaction");
      } else {
        // copy out statfs info
        for (size_t i = 0; i < mFsVect.size(); i++) {
          if (!mFsVect[i]) {
            eos_static_err("found 0 vector in filesystems vector %u", i);
            continue;
          }

          eos::common::FileSystem::fsid_t fsid = 0;
          if (!(fsid = mFsVect[i]->GetId())) {
            // during the boot phase we can find a filesystem without ID
            continue;
          }

          std::string r_open_hotfiles =
            hotFilesToString(gOFS.openedForReading.getHotFiles(fsid, 10));

          std::string w_open_hotfiles =
            hotFilesToString(gOFS.openedForWriting.getHotFiles(fsid, 10));

          // Retrieve Statistics from the local db
          std::map<std::string, size_t>::const_iterator isit;
          bool success = true;

          if (mFsVect[i]->GetStatus() == eos::common::FileSystem::kBooted) {
            if (next_consistency_stats < cycleStart) {
              eos_static_debug("msg=\"publish consistency stats\"");
              last_consistency_stats = cycleStart;
              XrdSysMutexHelper ISLock(mFsVect[i]->InconsistencyStatsMutex);
              gFmdDbMapHandler.GetInconsistencyStatistics(fsid,
                  *mFsVect[i]->GetInconsistencyStats(),
                  *mFsVect[i]->GetInconsistencySets());

              for (isit = mFsVect[i]->GetInconsistencyStats()->begin();
                   isit != mFsVect[i]->GetInconsistencyStats()->end(); isit++) {
                //eos_static_debug("%-24s => %lu", isit->first.c_str(), isit->second);
                std::string sname = "stat.fsck.";
                sname += isit->first;
                success &= mFsVect[i]->SetLongLong(sname.c_str(), isit->second);
              }
            }
          }

          std::unique_ptr<eos::common::Statfs> statfs = mFsVect[i]->GetStatfs();

          // call the update function which stores into the filesystem shared hash
          if (statfs) {
            // call the update function which stores into the filesystem shared hash
            if (!mFsVect[i]->SetStatfs(statfs->GetStatfs())) {
              eos_static_err("cannot SetStatfs on filesystem %s",
                             mFsVect[i]->GetPath().c_str());
            }
          }

          // Set current load stats, io-target specific implementation may override
          // fst load implementation
          {
            double readratemb;
            double writeratemb;
            double diskload;
            std::map<std::string, std::string> iostats;

            if (mFsVect[i]->getFileIOStats(iostats)) {
              readratemb = strtod(iostats["read-mb-second"].c_str(), 0);
              writeratemb = strtod(iostats["write-mb-second"].c_str(), 0);
              diskload = strtod(iostats["load"].c_str(), 0);
            } else {
              readratemb = mFstLoad.GetDiskRate(mFsVect[i]->GetPath().c_str(),
                                                "readSectors") * 512.0 / 1000000.0;
              writeratemb = mFstLoad.GetDiskRate(mFsVect[i]->GetPath().c_str(),
                                                 "writeSectors") * 512.0 / 1000000.0;
              diskload = mFstLoad.GetDiskRate(mFsVect[i]->GetPath().c_str(),
                                              "millisIO") / 1000.0;
            }

            success &= mFsVect[i]->SetDouble("stat.disk.readratemb", readratemb);
            success &= mFsVect[i]->SetDouble("stat.disk.writeratemb",
                                             writeratemb);
            success &= mFsVect[i]->SetDouble("stat.disk.load", diskload);
          }
          // copy out net info
          {
            // File system implementation may override standard implementation
            std::map<std::string, std::string> health;

            if (!mFsVect[i]->getHealth(health)) {
              health = mFstHealth.getDiskHealth(mFsVect[i]->GetPath());
            }

            success &= mFsVect[i]->SetString("stat.health",
                                             (health.count("summary") ? health["summary"].c_str() : "N/A"));
            success &= mFsVect[i]->SetLongLong("stat.health.indicator",
                                               strtoll(health["indicator"].c_str(), 0, 10));
            success &= mFsVect[i]->SetLongLong("stat.health.drives_total",
                                               strtoll(health["drives_total"].c_str(), 0, 10));
            success &= mFsVect[i]->SetLongLong("stat.health.drives_failed",
                                               strtoll(health["drives_failed"].c_str(), 0, 10));
            success &= mFsVect[i]->SetLongLong("stat.health.redundancy_factor",
                                               strtoll(health["redundancy_factor"].c_str(), 0, 10));
          }
          long long r_open = (long long) gOFS.openedForReading.getOpenOnFilesystem(fsid);
          long long w_open = (long long) gOFS.openedForWriting.getOpenOnFilesystem(fsid);

          success &= mFsVect[i]->SetLongLong("stat.ropen", r_open);
          success &= mFsVect[i]->SetLongLong("stat.wopen", w_open);
          success &= mFsVect[i]->SetLongLong("stat.statfs.freebytes",
                                             mFsVect[i]->GetLongLong("stat.statfs.bfree") *
                                             mFsVect[i]->GetLongLong("stat.statfs.bsize"));
          success &= mFsVect[i]->SetLongLong("stat.statfs.usedbytes",
                                             (mFsVect[i]->GetLongLong("stat.statfs.blocks") -
                                              mFsVect[i]->GetLongLong("stat.statfs.bfree")) *
                                             mFsVect[i]->GetLongLong("stat.statfs.bsize"));
          success &= mFsVect[i]->SetDouble("stat.statfs.filled",
                                           100.0 * ((mFsVect[i]->GetLongLong("stat.statfs.blocks") -
                                               mFsVect[i]->GetLongLong("stat.statfs.bfree"))) /
                                           (1 + mFsVect[i]->GetLongLong("stat.statfs.blocks")));
          success &= mFsVect[i]->SetLongLong("stat.statfs.capacity",
                                             mFsVect[i]->GetLongLong("stat.statfs.blocks") *
                                             mFsVect[i]->GetLongLong("stat.statfs.bsize"));
          success &= mFsVect[i]->SetLongLong("stat.statfs.fused",
                                             (mFsVect[i]->GetLongLong("stat.statfs.files") -
                                              mFsVect[i]->GetLongLong("stat.statfs.ffree")) *
                                             mFsVect[i]->GetLongLong("stat.statfs.bsize"));
          success &= mFsVect[i]->SetLongLong("stat.usedfiles",
                                             gFmdDbMapHandler.GetNumFiles(fsid));
          success &= mFsVect[i]->SetString("stat.boot",
                                           mFsVect[i]->GetStatusAsString(mFsVect[i]->GetStatus()));
          success &= mFsVect[i]->SetString("stat.geotag", lNodeGeoTag.c_str());
          success &= mFsVect[i]->SetLongLong("stat.publishtimestamp",
            eos::common::getEpochInMilliseconds().count());
          success &= mFsVect[i]->SetLongLong("stat.drainer.running",
                                             mFsVect[i]->GetDrainQueue()->GetRunningAndQueued());
          success &= mFsVect[i]->SetLongLong("stat.balancer.running",
                                             mFsVect[i]->GetBalanceQueue()->GetRunningAndQueued());
          success &= mFsVect[i]->SetLongLong("stat.disk.iops",
                                             mFsVect[i]->getIOPS());
          success &= mFsVect[i]->SetDouble("stat.disk.bw",
                                           mFsVect[i]->getSeqBandwidth()); // in MB
          success &= mFsVect[i]->SetLongLong("stat.http.port", gOFS.mHttpdPort);

          // Copy out hot file list
          success &= mFsVect[i]->SetString("stat.ropen.hotfiles",
                                             r_open_hotfiles.c_str());
          success &= mFsVect[i]->SetString("stat.wopen.hotfiles",
                                             w_open_hotfiles.c_str());

          CheckFilesystemFullness(i, fsid);

          if (!success) {
            eos_static_err("cannot set net parameters on filesystem %s",
                           mFsVect[i]->GetPath().c_str());
          }
        }

        {
          // set node status values
          gOFS.ObjectManager.HashMutex.LockRead();
          // we received a new symkey
          XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(
                                    Config::gConfig.getFstNodeConfigQueue("Publish").c_str(),
                                    "hash");

          if (hash) {
            hash->Set("stat.sys.kernel", eos::fst::Config::gConfig.KernelVersion.c_str());
            hash->Set("stat.sys.vsize", osstat.vsize);
            hash->Set("stat.sys.rss", osstat.rss);
            hash->Set("stat.sys.threads", osstat.threads);
            hash->Set("stat.sys.eos.version", eosVersion.c_str());
            hash->Set("stat.sys.xrootd.version", xrootdVersion.c_str());
            hash->Set("stat.sys.keytab", eos::fst::Config::gConfig.KeyTabAdler.c_str());
            hash->Set("stat.sys.uptime", publish_uptime.c_str());
            hash->Set("stat.sys.sockets", publish_sockets.c_str());
            hash->Set("stat.sys.eos.start", eos::fst::Config::gConfig.StartDate.c_str());
            hash->Set("stat.geotag", lNodeGeoTag.c_str());
            hash->Set("http.port", gOFS.mHttpdPort);
            hash->Set("debug.state",
                      eos::common::StringConversion::ToLower
                      (g_logging.GetPriorityString
                       (g_logging.gPriorityLevel)).c_str());
            // copy out net info
            hash->Set("stat.net.ethratemib", netspeed / (8 * 1024 * 1024));
            hash->Set("stat.net.inratemib", mFstLoad.GetNetRate(lEthernetDev.c_str(),
                      "rxbytes") / 1024.0 / 1024.0);
            hash->Set("stat.net.outratemib", mFstLoad.GetNetRate(lEthernetDev.c_str(),
                      "txbytes") / 1024.0 / 1024.0);
            hash->Set("stat.publishtimestamp", SSTR(eos::common::getEpochInMilliseconds().count()));
          }

          gOFS.ObjectManager.HashMutex.UnLockRead();
        }

        gOFS.ObjectManager.CloseMuxTransaction();
        // Report the consistency stats only once every 5 min
        next_consistency_stats = last_consistency_stats + sConsistencyTimeout;
      }
    }

    std::chrono::steady_clock::time_point cycleEnd = std::chrono::steady_clock::now();

    std::chrono::milliseconds cycleDuration =
      std::chrono::duration_cast<std::chrono::milliseconds>(cycleEnd - cycleStart);
    std::chrono::milliseconds sleepTime = randomizedReportInterval - cycleDuration;

    eos_static_debug("msg=\"publish interval\" %d %d",
      randomizedReportInterval.count(), cycleDuration.count());

    if (cycleDuration > randomizedReportInterval) {
      eos_static_warning("Publisher cycle exceeded %d millisecons - took %d milliseconds",
                         randomizedReportInterval.count(), cycleDuration.count());
    } else {
      assistant.wait_for(sleepTime);
    }
  }

  (void) unlink(tmp_name);
}

EOSFSTNAMESPACE_END
