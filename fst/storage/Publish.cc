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
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Publish
//------------------------------------------------------------------------------
void
Storage::Publish()
{
  eos_static_info("Publisher activated ...");
  struct timeval tv1, tv2;
  struct timezone tz;
  unsigned long long netspeed = 1000000000;
  // Get our network speed
  char tmp_name[] = "/tmp/fst.publish.XXXXXX";
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    eos_static_err("failed to create temporary file for ip route command");
    return;
  }

  (void) close(tmp_fd);
  XrdOucString getnetspeed = "ip route list | sed -ne '/^default/s/.*dev //p' |"
                             " xargs ethtool | grep Speed | cut -d ':' -f2 | cut -d 'M' -f1 >> ";

  if (getenv("EOS_FST_NETWORK_SPEED")) {
    getnetspeed = "echo ";
    getnetspeed += getenv("EOS_FST_NETWORK_SPEED");
    getnetspeed += " >> ";
  }

  getnetspeed += tmp_name;
  eos::common::ShellCmd scmd1(getnetspeed.c_str());
  eos::common::cmd_status rc = scmd1.wait(5);

  if (rc.exit_code) {
    eos_static_err("ip route list call failed to get netspeed");
  }

  XrdOucString lNodeGeoTag = (getenv("EOS_GEOTAG") ? getenv("EOS_GEOTAG") : "");
  XrdOucString lEthernetDev = (getenv("EOS_FST_NETWORK_INTERFACE") ?
                               getenv("EOS_FST_NETWORK_INTERFACE") : "eth0");
  FILE* fnetspeed = fopen(tmp_name, "r");

  if (fnetspeed) {
    if ((fscanf(fnetspeed, "%llu", &netspeed)) == 1) {
      // we get MB as a number => convert into bytes
      netspeed *= 1000000;
      eos_static_info("ethtool:networkspeed=%.02f GB/s",
                      1.0 * netspeed / 1000000000.0);
    }

    fclose(fnetspeed);
  }

  eos_static_info("publishing:networkspeed=%.02f GB/s",
                  1.0 * netspeed / 1000000000.0);
  // Wait before publishing
  XrdSysTimer sleeper;
  sleeper.Snooze(3);

  while (!eos::fst::Config::gConfig.FstNodeConfigQueue.length()) {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }

  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  eos::common::FileSystem::fsid_t fsid = 0;
  std::string publish_uptime = "";
  std::string publish_sockets = "";

  while (1) {
    {
      // Retrieve uptime information
      XrdOucString uptime = "uptime | tr -d \"\n\" > ";
      uptime += tmp_name;
      eos::common::ShellCmd scmd2(uptime.c_str());
      rc = scmd2.wait(5);

      if (rc.exit_code) {
        eos_static_err("retrieve uptime call failed");
      }

      eos::common::StringConversion::LoadFileIntoString(tmp_name, publish_uptime);
      XrdOucString sockets = "cat /proc/net/tcp | wc -l | tr -d \"\n\" >";
      sockets += tmp_name;
      eos::common::ShellCmd scmd3(sockets.c_str());
      rc = scmd3.wait(5);

      if (rc.exit_code) {
        eos_static_err("retrieve #socket call failed");
      }

      eos::common::StringConversion::LoadFileIntoString(tmp_name, publish_sockets);
    }
    time_t now = time(NULL);
    gettimeofday(&tv1, &tz);
    // TODO: derive this from a global variable
    int PublishInterval = 10;
    {
      XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
      PublishInterval = eos::fst::Config::gConfig.PublishInterval;
    }

    if ((PublishInterval < 2) || (PublishInterval > 3600)) {
      // default to 10 +- 5 seconds
      PublishInterval = 10;
    }

    unsigned int lReportIntervalMilliSeconds = (PublishInterval * 500) +
        (unsigned int)((PublishInterval * 1000.0) * rand() / RAND_MAX);
    eos::common::LinuxStat::linux_stat_t osstat;

    if (!eos::common::LinuxStat::GetStat(osstat)) {
      eos_err("failed to get the memory usage information");
    }

    {
      // run through our defined filesystems and publish with a MuxTransaction all changes
      eos::common::RWMutexReadLock lock(mFsMutex);
      static time_t last_consistency_stats = 0;
      static time_t next_consistency_stats = 0;

      if (!gOFS.ObjectManager.OpenMuxTransaction()) {
        eos_static_err("cannot open mux transaction");
      } else {
        // copy out statfs info
        for (size_t i = 0; i < mFsVect.size(); i++) {
          if (!mFsVect[i]) {
            eos_static_err("found 0 vector in filesystems vector %u", i);
            continue;
          }

          if (!(fsid = mFsVect[i]->GetId())) {
            // during the boot phase we can find a filesystem without ID
            continue;
          }

          XrdOucString r_open_hotfiles;
          XrdOucString w_open_hotfiles;
          {
            std::map<unsigned int, std::set<unsigned long long>> Rhotfiles;
            std::map<unsigned int, std::set<unsigned long long>> Whotfiles;
            {
              XrdSysMutexHelper fLock(gOFS.OpenFidMutex);

              if (gOFS.ROpenFid.count(fsid)) {
                for (auto it = gOFS.ROpenFid[fsid].begin(); it != gOFS.ROpenFid[fsid].end();
                     ++it) {
                  Rhotfiles[it->second].insert(it->first);
                }
              }

              if (gOFS.WOpenFid.count(fsid)) {
                for (auto it = gOFS.WOpenFid[fsid].begin(); it != gOFS.WOpenFid[fsid].end();
                     ++it) {
                  Whotfiles[it->second].insert(it->first);
                }
              }
            }
            size_t cnt = 0;

            for (auto it = Rhotfiles.rbegin(); it != Rhotfiles.rend(); ++it) {
              XrdOucString hexfid;

              for (auto fit = it->second.begin(); fit != it->second.end(); ++fit) {
                eos::common::FileId::Fid2Hex(*fit, hexfid);
                r_open_hotfiles += (int) it->first;
                r_open_hotfiles += ":";
                r_open_hotfiles += hexfid.c_str();
                r_open_hotfiles += " ";
                ++cnt;

                if (cnt == 10) {
                  break;
                }
              }
            }

            cnt = 0;

            for (auto it = Whotfiles.rbegin(); it != Whotfiles.rend(); ++it) {
              XrdOucString hexfid;

              for (auto fit = it->second.begin(); fit != it->second.end(); ++fit) {
                eos::common::FileId::Fid2Hex(*fit, hexfid);
                w_open_hotfiles += (int) it->first;
                w_open_hotfiles += ":";
                w_open_hotfiles += hexfid.c_str();
                w_open_hotfiles += " ";
                ++cnt;

                if (cnt == 10) {
                  break;
                }
              }
            }
          }
          // Retrieve Statistics from the SQLITE DB
          std::map<std::string, size_t>::const_iterator isit;
          bool success = true;

          if (mFsVect[i]->GetStatus() == eos::common::FileSystem::kBooted) {
            if (next_consistency_stats < now) {
              eos_static_debug("msg=\"publish consistency stats\"");
              last_consistency_stats = now;
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

          eos::common::Statfs* statfs = 0;

          // call the update function which stores into the filesystem shared hash
          if ((statfs = mFsVect[i]->GetStatfs())) {
            // call the update function which stores into the filesystem shared hash
            if (!mFsVect[i]->SetStatfs(statfs->GetStatfs())) {
              eos_static_err("cannot SetStatfs on filesystem %s",
                             mFsVect[i]->GetPath().c_str());
            }
          }

          // Copy out net info
          success &= mFsVect[i]->SetDouble("stat.net.ethratemib",
                                           netspeed / (8 * 1024 * 1024));
          success &= mFsVect[i]->SetDouble("stat.net.inratemib",
                                           mFstLoad.GetNetRate(lEthernetDev.c_str(), "rxbytes") / 1024.0 / 1024.0);
          success &= mFsVect[i]->SetDouble("stat.net.outratemib",
                                           mFstLoad.GetNetRate(lEthernetDev.c_str(), "txbytes") / 1024.0 / 1024.0);
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
              health = mFstHealth.getDiskHealth(mFsVect[i]->GetPath().c_str());
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
          long long r_open = 0;
          long long w_open = 0;
          {
            XrdSysMutexHelper fLock(gOFS.OpenFidMutex);
            r_open = (long long) gOFS.ROpenFid[fsid].size();
            w_open = (long long) gOFS.WOpenFid[fsid].size();
          }
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
          {
            eos::common::RWMutexReadLock lock(gFmdDbMapHandler.Mutex);
            FmdSqliteWriteLock vlock(fsid);
            success &= mFsVect[i]->SetLongLong("stat.usedfiles",
                                               (long long)(gFmdDbMapHandler.mDbMap.count(fsid) ?
                                                   gFmdDbMapHandler.mDbMap[fsid]->size() : 0));
          }
          success &= mFsVect[i]->SetString("stat.boot",
                                           mFsVect[i]->GetStatusAsString(mFsVect[i]->GetStatus()));
          success &= mFsVect[i]->SetString("stat.geotag", lNodeGeoTag.c_str());
          struct timeval tvfs;
          gettimeofday(&tvfs, &tz);
          size_t nowms = tvfs.tv_sec * 1000 + tvfs.tv_usec / 1000;
          success &= mFsVect[i]->SetLongLong("stat.publishtimestamp", nowms);
          success &= mFsVect[i]->SetLongLong("stat.drainer.running",
                                             mFsVect[i]->GetDrainQueue()->GetRunningAndQueued());
          success &= mFsVect[i]->SetLongLong("stat.balancer.running",
                                             mFsVect[i]->GetBalanceQueue()->GetRunningAndQueued());
          success &= mFsVect[i]->SetLongLong("stat.disk.iops",
                                             mFsVect[i]->getIOPS());
          success &= mFsVect[i]->SetDouble("stat.disk.bw",
                                           mFsVect[i]->getSeqBandwidth()); // in MB
          {
            // we have to set something which is not empty to update the value
            if (!r_open_hotfiles.length()) {
              r_open_hotfiles = " ";
            }

            if (!w_open_hotfiles.length()) {
              w_open_hotfiles = " ";
            }

            // Copy out hot file list
            success &= mFsVect[i]->SetString("stat.ropen.hotfiles",
                                             r_open_hotfiles.c_str());
            success &= mFsVect[i]->SetString("stat.wopen.hotfiles",
                                             w_open_hotfiles.c_str());
          }
          {
            long long fbytes = mFsVect[i]->GetLongLong("stat.statfs.freebytes");
            XrdSysMutexHelper(mFsFullMapMutex);
            // stop the writers if it get's critical under 5 GB space
            int full_gb = 5;

            if (getenv("EOS_FS_FULL_SIZE_IN_GB")) {
              full_gb = atoi(getenv("EOS_FS_FULL_SIZE_IN_GB"));
            }

            if ((fbytes < full_gb * 1024ll * 1024ll * 1024ll)) {
              mFsFullMap[fsid] = true;
            } else {
              mFsFullMap[fsid] = false;
            }

            if ((fbytes < 1024ll * 1024ll * 1024ll) ||
                (fbytes <= mFsVect[i]->GetLongLong("headroom"))) {
              mFsFullWarnMap[fsid] = true;
            } else {
              mFsFullWarnMap[fsid] = false;
            }
          }

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
                                    Config::gConfig.FstNodeConfigQueue.c_str(), "hash");

          if (hash) {
            hash->Set("stat.sys.kernel", eos::fst::Config::gConfig.KernelVersion.c_str());
            hash->Set("stat.sys.vsize", osstat.vsize);
            hash->Set("stat.sys.rss", osstat.rss);
            hash->Set("stat.sys.threads", osstat.threads);
            {
              XrdOucString v = VERSION;
              v += "-";
              v += RELEASE;
              hash->Set("stat.sys.eos.version", v.c_str());
            }
            hash->Set("stat.sys.keytab", eos::fst::Config::gConfig.KeyTabAdler.c_str());
            hash->Set("stat.sys.uptime", publish_uptime.c_str());
            hash->Set("stat.sys.sockets", publish_sockets.c_str());
            hash->Set("stat.sys.eos.start", eos::fst::Config::gConfig.StartDate.c_str());
            hash->Set("stat.geotag", lNodeGeoTag.c_str());
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
            struct timeval tvfs;
            gettimeofday(&tvfs, &tz);
            size_t nowms = tvfs.tv_sec * 1000 + tvfs.tv_usec / 1000;
            hash->Set("stat.publishtimestamp", nowms);
          }

          gOFS.ObjectManager.HashMutex.UnLockRead();
        }

        gOFS.ObjectManager.CloseMuxTransaction();
        next_consistency_stats = last_consistency_stats +
                                 60; // report the consistency only once per minute
      }
    }

    gettimeofday(&tv2, &tz);
    int lCycleDuration = (int)((tv2.tv_sec * 1000.0) - (tv1.tv_sec * 1000.0) +
                               (tv2.tv_usec / 1000.0) - (tv1.tv_usec / 1000.0));
    int lSleepTime = lReportIntervalMilliSeconds - lCycleDuration;
    eos_static_debug("msg=\"publish interval\" %d %d", lReportIntervalMilliSeconds,
                     lCycleDuration);

    if (lSleepTime < 0) {
      eos_static_warning("Publisher cycle exceeded %d millisecons - took %d milliseconds",
                         lReportIntervalMilliSeconds, lCycleDuration);
    } else {
      XrdSysTimer sleeper;
      sleeper.Snooze(lSleepTime / 1000);
    }
  }

  (void) unlink(tmp_name);
}

EOSFSTNAMESPACE_END
