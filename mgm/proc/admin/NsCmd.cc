//------------------------------------------------------------------------------
//! @file NsCmd.cc
//! @author Elvin Sindrilaru - CERN
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

#include "NsCmd.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "common/LinuxFds.hh"
#include "common/BehaviourConfig.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/ContainerIterators.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "namespace/ns_quarkdb/utils/QuotaRecomputer.hh"
#include "namespace/ns_quarkdb/NamespaceGroup.hh"
#include "namespace/ns_quarkdb/flusher/MetadataFlusher.hh"
#include "namespace/ns_quarkdb/QClPerformance.hh"
#include "namespace/Resolver.hh"
#include "namespace/Constants.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/fsck/Fsck.hh"
#include "mgm/Quota.hh"
#include "mgm/Stat.hh"
#include "mgm/ZMQ.hh"
#include "mgm/convert/ConverterEngine.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behaviour of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
NsCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::NsProto ns = mReqProto.ns();
  eos::console::NsProto::SubcmdCase subcmd = ns.subcmd_case();

  if (subcmd == eos::console::NsProto::kStat) {
    StatSubcmd(ns.stat(), reply);
  } else if (subcmd == eos::console::NsProto::kMutex) {
    MutexSubcmd(ns.mutex(), reply);
  } else if (subcmd == eos::console::NsProto::kCompact) {
    CompactSubcmd(ns.compact(), reply);
  } else if (subcmd == eos::console::NsProto::kMaster) {
    MasterSubcmd(ns.master(), reply);
  } else if (subcmd == eos::console::NsProto::kTree) {
    TreeSizeSubcmd(ns.tree(), reply);
  } else if (subcmd == eos::console::NsProto::kCache) {
    CacheSubcmd(ns.cache(), reply);
  } else if (subcmd == eos::console::NsProto::kQuota) {
    QuotaSizeSubcmd(ns.quota(), reply);
  } else if (subcmd == eos::console::NsProto::kDrain) {
    DrainSubcmd(ns.drain(), reply);
  } else if (subcmd == eos::console::NsProto::kReserve) {
    ReserveIdsSubCmd(ns.reserve(), reply);
  } else if (subcmd == eos::console::NsProto::kBenchmark) {
    BenchmarkSubCmd(ns.benchmark(), reply);
  } else if (subcmd == eos::console::NsProto::kTracker) {
    TrackerSubCmd(ns.tracker(), reply);
  } else if (subcmd == eos::console::NsProto::kBehaviour) {
    BehaviourSubCmd(ns.behaviour(), reply);
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: not supported");
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute mutex subcommand
//------------------------------------------------------------------------------
void
NsCmd::MutexSubcmd(const eos::console::NsProto_MutexProto& mutex,
                   eos::console::ReplyProto& reply)
{
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (mVid.uid == 0) {
    bool no_option = true;
    std::ostringstream oss;

    if (mutex.sample_rate1() || mutex.sample_rate10() ||
        mutex.sample_rate100() || mutex.toggle_timing() ||
        mutex.toggle_order() || mutex.blockedtime()) {
      no_option = false;
    }

    eos::common::RWMutex* fs_mtx = &FsView::gFsView.ViewMutex;
    eos::common::RWMutex* quota_mtx = &Quota::pMapMutex;
    eos::common::RWMutex* ns_mtx = &gOFS->eosViewRWMutex;
    eos::common::RWMutex* fusex_client_mtx = &gOFS->zMQ->gFuseServer.Client();
    //eos::common::RWMutex* fusex_cap_mtx = &gOFS->zMQ->gFuseServer.Cap();

    if (no_option) {
      size_t cycleperiod = eos::common::RWMutex::GetLockUnlockDuration();
      std::string line = "# ------------------------------------------------------"
                         "------------------------------";
      oss << line << std::endl
          << "# Mutex Monitoring Management" << std::endl
          << line << std::endl
          << "order checking is : "
          << (eos::common::RWMutex::GetOrderCheckingGlobal() ? "on " : "off")
          << " (estimated order checking latency for 1 rule ";
      size_t orderlatency = eos::common::RWMutex::GetOrderCheckingLatency();
      oss <<  orderlatency << " nsec / "
          << int(double(orderlatency) / cycleperiod * 100)
          << "% of the mutex lock/unlock cycle duration)" << std::endl
          << "deadlock checking is : "
          << (eos::common::RWMutex::GetDeadlockCheckingGlobal() ? "on" : "off")
          << std::endl
          << "timing         is : "
          << (fs_mtx->GetTiming() ? "on " : "off")
          << " (estimated timing latency for 1 lock ";
      size_t timinglatency = eos::common::RWMutex::GetTimingLatency();
      oss << timinglatency << " nsec / "
          << int(double(timinglatency) / cycleperiod * 100)
          << "% of the mutex lock/unlock cycle duration)" << std::endl
          << "sampling rate  is : ";
      float sr = fs_mtx->GetSampling();
      char ssr[32];
      sprintf(ssr, "%f", sr);
      oss << (sr < 0 ? "NA" : ssr);

      if (sr > 0) {
        oss << " (estimated average timing latency "
            << int(double(timinglatency) * sr) << " nsec / "
            << int((timinglatency * sr) / cycleperiod * 100)
            << "% of the mutex lock/unlock cycle duration)";
      }

      oss << std::endl;
      oss << "blockedtiming  is : ";
      oss << ns_mtx->BlockedForMsInterval() << " ms" << std::endl;
    }

    if (mutex.toggle_timing()) {
      if (fs_mtx->GetTiming()) {
        fs_mtx->SetTiming(false);
        quota_mtx->SetTiming(false);
        ns_mtx->SetTiming(false);
        oss << "mutex timing is off" << std::endl;
      } else {
        fs_mtx->SetTiming(true);
        quota_mtx->SetTiming(true);
        ns_mtx->SetTiming(true);
        oss << "mutex timing is on" << std::endl;
      }
    }

    if (mutex.toggle_order()) {
      if (eos::common::RWMutex::GetOrderCheckingGlobal()) {
        eos::common::RWMutex::SetOrderCheckingGlobal(false);
        oss << "mutex order checking is off" << std::endl;
      } else {
        eos::common::RWMutex::SetOrderCheckingGlobal(true);
        oss << "mutex order checking is on" << std::endl;
      }
    }

    if (mutex.toggle_deadlock()) {
      if (eos::common::RWMutex::GetDeadlockCheckingGlobal()) {
        eos::common::RWMutex::SetDeadlockCheckingGlobal(false);
        oss << "mutex deadlock checking is off" << std::endl;
      } else {
        eos::common::RWMutex::SetDeadlockCheckingGlobal(true);
        oss << "mutex deadlock checking is on" << std::endl;
      }
    }

    if (mutex.blockedtime()) {
      fs_mtx->SetBlockedForMsInterval(mutex.blockedtime());
      ns_mtx->SetBlockedForMsInterval(mutex.blockedtime());
      quota_mtx->SetBlockedForMsInterval(mutex.blockedtime());
      fusex_client_mtx->SetBlockedForMsInterval(mutex.blockedtime());
      //fusex_cap_mtx->SetBlockedForMsInterval(mutex.blockedtime());
      oss << "blockedtiming set to " << ns_mtx->BlockedForMsInterval() << " ms" <<
          std::endl;
    }

    if (mutex.sample_rate1() || mutex.sample_rate10() ||
        mutex.sample_rate100()) {
      float rate = 0.0;

      if (mutex.sample_rate1()) {
        rate = 0.01;
      } else if (mutex.sample_rate10()) {
        rate = 0.1;
      } else if (mutex.sample_rate100()) {
        rate = 1.0;
      }

      fs_mtx->SetSampling(true, rate);
      quota_mtx->SetSampling(true, rate);
      ns_mtx->SetSampling(true, rate);
    }

    reply.set_std_out(oss.str());
  } else {
    reply.set_std_err("error: you have to take role 'root' to execute this"
                      " command");
    reply.set_retc(EPERM);
  }

#endif
}

//------------------------------------------------------------------------------
// Execute stat command
//------------------------------------------------------------------------------
void
NsCmd::StatSubcmd(const eos::console::NsProto_StatProto& stat,
                  eos::console::ReplyProto& reply)
{
  using eos::common::StringConversion;
  std::ostringstream oss;
  std::ostringstream err;
  int retc = 0;

  if (stat.reset()) {
    gOFS->MgmStats.Clear();
    oss << "success: all counters have been reset" << std::endl;
  }

  uint64_t f = gOFS->eosFileService->getNumFiles();
  uint64_t d = gOFS->eosDirectoryService->getNumContainers();
  eos::common::FileId::fileid_t fid_now = gOFS->eosFileService->getFirstFreeId();
  eos::common::FileId::fileid_t cid_now =
    gOFS->eosDirectoryService->getFirstFreeId();
  struct stat statf;
  struct stat statd;
  memset(&statf, 0, sizeof(struct stat));
  memset(&statd, 0, sizeof(struct stat));
  XrdOucString clfsize = "";
  XrdOucString cldsize = "";
  XrdOucString clfratio = "";
  XrdOucString cldratio = "";
  XrdOucString sizestring = "";

  // Statistics for the changelog files if they exist
  if ((gOFS->MgmNsFileChangeLogFile.length() != 0) &&
      (gOFS->MgmNsDirChangeLogFile.length() != 0) &&
      (!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &statf)) &&
      (!::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &statd))) {
    StringConversion::GetReadableSizeString(clfsize,
                                            (unsigned long long) statf.st_size, "B");
    StringConversion::GetReadableSizeString(cldsize,
                                            (unsigned long long) statd.st_size, "B");
    StringConversion::GetReadableSizeString(clfratio,
                                            (unsigned long long) f ?
                                            (1.0 * statf.st_size) / f : 0, "B");
    StringConversion::GetReadableSizeString(cldratio,
                                            (unsigned long long) d ?
                                            (1.0 * statd.st_size) / d : 0, "B");
  }

  time_t fboot_time = 0;
  time_t boot_time = 0;
  std::string bootstring = namespaceStateToString(gOFS->mNamespaceState);

  if (bootstring == "booting") {
    fboot_time = time(nullptr) - gOFS->mFileInitTime;
    boot_time = time(nullptr) - gOFS->mStartTime;
  } else {
    fboot_time = gOFS->mFileInitTime;
    boot_time = gOFS->mTotalInitTime;
  }

  // Statistics for memory usage
  eos::common::LinuxMemConsumption::linux_mem_t mem;

  if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem)) {
    err << "error: failed to get the memory usage information" << std::endl;
    retc = errno;
  }

  eos::common::LinuxStat::linux_stat_t pstat;

  if (!eos::common::LinuxStat::GetStat(pstat)) {
    err << "error: failed to get the process stat information" << std::endl;
    retc = errno;
  }

  eos::common::LinuxFds::linux_fds_t fds;

  if (!eos::common::LinuxFds::GetFdUsage(fds)) {
    err << "error: failed to get the process fd information" << std::endl;
    retc = errno;
  }

  std::string master_status = gOFS->mMaster->PrintOut();
  XrdOucString compact_status = "";
  size_t eosxd_nclients = 0;
  size_t eosxd_active_clients = 0;
  size_t eosxd_locked_clients = 0;
  gOFS->zMQ->gFuseServer.Client().ClientStats(eosxd_nclients,
      eosxd_active_clients, eosxd_locked_clients);
  bool monitoring = stat.monitor() || WantsJsonOutput();
  CacheStatistics fileCacheStats = gOFS->eosFileService->getCacheStatistics();
  CacheStatistics containerCacheStats =
    gOFS->eosDirectoryService->getCacheStatistics();
  common::MutexLatencyWatcher::LatencySpikes viewLatency =
    gOFS->mViewMutexWatcher.getLatencySpikes();
  double eosViewMutexPenultimateSecWriteLockTimePercentage =
    (gOFS->eosViewRWMutex.getNbMsMutexWriteLockedPenultimateSecond().count() /
     1000.0) * 100.0;

  if (eosViewMutexPenultimateSecWriteLockTimePercentage > 100) {
    eosViewMutexPenultimateSecWriteLockTimePercentage = 100;
  }

  double readcontention = gOFS->MgmStats.GetReadContention();
  double writecontention = gOFS->MgmStats.GetWriteContention();

  if (monitoring) {
    oss << "uid=all gid=all ns.total.files=" << f
        << "\nuid=all gid=all ns.total.directories=" << d
        << "\nuid=all gid=all ns.current.fid=" << fid_now
        << "\nuid=all gid=all ns.current.cid=" << cid_now
        << "\nuid=all gid=all ns.generated.fid=" << (int)(fid_now - gOFS->mBootFileId)
        << "\nuid=all gid=all ns.generated.cid=" << (int)(cid_now -
            gOFS->mBootContainerId)
        << "\nuid=all gid=all ns.contention.read=" << readcontention
        << "\nuid=all gid=all ns.contention.write=" << writecontention
        << "\nuid=all gid=all ns.cache.files.maxsize=" << fileCacheStats.maxNum
        << "\nuid=all gid=all ns.cache.files.occupancy=" << fileCacheStats.occupancy
        << "\nuid=all gid=all ns.cache.files.requests=" << fileCacheStats.numRequests
        << "\nuid=all gid=all ns.cache.files.hits=" << fileCacheStats.numHits
        << "\nuid=all gid=all ns.cache.containers.maxsize=" <<
        containerCacheStats.maxNum
        << "\nuid=all gid=all ns.cache.containers.occupancy=" <<
        containerCacheStats.occupancy
        << "\nuid=all gid=all ns.cache.containers.requests=" <<
        containerCacheStats.numRequests
        << "\nuid=all gid=all ns.cache.containers.hits=" << containerCacheStats.numHits
        << "\nuid=all gid=all ns.total.files.changelog.size="
        << StringConversion::GetSizeString(clfsize, (unsigned long long) statf.st_size)
        << std::endl
        << "uid=all gid=all ns.total.directories.changelog.size="
        << StringConversion::GetSizeString(cldsize, (unsigned long long) statd.st_size)
        << std::endl
        << "uid=all gid=all ns.total.files.changelog.avg_entry_size="
        << StringConversion::GetSizeString(clfratio, (unsigned long long) f ?
                                           (1.0 * statf.st_size) / f : 0)
        << std::endl
        << "uid=all gid=all ns.total.directories.changelog.avg_entry_size="
        << StringConversion::GetSizeString(cldratio, (unsigned long long) d ?
                                           (1.0 * statd.st_size) / d : 0)
        << std::endl
        << "uid=all gid=all " << compact_status.c_str() << std::endl
        << "uid=all gid=all ns.boot.status=" << bootstring << std::endl
        << "uid=all gid=all ns.boot.time=" << boot_time << std::endl
        << "uid=all gid=all ns.boot.file.time=" << fboot_time << std::endl
        << "uid=all gid=all " << master_status.c_str() << std::endl
        << "uid=all gid=all ns.memory.virtual=" << mem.vmsize << std::endl
        << "uid=all gid=all ns.memory.resident=" << mem.resident << std::endl
        << "uid=all gid=all ns.memory.share=" << mem.share << std::endl
        << "uid=all gid=all ns.stat.threads=" << pstat.threads << std::endl
        << "uid=all gid=all ns.fds.all=" << fds.all << std::endl
        << "uid=all gid=all ns.fusex.caps=" << gOFS->zMQ->gFuseServer.Cap().ncaps() <<
        std::endl
        << "uid=all gid=all ns.fusex.clients=" <<
        eosxd_nclients << std::endl
        << "uid=all gid=all ns.fusex.activeclients=" <<
        eosxd_active_clients << std::endl
        << "uid=all gid=all ns.fusex.lockedclients=" <<
        eosxd_locked_clients << std::endl
        << "uid=all gid=all ns.hanging=" << gOFS->mViewMutexWatcher.isLockedUp() <<
        std::endl
        << "uid=all gid=all ns.hanging.since=" << gOFS->mViewMutexWatcher.hangingSince()
        << std::endl
        << "uid=all gid=all ns.latencypeak.eosviewmutex.last=" <<
        viewLatency.last.count() << std::endl
        << "uid=all gid=all ns.latencypeak.eosviewmutex.1min=" <<
        viewLatency.lastMinute.count() << std::endl
        << "uid=all gid=all ns.latencypeak.eosviewmutex.2min=" <<
        viewLatency.last2Minutes.count() << std::endl
        << "uid=all gid=all ns.latencypeak.eosviewmutex.5min=" <<
        viewLatency.last5Minutes.count() << std::endl
        << "uid=all gid=all ns.eosviewmutex.penultimateseclocktimepercent="
        << eosViewMutexPenultimateSecWriteLockTimePercentage << std::endl;

    if (!gOFS->namespaceGroup->isInMemory()) {
      auto* qdb_group = dynamic_cast<eos::QuarkNamespaceGroup*>
                        (gOFS->namespaceGroup.get());
      auto* perf_monitor = dynamic_cast<eos::QClPerfMonitor*>
                           (qdb_group->getPerformanceMonitor().get());
      std::map<std::string, unsigned long long> info = perf_monitor->GetPerfMarkers();
      oss << "uid=all gid=all ns.qclient.persistency_type="
          << qdb_group->getMetadataFlusher()->getPersistencyType() << "\n";

      if (info.find("rtt_min") != info.end()) {
        oss << "uid=all gid=all ns.qclient.rtt_ms.min="
            << info["rtt_min"] / 1000 << std::endl
            << "uid=all gid=all ns.qclient.rtt_ms.avg="
            << info["rtt_avg"] / 1000 << std::endl
            << "uid=all gid=all ns.qclient.rtt_ms.max="
            << info["rtt_max"] / 1000 << std::endl
            << "uid=all gid=all ns.qclient.rtt_ms_peak.1min="
            << info["rtt_peak_1m"] / 1000 << std::endl
            << "uid=all gid=all ns.qclient.rtt_ms_peak.2min="
            << info["rtt_peak_2m"] / 1000 << std::endl
            << "uid=all gid=all ns.qclient.rtt_ms_peak.5min="
            << info["rtt_peak_5m"] / 1000 << std::endl;
      }
    }

    if (pstat.vsize > gOFS->LinuxStatsStartup.vsize) {
      oss << "uid=all gid=all ns.memory.growth=" << (unsigned long long)
          (pstat.vsize - gOFS->LinuxStatsStartup.vsize) << std::endl;
    } else {
      oss << "uid=all gid=all ns.memory.growth=-" << (unsigned long long)
          (-pstat.vsize + gOFS->LinuxStatsStartup.vsize) << std::endl;
    }

    oss << "uid=all gid=all ns.uptime="
        << (int)(time(NULL) - gOFS->mStartTime) << std::endl
        << "uid=all gid=all "
        << gOFS->mDrainEngine.GetThreadPoolInfo() << std::endl
        << "uid=all gid=all "
        << gOFS->mFsckEngine->GetThreadPoolInfo() << std::endl
        << "uid=all gid=all "
        << (gOFS->mConverterEngine ?
            gOFS->mConverterEngine->GetThreadPoolInfo() :
            "info=\"converter driver not running\"")
        << std::endl;
    FsView::gFsView.DumpBalancerPoolInfo(oss, "uid=all gid=all ");

    // Only display the tape enabled state if it is set to true in order to
    // simplify the disk-only use of EOS
    if (gOFS->mTapeEnabled) {
      oss << "uid=all gid=all ns.tapeenabled=true" << std::endl;
      // GC should only be active on the master MGM node
      oss << "uid=all gid=all tgc.is_active="
          << (gOFS->mTapeGc->isGcActive() ? "true" : "false")
          << std::endl;
      // Tape GC stats are only displayed if enabled for at least one EOS space
      const auto tgcStats = gOFS->mTapeGc->getStats();

      if (!tgcStats.empty()) {
        oss << "uid=all gid=all tgc.stats=evicts";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcSpaceStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcSpaceStats.nbEvicts;
        }

        oss << std::endl;
        oss << "uid=all gid=all tgc.stats=queuesize";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcSpaceStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcSpaceStats.lruQueueSize;
        }

        oss << std::endl;
        oss << "uid=all gid=all tgc.stats=totalbytes";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcStats.spaceStats.totalBytes;
        }

        oss << std::endl;
        oss << "uid=all gid=all tgc.stats=availbytes";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcStats.spaceStats.availBytes;
        }

        oss << std::endl;
        oss << "uid=all gid=all tgc.stats=qrytimestamp";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcSpaceStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcSpaceStats.queryTimestamp;
        }

        oss << std::endl;
      }
    }
  } else {
    std::string line = "# ------------------------------------------------------"
                       "------------------------------";
    oss << line << std::endl
        << "# Namespace Statistics" << std::endl
        << line << std::endl
        << "ALL      Files                            "
        << f << " [" << bootstring << "] (" << fboot_time << "s)" << std::endl
        << "ALL      Directories                      "
        << d <<  std::endl
        << "ALL      Total boot time                  "
        << boot_time << " s" << std::endl
        << "ALL      Contention                       write: "
        << std::fixed << std::setprecision(2) << writecontention << " % read:" <<
        std::fixed << std::setprecision(2) << readcontention << " %" << std::endl
        << line << std::endl;

    if (compact_status.length()) {
      oss << "ALL      Compactification                 "
          << compact_status.c_str() << std::endl
          << line << std::endl;
    }

    oss << "ALL      Replication                      "
        << master_status.c_str() << std::endl;
    oss << line << std::endl;

    if (clfsize.length() && cldsize.length()) {
      oss << "ALL      File Changelog Size              " << clfsize << std::endl
          << "ALL      Dir  Changelog Size              " << cldsize << std::endl
          << line << std::endl
          << "ALL      avg. File Entry Size             " << clfratio << std::endl
          << "ALL      avg. Dir  Entry Size             " << cldratio << std::endl
          << line << std::endl;
    }

    oss << "ALL      files created since boot         "
        << (int)(fid_now - gOFS->mBootFileId) << std::endl
        << "ALL      container created since boot     "
        << (int)(cid_now - gOFS->mBootContainerId) << std::endl
        << line << std::endl
        << "ALL      current file id                  " << fid_now
        << std::endl
        << "ALL      current container id             " << cid_now
        << std::endl
        << line << std::endl
        << "ALL      eosxd caps                       "
        << gOFS->zMQ->gFuseServer.Cap().Dump() << std::endl
        << "ALL      eosxd clients                    " <<
        eosxd_nclients << std::endl
        << "ALL      eosxd active clients             " <<
        eosxd_active_clients << std::endl
        << "ALL      eosxd locked clients             " <<
        eosxd_locked_clients << std::endl
        << line << std::endl;

    if (fileCacheStats.enabled || containerCacheStats.enabled) {
      oss << "ALL      File cache max num               " << fileCacheStats.maxNum
          << std::endl
          << "ALL      File cache occupancy             " << fileCacheStats.occupancy
          << std::endl
          << "ALL      In-flight FileMD                 " << fileCacheStats.inFlight
          << std::endl
          << "ALL      Container cache max num          " << containerCacheStats.maxNum
          << std::endl
          << "ALL      Container cache occupancy        " << containerCacheStats.occupancy
          << std::endl
          << "ALL      In-flight ContainerMD            " << containerCacheStats.inFlight
          << std::endl
          << line << std::endl;
    }

    oss << "ALL      eosViewRWMutex status            " <<
        (gOFS->mViewMutexWatcher.isLockedUp() ? "locked-up" : "available")
        << " (" << gOFS->mViewMutexWatcher.hangingSince() << "s) " << std::endl;
    oss << "ALL      eosViewRWMutex peak-latency      " << viewLatency.last.count()
        << "ms (last) "
        << viewLatency.lastMinute.count() << "ms (1 min) " <<
        viewLatency.last2Minutes.count() << "ms (2 min) " <<
        viewLatency.last5Minutes.count() << "ms (5 min)"
        << std::endl;
    oss << "ALL      eosViewRWMutex locked for " <<
        eosViewMutexPenultimateSecWriteLockTimePercentage
        << "% of the penultimate second" << std::endl << line << std::endl;

    if (!gOFS->namespaceGroup->isInMemory()) {
      auto* qdb_group = dynamic_cast<eos::QuarkNamespaceGroup*>
                        (gOFS->namespaceGroup.get());
      auto* perf_monitor = dynamic_cast<eos::QClPerfMonitor*>
                           (qdb_group->getPerformanceMonitor().get());
      std::map<std::string, unsigned long long> info = perf_monitor->GetPerfMarkers();
      oss << "ALL      QClient Persistency              "
          << qdb_group->getMetadataFlusher()->getPersistencyType() << "\n";

      if (info.find("rtt_min") != info.end()) {
        oss << "ALL      QClient overall RTT              "
            << info["rtt_min"] / 1000 << "ms (min)  "
            << info["rtt_avg"] / 1000 << "ms (avg)  "
            << info["rtt_max"] / 1000 << "ms (max)  "
            << std::endl
            << "ALL      QClient recent peak RTT          "
            << info["rtt_peak_1m"] / 1000 << "ms (1 min) "
            << info["rtt_peak_2m"] / 1000 << "ms (2 min) "
            << info["rtt_peak_5m"] / 1000 << "ms (5 min)"
            << std::endl  << line << std::endl;
      }
    }

    // Do them one at a time otherwise sizestring is saved only the first time
    oss << "ALL      memory virtual                   "
        << StringConversion::GetReadableSizeString(sizestring, (unsigned long long)
            mem.vmsize, "B")
        << std::endl;
    oss << "ALL      memory resident                  "
        << StringConversion::GetReadableSizeString(sizestring, (unsigned long long)
            mem.resident, "B")
        << std::endl;
    oss << "ALL      memory share                     "
        << StringConversion::GetReadableSizeString(sizestring, (unsigned long long)
            mem.share, "B")
        << std::endl
        << "ALL      memory growths                   ";

    if (pstat.vsize > gOFS->LinuxStatsStartup.vsize) {
      oss << StringConversion::GetReadableSizeString
          (sizestring, (unsigned long long)(pstat.vsize - gOFS->LinuxStatsStartup.vsize),
           "B")
          << std::endl;
    } else {
      oss << StringConversion::GetReadableSizeString
          (sizestring, (unsigned long long)(-pstat.vsize + gOFS->LinuxStatsStartup.vsize),
           "B")
          << std::endl;
    }

    oss << "ALL      threads                          " <<  pstat.threads
        << std::endl
        << "ALL      fds                              " << fds.all
        << std::endl
        << "ALL      uptime                           "
        << (int)(time(NULL) - gOFS->mStartTime) << std::endl
        << line << std::endl
        << "ALL      drain info                       "
        << gOFS->mDrainEngine.GetThreadPoolInfo() << std::endl
        << "ALL      fsck info                        "
        << gOFS->mFsckEngine->GetThreadPoolInfo() << std::endl
        << "ALL      converter info                   "
        << (gOFS->mConverterEngine ?
            gOFS->mConverterEngine->GetThreadPoolInfo() :
            "info=\"converter driver not running\"")
        << std::endl;
    std::string_view prefix {"ALL      balancer info                    "};
    FsView::gFsView.DumpBalancerPoolInfo(oss, prefix);
    oss << line << std::endl
        << gOFS->mFidTracker.PrintStats() << std::endl
        << line << std::endl;

    // Only display the tape enabled state if it is set to true in order to
    // simplify the disk-only use of EOS
    if (gOFS->mTapeEnabled) {
      oss << "ALL      tapeenabled                      true" << std::endl;
      // GC should only be active on the master MGM node
      oss << "ALL      tgc is active                    "
          << (gOFS->mTapeGc->isGcActive() ? "true" : "false")
          << std::endl;
      // Tape GC stats are only displayed if enabled for at least one EOS space
      const auto tgcStats = gOFS->mTapeGc->getStats();

      if (!tgcStats.empty()) {
        oss << "ALL      tgc.stats=evicts             ";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcSpaceStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcSpaceStats.nbEvicts;
        }

        oss << std::endl;
        oss << "ALL      tgc.stats=queuesize             ";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcSpaceStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcSpaceStats.lruQueueSize;
        }

        oss << std::endl;
        oss << "ALL      tgc.stats=totalbytes            ";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcStats.spaceStats.totalBytes;
        }

        oss << std::endl;
        oss << "ALL      tgc.stats=availbytes            ";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcStats.spaceStats.availBytes;
        }

        oss << std::endl;
        oss << "ALL      tgc.stats=qrytimestamp          ";

        for (auto itor = tgcStats.begin(); itor != tgcStats.end(); itor++) {
          const std::string& tgcSpace = itor->first;
          const tgc::TapeGcStats& tgcSpaceStats = itor->second;
          oss << " " << tgcSpace << "=" << tgcSpaceStats.queryTimestamp;
        }

        oss << std::endl;
      }

      oss << line << std::endl;
    }
  }

  if (!stat.summary()) {
    XrdOucString stats_out;
    gOFS->MgmStats.PrintOutTotal(stats_out, stat.groupids(), monitoring,
                                 stat.numericids(), stat.apps());
    oss << stats_out.c_str();
  }

  oss << gOFS->mTracker.PrintOut(monitoring);

  if (WantsJsonOutput()) {
    std::string out = ResponseToJsonString(oss.str(), err.str(), retc);
    oss.clear(), oss.str(out);
  } else {
    if (!monitoring && (mReqProto.dontcolor() == false)) {
      std::string out = oss.str();
      oss.clear();
      TextHighlight(out);
      oss.str(out);
    }
  }

  reply.set_retc(retc);
  reply.set_std_out(oss.str());
  reply.set_std_err(err.str());
}

//------------------------------------------------------------------------------
// Execute master command
//------------------------------------------------------------------------------
void
NsCmd::MasterSubcmd(const eos::console::NsProto_MasterProto& master,
                    eos::console::ReplyProto& reply)
{
  using eos::console::NsProto_MasterProto;

  if (master.op() == NsProto_MasterProto::DISABLE) {
    reply.set_std_err("error: operation deprecated");
    reply.set_retc(ENOTSUP);
  } else if (master.op() == NsProto_MasterProto::ENABLE) {
    reply.set_std_err("error: operation deprecated");
    reply.set_retc(ENOTSUP);
  } else if (master.op() == NsProto_MasterProto::LOG) {
    std::string out;
    gOFS->mMaster->GetLog(out);
    reply.set_std_out(out.c_str());
  } else if (master.op() == NsProto_MasterProto::LOG_CLEAR) {
    gOFS->mMaster->ResetLog();
    reply.set_std_out("success: cleaned the master log");
  } else if (master.host().length()) {
    std::string out, err;

    if (!gOFS->mMaster->SetMasterId(master.host(), 1094, err)) {
      reply.set_std_err(err.c_str());
      reply.set_retc(EIO);
    } else {
      out += "success: current master will step down\n";
      reply.set_std_out(out.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Execute compact command
//------------------------------------------------------------------------------
void
NsCmd::CompactSubcmd(const eos::console::NsProto_CompactProto& compact,
                     eos::console::ReplyProto& reply)
{
  reply.set_std_err("error: operation supported by master object");
  reply.set_retc(ENOTSUP);
  return;
}

//------------------------------------------------------------------------------
// Execute tree size recompute command
//------------------------------------------------------------------------------
void
NsCmd::TreeSizeSubcmd(const eos::console::NsProto_TreeSizeProto& tree,
                      eos::console::ReplyProto& reply)
{
  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);
  std::shared_ptr<IContainerMD> cont;

  try {
    cont = eos::Resolver::resolveContainer(gOFS->eosView, tree.container());
  } catch (const eos::MDException& e) {
    reply.set_std_err(SSTR(e.what()));
    reply.set_retc(e.getErrno());
    return;
  }

  if (cont == nullptr) {
    reply.set_std_err("error: container not found");
    reply.set_retc(ENOENT);
    return;
  }

  std::shared_ptr<eos::IContainerMD> tmp_cont {nullptr};
  std::list< std::list<eos::IContainerMD::id_t> > bfs =
    BreadthFirstSearchContainers(cont.get(), tree.depth());

  for (auto it_level = bfs.crbegin(); it_level != bfs.crend(); ++it_level) {
    for (const auto& id : *it_level) {
      try {
        tmp_cont = gOFS->eosDirectoryService->getContainerMD(id);
      } catch (const eos::MDException& e) {
        eos_err("error=\"%s\"", e.what());
        continue;
      }

      UpdateTreeSize(tmp_cont);
    }
  }
}

//------------------------------------------------------------------------------
// Execute quota size recompute command
//------------------------------------------------------------------------------
void
NsCmd::QuotaSizeSubcmd(const eos::console::NsProto_QuotaSizeProto& tree,
                       eos::console::ReplyProto& reply)
{
  std::string cont_uri {""};
  eos::IContainerMD::id_t cont_id {0ull};
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
    std::shared_ptr<IContainerMD> cont {nullptr};

    try {
      cont = eos::Resolver::resolveContainer(gOFS->eosView, tree.container());
    } catch (const eos::MDException& e) {
      reply.set_std_err(SSTR(e.what()));
      reply.set_retc(e.getErrno());
      return;
    }

    if ((cont->getFlags() & eos::QUOTA_NODE_FLAG) == 0) {
      reply.set_std_err("error: directory is not a quota node");
      reply.set_retc(EINVAL);
      return;
    }

    cont_uri = gOFS->eosView->getUri(cont.get());
    cont_id = cont->getId();
  }
  // Recompute the quota node
  QuotaNodeCore qnc;
  bool update = false;

  if (tree.used_bytes() || tree.used_inodes()) {
    QuotaNodeCore::UsageInfo usage;
    usage.space = tree.used_bytes();
    usage.physicalSpace = tree.physical_bytes();
    usage.files = tree.used_inodes();

    if (tree.uid().size() && !tree.gid().size()) {
      // set by user
      qnc.setByUid(strtoul(tree.uid().c_str(), 0, 10), usage);
    } else if (tree.gid().size() && !tree.uid().size())  {
      // set by group
      qnc.setByGid(strtoul(tree.gid().c_str(), 0, 10), usage);
    } else {
      reply.set_std_err("error: to overwrite quota you have to set a user or group id - never both");
      reply.set_retc(EINVAL);
      return;
    }

    update = true;
  } else {
    if (gOFS->eosView->inMemory()) {
      reply.set_std_err("error: quota recomputation is only available for "
                        "QDB namespace");
      reply.set_retc(EINVAL);
      return;
    }

    std::unique_ptr<qclient::QClient> qcl =
      std::make_unique<qclient::QClient>(gOFS->mQdbContactDetails.members,
                                         gOFS->mQdbContactDetails.constructOptions());
    eos::QuotaRecomputer recomputer(qcl.get(),
                                    static_cast<QuarkNamespaceGroup*>(gOFS->namespaceGroup.get())->getExecutor());
    eos::MDStatus status = recomputer.recompute(cont_uri, cont_id, qnc);

    if (!status.ok()) {
      reply.set_std_err(status.getError());
      reply.set_retc(status.getErrno());
      return;
    }

    // Remove all the entries, which should not be updated if any uid/gid
    // specified.
    if (tree.uid().size() || tree.gid().size()) {
      qnc.filterByUid(strtoul(tree.uid().c_str(), 0, 10));
      qnc.filterByGid(strtoul(tree.gid().c_str(), 0, 10));
      update = true;
    }
  }

  // Update the quota note
  try {
    eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);
    auto cont = gOFS->eosDirectoryService->getContainerMD(cont_id);

    if ((cont->getFlags() & eos::QUOTA_NODE_FLAG) == 0) {
      eos_err("msg=\"quota recomputation failed, directory is not (anymore) a "
              "quota node\" cxid=%08llx path=\"%s\"", cont_id, cont_uri.c_str());
      reply.set_std_err("error: directory is not a quota node (anymore)");
      reply.set_retc(EINVAL);
      return;
    }

    eos::IQuotaNode* quotaNode = gOFS->eosView->getQuotaNode(cont.get());

    if (update) {
      quotaNode->updateCore(qnc);
      eos_info("msg=\"quota update successful\" cxid=%08llx path=\"%s\"",
               cont_id, cont_uri.c_str());
    } else {
      quotaNode->replaceCore(qnc);
      eos_info("msg=\"quota recomputation successful\" cxid=%08llx path=\"%s\"",
               cont_id, cont_uri.c_str());
    }
  } catch (const eos::MDException& e) {
    eos_err("msg=\"quota recomputation failed, directory removed\" "
            "cxid=%08llx path=\"%s\"", cont_id, cont_uri.c_str());
    reply.set_std_err(SSTR(e.what()));
    reply.set_retc(e.getErrno());
    return;
  }

  reply.set_retc(0);
  return;
}

//------------------------------------------------------------------------------
// Recompute and update tree size of the given container assuming its
// subcontainers tree size values are correct and adding the size of files
// attached directly to the current container
//------------------------------------------------------------------------------
void
NsCmd::UpdateTreeSize(eos::IContainerMDPtr cont) const
{
  eos_debug("cont name=%s, id=%llu", cont->getName().c_str(), cont->getId());
  std::shared_ptr<eos::IFileMD> tmp_fmd {nullptr};
  std::shared_ptr<eos::IContainerMD> tmp_cont {nullptr};
  uint64_t tree_size = 0u;
  uint64_t tree_containers = 0u;
  uint64_t tree_files = 0u;

  for (auto fit = FileMapIterator(cont); fit.valid(); fit.next()) {
    try {
      tmp_fmd = gOFS->eosFileService->getFileMD(fit.value());
    } catch (const eos::MDException& e) {
      eos_err("error=\"%s\"", e.what());
      continue;
    }

    tree_size += tmp_fmd->getSize();
    tree_files += 1;
  }

  for (auto cit = ContainerMapIterator(cont); cit.valid(); cit.next()) {
    try {
      tmp_cont = gOFS->eosDirectoryService->getContainerMD(cit.value());
    } catch (const eos::MDException& e) {
      eos_err("error=\"%s\"", e.what());
      continue;
    }

    tree_size += tmp_cont->getTreeSize();
    tree_containers += tmp_cont->getTreeContainers() +
                       1; //Count the current cont' children + the subChildren (getDirCount())
    tree_files += tmp_cont->getTreeFiles();
  }

  cont->setTreeSize(tree_size);
  cont->setTreeFiles(tree_files);
  cont->setTreeContainers(tree_containers);
  gOFS->eosDirectoryService->updateStore(cont.get());
  gOFS->FuseXCastRefresh(cont->getIdentifier(), cont->getParentIdentifier());
}

//------------------------------------------------------------------------------
// Execute cache update command
//------------------------------------------------------------------------------
void
NsCmd::CacheSubcmd(const eos::console::NsProto_CacheProto& cache,
                   eos::console::ReplyProto& reply)
{
  using namespace eos::constants;
  using eos::console::NsProto_CacheProto;
  std::map<std::string, std::string> map_cfg;

  if (cache.op() == NsProto_CacheProto::SET_FILE) {
    if (cache.max_num() > 100) {
      map_cfg[sMaxNumCacheFiles] = std::to_string(cache.max_num());
      map_cfg[sMaxSizeCacheFiles] = std::to_string(cache.max_size());
      gOFS->mConfigEngine->SetConfigValue("ns", "cache-size-nfiles",
                                          std::to_string(cache.max_num()).c_str());
      gOFS->eosFileService->configure(map_cfg);
    }
  } else if (cache.op() == NsProto_CacheProto::SET_DIR) {
    if (cache.max_num() > 100) {
      map_cfg[sMaxNumCacheDirs] = std::to_string(cache.max_num());
      map_cfg[sMaxSizeCacheDirs] = std::to_string(cache.max_size());
      gOFS->mConfigEngine->SetConfigValue("ns", "cache-size-ndirs",
                                          std::to_string(cache.max_num()).c_str());
      gOFS->eosDirectoryService->configure(map_cfg);
    }
  } else if (cache.op() == NsProto_CacheProto::DROP_FILE) {
    map_cfg[sMaxNumCacheFiles] = std::to_string(UINT64_MAX);
    map_cfg[sMaxSizeCacheFiles] = std::to_string(UINT64_MAX);
    gOFS->eosFileService->configure(map_cfg);
  } else if (cache.op() == NsProto_CacheProto::DROP_DIR) {
    map_cfg[sMaxNumCacheDirs] = std::to_string(UINT64_MAX);
    map_cfg[sMaxSizeCacheDirs] = std::to_string(UINT64_MAX);
    gOFS->eosDirectoryService->configure(map_cfg);
  } else if (cache.op() == NsProto_CacheProto::DROP_ALL) {
    map_cfg[sMaxNumCacheFiles] = std::to_string(UINT64_MAX);
    map_cfg[sMaxSizeCacheFiles] = std::to_string(UINT64_MAX);
    map_cfg[sMaxNumCacheDirs] = std::to_string(UINT64_MAX);
    map_cfg[sMaxSizeCacheDirs] = std::to_string(UINT64_MAX);
    gOFS->eosFileService->configure(map_cfg);
    gOFS->eosDirectoryService->configure(map_cfg);
  } else if (cache.op() == NsProto_CacheProto::DROP_SINGLE_FILE) {
    bool found = gOFS->eosFileService->dropCachedFileMD(FileIdentifier(
                   cache.single_to_drop()));
    reply.set_retc(!found);
  } else if (cache.op() == NsProto_CacheProto::DROP_SINGLE_CONTAINER) {
    bool found = gOFS->eosDirectoryService->dropCachedContainerMD(
                   ContainerIdentifier(cache.single_to_drop()));
    reply.set_retc(!found);
  }
}

//------------------------------------------------------------------------------
// Do a breadth first search of all the subcontainers under the given
// container
//------------------------------------------------------------------------------
std::list< std::list<eos::IContainerMD::id_t> >
NsCmd::BreadthFirstSearchContainers(eos::IContainerMD* cont,
                                    uint32_t max_depth) const
{
  uint32_t num_levels = 0u;
  std::shared_ptr<eos::IContainerMD> tmp_cont;
  std::list< std::list<eos::IContainerMD::id_t> > depth(256);
  auto it_lvl = depth.begin();
  it_lvl->push_back(cont->getId());

  while (it_lvl->size() && (it_lvl != depth.end())) {
    auto it_next_lvl = it_lvl;
    ++it_next_lvl;

    for (const auto& cid : *it_lvl) {
      try {
        tmp_cont = gOFS->eosDirectoryService->getContainerMD(cid);
      } catch (const eos::MDException& e) {
        // ignore error
        eos_err("error=\"%s\"", e.what());
        continue;
      }

      for (auto subcont_it = ContainerMapIterator(tmp_cont); subcont_it.valid();
           subcont_it.next()) {
        it_next_lvl->push_back(subcont_it.value());
      }
    }

    it_lvl = it_next_lvl;
    ++num_levels;

    if (max_depth && (num_levels == max_depth)) {
      break;
    }
  }

  depth.resize(num_levels);
  return depth;
}

//------------------------------------------------------------------------------
// Update the maximum size of the thread pool used for drain jobs
//------------------------------------------------------------------------------
void
NsCmd::DrainSubcmd(const eos::console::NsProto_DrainProto& drain,
                   eos::console::ReplyProto& reply)
{
  using eos::console::NsProto_DrainProto;

  if (drain.op() == NsProto_DrainProto::LIST) {
    if (gOFS) {
      reply.set_std_out(gOFS->mDrainEngine.SerializeConfig());
    }
  } else if (drain.op() == NsProto_DrainProto::SET) {
    if (drain.key().empty() || drain.value().empty()) {
      reply.set_std_err("error: both key and value need to be specified");
      reply.set_retc(EINVAL);
      return;
    } else {
      if (gOFS) {
        if (!gOFS->mDrainEngine.SetConfig(drain.key(), drain.value())) {
          reply.set_std_err("error: failed applying drainer configuration");
          reply.set_retc(EINVAL);
          return;
        }
      }
    }
  } else {
    reply.set_std_err("error: unknown drainer operation");
    reply.set_retc(EINVAL);
    return;
  }

  reply.set_retc(0);
  return;
}

//------------------------------------------------------------------------------
// Execute reserve ids command
//------------------------------------------------------------------------------
void
NsCmd::ReserveIdsSubCmd(const eos::console::NsProto_ReserveIdsProto& reserve,
                        eos::console::ReplyProto& reply)
{
  if (reserve.fileid() > 0) {
    gOFS->eosFileService->blacklistBelow(FileIdentifier(reserve.fileid()));
  }

  if (reserve.containerid() > 0) {
    gOFS->eosDirectoryService->blacklistBelow(ContainerIdentifier(
          reserve.containerid()));
  }
}

//------------------------------------------------------------------------------
// Execute benchmark command
//------------------------------------------------------------------------------
void
NsCmd::BenchmarkSubCmd(const eos::console::NsProto_BenchmarkProto& benchmark,
                       eos::console::ReplyProto& reply)
{
  size_t n_threads = (benchmark.threads() < 1024) ? benchmark.threads() : 1024;
  size_t n_subdirs = benchmark.subdirs();
  size_t n_subfiles = benchmark.subfiles();
  std::string bench_prefix = benchmark.prefix();
  eos_static_info("msg=\"runing benchmark\" nthreads=%lu ndirs=%lu nfiles=%lu",
                  n_threads,
                  n_subdirs,
                  n_subfiles);
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo error;
  std::string prefix = bench_prefix + "/benchmark/";
  std::stringstream oss;
  {
    eos::common::Timing bench("Benchmark");
    COMMONTIMING("START", &bench);
    std::vector<std::thread> workers;
    //pass 1 - create dir structure
    gOFS->_mkdir(prefix.c_str(), 0777,  error, vid, "", 0, false);

    for (size_t i = 0; i < n_threads; i++) {
      workers.push_back(std::thread([i, n_subdirs, n_subfiles, &vid, prefix]() {
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        std::string wdir = prefix + std::string("worker.") + std::to_string(i);
        XrdSecEntity client("sss");
        client.tident = "benchmark";
        gOFS->_mkdir(wdir.c_str(), 0777, error, vid, "", 0, false);

        for (size_t d = 0 ; d < n_subdirs ; d++) {
          std::string sdir = wdir + std::string("/d.") + std::to_string(
                               d) + std::string("/");
          gOFS->_mkdir(sdir.c_str(), 0777,  error, vid, "", 0, false);

          for (size_t f = 0 ; f < n_subfiles ; f++) {
            std::string fname = sdir + std::string("f.") + std::to_string(f);
          }
        }
      }));
    }

    std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
      t.join();
    });
    COMMONTIMING("STOP", &bench);
    double rt = bench.RealTime() / 1000.0;
    const char* l =  eos_static_log(LOG_SILENT,
                                    "[   mkdir     ] dirs=%lu time=%.02f dir-rate=%.02f", n_threads * n_subdirs, rt,
                                    n_threads * n_subdirs / rt);
    oss << l;
    oss << std::endl;
    eos_static_notice(l);
  }
  {
    eos::common::Timing bench("Benchmark");
    COMMONTIMING("START", &bench);
    std::vector<std::thread> workers;

    //pass 2 - create files
    for (size_t i = 0; i < n_threads; i++) {
      workers.push_back(std::thread([i, n_subdirs, n_subfiles, &vid, prefix]() {
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        std::string wdir = prefix + std::string("worker.") + std::to_string(i);
        XrdSecEntity client("sss");
        client.tident = "benchmark";

        for (size_t d = 0 ; d < n_subdirs ; d++) {
          std::string sdir = wdir + std::string("/d.") + std::to_string(
                               d) + std::string("/");

          for (size_t f = 0 ; f < n_subfiles ; f++) {
            std::string fname = sdir + std::string("f.") + std::to_string(f);
            XrdOucEnv env;
            XrdMgmOfsFile* file = new XrdMgmOfsFile((char*)"bench");

            if (file) {
              file->open(&vid, fname.c_str(), SFS_O_CREAT | SFS_O_RDWR, 0777, 0,
                         "eos.app=fuse&eos.bookingsize=0");
              delete file;
            }
          }
        }
      }));
    }

    std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
      t.join();
    });
    COMMONTIMING("STOP", &bench);
    double rt = bench.RealTime() / 1000.0;
    const char* l =  eos_static_log(LOG_SILENT,
                                    "[   create    ] files=%lu time=%.02f file-rate=%.02f Hz",
                                    n_threads * n_subdirs * n_subfiles, rt,
                                    1.0 * n_threads * n_subdirs * n_subfiles / rt);
    oss << l;
    oss << std::endl;
    eos_static_notice(l);
  }
  {
    eos::common::Timing bench("Benchmark");
    COMMONTIMING("START", &bench);
    std::vector<std::thread> workers;
    //pass 3 - exists structure
    gOFS->_mkdir(prefix.c_str(), 0777,  error, vid, "", 0, false);

    for (size_t i = 0; i < n_threads; i++) {
      workers.push_back(std::thread([i, n_subdirs, n_subfiles, &vid, prefix]() {
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        std::string wdir = prefix + std::string("worker.") + std::to_string(i);
        XrdSecEntity client("sss");
        client.tident = "benchmark";
        gOFS->_mkdir(wdir.c_str(), 0777, error, vid, "", 0, false);

        for (size_t d = 0 ; d < n_subdirs ; d++) {
          std::string sdir = wdir + std::string("/d.") + std::to_string(
                               d) + std::string("/");
          gOFS->_mkdir(sdir.c_str(), 0777,  error, vid, "", 0, false);

          for (size_t f = 0 ; f < n_subfiles ; f++) {
            std::string fname = sdir + std::string("f.") + std::to_string(f);
            XrdOucEnv env;
            XrdMgmOfsFile* file = new XrdMgmOfsFile((char*)"bench");

            if (file) {
              file->open(&vid, fname.c_str(), SFS_O_CREAT | SFS_O_RDWR, 0777, 0, 0);
              delete file;
            }
          }
        }
      }));
    }

    std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
      t.join();
    });
    COMMONTIMING("STOP", &bench);
    double rt = bench.RealTime() / 1000.0;
    const char* l =  eos_static_log(LOG_SILENT,
                                    "[   exists    ] files=%lu dirs=%lu time=%.02f dir-rate=%.02f file-rate=%.02f Hz",
                                    n_threads * n_subdirs * n_subfiles, n_threads * n_subdirs, rt,
                                    1.0 * n_threads * n_subdirs / rt,
                                    1.0 * n_threads * n_subdirs * n_subfiles / rt);
    oss << l;
    oss << std::endl;
    eos_static_notice(l);
  }
  {
    eos::common::Timing bench("Benchmark");
    COMMONTIMING("START", &bench);
    std::vector<std::thread> workers;

    //pass 4 - open files for reading
    for (size_t i = 0; i < n_threads; i++) {
      workers.push_back(std::thread([i, n_subdirs, n_subfiles, &vid, prefix]() {
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        std::string wdir = prefix + std::string("worker.") + std::to_string(i);
        XrdSecEntity client("sss");
        client.tident = "benchmark";

        for (size_t d = 0 ; d < n_subdirs ; d++) {
          std::string sdir = wdir + std::string("/d.") + std::to_string(
                               d) + std::string("/");

          for (size_t f = 0 ; f < n_subfiles ; f++) {
            std::string fname = sdir + std::string("f.") + std::to_string(f);
            XrdOucEnv env;
            XrdMgmOfsFile* file = new XrdMgmOfsFile((char*)"bench");

            if (file) {
              file->open(&vid, fname.c_str(), 0, 0, 0, "eos.app=fuse");
              delete file;
            }
          }
        }
      }));
    }

    std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
      t.join();
    });
    COMMONTIMING("STOP", &bench);
    double rt = bench.RealTime() / 1000.0;
    const char* l =  eos_static_log(LOG_SILENT,
                                    "[   read      ] files=%lu time=%.02f file-rate=%.02f Hz",
                                    n_threads * n_subdirs * n_subfiles, rt,
                                    1.0 * n_threads * n_subdirs * n_subfiles / rt);
    oss << l;
    oss << std::endl;
    eos_static_notice(l);
  }
  {
    eos::common::Timing bench("Benchmark");
    COMMONTIMING("START", &bench);
    std::vector<std::thread> workers;

    //pass 5 - open files for writing
    for (size_t i = 0; i < n_threads; i++) {
      workers.push_back(std::thread([i, n_subdirs, n_subfiles, &vid, prefix]() {
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        std::string wdir = prefix + std::string("worker.") + std::to_string(i);
        XrdSecEntity client("sss");
        client.tident = "benchmark";

        for (size_t d = 0 ; d < n_subdirs ; d++) {
          std::string sdir = wdir + std::string("/d.") + std::to_string(
                               d) + std::string("/");

          for (size_t f = 0 ; f < n_subfiles ; f++) {
            std::string fname = sdir + std::string("f.") + std::to_string(f);
            XrdOucEnv env;
            XrdMgmOfsFile* file = new XrdMgmOfsFile((char*)"bench");

            if (file) {
              file->open(&vid, fname.c_str(), SFS_O_RDWR, 0777, 0,
                         "eos.app=fuse&eos.bookingsize=0");
              delete file;
            }
          }
        }
      }));
    }

    std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
      t.join();
    });
    COMMONTIMING("STOP", &bench);
    double rt = bench.RealTime() / 1000.0;
    const char* l =  eos_static_log(LOG_SILENT,
                                    "[   write     ] files=%lu time=%.02f file-rate=%.02f Hz",
                                    n_threads * n_subdirs * n_subfiles, rt,
                                    1.0 * n_threads * n_subdirs * n_subfiles / rt);
    oss << l;
    oss << std::endl;
    eos_static_notice(l);
  }
  {
    eos::common::Timing bench("Benchmark");
    COMMONTIMING("START", &bench);
    std::vector<std::thread> workers;

    //pass 6 - delete structure
    for (size_t i = 0; i < n_threads; i++) {
      workers.push_back(std::thread([i, n_subdirs, n_subfiles, &vid, prefix]() {
        eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
        XrdOucErrInfo error;
        std::string wdir = prefix + std::string("worker.") + std::to_string(i);
        XrdSecEntity client("sss");
        client.tident = "benchmark";

        for (size_t d = 0 ; d < n_subdirs ; d++) {
          std::string sdir = wdir + std::string("/d.") + std::to_string(
                               d) + std::string("/");

          for (size_t f = 0 ; f < n_subfiles ; f++) {
            std::string fname = sdir + std::string("f.") + std::to_string(f);
            gOFS->_rem(fname.c_str(), error, vid, "");
          }

          gOFS->_remdir(sdir.c_str(), error, vid);
        }

        gOFS->_remdir(wdir.c_str(), error, vid);
      }));
    }

    std::for_each(workers.begin(), workers.end(), [](std::thread & t) {
      t.join();
    });
    gOFS->_remdir(prefix.c_str(), error, vid);
    COMMONTIMING("STOP", &bench);
    double rt = bench.RealTime() / 1000.0;
    const char* l =  eos_static_log(LOG_SILENT,
                                    "[   deletion  ] files=%lu dirs=%lu time=%.02f dir-rate=%.02f file-rate=%.02f Hz",
                                    n_threads * n_subdirs * n_subfiles, n_threads * n_subdirs, rt,
                                    1.0 * n_threads * n_subdirs / rt,
                                    1.0 * n_threads * n_subdirs * n_subfiles / rt);
    oss << l;
    oss << std::endl;
    eos_static_notice(l);
  }
  reply.set_retc(0);
  reply.set_std_out(oss.str().c_str());
}


//------------------------------------------------------------------------------
// Execute tracker command
//----------------------------------------------------------------------------
void
NsCmd::TrackerSubCmd(const eos::console::NsProto_TrackerProto& tracker,
                     eos::console::ReplyProto& reply)
{
  using eos::console::NsProto_TrackerProto;

  if (tracker.op() == NsProto_TrackerProto::NONE) {
    reply.set_std_err("error: no tracker operation specified");
    reply.set_retc(EINVAL);
    return;
  }

  const eos::mgm::TrackerType tt =
    gOFS->mFidTracker.StringToTrackerType(tracker.name());
  std::string output;

  if (tracker.op() == NsProto_TrackerProto::LIST) {
    output = gOFS->mFidTracker.PrintStats(true, true, tt);
  } else if (tracker.op() == NsProto_TrackerProto::CLEAR) {
    gOFS->mFidTracker.Clear(tt);
    output = "info: tracker successfully cleaned";
  } else {
    reply.set_std_err("error: unknown operation type");
    reply.set_retc(EINVAL);
    return;
  }

  reply.set_std_out(output);
  reply.set_retc(0);
  return;
}

//------------------------------------------------------------------------------
// Execute behaviour command
//------------------------------------------------------------------------------
void
NsCmd::BehaviourSubCmd(const eos::console::NsProto_BehaviourProto& behaviour,
                       eos::console::ReplyProto& reply)
{
  using eos::common::BehaviourConfig;
  using eos::console::NsProto_BehaviourProto;

  switch (behaviour.op()) {
  case NsProto_BehaviourProto::LIST: {
    auto map_behaviours = gOFS->mBehaviourCfg->List();
    std::ostringstream oss;

    for (const auto& elem : map_behaviours) {
      oss << elem.first << " => " << elem.second;
    }

    reply.set_std_out(oss.str());
    break;
  }

  case NsProto_BehaviourProto::SET: {
    auto btype = BehaviourConfig::ConvertStringToBehaviour(behaviour.name());

    if ((btype == eos::common::BehaviourType::None) ||
        (btype == eos::common::BehaviourType::All)) {
      reply.set_std_err("error: unkown behaviour type");
      reply.set_retc(EINVAL);
    } else {
      if (gOFS->mBehaviourCfg->Set(btype, behaviour.value())) {
        reply.set_std_out("info: behaviour set successfully");
      } else {
        reply.set_std_err("error: operation failed, check accepted "
                          "config values");
        reply.set_retc(EINVAL);
      }
    }

    break;
  }

  case NsProto_BehaviourProto::GET: {
    auto btype = BehaviourConfig::ConvertStringToBehaviour(behaviour.name());

    if ((btype == eos::common::BehaviourType::None) ||
        (btype == eos::common::BehaviourType::All)) {
      reply.set_std_err("error: unkown behaviour type");
      reply.set_retc(EINVAL);
    } else {
      if (gOFS->mBehaviourCfg->Exists(btype)) {
        std::string val = gOFS->mBehaviourCfg->Get(btype);
        reply.set_std_out(SSTR("behaviour=\"" << behaviour.name() << "\""
                               << " value=\"" << val << "\""));
      } else {
        reply.set_std_err("error: no such behaviour configured");
        reply.set_retc(EINVAL);
      }
    }

    break;
  }

  case NsProto_BehaviourProto::CLEAR: {
    auto btype = BehaviourConfig::ConvertStringToBehaviour(behaviour.name());

    if (btype == eos::common::BehaviourType::None) {
      reply.set_std_err("error: unkown behaviour type");
      reply.set_retc(EINVAL);
    } else {
      gOFS->mBehaviourCfg->Clear(btype);
      reply.set_std_out("info: behaviour(s) cleared successfully");
    }

    break;
  }

  default: {
    reply.set_std_err("error: unknown behaviour subcommand");
    reply.set_retc(EINVAL);
    break;
  }
  }

  return;
}

//------------------------------------------------------------------------------
// Apply text highlighting to ns output
//------------------------------------------------------------------------------
void
NsCmd::TextHighlight(std::string& text) const
{
  XrdOucString tmp = text.c_str();
  // Color replacements
  tmp.replace("[booted]", "\033[1m[booted]\033[0m");
  tmp.replace("[down]", "\033[49;31m[down]\033[0m");
  tmp.replace("[failed]", "\033[49;31m[failed]\033[0m");
  tmp.replace("[booting]", "\033[49;32m[booting]\033[0m");
  tmp.replace("[compacting]", "\033[49;34m[compacting]\033[0m");
  // Replication highlighting
  tmp.replace("master-rw", "\033[49;31mmaster-rw\033[0m");
  tmp.replace("master-ro", "\033[49;34mmaster-ro\033[0m");
  tmp.replace("slave-ro", "\033[1mslave-ro\033[0m");
  tmp.replace("=ok", "=\033[49;32mok\033[0m");
  tmp.replace("=compacting", "=\033[49;32mcompacting\033[0m");
  tmp.replace("=off", "=\033[49;34moff\033[0m");
  tmp.replace("=blocked", "=\033[49;34mblocked\033[0m");
  tmp.replace("=wait", "=\033[49;34mwait\033[0m");
  tmp.replace("=starting", "=\033[49;34mstarting\033[0m");
  tmp.replace("=true", "=\033[49;32mtrue\033[0m");
  tmp.replace("=false", "=\033[49;31mfalse\033[0m");
  text = tmp.c_str();
}


EOSMGMNAMESPACE_END
