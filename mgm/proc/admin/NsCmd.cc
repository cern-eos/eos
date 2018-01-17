//------------------------------------------------------------------------------
//! @file NsCmd.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "NsCmd.hh"
#include "common/LinuxMemConsumption.hh"
#include "common/LinuxStat.hh"
#include "namespace/interface/IChLogFileMDSvc.hh"
#include "namespace/interface/IChLogContainerMDSvc.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/Stat.hh"
#include "mgm/Master.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Method implementing the specific behvior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
NsCmd::ProcessRequest()
{
  eos::console::ReplyProto reply;
  eos::console::NsProto ns = mReqProto.ns();
  eos::console::NsProto::SubcmdCase subcmd = ns.subcmd_case();

  if (subcmd == eos::console::NsProto::kStat) {
    reply.set_std_out(StatSubcmd(ns.stat()));
  } else if (subcmd == eos::console::NsProto::kMutex) {
    MutexSubcmd(ns.mutex(), reply);
  } else if (subcmd == eos::console::NsProto::kCompact) {
    CompactSubcmd(ns.compact(), reply);
  } else if (subcmd == eos::console::NsProto::kMaster) {
    MasterSubcmd(ns.master(), reply);
  } else if (subcmd == eos::console::NsProto::kTree) {
    TreeSizeSubcmd(ns.tree(), reply);
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
  if (mVid.uid == 0) {
    bool no_option = true;
    std::ostringstream oss;

    if (mutex.sample_rate1() || mutex.sample_rate10() ||
        mutex.sample_rate100() || mutex.toggle_timing() ||
        mutex.toggle_order()) {
      no_option = false;
    }

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
          << "timing         is : "
          << (FsView::gFsView.ViewMutex.GetTiming() ? "on " : "off")
          << " (estimated timing latency for 1 lock ";
      size_t timinglatency = eos::common::RWMutex::GetTimingLatency();
      oss << timinglatency << " nsec / "
          << int(double(timinglatency) / cycleperiod * 100)
          << "% of the mutex lock/unlock cycle duration)" << std::endl
          << "sampling rate  is : ";
      float sr = FsView::gFsView.ViewMutex.GetSampling();
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
    }

    if (mutex.toggle_timing()) {
      if (FsView::gFsView.ViewMutex.GetTiming()) {
        FsView::gFsView.ViewMutex.SetTiming(false);
        Quota::pMapMutex.SetTiming(false);
        gOFS->eosViewRWMutex.SetTiming(false);
        oss << "mutex timing is off" << std::endl;
      } else {
        FsView::gFsView.ViewMutex.SetTiming(true);
        Quota::pMapMutex.SetTiming(true);
        gOFS->eosViewRWMutex.SetTiming(true);
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

      FsView::gFsView.ViewMutex.SetSampling(true, rate);
      Quota::pMapMutex.SetSampling(true, rate);
      gOFS->eosViewRWMutex.SetSampling(true, rate);
    }

    reply.set_std_out(oss.str());
  } else {
    reply.set_std_err("error: you have to take role 'root' to execute this"
                      " command");
    reply.set_retc(EPERM);
  }
}

//------------------------------------------------------------------------------
// Execute stat comand
//------------------------------------------------------------------------------
std::string
NsCmd::StatSubcmd(const eos::console::NsProto_StatProto& stat)
{
  using eos::common::StringConversion;
  std::ostringstream oss;

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
  if ((!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &statf)) &&
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

  XrdOucString bootstring;
  time_t boottime = 0;
  {
    XrdSysMutexHelper lock(gOFS->InitializationMutex);
    bootstring = gOFS->gNameSpaceState[gOFS->Initialized];

    if (bootstring == "booting") {
      boottime = time(NULL) - gOFS->InitializationTime;
    } else {
      boottime = gOFS->InitializationTime;
    }
  }
  // Statistics for memory usage
  eos::common::LinuxMemConsumption::linux_mem_t mem;

  if (!eos::common::LinuxMemConsumption::GetMemoryFootprint(mem)) {
    oss << "error: failed to get the memory usage information" << std::endl;
  }

  eos::common::LinuxStat::linux_stat_t pstat;

  if (!eos::common::LinuxStat::GetStat(pstat)) {
    oss << "error: failed to get the process stat information" << std::endl;
  }

  int64_t latencyf = 0, latencyd = 0, latencyp = 0;
  auto chlog_file_svc = dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);
  auto chlog_dir_svc = dynamic_cast<eos::IChLogContainerMDSvc*>
                       (gOFS->eosDirectoryService);

  if (chlog_file_svc && chlog_dir_svc) {
    latencyf = statf.st_size - chlog_file_svc->getFollowOffset();
    latencyd = statd.st_size - chlog_dir_svc->getFollowOffset();
    latencyp = chlog_file_svc->getFollowPending();
  }

  XrdOucString compact_status = "", master_status = "";
  gOFS->MgmMaster.PrintOutCompacting(compact_status);
  gOFS->MgmMaster.PrintOut(master_status);

  if (stat.monitor()) {
    oss << "uid=all gid=all ns.total.files=" << f << std::endl
        << "uid=all gid=all ns.total.directories=" << d << std::endl
        << "uid=all gid=all ns.current.fid=" << fid_now
        << " ns.current.cid=" << cid_now
        << " ns.generated.fid=" << (int)(fid_now - gOFS->BootFileId)
        << " ns.generated.cid=" << (int)(cid_now - gOFS->BootContainerId) << std::endl
        << "uid=all gid=all ns.total.files.changelog.size="
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
        << "uid=all gid=all ns.boot.status=" << bootstring.c_str() << std::endl
        << "uid=all gid=all ns.boot.time=" << boottime << std::endl
        << "uid=all gid=all ns.latency.files=" << latencyf << std::endl
        << "uid=all gid=all ns.latency.dirs=" << latencyd << std::endl
        << "uid=all gid=all ns.latency.pending.updates=" << latencyp << std::endl
        << "uid=all gid=all " << master_status.c_str() << std::endl
        << "uid=all gid=all ns.memory.virtual=" << mem.vmsize << std::endl
        << "uid=all gid=all ns.memory.resident=" << mem.resident << std::endl
        << "uid=all gid=all ns.memory.share=" << mem.share << std::endl
        << "uid=all gid=all ns.stat.threads=" << pstat.threads << std::endl;

    if (pstat.vsize > gOFS->LinuxStatsStartup.vsize) {
      oss << "uid=all gid=all ns.memory.growth=" << (unsigned long long)
          (pstat.vsize - gOFS->LinuxStatsStartup.vsize) << std::endl;
    } else {
      oss << "uid=all gid=all ns.memory.growth=-" << (unsigned long long)
          (-pstat.vsize + gOFS->LinuxStatsStartup.vsize) << std::endl;
    }

    oss << "uid=all gid=all ns.uptime="
        << (int)(time(NULL) - gOFS->StartTime)
        << std::endl;
  } else {
    std::string line = "# ------------------------------------------------------"
                       "------------------------------";
    oss << line << std::endl
        << "# Namespace Statistics" << std::endl
        << line << std::endl
        << "ALL      Files                            "
        << f << " [" << bootstring << "] (" << boottime << "s)" << std::endl
        << "ALL      Directories                      "
        << d <<  std::endl
        << line << std::endl
        << "ALL      Compactification                 "
        << compact_status.c_str() << std::endl
        << line << std::endl
        << "ALL      Replication                      "
        << master_status.c_str() << std::endl;

    if (!gOFS->MgmMaster.IsMaster()) {
      oss << "ALL      Namespace Latency Files          " << latencyf << std::endl
          << "ALL      Namespace Latency Directories    " << latencyd << std::endl
          << "ALL      Namespace Pending Updates        " << latencyp << std::endl;
    }

    oss << line << std::endl
        << "ALL      File Changelog Size              " << clfsize << std::endl
        << "ALL      Dir  Changelog Size              " << cldsize << std::endl
        << line << std::endl
        << "ALL      avg. File Entry Size             " << clfratio << std::endl
        << "ALL      avg. Dir  Entry Size             " << cldratio << std::endl
        << line << std::endl
        << "ALL      files created since boot         "
        << (int)(fid_now - gOFS->BootFileId) << std::endl
        << "ALL      container created since boot     "
        << (int)(cid_now - gOFS->BootContainerId) << std::endl
        << line << std::endl
        << "ALL      current file id                  " << fid_now
        << std::endl
        << "ALL      current container id             " << cid_now
        << std::endl
        << line << std::endl
        << "ALL      memory virtual                   "
        << StringConversion::GetReadableSizeString(sizestring, (unsigned long long)
            mem.vmsize, "B")
        << std::endl
        << "ALL      memory resident                  "
        << StringConversion::GetReadableSizeString(sizestring, (unsigned long long)
            mem.resident, "B")
        << std::endl
        << "ALL      memory share                     "
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
        << "ALL      uptime                           "
        << (int)(time(NULL) - gOFS->StartTime) << std::endl
        << line << std::endl;
  }

  if (!stat.summary()) {
    XrdOucString stats_out;
    gOFS->MgmStats.PrintOutTotal(stats_out, stat.groupids(), stat.monitor(),
                                 stat.numericids());
    oss << stats_out.c_str();
  }

  return oss.str();
}

//------------------------------------------------------------------------------
// Execute master comand
//------------------------------------------------------------------------------
void
NsCmd::MasterSubcmd(const eos::console::NsProto_MasterProto& master,
                    eos::console::ReplyProto& reply)
{
  using eos::console::NsProto_MasterProto;

  if (master.op() == NsProto_MasterProto::DISABLE) {
    // Disable the master heart beat thread to do remote checks
    if (!gOFS->MgmMaster.DisableRemoteCheck()) {
      reply.set_std_err("warning: master heartbeat was already disabled!");
      reply.set_retc(EINVAL);
      retc = EINVAL;
    } else {
      reply.set_std_out("success: disabled master heartbeat check\n");
    }
  } else if (master.op() == NsProto_MasterProto::ENABLE) {
    // Enable the master heart beat thread to do remote checks
    if (!gOFS->MgmMaster.EnableRemoteCheck()) {
      reply.set_std_err("warning: master heartbeat was already enabled!");
      reply.set_retc(EINVAL);
    } else {
      reply.set_std_out("success: enabled master heartbeat check");
    }
  } else if (master.op() == NsProto_MasterProto::LOG) {
    XrdOucString out;
    gOFS->MgmMaster.GetLog(out);
    reply.set_std_out(out.c_str());
  } else if (master.op() == NsProto_MasterProto::LOG_CLEAR) {
    gOFS->MgmMaster.ResetLog();
    reply.set_std_out("success: cleaned the master log");
  } else if (master.host().length()) {
    XrdOucString out, err;
    XrdOucString ouc_master(master.host().c_str());

    if (!gOFS->MgmMaster.Set(ouc_master, out, err)) {
      reply.set_std_err(err.c_str());
      reply.set_retc(EIO);
    } else {
      out += "success: <";
      out += gOFS->MgmMaster.GetMasterHost();
      out += "> is now the master\n";
      reply.set_std_out(out.c_str());
    }
  }
}

//------------------------------------------------------------------------------
// Execute compact comand
//------------------------------------------------------------------------------
void
NsCmd::CompactSubcmd(const eos::console::NsProto_CompactProto& compact,
                     eos::console::ReplyProto& reply)
{
  using eos::console::NsProto_CompactProto;

  if (mVid.uid == 0) {
    if (compact.on()) {
      gOFS->MgmMaster.ScheduleOnlineCompacting((time(NULL) + compact.delay()),
          compact.interval());

      if (compact.type() == NsProto_CompactProto::FILES) {
        gOFS->MgmMaster.SetCompactingType(true, false, false);
      } else if (compact.type() == NsProto_CompactProto::DIRS) {
        gOFS->MgmMaster.SetCompactingType(false, true, false);
      } else if (compact.type() == NsProto_CompactProto::ALL) {
        gOFS->MgmMaster.SetCompactingType(true, true, false);
      } else if (compact.type() == NsProto_CompactProto::FILES_REPAIR) {
        gOFS->MgmMaster.SetCompactingType(true, false, true);
      } else if (compact.type() == NsProto_CompactProto::DIRS_REPAIR) {
        gOFS->MgmMaster.SetCompactingType(false, true, true);
      } else if (compact.type() == NsProto_CompactProto::ALL_REPAIR) {
        gOFS->MgmMaster.SetCompactingType(true, true, true);
      }

      std::ostringstream oss;
      oss << "success: configured online compacting to run in "
          << compact.delay()
          << " seconds from now (might be delayed up to 60 seconds)";

      if (compact.interval()) {
        oss << " (re-compact every " << compact.interval() << " seconds)"
            << std::endl;
      } else {
        oss << std::endl;
      }

      reply.set_std_out(oss.str());
    } else {
      gOFS->MgmMaster.ScheduleOnlineCompacting(0, 0);
      reply.set_std_out("success: disabled online compacting\n");
    }
  } else {
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    reply.set_retc(EPERM);
  }
}

//------------------------------------------------------------------------------
// Execute tree size recompute comand
//------------------------------------------------------------------------------
void
NsCmd::TreeSizeSubcmd(const eos::console::NsProto_TreeSizeProto& tree,
                      eos::console::ReplyProto& reply)
{
  using eos::console::NsProto_TreeSizeProto;
  std::ostringstream oss;
  NsProto_TreeSizeProto::ContainerCase cont_type = tree.container_case();
  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);
  std::shared_ptr<IContainerMD> cont {nullptr};

  if (cont_type == NsProto_TreeSizeProto::kPath) {
    try {
      // @todo (esindril): how to deal with symlinks
      cont = gOFS->eosView->getContainer(tree.path());
    } catch (const eos::MDException& e) {
      oss << "error: " << e.what();
      reply.set_std_err(oss.str());
      reply.set_retc(e.getErrno());
      return;
    }
  } else {
    eos::IContainerMD::id_t cid = 0;

    try {
      if (cont_type == NsProto_TreeSizeProto::kCid) {
        cid = std::stoull(tree.cid(), nullptr, 10);
      } else {
        cid = std::stoull(tree.cxid(), nullptr, 16);
      }
    } catch (const std::exception& e) {
      oss << "error: " << e.what();
      reply.set_std_err(oss.str());
      reply.set_retc(EINVAL);
      return;
    }

    try {
      cont = gOFS->eosDirectoryService->getContainerMD(cid);
    } catch (const eos::MDException& e) {
      oss << "error: " << e.what();
      reply.set_std_err(oss.str());
      reply.set_retc(e.getErrno());
      return;
    }
  }

  if (cont == nullptr) {
    oss << "error: container not found";
    reply.set_std_err(oss.str());
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

      UpdateTreeSize(tmp_cont.get());
    }
  }
}

//------------------------------------------------------------------------------
// Recompute and update tree size of the given container assumming its
// subcontainers tree size values are correct and adding the size of files
// attached directly to the current container
//------------------------------------------------------------------------------
void
NsCmd::UpdateTreeSize(eos::IContainerMD* cont) const
{
  eos_debug("cont name=%s, id=%llu", cont->getName().c_str(), cont->getId());
  std::shared_ptr<eos::IFileMD> tmp_fmd {nullptr};
  std::shared_ptr<eos::IContainerMD> tmp_cont {nullptr};
  uint64_t tree_size = 0u;

  for (auto fit = cont->filesBegin(); fit != cont->filesEnd(); ++fit) {
    try {
      tmp_fmd = gOFS->eosFileService->getFileMD(fit->second);
    } catch (const eos::MDException& e) {
      eos_err("error=\"%s\"", e.what());
      continue;
    }

    tree_size += tmp_fmd->getSize();
  }

  for (auto cit = cont->subcontainersBegin();
       cit != cont->subcontainersEnd(); ++cit) {
    try {
      tmp_cont = gOFS->eosDirectoryService->getContainerMD(cit->second);
    } catch (const eos::MDException& e) {
      eos_err("error=\"%s\"", e.what());
      continue;
    }

    tree_size += tmp_cont->getTreeSize();
  }

  cont->setTreeSize(tree_size);
  gOFS->eosDirectoryService->updateStore(cont);
  gOFS->FuseXCast(cont->getId());
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

      for (auto subcont_it = tmp_cont->subcontainersBegin();
           subcont_it != tmp_cont->subcontainersEnd(); ++subcont_it) {
        it_next_lvl->push_back(subcont_it->second);
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
EOSMGMNAMESPACE_END
