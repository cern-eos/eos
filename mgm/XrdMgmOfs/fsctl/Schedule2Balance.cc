// ----------------------------------------------------------------------
// File: Schedule2Balance.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/SecEntity.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"
#include "authz/XrdCapability.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/FsView.hh"
#include "mgm/IdTrackerWithValidity.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Utility functions to help with file balance scheduling
//----------------------------------------------------------------------------
namespace
{
// Build general transfer capability string
XrdOucString constructCapability(unsigned long lid, unsigned long long cid,
                                 const char* path, unsigned long long fid,
                                 int drain_fsid, const char* localprefix,
                                 int fsid)
{
  using eos::common::StringConversion;
  XrdOucString capability = "";
  XrdOucString sizestring;
  capability += "&mgm.lid=";
  capability += StringConversion::GetSizeString(sizestring,
                (unsigned long long) lid);
  capability += "&mgm.cid=";
  capability += StringConversion::GetSizeString(sizestring, cid);
  capability += "&mgm.ruid=1";
  capability += "&mgm.rgid=1";
  capability += "&mgm.uid=1";
  capability += "&mgm.gid=1";
  capability += "&mgm.path=";
  capability += path;
  capability += "&mgm.manager=";
  capability += gOFS->ManagerId.c_str();
  capability += "&mgm.fid=";
  capability += eos::common::FileId::Fid2Hex(fid).c_str();
  capability += "&mgm.sec=";
  capability += eos::common::SecEntity::ToKey(0, "eos/balancing").c_str();
  capability += "&mgm.drainfsid=";
  capability += drain_fsid;
  capability += "&mgm.localprefix=";
  capability += localprefix;
  capability += "&mgm.fsid=";
  capability += fsid;
  return capability;
}

// Build source specific capability string
XrdOucString constructSourceCapability(unsigned long lid,
                                       unsigned long long cid,
                                       const char* path, unsigned long long fid,
                                       int drain_fsid, const char* localprefix,
                                       int fsid, const char* hostport)
{
  XrdOucString capability = "mgm.access=read";
  capability += constructCapability(lid, cid, path, fid,
                                    drain_fsid, localprefix, fsid);
  capability += "&mgm.sourcehostport=";
  capability += hostport;
  return capability.c_str();
}

// Build target specific capability string
XrdOucString constructTargetCapability(unsigned long lid,
                                       unsigned long long cid,
                                       const char* path, unsigned long long fid,
                                       int drain_fsid, const char* localprefix,
                                       int fsid, const char* hostport,
                                       unsigned long long size,
                                       unsigned long source_lid,
                                       uid_t source_uid,
                                       gid_t source_gid)
{
  using eos::common::StringConversion;
  XrdOucString sizestring;
  XrdOucString capability = "mgm.access=write";
  capability += constructCapability(lid, cid, path, fid,
                                    drain_fsid, localprefix, fsid);
  capability += "&mgm.targethostport=";
  capability += hostport;
  capability += "&mgm.bookingsize=";
  capability += StringConversion::GetSizeString(sizestring, size);
  capability += "&mgm.source.lid=";
  capability += StringConversion::GetSizeString(sizestring,
                (unsigned long long) source_lid);
  capability += "&mgm.source.ruid=";
  capability += StringConversion::GetSizeString(sizestring,
                (unsigned long long) source_uid);
  capability += "&mgm.source.rgid=";
  capability += StringConversion::GetSizeString(sizestring,
                (unsigned long long) source_gid);
  return capability;
}

int issueFullCapability(XrdOucString source_cap, XrdOucString target_cap,
                        unsigned long long capValidity,
                        const char* source_hostport,
                        const char* target_hostport,
                        unsigned long long fid,
                        XrdOucString& full_capability,
                        XrdOucErrInfo& error)
{
  XrdOucEnv insourcecap_env(source_cap.c_str());
  XrdOucEnv intargetcap_env(target_cap.c_str());
  XrdOucEnv* sourcecap_env = 0;
  XrdOucEnv* targetcap_env = 0;
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  int rc = gCapabilityEngine.Create(&insourcecap_env, sourcecap_env,
                                    symkey, capValidity);

  if (rc) {
    error.setErrInfo(rc, "source");
    return rc;
  }

  rc = gCapabilityEngine.Create(&intargetcap_env, targetcap_env,
                                symkey, capValidity);

  if (rc) {
    error.setErrInfo(rc, "target");
    return rc;
  }

  int caplen = 0;
  source_cap = sourcecap_env->Env(caplen);
  target_cap = targetcap_env->Env(caplen);
  source_cap.replace("cap.sym", "source.cap.sym");
  source_cap.replace("cap.msg", "source.cap.msg");
  source_cap += "&source.url=root://";
  source_cap += source_hostport;
  source_cap += "//replicate:";
  source_cap += eos::common::FileId::Fid2Hex(fid).c_str();
  target_cap.replace("cap.sym", "target.cap.sym");
  target_cap.replace("cap.msg", "target.cap.msg");
  target_cap += "&target.url=root://";
  target_cap += target_hostport;
  target_cap += "//replicate:";
  target_cap += eos::common::FileId::Fid2Hex(fid).c_str();
  full_capability = source_cap;
  full_capability += target_cap;

  if (sourcecap_env) {
    delete sourcecap_env;
  }

  if (targetcap_env) {
    delete targetcap_env;
  }

  return 0;
}
}

//------------------------------------------------------------------------------
// Get source file system for balancing jobs given the target fs
//------------------------------------------------------------------------------
int
XrdMgmOfs::BalanceGetFsSrc(eos::common::FileSystem::fsid_t tgt_fsid,
                           eos::common::FileSystem::fs_snapshot_t& tgt_snapshot,
                           eos::common::FileSystem::fs_snapshot_t& src_snapshot,
                           XrdOucErrInfo& error)
{
  // Static map with iterator position for the next group scheduling
  static XrdSysMutex s_grp_cycle_mutex;
  static std::map<std::string, size_t> s_grp_cycle;
  static const char* epname = "Schedule2Balance";
  // ------> FS read lock
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::mgm::FileSystem* tgt_fs = FsView::gFsView.mIdView.lookupByID(tgt_fsid);

  if (!tgt_fs) {
    eos_thread_err("msg=\"target filesystem not found in the view\" fsid=%u",
                   tgt_fsid);
    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    return Emsg(epname, error, EINVAL, "schedule - fsid not known [EINVAL]",
                std::to_string(tgt_fsid).c_str());
  }

  tgt_fs->SnapShotFileSystem(tgt_snapshot);
  auto it_grp = FsView::gFsView.mGroupView.find(tgt_snapshot.mGroup);

  if ((it_grp == FsView::gFsView.mGroupView.end()) ||
      (it_grp->second == nullptr)) {
    eos_thread_err("msg=\"group not found in the view\" group=%s",
                   tgt_snapshot.mGroup.c_str());
    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    return Emsg(epname, error, EINVAL, "schedule - group not known [EINVAL]",
                tgt_snapshot.mGroup.c_str());
  }

  FsGroup* group = it_grp->second;
  size_t groupsize = group->size();
  // Select the next fs in the group to get a file
  size_t gposition = 0;
  {
    XrdSysMutexHelper lock(s_grp_cycle_mutex);

    if (s_grp_cycle.count(tgt_snapshot.mGroup)) {
      gposition = s_grp_cycle[tgt_snapshot.mGroup] % groupsize;
    } else {
      gposition = 0;
      s_grp_cycle[tgt_snapshot.mGroup] = 0;
    }

    // Shift the iterator for the next schedule call to the following
    // filesystem in the group
    s_grp_cycle[tgt_snapshot.mGroup]++;
    s_grp_cycle[tgt_snapshot.mGroup] %= groupsize;
  }
  eos_thread_debug("group=%s cycle=%lu", tgt_snapshot.mGroup.c_str(),
                   gposition);
  // Try to find a file which is smaller than the free bytes and has no
  // replica on the target filesystem. We start at a random position not
  // to move data of the same period to a single disk
  FsGroup::const_iterator group_iterator = group->begin();
  std::advance(group_iterator, gposition);
  eos::common::FileSystem* src_fs = nullptr;

  for (size_t n = 0; n < group->size(); ++n) {
    // Skip over unusable file systems
    if (*group_iterator == tgt_fsid) {
      src_fs = nullptr;

      if (++group_iterator == group->end()) {
        group_iterator = group->begin();
      }

      continue;
    }

    src_fs = FsView::gFsView.mIdView.lookupByID(*group_iterator);

    if (!src_fs) {
      continue;
    }

    src_fs->SnapShotFileSystem(src_snapshot);

    if ((src_snapshot.mDiskFilled < src_snapshot.mNominalFilled) ||
        (src_snapshot.mStatus != eos::common::BootStatus::kBooted) ||
        (src_snapshot.mConfigStatus < eos::common::ConfigStatus::kRO) ||
        (src_snapshot.mErrCode != 0) ||
        (src_snapshot.GetActiveStatus() ==
         eos::common::ActiveStatus::kOffline)) {
      src_fs = nullptr;
      // Whenever we jump a filesystem we advance also the cyclic group
      // pointer for the next round
      XrdSysMutexHelper lock(s_grp_cycle_mutex);
      s_grp_cycle[tgt_snapshot.mGroup]++;
      s_grp_cycle[tgt_snapshot.mGroup] %= groupsize;

      if (++group_iterator == group->end()) {
        group_iterator = group->begin();
      }

      continue;
    }

    // We found a suitable source file system to balance from
    break;
  }

  if (src_fs == nullptr) {
    eos_thread_debug("msg=\"no source available\"");
    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    error.setErrInfo(0, "");
    return SFS_DATA;
  }

  return SFS_OK;
}


//----------------------------------------------------------------------------
// Schedule a balance transfer
//----------------------------------------------------------------------------
int
XrdMgmOfs::Schedule2Balance(const char* path,
                            const char* ininfo,
                            XrdOucEnv& env,
                            XrdOucErrInfo& error,
                            eos::common::VirtualIdentity& vid,
                            const XrdSecEntity* client)
{
  static const char* epname = "Schedule2Balance";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Scheduled2Balance");
  gOFS->MgmStats.Add("Schedule2Balance", 0, 0, 1);
  char* alogid = env.Get("mgm.logid");
  char* afsid = env.Get("mgm.target.fsid");
  char* afreebytes = env.Get("mgm.target.freebytes");

  if (alogid) {
    tlLogId.SetLogId(alogid, error.getErrUser());
  }

  if (!afsid || !afreebytes) {
    int envlen;
    eos_thread_err("msg=\"schedule2balance does not contain all meta information"
                   " env=\"%s\"", env.Env(envlen));
    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    return Emsg(epname, error, EINVAL, "schedule - missing parameters [EINVAL]");
  }

  eos::common::FileSystem::fs_snapshot_t tgt_snapshot;
  eos::common::FileSystem::fs_snapshot_t src_snapshot;
  eos::common::FileSystem::fsid_t tgt_fsid = atoi(afsid);
  unsigned long long freebytes = strtoull(afreebytes, 0, 10);
  eos_thread_info("cmd=schedule2balance fsid=%u freebytes=%llu logid=%s",
                  tgt_fsid, freebytes, alogid ? alogid : "");
  // Get filesystem information tarrget where we balance to
  int retc = BalanceGetFsSrc(tgt_fsid, tgt_snapshot, src_snapshot, error);

  if (retc != SFS_OK) {
    return retc;
  }

  eos::common::FileSystem::fsid_t src_fsid = src_snapshot.mId;
  // ------> NS read lock
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
  uint64_t nfids = gOFS->eosFsView->getNumFilesOnFs(src_fsid);
  eos_thread_debug("group=%s src_fsid=%u tgt_fsid=%u n_source_fids=%llu",
                   src_snapshot.mGroup.c_str(), src_fsid, tgt_fsid, nfids);

  for (uint64_t attempts = 0; attempts < nfids; ++attempts) {
    eos::IFileMD::id_t fid;

    if (!gOFS->eosFsView->getApproximatelyRandomFileInFs(src_fsid, fid)) {
      break;
    }

    if (!gOFS->eosView->inMemory()) {
      ns_rd_lock.Release();
      eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
      ns_rd_lock.Grab(gOFS->eosViewRWMutex);
    }

    // Check that the target does not have this file
    if (gOFS->eosFsView->hasFileId(fid, tgt_fsid)) {
      eos_static_debug("msg=\"skip file existing on target fs\" fxid=%08llx "
                       "tgt_fsid=%u", fid, tgt_fsid);
      continue;
    }

    // Update tracker for scheduled fid balance jobs
    mBalancingTracker.DoCleanup();

    if (mBalancingTracker.HasEntry(fid)) {
      eos_thread_debug("msg=\"skip recently scheduled file\" fxid=%08llx", fid);
      continue;
    }

    // Grab file metadata object
    std::shared_ptr<eos::IFileMD> fmd;
    unsigned long long cid = 0;
    unsigned long long size = 0;
    long unsigned int lid = 0;
    uid_t uid = 0;
    gid_t gid = 0;
    std::string fullpath = "";

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      fullpath = gOFS->eosView->getUri(fmd.get());
      XrdOucString savepath = fullpath.c_str();

      while (savepath.replace("&", "#AND#")) {}

      fullpath = savepath.c_str();
      lid = fmd->getLayoutId();
      cid = fmd->getContainerId();
      size = fmd->getSize();
      uid = fmd->getCUid();
      gid = fmd->getCGid();
    } catch (eos::MDException& e) {
      fmd.reset();
    }

    if (!fmd) {
      eos_thread_debug("msg=\"skip no fmd record found\"fxid=%08llx", fid);
      continue;
    }

    if (size == 0) {
      eos_thread_debug("msg=\"skip zero size file\" fxid=%08llx", fid);
      continue;
    }

    if (size >= freebytes) {
      eos_thread_warning("msg=\"skip file bigger than free bytes\" fxid=%08llx "
                         "fsize=%llu free_bytes=%llu", fid, size, freebytes);
      continue;
    }

    // We can release the NS lock since we will definetely return from this
    // function and we have all the necessary info at the local scope
    ns_rd_lock.Release();
    // Schedule file transfer
    eos_thread_info("subcmd=scheduling fxid=%08llx src_fsid=%u tgt_fsid=%u",
                    fid, src_fsid, tgt_fsid);
    using eos::common::LayoutId;
    unsigned long tgt_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);

    if (LayoutId::GetLayoutType(lid) == LayoutId::kReplica) {
      // Mask block checksums (set to kNone) for replica layouts
      tgt_lid = LayoutId::SetBlockChecksum(tgt_lid, LayoutId::kNone);
    } else if (LayoutId::IsRain(lid)) {
      // Disable checksum check for RAIN layouts since we're reading one
      // stripe through a plain layout and this would compare the stripe
      // checkusm with the full RAIN file checksum
      tgt_lid = LayoutId::SetChecksum(tgt_lid, LayoutId::kNone);
    }

    // Construct capability strings
    XrdOucString source_capability =
      constructSourceCapability(tgt_lid, cid, fullpath.c_str(), fid,
                                src_fsid, src_snapshot.mPath.c_str(),
                                src_snapshot.mId,
                                src_snapshot.mHostPort.c_str());
    XrdOucString tgt_capability =
      constructTargetCapability(tgt_lid, cid, fullpath.c_str(), fid,
                                src_fsid, tgt_snapshot.mPath.c_str(),
                                tgt_snapshot.mId,
                                tgt_snapshot.mHostPort.c_str(),
                                size, lid, uid, gid);
    // Issue full capability string
    XrdOucErrInfo capError;
    XrdOucString full_capability;
    int rc = issueFullCapability(source_capability, tgt_capability,
                                 mCapabilityValidity,
                                 src_snapshot.mHostPort.c_str(),
                                 tgt_snapshot.mHostPort.c_str(),
                                 fid, full_capability, capError);

    if (rc) {
      std::ostringstream errstream;
      errstream << "create " << capError.getErrText()
                << " capability [EADV]";
      eos_thread_err("unable to create %s capability - ec=%d",
                     capError.getErrText(), capError.getErrInfo());
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      return Emsg(epname, error, rc, errstream.str().c_str());
    }

    bool scheduled = false;
    std::unique_ptr<eos::common::TransferJob>
    txjob(new eos::common::TransferJob(full_capability.c_str()));
    {
      // ----> FS read lock
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      FileSystem* fs = FsView::gFsView.mIdView.lookupByID(tgt_fsid);

      if (fs) {
        if (fs->GetBalanceQueue()->Add(txjob.get())) {
          eos_thread_info("cmd=schedule2balance fxid=%08llx source_fs=%u "
                          "target_fs=%u", fid, src_fsid, tgt_fsid);
          eos_thread_debug("job=%s", full_capability.c_str());
          scheduled = true;
        }
      }
    }

    if (scheduled) {
      // Track new scheduled job
      mBalancingTracker.AddEntry(fid);
      XrdOucString response = "submitted";
      error.setErrInfo(response.length() + 1, response.c_str());
      gOFS->MgmStats.Add("Scheduled2Balance", 0, 0, 1);
    } else {
      eos_thread_err("cmd=schedule2balance msg=\"failed to submit job\""
                     " job=%s", full_capability.c_str());
      error.setErrInfo(0, "");
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    }

    EXEC_TIMING_END("Scheduled2Balance");
    return SFS_DATA;
  }

  gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
  error.setErrInfo(0, "");
  return SFS_DATA;
}
