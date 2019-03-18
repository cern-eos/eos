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

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Utility functions to help with file balance scheduling
//----------------------------------------------------------------------------
namespace {
  // Build general transfer capability string
  XrdOucString constructCapability(unsigned long lid, unsigned long long cid,
                                   const char* path, unsigned long long fid,
                                   int drain_fsid, const char* localprefix,
                                   int fsid) {
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
  XrdOucString constructSourceCapability(unsigned long lid, unsigned long long cid,
                                         const char* path, unsigned long long fid,
                                         int drain_fsid, const char* localprefix,
                                         int fsid,const char* hostport) {
    XrdOucString capability = "mgm.access=read";

    capability += constructCapability(lid, cid, path, fid,
                                      drain_fsid, localprefix, fsid);
    capability += "&mgm.sourcehostport=";
    capability += hostport;

    return capability.c_str();
  }

  // Build target specific capability string
  XrdOucString constructTargetCapability(unsigned long lid, unsigned long long cid,
                                         const char* path, unsigned long long fid,
                                         int drain_fsid, const char* localprefix,
                                         int fsid, const char* hostport,
                                         unsigned long long size,
                                         unsigned long source_lid,
                                         uid_t source_uid,
                                         gid_t source_gid) {
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
                          XrdOucErrInfo& error) {
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

    XrdOucString hexfid;
    eos::common::FileId::Fid2Hex(fid, hexfid);

    int caplen = 0;
    source_cap = sourcecap_env->Env(caplen);
    target_cap = targetcap_env->Env(caplen);

    source_cap.replace("cap.sym", "source.cap.sym");
    source_cap.replace("cap.msg", "source.cap.msg");
    source_cap += "&source.url=root://";
    source_cap += source_hostport;
    source_cap += "//replicate:";
    source_cap += hexfid.c_str();

    target_cap.replace("cap.sym", "target.cap.sym");
    target_cap.replace("cap.msg", "target.cap.msg");
    target_cap += "&target.url=root://";
    target_cap += target_hostport;
    target_cap += "//replicate:";
    target_cap += hexfid.c_str();

    full_capability = source_cap;
    full_capability += target_cap;

    if (sourcecap_env) { delete sourcecap_env; }
    if (targetcap_env) { delete targetcap_env; }

    return 0;
  }
}

//----------------------------------------------------------------------------
// Schedule a balance transfer
//----------------------------------------------------------------------------
int
XrdMgmOfs::Schedule2Balance(const char* path,
                            const char* ininfo,
                            XrdOucEnv& env,
                            XrdOucErrInfo& error,
                            eos::common::LogId& ThreadLogId,
                            eos::common::Mapping::VirtualIdentity& vid,
                            const XrdSecEntity* client)
{
  static const char* epname = "Schedule2Balance";

  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Scheduled2Balance");

  gOFS->MgmStats.Add("Schedule2Balance", 0, 0, 1);

  // Static map with iterator position for the next
  // group scheduling and its mutex
  static std::map<std::string, size_t> sGroupCycle;
  static XrdSysMutex sGroupCycleMutex;
  static time_t sScheduledFidCleanupTime = 0;

  char* alogid = env.Get("mgm.logid");
  char* simulate = env.Get("mgm.simulate"); // Used to test the routing
  char* afsid = env.Get("mgm.target.fsid");
  char* afreebytes = env.Get("mgm.target.freebytes");

  if (alogid) {
    ThreadLogId.SetLogId(alogid, error.getErrUser());
  }

  if (!afsid || !afreebytes) {
    int envlen;
    eos_thread_err("schedule2balance does not contain all meta information: %s",
                   env.Env(envlen));

    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    return Emsg(epname, error, EINVAL,
                "schedule - missing parameters [EINVAL]");
  }

  eos::common::FileSystem::fsid_t source_fsid = 0;
  eos::common::FileSystem::fs_snapshot source_snapshot;

  eos::common::FileSystem::fsid_t target_fsid = atoi(afsid);
  eos::common::FileSystem::fs_snapshot target_snapshot;

  unsigned long long freebytes = strtoull(afreebytes, 0, 10);

  eos_thread_info("cmd=schedule2balance fsid=%u freebytes=%llu logid=%s",
                  target_fsid, freebytes, alogid ? alogid : "");

  // Lock the view and get the filesystem information
  // for the target where we balance to
  while (1)
  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* target_fs = 0;

    target_fs = FsView::gFsView.mIdView[target_fsid];

    if (!target_fs) {
      eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      return Emsg(epname, error, EINVAL, "schedule - fsid not known [EINVAL]",
                  std::to_string(target_fsid).c_str());
    }

    target_fs->SnapShotFileSystem(target_snapshot);
    FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

    if (!group) {
      eos_thread_err("group=%s is not in group view",
                     target_snapshot.mGroup.c_str());
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      return Emsg(epname, error, EINVAL, "schedule - group not known [EINVAL]",
                  target_snapshot.mGroup.c_str());
    }

    size_t groupsize = group->size();
    eos_thread_debug("group=%s", target_snapshot.mGroup.c_str());

    // Select the next fs in the group to get a file move
    size_t gposition = 0;
    {
      XrdSysMutexHelper sLock(sGroupCycleMutex);

      if (sGroupCycle.count(target_snapshot.mGroup)) {
        gposition = sGroupCycle[target_snapshot.mGroup] % groupsize;
      } else {
        gposition = 0;
        sGroupCycle[target_snapshot.mGroup] = 0;
      }

      // Shift the iterator for the next schedule call
      // to the following filesystem in the group
      sGroupCycle[target_snapshot.mGroup]++;
      sGroupCycle[target_snapshot.mGroup] %= groupsize;
    }

    eos_thread_debug("group=%s cycle=%lu",
                     target_snapshot.mGroup.c_str(), gposition);

    // Try to find a file which is smaller than the free bytes and has no
    // replica on the target filesystem. We start at a random position not
    // to move data of the same period to a single disk
    FsGroup::const_iterator group_iterator = group->begin();
    std::advance(group_iterator, gposition);
    eos::common::FileSystem* source_fs = 0;

    for (size_t n = 0; n < group->size(); n++) {
      // Skip over unusable target file system
      if (*group_iterator == target_fsid) {
        source_fs = 0;

        if (++group_iterator == group->end()) {
          group_iterator = group->begin();
        }

        continue;
      }

      source_fs = FsView::gFsView.mIdView[*group_iterator];

      if (!source_fs) { continue; }

      source_fs->SnapShotFileSystem(source_snapshot);
      source_fsid = *group_iterator;

      if ((source_snapshot.mDiskFilled < source_snapshot.mNominalFilled) ||
          // This is not a source since it is empty
          (source_snapshot.mStatus != eos::common::BootStatus::kBooted) ||
          // This filesystem is not readable
          (source_snapshot.mConfigStatus < eos::common::FileSystem::kRO) ||
          (source_snapshot.mErrCode != 0) ||
          (source_fs->GetActiveStatus(source_snapshot) ==
           eos::common::FileSystem::kOffline)) {
        source_fs = 0;

        // Whenever we jump a filesystem we advance also the cyclic group
        // pointer for the next round
        XrdSysMutexHelper sLock(sGroupCycleMutex);
        sGroupCycle[target_snapshot.mGroup]++;
        sGroupCycle[target_snapshot.mGroup] %= groupsize;

        if (++group_iterator == group->end()) {
          group_iterator = group->begin();
        }

        continue;
      }

      break;
    }

    if (!source_fs) {
      eos_thread_debug("no source available");
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      error.setErrInfo(0, "");
      return SFS_DATA;
    }

    source_fs->SnapShotFileSystem(source_snapshot);
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    unsigned long long nfids = gOFS->eosFsView->getNumFilesOnFs(source_fsid);
    eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u "
                     "n_source_fids=%llu", target_snapshot.mGroup.c_str(),
                     gposition, source_fsid, target_fsid, nfids);

    for (unsigned long long attempts = 0; attempts < nfids; attempts++) {
      eos::IFileMD::id_t fid;

      if (!gOFS->eosFsView->getApproximatelyRandomFileInFs(source_fsid, fid)) {
        break;
      }

      if (!gOFS->eosView->inMemory()) {
        lock.Release();
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
        lock.Grab(gOFS->eosViewRWMutex);
      }

      // Check that the target does not have this file
      if (gOFS->eosFsView->hasFileId(fid, target_fsid)) {
        // Ignore file and move to the next
        eos_static_debug("skip fxid=%llx - file exists on target fsid=%u",
                          fid, target_fsid);
        continue;
      }

      // Update scheduled files mapping
      XrdSysMutexHelper sLock(ScheduledToBalanceFidMutex);
      time_t now = time(NULL);

      if (sScheduledFidCleanupTime < now) {
        // Next clean-up in 10 minutes
        sScheduledFidCleanupTime = now + 600;

        // Do some clean-up
        std::map<eos::common::FileId::fileid_t, time_t>::iterator it1;
        std::map<eos::common::FileId::fileid_t, time_t>::iterator it2;
        it2 = ScheduledToBalanceFid.begin();

        while (it2 != ScheduledToBalanceFid.end()) {
          it1 = it2;
          it2++;

          if (it1->second < now) {
            ScheduledToBalanceFid.erase(it1);
          }
        }
      }

      // Check that this file has not been scheduled during the 1h period
      if (ScheduledToBalanceFid.count(fid)
          && (ScheduledToBalanceFid[fid] > (now))) {
        // File has been scheduled in the last hour. Move to the next
        eos_thread_debug("skip fxid=%llx - scheduled during last hour", fid);
        continue;
      }

      //-----------------------------------------------------------------------
      // Grab file metadata object
      //-----------------------------------------------------------------------

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
        eos_thread_debug("skip fxid=%llx - cannot get fmd record", fid);
        continue;
      }

      if (size == 0) {
        eos_thread_debug("skip fxid=%llx - zero sized file", fid);
        continue;
      }

      if (size >= freebytes) {
        eos_thread_warning("skip fxid=%llx - file size >= free bytes "
                           "fsize=%llu free_bytes=%llu", fid, size, freebytes);
        continue;
      }

      //-----------------------------------------------------------------------
      // Schedule file transfer
      //-----------------------------------------------------------------------

      eos_thread_info("subcmd=scheduling fxid=%llx source_fsid=%u target_fsid=%u",
                      fid, source_fsid, target_fsid);

      using eos::common::LayoutId;
      unsigned long target_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);

      // Mask block checksums (set to kNone) for replica layouts
      if (LayoutId::GetLayoutType(lid) == LayoutId::kReplica) {
        target_lid = LayoutId::SetBlockChecksum(target_lid, LayoutId::kNone);
      }

      // Construct capability strings
      XrdOucString source_capability =
          constructSourceCapability(target_lid, cid, fullpath.c_str(), fid,
                                    source_fsid, source_snapshot.mPath.c_str(),
                                    source_snapshot.mId,
                                    source_snapshot.mHostPort.c_str());

      XrdOucString target_capability =
          constructTargetCapability(target_lid, cid, fullpath.c_str(), fid,
                                    source_fsid, target_snapshot.mPath.c_str(),
                                    target_snapshot.mId,
                                    target_snapshot.mHostPort.c_str(),
                                    size, lid, uid, gid);

      // Issue full capability string
      XrdOucErrInfo capError;
      XrdOucString full_capability;
      int rc = issueFullCapability(source_capability, target_capability,
                                   mCapabilityValidity,
                                   source_snapshot.mHostPort.c_str(),
                                   target_snapshot.mHostPort.c_str(),
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

      ScheduledToBalanceFid[fid] = time(NULL) + 3600;
      XrdOucString response = "submitted";
      error.setErrInfo(response.length() + 1, response.c_str());

      if (!simulate) {
        std::unique_ptr<eos::common::TransferJob>
            txjob(new eos::common::TransferJob(full_capability.c_str()));

        if (target_fs->GetBalanceQueue()->Add(txjob.get())) {
          eos_thread_info("cmd=schedule2balance fxid=%llx source_fs=%u "
                          "target_fs=%u", fid, source_fsid, target_fsid);
          eos_thread_debug("job=%s", full_capability.c_str());
        } else {
          eos_thread_err("cmd=schedule2balance msg=\"failed to submit job\""
                         " job=%s", full_capability.c_str());
          error.setErrInfo(0, "");
        }
      }

      gOFS->MgmStats.Add("Scheduled2Balance", 0, 0, 1);
      EXEC_TIMING_END("Scheduled2Balance");
      return SFS_DATA;
    }

    break;
  }

  gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
  error.setErrInfo(0, "");
  return SFS_DATA;
}
