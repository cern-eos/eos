// ----------------------------------------------------------------------
// File: Schedule2Drain.cc
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
#include "common/Path.hh"
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
#include "mgm/Scheduler.hh"
#include "mgm/Quota.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Utility functions to help with file drain scheduling
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
    capability += eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
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
                                        int fsid, const char* hostport) {
    XrdOucString capability = "mgm.access=read";

    capability += constructCapability(lid, cid, path, fid,
                                      drain_fsid, localprefix, fsid);
    capability += "&mgm.sourcehostport=";
    capability += hostport;

    return capability;
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

  // Build RAIN capability string
  XrdOucString RAINFullCapability(const char* path, int source_fsid) {
    XrdOucString rain_capability = "source.url=root://";
    rain_capability += gOFS->ManagerId;
    rain_capability += "/";
    rain_capability += path;
    rain_capability += "&target.url=/dev/null";

    XrdOucString source_env = "eos.pio.action=reconstruct";
    source_env += "&eos.pio.recfs=";
    source_env += source_fsid;

    rain_capability += "&source.env=";
    rain_capability += XrdMqMessage::Seal(source_env, "_AND_");
    rain_capability += "&tx.layout.reco=true";

    return rain_capability;
  }

  int checkFileAccess(unsigned long lid, unsigned long& fsindex,
                      std::vector<unsigned int>& locationfs,
                      XrdOucErrInfo& error) {
    eos::common::Mapping::VirtualIdentity_t h_vid;
    eos::common::Mapping::Root(h_vid);
    std::vector<unsigned int> unavailfs;
    std::string tried_cgi;
    int rc = 0;

    Scheduler::AccessArguments acsargs;
    acsargs.bookingsize = 0;
    acsargs.fsindex = &fsindex;
    acsargs.isRW = false;
    acsargs.lid = lid;
    acsargs.locationsfs = &locationfs;
    acsargs.tried_cgi = &tried_cgi;
    acsargs.unavailfs = &unavailfs;
    acsargs.vid = &h_vid;
    acsargs.schedtype = Scheduler::draining;

    if (!acsargs.isValid()) {
      error.setErrInfo(-1, "invalid arguments to FileAccess");
      return -1;
    }

    if ((rc = Quota::FileAccess(&acsargs))) {
      error.setErrInfo(rc, "no access to file");
    }

    return rc;
  }
}

//----------------------------------------------------------------------------
// Schedule a drain transfer
//----------------------------------------------------------------------------
int
XrdMgmOfs::Schedule2Drain(const char* path,
                          const char* ininfo,
                          XrdOucEnv& env,
                          XrdOucErrInfo& error,
                          eos::common::LogId& ThreadLogId,
                          eos::common::Mapping::VirtualIdentity& vid,
                          const XrdSecEntity* client)
{
  static const char* epname = "Schedule2Drain";

  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Scheduled2Drain");

  gOFS->MgmStats.Add("Schedule2Drain", 0, 0, 1);

  // Static map with iterator position for the next
  // group scheduling and its mutex
  static std::map<std::string, size_t> sGroupCycle;
  static XrdSysMutex sGroupCycleMutex;
  static std::map<eos::common::FileId::fileid_t,
                  std::pair<eos::common::FileSystem::fsid_t,
                            eos::common::FileSystem::fsid_t>> sZeroMove;
  static XrdSysMutex sZeroMoveMutex;
  static time_t sScheduledFidCleanupTime = 0;

  // Don't do anything if distributed drain is not enabled
  if (gOFS->mIsCentralDrain) {
    error.setErrInfo(0, "");
    return SFS_DATA;
  }

  char* alogid = env.Get("mgm.logid");

  if (alogid) {
    ThreadLogId.SetLogId(alogid, error.getErrUser());
  }

  // Deal with 0-size files 'scheduled' before,
  // which just need a move in the namespace
  bool has_zero_mv_files = false;
  {
    XrdSysMutexHelper zLock(sZeroMoveMutex);

    if (sZeroMove.size()) {
      has_zero_mv_files = true;
    }
  }

  if (has_zero_mv_files) {
    eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);
    XrdSysMutexHelper sLock(sZeroMoveMutex);
    auto it = sZeroMove.begin();

    while (it != sZeroMove.end()) {
      std::shared_ptr<eos::IFileMD> fmd;

      try {
        fmd = gOFS->eosFileService->getFileMD(it->first);
        std::string fullpath = gOFS->eosView->getUri(fmd.get());

        if (!fmd->getSize()) {
          fmd->unlinkLocation(it->second.first);
          fmd->removeLocation(it->second.first);
          fmd->addLocation(it->second.second);
          gOFS->eosView->updateFileStore(fmd.get());
          eos_thread_info("msg=\"drained 0-size file\" "
                          "fxid=%llx source-fsid=%u target-fsid=%u",
                          it->first, it->second.first, it->second.second);
        } else {
          // Check if this is an atomic file
          if (fullpath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) != std::string::npos) {
            fmd->unlinkLocation(it->second.first);
            fmd->removeLocation(it->second.first);
            gOFS->eosView->updateFileStore(fmd.get());
            eos_thread_info("msg=\"drained(unlinked) atomic upload file\" "
                            "fxid=%llx source-fsid=%u target-fsid=%u",
                            it->first, it->second.first, it->second.second);
          } else {
            eos_thread_warning("msg=\"unexpected file in zero-move list "
                               "with size!=0 and not atomic path - skipping\"");
          }
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                         e.getErrno(), e.getMessage().str().c_str());
      }

      sZeroMove.erase(it++);
    }
  }

  const char* afsid = env.Get("mgm.target.fsid");
  const char* afreebytes = env.Get("mgm.target.freebytes");

  if (!afsid || !afreebytes) {
    int envlen;
    eos_thread_err("schedule2drain does not contain all meta information: %s",
                   env.Env(envlen));

    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    return Emsg(epname, error, EINVAL,
                "schedule - missing parameters [EINVAL]");
  }

  eos::common::FileSystem::fsid_t source_fsid = 0;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem::fs_snapshot replica_source_snapshot;

  eos::common::FileSystem::fsid_t target_fsid = atoi(afsid);
  eos::common::FileSystem::fs_snapshot target_snapshot;
  eos::common::FileSystem* target_fs = 0;

  unsigned long long freebytes = strtoull(afreebytes, 0, 10);

  eos_thread_info("cmd=schedule2drain fsid=%u freebytes=%llu logid=%s",
                  target_fsid, freebytes, alogid ? alogid : "");

  // Retrieve filesystem information about the drain target
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  target_fs = FsView::gFsView.mIdView[target_fsid];

  if (!target_fs) {
    eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    return Emsg(epname, error, EINVAL,
                "schedule - filesystem ID is not known [EINVAL]");
  }

  target_fs->SnapShotFileSystem(target_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

  if (!group) {
    eos_thread_err("group=%s is not in group view",
                   target_snapshot.mGroup.c_str());
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    return Emsg(epname, error, EINVAL, "schedule - group is not known [EINVAL]",
                target_snapshot.mGroup.c_str());
  }

  // Select the next fs in the group to get a file to move
  size_t gposition = 0;
  {
    XrdSysMutexHelper sLock(sGroupCycleMutex);

    if (sGroupCycle.count(target_snapshot.mGroup)) {
      gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
    } else {
      gposition = 0;
      sGroupCycle[target_snapshot.mGroup] = 0;
    }

    // Shift the iterator for the next schedule call
    // to the following filesystem in the group
    sGroupCycle[target_snapshot.mGroup]++;
    sGroupCycle[target_snapshot.mGroup] %= group->size();
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
    // Look for a filesystem in drain mode
    int32_t drain_status =
        FsView::gFsView.mIdView[*group_iterator]->GetDrainStatus();

    if ((drain_status != eos::common::FileSystem::kDraining) &&
        (drain_status != eos::common::FileSystem::kDrainStalling)) {
      source_fs = 0;

      if (++group_iterator == group->end()) {
        group_iterator = group->begin();
      }

      continue;
    }

    source_fs = FsView::gFsView.mIdView[*group_iterator];

    if (source_fs) { break; }

    if (++group_iterator == group->end()) {
      group_iterator = group->begin();
    }
  }

  if (!source_fs) {
    eos_thread_debug("no source available");
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    error.setErrInfo(0, "");
    return SFS_DATA;
  }

  source_fs->SnapShotFileSystem(source_snapshot);
  source_fsid = *group_iterator;

  if (!gOFS->eosView->inMemory()) {
    eos_thread_crit("msg=\"old style draining enabled for QDB namespace. "
                    "Prefetching entire filesystem to minimize impact "
                    "on performance.\"");
    eos::Prefetcher::prefetchFilesystemFileListWithFileMDsAndParentsAndWait(
        gOFS->eosView, gOFS->eosFsView, source_fsid);
    eos::Prefetcher::prefetchFilesystemFileListAndWait(gOFS->eosView,
                                                       gOFS->eosFsView,
                                                       target_fsid);
  }

  // Lock namespace view here to avoid deadlock with the Commit.cc code on
  // the ScheduledToDrainFidMutex
  eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
  unsigned long long nfids = gOFS->eosFsView->getNumFilesOnFs(source_fsid);
  eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u "
                   "n_source_fids=%llu", target_snapshot.mGroup.c_str(),
                   gposition, source_fsid, target_fsid, nfids);

  for (auto it_fid = gOFS->eosFsView->getFileList(source_fsid);
       (it_fid && it_fid->valid()); it_fid->next()) {
    eos::IFileMD::id_t fid = it_fid->getElement();
    eos_thread_debug("checking fxid=%llx", fid);

    // Check that the target does not have this file
    if (gOFS->eosFsView->hasFileId(fid, target_fsid)) {
      // Ignore file and move to the next
      eos_static_debug("skip fxid=%llx - file exists on target fsid=%u",
                       fid, target_fsid);
      continue;
    }

    // Update scheduled files mapping
    time_t now = time(NULL);
    XrdSysMutexHelper sLock(ScheduledToDrainFidMutex);

    if (sScheduledFidCleanupTime < now) {
      // Next clean-up in 10 minutes
      sScheduledFidCleanupTime = now + 600;

      std::map<eos::common::FileId::fileid_t, time_t>::iterator it1;
      std::map<eos::common::FileId::fileid_t, time_t>::iterator it2;
      it2 = ScheduledToDrainFid.begin();

      while (it2 != ScheduledToDrainFid.end()) {
        it1 = it2;
        it2++;

        if (it1->second < now) {
          ScheduledToDrainFid.erase(it1);
        }
      }
    }

    // Check that this file has not been scheduled during the 1h period
    if (ScheduledToDrainFid.count(fid)
        && (ScheduledToDrainFid[fid] > (now))) {
      // File has been scheduled in the last hour. Move to the next
      eos_thread_debug("skip fxid=%llx - scheduled during last hour at %lu",
                       fid, ScheduledToDrainFid[fid]);
      continue;
    }

    //-----------------------------------------------------------------------
    // Grab file metadata object
    //-----------------------------------------------------------------------

    std::shared_ptr<eos::IFileMD> fmd;
    eos::IFileMD::LocationVector locations;
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
      locations = fmd->getLocations();
    } catch (eos::MDException& e) {
      fmd.reset();
    }

    if (!fmd) {
      eos_thread_debug("skip fxid=%llx - cannot get fmd record", fid);
      continue;
    }

    if (!size) {
      // This is a zero size file
      // We move the location by adding it to the static move map
      eos_thread_info("cmd=schedule2drain msg=zero-move fid=%llx source_fs=%u "
                      "target_fs=%u", fid, source_fsid, target_fsid);

      XrdSysMutexHelper zLock(sZeroMoveMutex);
      sZeroMove[fid] = std::make_pair(source_fsid, target_fsid);
      continue;
    }

    if (fullpath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) != std::string::npos) {
      // Drop a left-over atomic file instead of draining
      eos_thread_info("cmd=schedule2drain msg=zero-move fid=%llx source_fs=%u "
                      "target_fs=%u", fid, source_fsid, target_fsid);

      XrdSysMutexHelper zLock(sZeroMoveMutex);
      sZeroMove[fid] = std::make_pair(source_fsid, target_fsid);
      continue;
    }

    //-----------------------------------------------------------------------
    // Prepare file transfer parameters
    //-----------------------------------------------------------------------

    using eos::common::LayoutId;
    std::vector<unsigned int> locationfs;

    for (auto& location: locations) {
      // Ignore filesystem id 0
      if (!location) { continue; }

      if (source_snapshot.mId == location) {
        if (source_snapshot.mConfigStatus == eos::common::FileSystem::kDrain) {
          // Only add filesystems which are not in drain dead to the
          // list of possible locations
          locationfs.push_back(location);
        }
      } else {
        locationfs.push_back(location);
      }
    }

    XrdOucString full_capability = "";

    if ((LayoutId::GetLayoutType(lid) == LayoutId::kRaidDP ||
         LayoutId::GetLayoutType(lid) == LayoutId::kArchive ||
         LayoutId::GetLayoutType(lid) == LayoutId::kRaid6) &&
        source_snapshot.mConfigStatus == eos::common::FileSystem::kDrainDead) {
      //------------------------------------------------------------------
      // RAIN layouts (not replica) drain by running a reconstruction
      // 'eoscp -c' ... if they are in draindead
      // They are easy to configure, they just call an open with
      // reconstruction/replacement option and the real scheduling
      // is done when 'eoscp' is executed.
      //------------------------------------------------------------------
      eos_thread_info("msg=\"creating RAIN reconstruction job\" path=%s",
                      fullpath.c_str());

      full_capability = RAINFullCapability(fullpath.c_str(), source_snapshot.mId);
    } else {
      // Plain/replica layouts get source/target scheduled here
      // Schedule access to that file with the original layout
      long unsigned int fsindex = 0;

      // Exclude another scheduling for RAIN files
      // There is no alternative location here
      if (LayoutId::GetLayoutType(lid) == LayoutId::kRaidDP ||
          LayoutId::GetLayoutType(lid) == LayoutId::kArchive ||
          LayoutId::GetLayoutType(lid) == LayoutId::kRaid6) {
        // Point to the stripe which is accessible but should be drained
        locationfs.clear();
        locationfs.push_back(source_fsid);
        fsindex = 0;
      } else {
        // Check file access of drain file
        XrdOucErrInfo accError;
        int rc = checkFileAccess(lid, fsindex, locationfs, accError);

        if (rc) {
          // We schedule to retry the file after 60 seconds
          eos_thread_err("cmd=schedule2drain msg=\"%s\" fxid=%llx retc=%d",
                         accError.getErrText(), fid, rc);
          ScheduledToDrainFid[fid] = time(NULL) + 60;
          continue;
        }
      }

      if (size >= freebytes) {
        eos_thread_warning("skip fxid=%llx - file size >= free bytes "
                           "fsize=%llu free_bytes=%llu", fid, size, freebytes);
        continue;
      }

      // We schedule fid from replica_source => target_fs
      eos::common::FileSystem* replica_source_fs = 0;
      unsigned int replica_fsid = locationfs[fsindex];

      if ((!FsView::gFsView.mIdView.count(replica_fsid)) ||
          (replica_source_fs = FsView::gFsView.mIdView[replica_fsid]) == 0) {
        continue;
      }

      replica_source_fs->SnapShotFileSystem(replica_source_snapshot);

      eos_thread_info("subcmd=scheduling fid=%llx "
                      "drain_fsid=%u replica_source_fsid=%u target_fsid=%u",
                      fid, source_fsid, replica_fsid, target_fsid);

      unsigned long target_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);

      // Mask block checksums (set to kNone) for replica layouts
      if (LayoutId::GetLayoutType(lid) == LayoutId::kReplica) {
        target_lid = LayoutId::SetBlockChecksum(target_lid, LayoutId::kNone);
      }

      // Construct capability strings
      XrdOucString replica_source_capability =
          constructSourceCapability(target_lid, cid, fullpath.c_str(),
                                    fid, source_fsid,
                                    replica_source_snapshot.mPath.c_str(),
                                    replica_source_snapshot.mId,
                                    replica_source_snapshot.mHostPort.c_str());

      XrdOucString target_capability =
          constructTargetCapability(target_lid, cid, fullpath.c_str(), fid,
                                    source_fsid, target_snapshot.mPath.c_str(),
                                    target_snapshot.mId,
                                    target_snapshot.mHostPort.c_str(),
                                    size, lid, uid, gid);

      // Issue full capability string
      XrdOucErrInfo capError;
      int rc = issueFullCapability(replica_source_capability, target_capability,
                                   mCapabilityValidity,
                                   replica_source_snapshot.mHostPort.c_str(),
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
    }

    //-----------------------------------------------------------------------
    // Schedule file transfer
    //-----------------------------------------------------------------------

    std::unique_ptr<eos::common::TransferJob>
        txjob(new eos::common::TransferJob(full_capability.c_str()));

    if (target_fs->GetDrainQueue()->Add(txjob.get())) {
      eos_thread_info("cmd=schedule2drain msg=queued fid=%llx source_fs=%u "
                      "target_fs=%u", fid, source_fsid, target_fsid);
      eos_thread_debug("cmd=schedule2drain job=%s", full_capability.c_str());

      ScheduledToDrainFid[fid] = time(NULL) + 3600;
      XrdOucString response = "submitted";
      error.setErrInfo(response.length() + 1, response.c_str());
    } else {
      eos_thread_err("cmd=schedule2drain msg=\"failed to submit job\" "
                     "job=%s", full_capability.c_str());
      error.setErrInfo(0, "");
    }

    gOFS->MgmStats.Add("Scheduled2Drain", 0, 0, 1);
    EXEC_TIMING_END("Scheduled2Drain");
    return SFS_DATA;
  }

  eos_thread_debug("no files to schedule for drain in group=%s",
                   target_snapshot.mGroup.c_str());
  gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
  error.setErrInfo(0, "");
  return SFS_DATA;
}
