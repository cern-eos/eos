// ----------------------------------------------------------------------
// File: Schedule2Drain.cc
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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------
{
  using eos::common::LayoutId;
  using eos::common::StringConversion;
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Scheduled2Drain");
  gOFS->MgmStats.Add("Schedule2Drain", 0, 0, 1);
  XrdOucString sfsid = env.Get("mgm.target.fsid");
  XrdOucString sfreebytes = env.Get("mgm.target.freebytes");
  char* alogid = env.Get("mgm.logid");
  // Static map with iterator position for the next group scheduling
  static std::map<std::string, size_t> sGroupCycle;
  static XrdSysMutex sGroupCycleMutex;
  static XrdSysMutex sZeroMoveMutex;
  static std::map < eos::common::FileId::fileid_t,
  std::pair < eos::common::FileSystem::fsid_t,
  eos::common::FileSystem::fsid_t >> sZeroMove;
  static time_t sScheduledFidCleanupTime = 0;

  // Deal with 0-size files 'scheduled' before, which just need a move in the
  // namespace
  bool has_zero_mv_files = false;
  {
    XrdSysMutexHelper zLock(sZeroMoveMutex);

    if (sZeroMove.size())
    {
      has_zero_mv_files = true;
    }
  }

  if (has_zero_mv_files)
  {
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
          eos_info("msg=\"drained 0-size file\" fxid=%llx source-fsid=%u "
          "target-fsid=%u", it->first, it->second.first, it->second.second);
        } else {
          // Check if this is atomic file
          if (fullpath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) != std::string::npos) {
            fmd->unlinkLocation(it->second.first);
            fmd->removeLocation(it->second.first);
            gOFS->eosView->updateFileStore(fmd.get());
            eos_info("msg=\"drained(unlinked) atomic upload file\" fxid=%llx source-fsid=%u "
                     "target-fsid=%u", it->first, it->second.first, it->second.second);
          } else {
            eos_warning("msg=\"unexpected file in zero-move list with size!=0 "
                        "and not atomic path - skipping\"");
          }
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }

      sZeroMove.erase(it++);
    }
  }

  if (alogid)
  {
    ThreadLogId.SetLogId(alogid, tident);
  }

  if ((!sfsid.length()) || (!sfreebytes.length()))
  {
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    return Emsg(epname, error, EINVAL,
                "unable to schedule - missing parameters [EINVAL]");
  }

  eos::common::FileSystem::fsid_t target_fsid = atoi(sfsid.c_str());
  eos::common::FileSystem::fsid_t source_fsid = 0;
  eos::common::FileSystem::fs_snapshot target_snapshot;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem::fs_snapshot replica_source_snapshot;
  eos::common::FileSystem* target_fs = 0;
  unsigned long long freebytes = (sfreebytes.c_str()) ?
                                 strtoull(sfreebytes.c_str(), 0, 10) : 0;
  eos_thread_info("cmd=schedule2drain fsid=%d freebytes=%llu logid=%s",
                  target_fsid, freebytes, alogid ? alogid : "");

  // Lock the view and get the filesystem information for the target where
  // we want to drain to
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  target_fs = FsView::gFsView.mIdView[target_fsid];

  if (!target_fs)
  {
    eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    return Emsg(epname, error, EINVAL,
                "unable to schedule - filesystem ID is not known");
  }

  target_fs->SnapShotFileSystem(target_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

  if (!group)
  {
    eos_thread_err("group=%s is not in group view", target_snapshot.mGroup.c_str());
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    return Emsg(epname, error, EINVAL,
                "unable to schedule - group is not known [EINVAL]");
  }

  // Select the next fs in the group to get a file to move
  size_t gposition = 0;
  {
    XrdSysMutexHelper scope_lock(sGroupCycleMutex);

    if (sGroupCycle.count(target_snapshot.mGroup))
    {
      gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
    } else {
      gposition = 0;
      sGroupCycle[target_snapshot.mGroup] = 0;
    }

    // Shift the iterator for the next schedule call to the following
    // filesystem in the group
    sGroupCycle[target_snapshot.mGroup]++;
    sGroupCycle[target_snapshot.mGroup] %= group->size();
  }

  eos_thread_debug("group=%s cycle=%lu", target_snapshot.mGroup.c_str(),
  gposition);
  // Try to find a file which is smaller than freebytes and has no replica
  // on the target filesystem. We start at a random position not to move
  // data of the same period to a single disk.
  FsGroup::const_iterator group_iterator = group->begin();
  std::advance(group_iterator, gposition);
  eos::common::FileSystem* source_fs = 0;

  for (size_t n = 0; n < group->size(); n++)
  {
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

    if (source_fs) {
      break;
    } else {
      if (++group_iterator == group->end()) {
        group_iterator = group->begin();
      }
    }
  }

  if (!source_fs)
  {
    eos_thread_debug("no source available");
    gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
    error.setErrInfo(0, "");
    return SFS_DATA;
  }

  source_fs->SnapShotFileSystem(source_snapshot);
  source_fsid = *group_iterator;
  // Lock namespace view here to avoid deadlock with the Commit.cc code on
  // the ScheduledToDrainFidMutex
  eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
  // @todo (esindril): This should be reviewed as copying the list might be
  // too slow when drain activity is high.
  eos::IFsView::FileList source_filelist =
    gOFS->eosFsView->getFileList(source_fsid);
  eos::IFsView::FileList target_filelist =
    gOFS->eosFsView->getFileList(target_fsid);
  unsigned long long nfids = (unsigned long long) source_filelist.size();
  eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u "
                   "n_source_fids=%llu", target_snapshot.mGroup.c_str(),
                   gposition, source_fsid, target_fsid, nfids);
  eos::IFsView::FileIterator fit = source_filelist.begin();

  while (fit != source_filelist.end())
  {
    eos_thread_debug("checking fid %llx", *fit);
    // Check that the target does not have this file
    eos::IFileMD::id_t fid = *fit;

    if (target_filelist.count(fid)) {
      // Move on to the next file, we have this file already
      ++fit;
      continue;
    } else {
      // Check that this file has not been scheduled during the last 1h
      time_t now = time(NULL);
      XrdSysMutexHelper sLock(ScheduledToDrainFidMutex);

      if (sScheduledFidCleanupTime < now) {
        // Do clean-up and set the next one in 10 minutes
        sScheduledFidCleanupTime = now + 600;
        std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it1;
        std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it2;
        it1 = it2 = ScheduledToDrainFid.begin();

        while (it2 != ScheduledToDrainFid.end()) {
          it1 = it2;
          it2++;

          if (it1->second < now) {
            ScheduledToDrainFid.erase(it1);
          }
        }
      }

      // Iterate to the next file if we have scheduled this file during the
      // last hour or anyway it is empty
      if ((ScheduledToDrainFid.count(fid) && ((ScheduledToDrainFid[fid] > (now))))) {
        eos_thread_debug("file %llx has already been scheduled at %lu", fid,
                         ScheduledToDrainFid[fid]);
        ++fit;
        continue;
      } else {
        std::string fullpath = "";
        std::shared_ptr<eos::IFileMD> fmd;

        try {
          fmd = gOFS->eosFileService->getFileMD(fid);
          fullpath = gOFS->eosView->getUri(fmd.get());
          XrdOucString savepath = fullpath.c_str();

          while (savepath.replace("&", "#AND#")) {}

          fullpath = savepath.c_str();
          fmd = gOFS->eosFileService->getFileMD(fid);
        } catch (eos::MDException& e) {
          ++fit;
          continue;
        }

        if (!fmd) {
          ++fit;
          continue;
        }

        std::vector<unsigned int> locationfs;
        long unsigned int lid = fmd->getLayoutId();
        unsigned long long cid = fmd->getContainerId();
        unsigned long long size = fmd->getSize();
        uid_t uid = fmd->getCUid();
        gid_t gid = fmd->getCGid();
        eos::IFileMD::LocationVector::const_iterator lociter;
        eos::IFileMD::LocationVector loc_vect = fmd->getLocations();

        for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
          // Ignore filesystem id 0
          if ((*lociter)) {
            if (source_snapshot.mId == *lociter) {
              if (source_snapshot.mConfigStatus == eos::common::FileSystem::kDrain) {
                // Only add filesystems which are not in drain dead to the
                // list of possible locations
                locationfs.push_back(*lociter);
              }
            } else {
              locationfs.push_back(*lociter);
            }
          }
        }

        XrdOucString fullcapability = "";
        XrdOucString hexfid = "";

        if (((LayoutId::GetLayoutType(lid) == LayoutId::kRaidDP) ||
             (LayoutId::GetLayoutType(lid) == LayoutId::kArchive) ||
             (LayoutId::GetLayoutType(lid) == LayoutId::kRaid6)) &&
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
          fullcapability += "source.url=root://";
          fullcapability += gOFS->ManagerId;
          fullcapability += "/";
          fullcapability += fullpath.c_str();
          fullcapability += "&target.url=/dev/null";
          XrdOucString source_env;
          source_env = "eos.pio.action=reconstruct&";
          source_env += "eos.pio.recfs=";
          source_env += (int) source_snapshot.mId;
          fullcapability += "&source.env=";
          fullcapability += XrdMqMessage::Seal(source_env, "_AND_");
          fullcapability += "&tx.layout.reco=true";
        } else {
          // Plain/replica layouts get source/target scheduled here
          XrdOucString sizestring = "";
          long unsigned int fsindex = 0;
          // Schedule access to that file with the original layout
          int retc = 0;
          std::vector<unsigned int> unavailfs; // not used
          eos::common::Mapping::VirtualIdentity_t h_vid;
          eos::common::Mapping::Root(h_vid);

          // Exclude another scheduling for RAIN files - there is no
          // alternitive location here.
          if (((LayoutId::GetLayoutType(lid) != LayoutId::kRaidDP) &&
               (LayoutId::GetLayoutType(lid) != LayoutId::kArchive) &&
               (LayoutId::GetLayoutType(lid) != LayoutId::kRaid6))) {
            std::string tried_cgi;
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
              // There is something wrong in the arguments of file access
              // inaccessible files we retry after 60 seconds
              eos_thread_err("cmd=schedule2drain msg=\"invalid argument to "
                             "FileAccess %llx retc=%d\"", fid, retc);
              ScheduledToDrainFid[fid] = time(NULL) + 60;
              fit++;
              continue;
            } else if (Quota::FileAccess(&acsargs)) {
              // Inaccessible files we retry after 60 seconds
              eos_thread_err("cmd=schedule2drain msg=\"no access to file %llx "
                             "retc=%d\"", fid, retc);
              ScheduledToDrainFid[fid] = time(NULL) + 60;
              fit++;
              continue;
            }
          } else {
            // point to the stripe which is accessible but should be drained
            locationfs.clear();
            locationfs.push_back(source_fsid);
            fsindex = 0;
          }

          if (size < freebytes) {
            eos::common::FileSystem* replica_source_fs = 0;

            if ((FsView::gFsView.mIdView.count(locationfs[fsindex]) == 0) ||
                (replica_source_fs = FsView::gFsView.mIdView[locationfs[fsindex]]) == 0) {
              fit++;
              continue;
            }

            replica_source_fs->SnapShotFileSystem(replica_source_snapshot);
            // We can schedule fid from replica_source => target_it
            eos_thread_info("cmd=schedule2drain subcmd=scheduling fid=%llx "
                            "drain_fsid=%u replica_source_fsid=%u target_fsid=%u",
                            fid, source_fsid, locationfs[fsindex], target_fsid);
            XrdOucString replica_source_capability = "";
            XrdOucString sizestring;
            unsigned long long target_lid = lid & 0xffffff0f;

            if (LayoutId::GetBlockChecksum(lid) != LayoutId::kNone) {
              // mask block checksums (e.g. for replica layouts)
              target_lid &= 0xff0fffff;
            }

            replica_source_capability += "mgm.access=read";
            replica_source_capability += "&mgm.lid=";
            replica_source_capability +=
              StringConversion::GetSizeString(sizestring, (unsigned long long) target_lid);
            // make's it a plain replica
            replica_source_capability += "&mgm.cid=";
            replica_source_capability +=
              StringConversion::GetSizeString(sizestring, cid);
            replica_source_capability += "&mgm.ruid=";
            replica_source_capability += (int) 1;
            replica_source_capability += "&mgm.rgid=";
            replica_source_capability += (int) 1;
            replica_source_capability += "&mgm.uid=";
            replica_source_capability += (int) 1;
            replica_source_capability += "&mgm.gid=";
            replica_source_capability += (int) 1;
            replica_source_capability += "&mgm.path=";
            replica_source_capability += fullpath.c_str();
            replica_source_capability += "&mgm.manager=";
            replica_source_capability += gOFS->ManagerId.c_str();
            replica_source_capability += "&mgm.fid=";
            eos::common::FileId::Fid2Hex(fid, hexfid);
            replica_source_capability += hexfid;
            replica_source_capability += "&mgm.sec=";
            replica_source_capability +=
              eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
            replica_source_capability += "&mgm.drainfsid=";
            replica_source_capability += (int) source_fsid;
            // build the replica_source_capability contents
            replica_source_capability += "&mgm.localprefix=";
            replica_source_capability += replica_source_snapshot.mPath.c_str();
            replica_source_capability += "&mgm.fsid=";
            replica_source_capability += (int) replica_source_snapshot.mId;
            replica_source_capability += "&mgm.sourcehostport=";
            replica_source_capability += replica_source_snapshot.mHostPort.c_str();
            XrdOucString target_capability = "";
            target_capability += "mgm.access=write";
            target_capability += "&mgm.lid=";
            target_capability +=
              StringConversion::GetSizeString(sizestring,
                                              (unsigned long long) target_lid);
            // make's it a plain replica
            target_capability += "&mgm.source.lid=";
            target_capability +=
              StringConversion::GetSizeString(sizestring,
                                              (unsigned long long) lid);
            target_capability += "&mgm.source.ruid=";
            target_capability +=
              StringConversion::GetSizeString(sizestring,
                                              (unsigned long long) uid);
            target_capability += "&mgm.source.rgid=";
            target_capability +=
              StringConversion::GetSizeString(sizestring,
                                              (unsigned long long) gid);
            target_capability += "&mgm.cid=";
            target_capability += StringConversion::GetSizeString(sizestring, cid);
            target_capability += "&mgm.ruid=";
            target_capability += (int) 1;
            target_capability += "&mgm.rgid=";
            target_capability += (int) 1;
            target_capability += "&mgm.uid=";
            target_capability += (int) 1;
            target_capability += "&mgm.gid=";
            target_capability += (int) 1;
            target_capability += "&mgm.path=";
            target_capability += fullpath.c_str();
            target_capability += "&mgm.manager=";
            target_capability += gOFS->ManagerId.c_str();
            target_capability += "&mgm.fid=";
            target_capability += hexfid;
            target_capability += "&mgm.sec=";
            target_capability += eos::common::SecEntity::ToKey(0, "eos/draining").c_str();
            target_capability += "&mgm.drainfsid=";
            target_capability += (int) source_fsid;
            // build the target_capability contents
            target_capability += "&mgm.localprefix=";
            target_capability += target_snapshot.mPath.c_str();
            target_capability += "&mgm.fsid=";
            target_capability += (int) target_snapshot.mId;
            target_capability += "&mgm.targethostport=";
            target_capability += target_snapshot.mHostPort.c_str();
            target_capability += "&mgm.bookingsize=";
            target_capability += StringConversion::GetSizeString(sizestring, size);
            // issue a replica_source_capability
            XrdOucEnv insource_capability(replica_source_capability.c_str());
            XrdOucEnv intarget_capability(target_capability.c_str());
            XrdOucEnv* source_capabilityenv = 0;
            XrdOucEnv* target_capabilityenv = 0;
            eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
            int caprc = 0;

            if ((caprc = gCapabilityEngine.Create(&insource_capability,
                                                  source_capabilityenv,
                                                  symkey, mCapabilityValidity)) ||
                (caprc = gCapabilityEngine.Create(&intarget_capability, target_capabilityenv,
                                                  symkey, mCapabilityValidity))) {
              eos_thread_err("unable to create source/target capability - errno=%u", caprc);
              gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
              return Emsg(epname, error, caprc, "create source/target capability [EADV]");
            } else {
              int caplen = 0;
              XrdOucString source_cap = source_capabilityenv->Env(caplen);
              XrdOucString target_cap = target_capabilityenv->Env(caplen);
              source_cap.replace("cap.sym", "source.cap.sym");
              target_cap.replace("cap.sym", "target.cap.sym");
              source_cap.replace("cap.msg", "source.cap.msg");
              target_cap.replace("cap.msg", "target.cap.msg");
              source_cap += "&source.url=root://";
              source_cap += replica_source_snapshot.mHostPort.c_str();
              source_cap += "//replicate:";
              source_cap += hexfid;
              target_cap += "&target.url=root://";
              target_cap += target_snapshot.mHostPort.c_str();
              target_cap += "//replicate:";
              target_cap += hexfid;
              fullcapability += source_cap;
              fullcapability += target_cap;
            }

            if (source_capabilityenv) {
              delete source_capabilityenv;
            }

            if (target_capabilityenv) {
              delete target_capabilityenv;
            }
          } else {
            fit++;
            continue;
          }
        }

        if (!size) {
          // This is a zero size file, we just move the location by adding
          // it to the static move map
          eos_thread_info("cmd=schedule2drain msg=zero-move fid=%x source_fs=%u "
                          "target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
          XrdSysMutexHelper zLock(sZeroMoveMutex);
          sZeroMove[fid] = std::make_pair(source_fsid, target_fsid);
          fit++;
          continue;
        } else {
          if (fullpath.find(EOS_COMMON_PATH_ATOMIC_FILE_PREFIX) != std::string::npos) {
            // if we need to drain a left-over atomic file we just drop it
            eos_thread_info("cmd=schedule2drain msg=zero-move fid=%x "
                            "source_fs=%u target_fs=%u", hexfid.c_str(),
                            source_fsid, target_fsid);
            XrdSysMutexHelper zLock(sZeroMoveMutex);
            sZeroMove[fid] = std::make_pair(source_fsid, target_fsid);
            fit++;
            continue;
          }

          std::unique_ptr<eos::common::TransferJob>
          txjob(new eos::common::TransferJob(fullcapability.c_str()));

          if (target_fs->GetDrainQueue()->Add(txjob.get())) {
            eos_thread_info("cmd=schedule2drain msg=queued fid=%x source_fs=%u "
                            "target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
            eos_thread_debug("cmd=schedule2drain job=%s", fullcapability.c_str());
            ScheduledToDrainFid[fid] = time(NULL) + 3600;
            // Send submitted response
            XrdOucString response = "submitted";
            error.setErrInfo(response.length() + 1, response.c_str());
          } else {
            eos_thread_err("cmd=schedule2drain msg=\"failed to submit job\""
                           " job=%s", fullcapability.c_str());
            error.setErrInfo(0, "");
          }
        }

        gOFS->MgmStats.Add("Scheduled2Drain", 0, 0, 1);
        EXEC_TIMING_END("Scheduled2Drain");
        return SFS_DATA;
      }
    }
  }

  // @todo (esindril): This should introduce a delay for the fsts asking for
  // new jobs rather than just hammering the MGM
  eos_thread_debug("no files to schedule for drain in group=%s",
                   target_snapshot.mGroup.c_str());
  gOFS->MgmStats.Add("SchedulingFailedDrain", 0, 0, 1);
  error.setErrInfo(0, "");
  return SFS_DATA;
}
