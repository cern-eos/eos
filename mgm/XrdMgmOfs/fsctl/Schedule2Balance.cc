// ----------------------------------------------------------------------
// File: Schedule2Balance.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("Scheduled2Balance");
  gOFS->MgmStats.Add("Schedule2Balance", 0, 0, 1);

  XrdOucString sfsid = env.Get("mgm.target.fsid");
  XrdOucString sfreebytes = env.Get("mgm.target.freebytes");
  char* alogid = env.Get("mgm.logid");
  char* simulate = env.Get("mgm.simulate"); // used to test the routing

  // static map with iterator position for the next group scheduling and it's mutex
  static std::map<std::string, size_t> sGroupCycle;
  static XrdSysMutex sGroupCycleMutex; 
  static time_t sScheduledFidCleanupTime = 0;

  if (alogid)
  {
    ThreadLogId.SetLogId(alogid, tident);
  }

  if ((!sfsid.length()) || (!sfreebytes.length()))
  {
    gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
    return Emsg(epname, error, EINVAL, "unable to schedule - missing parameters [EINVAL]");
  }

  eos::common::FileSystem::fsid_t target_fsid = atoi(sfsid.c_str());
  eos::common::FileSystem::fsid_t source_fsid = 0;
  eos::common::FileSystem::fs_snapshot target_snapshot;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem* target_fs = 0;

  unsigned long long freebytes = (sfreebytes.c_str()) ? strtoull(sfreebytes.c_str(), 0, 10) : 0;

  eos_thread_info("cmd=schedule2balance fsid=%d freebytes=%llu logid=%s",
                  target_fsid, freebytes, alogid ? alogid : "");

  while (1)
    // lock the view and get the filesystem information for the target where be balance to
  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    target_fs = FsView::gFsView.mIdView[target_fsid];
    if (!target_fs)
    {
      eos_thread_err("fsid=%u is not in filesystem view", target_fsid);
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      return Emsg(epname, error, EINVAL,
                  "unable to schedule - filesystem ID is not known");
    }

    target_fs->SnapShotFileSystem(target_snapshot);
    FsGroup* group = FsView::gFsView.mGroupView[target_snapshot.mGroup];

    size_t groupsize = FsView::gFsView.mGroupView[target_snapshot.mGroup]->size();
    if (!group)
    {
      eos_thread_err("group=%s is not in group view", target_snapshot.mGroup.c_str());
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      return Emsg(epname, error, EINVAL,
                  "unable to schedule - group is not known [EINVAL]");
    }

    eos_thread_debug("group=%s", target_snapshot.mGroup.c_str());

    // select the next fs in the group to get a file to move
    size_t gposition = 0;
    sGroupCycleMutex.Lock();
    if (sGroupCycle.count(target_snapshot.mGroup))
    {
      gposition = sGroupCycle[target_snapshot.mGroup] % group->size();
    }
    else
    {
      gposition = 0;
      sGroupCycle[target_snapshot.mGroup] = 0;
    }
    // shift the iterator for the next schedule call to the following filesystem in the group
    sGroupCycle[target_snapshot.mGroup]++;
    sGroupCycle[target_snapshot.mGroup] %= groupsize;
    sGroupCycleMutex.UnLock();

    eos_thread_debug("group=%s cycle=%lu",
                     target_snapshot.mGroup.c_str(), gposition);
    // try to find a file, which is smaller than the free bytes and has no replica on the target filesystem
    // we start at a random position not to move data of the same period to a single disk

    group = FsView::gFsView.mGroupView[target_snapshot.mGroup];
    FsGroup::const_iterator group_iterator;
    group_iterator = group->begin();
    std::advance(group_iterator, gposition);

    eos::common::FileSystem* source_fs = 0;
    for (size_t n = 0; n < group->size(); n++)
    {
      // skip over the target file system, that isn't usable
      if (*group_iterator == target_fsid)
      {
        source_fs = 0;
        group_iterator++;
        if (group_iterator == group->end()) group_iterator = group->begin();
        continue;
      }
      source_fs = FsView::gFsView.mIdView[*group_iterator];
      if (!source_fs)
        continue;
      source_fs->SnapShotFileSystem(source_snapshot);
      source_fsid = *group_iterator;
      if ((source_snapshot.mDiskFilled < source_snapshot.mNominalFilled) || // this is not a source since it is empty
          (source_snapshot.mStatus != eos::common::FileSystem::kBooted) || // this filesystem is not readable
          (source_snapshot.mConfigStatus < eos::common::FileSystem::kRO) ||
          (source_snapshot.mErrCode != 0) ||
          (source_fs->GetActiveStatus(source_snapshot) == eos::common::FileSystem::kOffline))
      {
        source_fs = 0;
        group_iterator++;
        if (group_iterator == group->end()) group_iterator = group->begin();
        continue;
      }
      break;
    }

    if (!source_fs)
    {
      eos_thread_debug("no source available");
      gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
      error.setErrInfo(0, "");
      return SFS_DATA;
    }
    source_fs->SnapShotFileSystem(source_snapshot);

    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    eos::FileSystemView::FileList source_filelist;
    eos::FileSystemView::FileList target_filelist;

    try
    {
      source_filelist = gOFS->eosFsView->getFileList(source_fsid);
    }
    catch (eos::MDException &e)
    {
      source_filelist.set_deleted_key(0);
      source_filelist.set_empty_key(0xffffffffffffffff);
    }

    try
    {
      target_filelist = gOFS->eosFsView->getFileList(target_fsid);
    }
    catch (eos::MDException &e)
    {
      target_filelist.set_deleted_key(0);
      target_filelist.set_empty_key(0xffffffffffffffff);
    }

    unsigned long long nfids = (unsigned long long) source_filelist.size();

    eos_thread_debug("group=%s cycle=%lu source_fsid=%u target_fsid=%u n_source_fids=%llu",
                     target_snapshot.mGroup.c_str(), gposition, source_fsid, target_fsid, nfids);
    unsigned long long rpos = (unsigned long long) ((0.999999 * random() * nfids) / RAND_MAX);
    eos::FileSystemView::FileIterator fit = source_filelist.begin();
    std::advance(fit, rpos);
    while (fit != source_filelist.end())
    {
      // check that the target does not have this file
      eos::IFileMD::id_t fid = *fit;
      if (target_filelist.count(fid))
      {
        // iterate to the next file, we have this file already
        fit++;
        continue;
      }
      else
      {
        // check that this file has not been scheduled during the 1h period
        XrdSysMutexHelper sLock(ScheduledToBalanceFidMutex);
        time_t now = time(NULL);
        if (sScheduledFidCleanupTime < now)
        {
          // next clean-up in 10 minutes
          sScheduledFidCleanupTime = now + 600;
          // do some cleanup
          std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it1;
          std::map<eos::common::FileSystem::fsid_t, time_t>::iterator it2;
          it1 = it2 = ScheduledToBalanceFid.begin();
          while (it2 != ScheduledToBalanceFid.end())
          {
            it1 = it2;
            it2++;
            if (it1->second < now)
            {
              ScheduledToBalanceFid.erase(it1);
            }
          }
        }
        if ((ScheduledToBalanceFid.count(fid) && ((ScheduledToBalanceFid[fid] > (now)))))
        {
          // iterate to the next file, we have scheduled this file during the last hour or anyway it is empty
          fit++;
          continue;
        }
        else
        {
          eos::IFileMD* fmd = 0;
          unsigned long long cid = 0;
          unsigned long long size = 0;
          long unsigned int lid = 0;
          uid_t uid = 0;
          gid_t gid = 0;
          std::string fullpath = "";

          try
          {
            fmd = gOFS->eosFileService->getFileMD(fid);
            fullpath = gOFS->eosView->getUri(fmd);
	    XrdOucString savepath=fullpath.c_str();
	    while (savepath.replace("&", "#AND#")){}
	    fullpath = savepath.c_str();
            fmd = gOFS->eosFileService->getFileMD(fid);
            lid = fmd->getLayoutId();
            cid = fmd->getContainerId();
            size = fmd->getSize();
            uid = fmd->getCUid();
            gid = fmd->getCGid();
          }
          catch (eos::MDException &e)
          {
            fmd = 0;
          }

          if (fmd)
          {
            if ((size > 0) && (size < freebytes))
            {
              // we can schedule fid from source => target_it
              eos_thread_info("subcmd=scheduling fid=%llx source_fsid=%u target_fsid=%u",
                              fid, source_fsid, target_fsid);

              XrdOucString source_capability = "";
              XrdOucString sizestring;
              source_capability += "mgm.access=read";
              source_capability += "&mgm.lid=";
              source_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                (unsigned long long) lid & 0xffffff0f);
              // make's it a plain replica
              source_capability += "&mgm.cid=";
              source_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
              source_capability += "&mgm.ruid=";
              source_capability += (int) 1;
              source_capability += "&mgm.rgid=";
              source_capability += (int) 1;
              source_capability += "&mgm.uid=";
              source_capability += (int) 1;
              source_capability += "&mgm.gid=";
              source_capability += (int) 1;
              source_capability += "&mgm.path=";
              source_capability += fullpath.c_str();
              source_capability += "&mgm.manager=";
              source_capability += gOFS->ManagerId.c_str();
              source_capability += "&mgm.fid=";
              XrdOucString hexfid;
              eos::common::FileId::Fid2Hex(fid, hexfid);
              source_capability += hexfid;

              source_capability += "&mgm.sec=";
              source_capability += eos::common::SecEntity::ToKey(0, "eos/balancing").c_str();

              source_capability += "&mgm.drainfsid=";
              source_capability += (int) source_fsid;

              // build the source_capability contents
              source_capability += "&mgm.localprefix=";
              source_capability += source_snapshot.mPath.c_str();
              source_capability += "&mgm.fsid=";
              source_capability += (int) source_snapshot.mId;
              source_capability += "&mgm.sourcehostport=";
              source_capability += source_snapshot.mHostPort.c_str();

              XrdOucString target_capability = "";
              target_capability += "mgm.access=write";
              target_capability += "&mgm.lid=";
              target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                (unsigned long long) lid & 0xffffff0f);
              // make's it a plain replica
              target_capability += "&mgm.source.lid=";
              target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                (unsigned long long) lid);
              target_capability += "&mgm.source.ruid=";
              target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                (unsigned long long) uid);
              target_capability += "&mgm.source.rgid=";
              target_capability += eos::common::StringConversion::GetSizeString(sizestring,
                                                                                (unsigned long long) gid);

              target_capability += "&mgm.cid=";
              target_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
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
              target_capability += eos::common::SecEntity::ToKey(0, "eos/balancing").c_str();

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
              target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
              // issue a source_capability
              XrdOucEnv insource_capability(source_capability.c_str());
              XrdOucEnv intarget_capability(target_capability.c_str());
              XrdOucEnv* source_capabilityenv = 0;
              XrdOucEnv* target_capabilityenv = 0;
              XrdOucString fullcapability = "";
              eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

              int caprc = 0;
              if ((caprc = gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
                  (caprc = gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey)))
              {
                eos_thread_err("unable to create source/target capability - errno=%u", caprc);
                gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
                return Emsg(epname, error, caprc, "create source/target capability [EADV]");
              }
              else
              {
                int caplen = 0;
                XrdOucString source_cap = source_capabilityenv->Env(caplen);
                XrdOucString target_cap = target_capabilityenv->Env(caplen);
                source_cap.replace("cap.sym", "source.cap.sym");
                target_cap.replace("cap.sym", "target.cap.sym");
                source_cap.replace("cap.msg", "source.cap.msg");
                target_cap.replace("cap.msg", "target.cap.msg");
                source_cap += "&source.url=root://";
                source_cap += source_snapshot.mHostPort.c_str();
                source_cap += "//replicate:";
                source_cap += hexfid;
                target_cap += "&target.url=root://";
                target_cap += target_snapshot.mHostPort.c_str();
                target_cap += "//replicate:";
                target_cap += hexfid;
                fullcapability += source_cap;
                fullcapability += target_cap;

                // send submitted response
                XrdOucString response = "submitted";
                error.setErrInfo(response.length() + 1, response.c_str());

                eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());

                if (!simulate)
                {
                  if (target_fs->GetBalanceQueue()->Add(txjob))
                  {
                    eos_thread_info("cmd=queued fid=%x source_fs=%u target_fs=%u", hexfid.c_str(), source_fsid, target_fsid);
                    eos_thread_debug("job=%s", fullcapability.c_str());
                  }
                }

                if (txjob)
                {
                  delete txjob;
                }

                if (source_capabilityenv)
                  delete source_capabilityenv;
                if (target_capabilityenv)
                  delete target_capabilityenv;

                gOFS->MgmStats.Add("Scheduled2Balance", 0, 0, 1);
                EXEC_TIMING_END("Scheduled2Balance");
                return SFS_DATA;
              }
            }
            else
            {
              fit++;
              continue;
            }
          }
          else
          {
            fit++;
            continue;
          }
        }
      }
    }
    break;
  }
  gOFS->MgmStats.Add("SchedulingFailedBalance", 0, 0, 1);
  error.setErrInfo(0, "");
  return SFS_DATA;
}
