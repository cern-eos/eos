//------------------------------------------------------------------------------
// File: FileSystemStatusUtils.cc
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "FileSystemStatusUtils.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "common/Logging.hh"

namespace eos::mgm::fsutils
{

void ApplyDrainedStatus(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  auto fs = FsView::gFsView.mIdView.lookupByID(fsid);
  eos_static_notice("msg=\"Drain complete\" fsid=%d", fsid);

  if (fs) {
    auto status = eos::common::DrainStatus::kDrained;
    eos::common::FileSystemUpdateBatch batch;
    batch.setDrainStatusLocal(status);
    batch.setLongLongLocal("local.drain.bytesleft", 0);
    batch.setLongLongLocal("local.drain.timeleft", 0);
    batch.setLongLongLocal("local.drain.failed", 0);
    batch.setLongLongLocal("local.drain.files", 0);

    if (!gOFS->Shutdown) {
      // If drain done and the system is not shutting down then set the
      // file system to "empty" state
      batch.setLongLongLocal("local.drain.progress", 100);
      batch.setLongLongLocal("local.drain.failed", 0);
      batch.setStringDurable("configstatus", "empty");
      FsView::gFsView.StoreFsConfig(fs);
    }

    fs->applyBatch(batch);
  }
}

void
ApplyFailedDrainStatus(eos::common::FileSystem::fsid_t fsid,
                       uint64_t numFailedJobs)
{
  eos_static_notice("msg=\"failed drain\" fsid=%d", fsid);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  auto fs = FsView::gFsView.mIdView.lookupByID(fsid);

  if (fs) {
    auto drain_status = eos::common::DrainStatus::kDrainFailed;
    eos::common::FileSystemUpdateBatch batch;
    batch.setDrainStatusLocal(drain_status);
    batch.setLongLongLocal("local.drain.timeleft", 0);
    batch.setLongLongLocal("local.drain.progress", 100);
    batch.setLongLongLocal("local.drain.failed", numFailedJobs);
    fs->applyBatch(batch);
  }
}

std::vector<eos::common::FileSystem::fsid_t>
FsidsinGroup(const std::string& groupname,
             eos::common::ActiveStatus active_status,
             eos::common::DrainStatus drain_status)
{
  std::vector<eos::common::FileSystem::fsid_t> result;
  eos::common::RWMutexReadLock rlock(FsView::gFsView.ViewMutex);
  auto group_it = FsView::gFsView.mGroupView.find(groupname);

  if (group_it == FsView::gFsView.mGroupView.end()) {
    eos_static_err("msg=\"group not found: \" %s", groupname.c_str());
    return {};
  }

  for (auto fs_it = group_it->second->begin();
       fs_it != group_it->second->end();
       ++fs_it) {
    auto target = FsView::gFsView.mIdView.lookupByID(*fs_it);

    if (target &&
        target->GetActiveStatus() == eos::common::ActiveStatus::kOnline &&
        target->GetDrainStatus() == eos::common::DrainStatus::kNoDrain) {
      result.emplace_back(*fs_it);
    }
  }

  return result;
}

std::map<eos::common::FileSystem::fsid_t, FsidStatus>
GetGroupFsStatus(const std::string& groupname)
{
  eos::common::RWMutexReadLock rlock(FsView::gFsView.ViewMutex);
  auto group_it = FsView::gFsView.mGroupView.find(groupname);

  if (group_it == FsView::gFsView.mGroupView.end()) {
    eos_static_err("msg=\"group not found: \" %s", groupname.c_str());
    return {};
  }

  fs_status_map_t result;

  for (auto fs_it = group_it->second->begin();
       fs_it != group_it->second->end();
       ++fs_it) {
    auto target = FsView::gFsView.mIdView.lookupByID(*fs_it);

    if (target) {
      result.emplace(*fs_it, FsidStatus{target->GetActiveStatus(),
                                        target->GetDrainStatus()});
    }
  }

  return result;
}

} // eos::mgm::fsutils
