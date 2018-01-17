//------------------------------------------------------------------------------
//! @file Drainer.cc
//! @author Andrea Manzi - CERN
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

#include "mgm/drain/Drainer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/drain/DrainFS.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/FsView.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Drainer::Drainer()
{
  XrdSysThread::Run(&mThread, Drainer::StaticDrainer,
                    static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                    "Drainer Thread");
}

//------------------------------------------------------------------------------
// Stop running thread
//------------------------------------------------------------------------------
void
Drainer::Stop()
{
  XrdSysThread::Cancel(mThread);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Drainer::~Drainer()
{
  Stop();

  if (!gOFS->Shutdown) {
    XrdSysThread::Join(mThread, NULL);
  }
}

//------------------------------------------------------------------------------
// Start draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StartFSDrain(unsigned int sourceFsId, unsigned int targetFsId, XrdOucString& err)
{
  if (!gOFS->MgmOfsCentralDraining) {
    err =  "error: central drain is not enabled in the configuration";
    return false;
  }

  eos_notice("fs to drain=%d ", sourceFsId);
  eos::common::FileSystem::fs_snapshot_t  source_drain_snapshot;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  {
    auto it_fs = FsView::gFsView.mIdView.find(sourceFsId);

    if (it_fs == FsView::gFsView.mIdView.end()) {
      err = "error: the given FS does not exist";
      return false;
    }

    it_fs->second->SnapShotFileSystem(source_drain_snapshot, false);

    FileSystem::fsstatus_t status = it_fs->second->GetConfigStatus();

    if (status == eos::common::FileSystem::kDrain) {
      err = "error: the given FS is already under draining";
      return false;
    }
  }

  //check that the destination FS, if specified, is in the same space and group of the source
  if(targetFsId) {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    auto it_fs = FsView::gFsView.mIdView.find(targetFsId);
    eos::common::FileSystem::fs_snapshot_t  destination_drain_snapshot;
    if (it_fs == FsView::gFsView.mIdView.end()) {
      err = "error: the destination FS does not exist";
      return false;
    }

    it_fs->second->SnapShotFileSystem(destination_drain_snapshot, false);
    if ( source_drain_snapshot.mSpace == destination_drain_snapshot.mSpace &&
         source_drain_snapshot.mGroup == destination_drain_snapshot.mGroup ) {}
    else {
      err = "error: the destination FS does not belong to the same space and scheduling group as the source";
      return false;
     }
  }

  mDrainMutex.Lock();

  auto it_drainfs = mDrainFS.find(source_drain_snapshot.mHostPort);

  if (it_drainfs != mDrainFS.end()) {
    //check if the FS is already under draining for this node
    auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
    [sourceFsId](const shared_ptr<DrainFS> & element) {
      return element.get()->GetFsId() == sourceFsId;
    });

    if (it != it_drainfs->second.end()) { //fs is already draining
      err = "error: a central FS drain has already started for the given FS ";
      mDrainMutex.UnLock();
      return false;
    } else {
      //check if we have reached the max fs per node for this node
      if (it_drainfs->second.size() >= GetSpaceConf(source_drain_snapshot.mSpace)) {
        err = "error: reached maximum number of draining fs for the node";
         mDrainMutex.UnLock();
        return false;
      }
    }
  }

  //start the drain
  shared_ptr<DrainFS> fs = shared_ptr<DrainFS>(new DrainFS(sourceFsId, targetFsId));

  if (it_drainfs != mDrainFS.end()) {
    it_drainfs->second.insert(fs);
  } else {
    std::set<shared_ptr<DrainFS>> fs_set;
    fs_set.insert(fs);
    mDrainFS.insert(std::make_pair(std::string(source_drain_snapshot.mHostPort), fs_set));
  }
  XrdSysThread::Run(&fs->mThread,
                      DrainFS::StaticThreadProc,
                      static_cast<void*>(fs.get()),
                      XRDSYSTHREAD_HOLD,
                      "DrainFS Thread");
  mDrainMutex.UnLock();
  return true;
}

//------------------------------------------------------------------------------
// Stop draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StopFSDrain(unsigned int fsId, XrdOucString& err)
{
  if (!gOFS->MgmOfsCentralDraining) {
    err =  "error: central drain is not enabled in the configuration";
    return false;
  }

  eos_notice("fs to stop draining=%d ", fsId);
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  {
    auto it_fs = FsView::gFsView.mIdView.find(fsId);
    if (it_fs == FsView::gFsView.mIdView.end()) {
      err = "error: the given FS does not exist";
      return false;
    }
    it_fs->second->SnapShotFileSystem(drain_snapshot, false);
  }
  auto it_drainfs = mDrainFS.find(drain_snapshot.mHostPort);

  if (it_drainfs == mDrainFS.end()) {
    //fs is not drainin
    err = "error: a central FS drain has not started for the given FS ";
    return false;
  }

  //check if the FS is already under draining for this node
  auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
  [fsId](const shared_ptr<DrainFS>& element) {
    return element.get()->GetFsId() == fsId;
  });

  if (it == it_drainfs->second.end()) {
    //fs is not draining
    err = "error: a central FS drain has not started for the given FS ";
    return false;
  }

  (*it)->DrainStop();
  return true;
}

//------------------------------------------------------------------------------
// Clear file system from the given map
//------------------------------------------------------------------------------
bool
Drainer::ClearFSDrain(unsigned int fsId, XrdOucString& err)
{
  if (!gOFS->MgmOfsCentralDraining) {
    err =  "error: central drain is not enabled in the configuration";
    return false;
  }

  eos_notice("fs to clear=%d ", fsId);
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  {

     auto it_fs = FsView::gFsView.mIdView.find(fsId);

    if (it_fs == FsView::gFsView.mIdView.end()) {
      err = "error: the given FS does not exist";
      return false;
    }

    it_fs->second->SnapShotFileSystem(drain_snapshot, false);
  }
  auto it_drainfs = mDrainFS.find(drain_snapshot.mHostPort);

  if (it_drainfs == mDrainFS.end()) {
    // fs is not draining
    err = "error: the given FS is not drained or under drain";
    return false;
  }
  mDrainMutex.Lock();
  // Check if the FS is already under draining for this node
  auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
  [fsId](const shared_ptr<DrainFS>& element) {
    return element.get()->GetFsId() == fsId;
  });

  if (it == it_drainfs->second.end()) {
    err = "error: a central FS drain has not started for the given FS ";
    mDrainMutex.UnLock();
    return false;
  }

  it_drainfs->second.erase(it);
  mDrainMutex.UnLock();
  return true;
}

//------------------------------------------------------------------------------
// Get draining status
//------------------------------------------------------------------------------
bool
Drainer::GetDrainStatus(unsigned int fsId, XrdOucString& out, XrdOucString& err)
{
  if (!gOFS->MgmOfsCentralDraining) {
    err =  "error: central drain is not enabled in the configuration";
    return false;
  }

  if (mDrainFS.size() > 0) {
  } else {
    out +=  "No Drain activities are recorded on the System.\n";
    return true;
  }

  if (fsId == 0) {
    TableFormatterBase table;
    std::vector<std::string> selections;
    auto it_drainfs = mDrainFS.begin();

    while (it_drainfs != mDrainFS.end()) {
      auto it_map = (*it_drainfs).second.begin();

      while (it_map != (*it_drainfs).second.end()) {
        PrintTable(table, (*it_drainfs).first, (*it_map).get());
        it_map++;
      }

      it_drainfs++;
    }

    TableHeader table_header;
    table_header.push_back(std::make_tuple("node", 30, "s"));
    table_header.push_back(std::make_tuple("fs id", 10, "s"));
    table_header.push_back(std::make_tuple("drain status", 30, "s"));
    table.SetHeader(table_header);
    out =  table.GenerateTable(HEADER, selections).c_str();
  } else {
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    {
      auto it_fs = FsView::gFsView.mIdView.find(fsId);

      if (it_fs == FsView::gFsView.mIdView.end()) {
        err = "error: the given FS does not exist";
        return false;
      }

      it_fs->second->SnapShotFileSystem(drain_snapshot, false);
    }
    auto it_drainfs = mDrainFS.find(drain_snapshot.mHostPort);

    if (it_drainfs == mDrainFS.end()) {
      //fs is not draining
      err = "error: a central FS drain has not started for the given FS on the node";
      return false;
    }
    //check if the FS is already under draining for this node
    auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
        [fsId](const shared_ptr<DrainFS>&  element) {
        return element.get()->GetFsId() == fsId;
    });

    if (it == it_drainfs->second.end()) {
      err = "error: a central FS drain has not started for the given FS";
      return false;
    }
    TableFormatterBase table;
    std::vector<std::string> selections;
    TableHeader table_header;
    table_header.push_back(std::make_tuple("node", 30, "s"));
    table_header.push_back(std::make_tuple("fs id", 10, "s"));
    table_header.push_back(std::make_tuple("drain status", 30, "s"));
    PrintTable(table, drain_snapshot.mHostPort, (*it).get());
    table.SetHeader(table_header);
    out += table.GenerateTable(HEADER, selections).c_str();
    //second table
    TableFormatterBase table_jobs;
    TableHeader table_header_jobs;
    table_header_jobs.push_back(std::make_tuple("file id", 30, "s"));
    table_header_jobs.push_back(std::make_tuple("source fs", 30, "s"));
    table_header_jobs.push_back(std::make_tuple("destination fs", 30, "s"));
    table_header_jobs.push_back(std::make_tuple("error", 100, "s"));
    table_jobs.SetHeader(table_header_jobs);
    std::vector<shared_ptr<DrainTransferJob>>::const_iterator job_vect_it = (*it)->GetFailedJobs().begin();

    if (job_vect_it != (*it)->GetFailedJobs().end()) {
      out += "List of Files failed to be drained:\n\n";

      while (job_vect_it != (*it)->GetFailedJobs().end()) {
        PrintJobsTable(table_jobs, (*job_vect_it).get());
        job_vect_it++;
      }

      out +=  table_jobs.GenerateTable(HEADER, selections).c_str();
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
Drainer::PrintTable(TableFormatterBase& table, std::string node,
                    DrainFS* fs)
{
  TableData table_data;
  table_data.emplace_back();
  table_data.back().push_back(TableCell(node, "s"));
  table_data.back().push_back(TableCell(fs->GetFsId(), "s"));
  table_data.back().push_back(TableCell(FileSystem::GetDrainStatusAsString(
                                          fs->GetDrainStatus()), "s"));
  table.AddRows(table_data);
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
Drainer::PrintJobsTable(TableFormatterBase& table, DrainTransferJob* job)
{
  TableData table_data;
  table_data.emplace_back();
  table_data.back().push_back(TableCell(job->GetFileId(), "l"));
  table_data.back().push_back(TableCell(job->GetSourceFS(), "l"));
  table_data.back().push_back(TableCell(job->GetTargetFS(), "l"));
  table_data.back().push_back(TableCell(job->GetErrorString(), "s"));
  table.AddRows(table_data);
}

//------------------------------------------------------------------------------
// Static thread startup function
//------------------------------------------------------------------------------
void*
Drainer::StaticDrainer(void* arg)
{
  return reinterpret_cast<Drainer*>(arg)->Drain();
}

void*
Drainer::Drain()
{
  // Wait that the namespace is initialized
  bool go = false;
  XrdSysThread::SetCancelOn();
  XrdSysTimer sleeper;
  eos_static_debug("Drainer starting");

  do {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);

      if (gOFS->Initialized == gOFS->kBooted) {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    sleeper.Wait(1000);
  } while (!go);

  while (1) {
    uint64_t timeout_ms = 100;

    while (FsView::gFsView.ViewMutex.TimedRdLock(timeout_ms)) {
      XrdSysThread::CancelPoint();
    }

    XrdSysThread::SetCancelOff();
    auto space = FsView::gFsView.mSpaceView.begin();

    while (space != FsView::gFsView.mSpaceView.end()) {
      int maxdrainingfs = 5;
      std::string spacename = space->second->GetMember("name");

      if (FsView::gFsView.mSpaceView[spacename]->GetConfigMember("drainer.node.nfs")
          != "") {
        maxdrainingfs = atoi(
                          FsView::gFsView.mSpaceView[spacename]->GetConfigMember("drainer.node.nfs").c_str());
      } else {
        FsView::gFsView.mSpaceView[spacename]->SetConfigMember("drainer.node.nfs", "5",
            true, "/eos/*/mgm");
      }

      // Get the space configuration
      drainConfMutex.Lock();

      if (maxFSperNodeConfMap.count(spacename))  {
        auto it_conf = maxFSperNodeConfMap.find(spacename);
        (*it_conf).second = maxdrainingfs;
      } else {
        maxFSperNodeConfMap.insert(std::make_pair(spacename, maxdrainingfs));
      }

      drainConfMutex.UnLock();
      space++;
    }

    //execute only once at boot time
    if (go) {
      for (auto it_fs = FsView::gFsView.mIdView.begin();
                 it_fs != FsView::gFsView.mIdView.end(); it_fs++) {
        eos::common::FileSystem::fs_snapshot_t drain_snapshot;
        it_fs->second->SnapShotFileSystem(drain_snapshot, false);
        FileSystem::fsstatus_t confstatus = it_fs->second->GetConfigStatus();
        FileSystem::fsstatus_t drainstatus = it_fs->second->GetDrainStatus();

        if (confstatus == eos::common::FileSystem::kRO) {
          if (drainstatus != eos::common::FileSystem::kNoDrain &&  drainstatus !=  eos::common::FileSystem::kDrained) {
            //start the drain
            XrdOucString err;
            if (!StartFSDrain(it_fs->first,0, err)) {
	      eos_notice("Failed to start the drain for fs %d: %s", it_fs->first, err.c_str());
            }
          }

        }
      }
      go=false;
    }

    FsView::gFsView.ViewMutex.UnLockRead();
    XrdSysThread::SetCancelOn();
    sleeper.Wait(10000);
  }

  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
unsigned int
Drainer::GetSpaceConf(const std::string& space)
{
  //check space conf to see if we reached the max configured draining fs per node
  if (maxFSperNodeConfMap.count(space)) {
    return maxFSperNodeConfMap[space];
  } else {
    return 0;
  }
}

EOSMGMNAMESPACE_END
