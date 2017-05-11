// ----------------------------------------------------------------------
// File: DrainFS.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "mgm/drain/DrainFS.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN


DrainFS::~DrainFS ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 * 
 * Cancels and joins the drain thread.
 */
/*----------------------------------------------------------------------------*/
{
  eos_static_info("waiting for join ...");
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    if (!gOFS->Shutdown)
    {
      XrdSysThread::Join(mThread, NULL);
    }
    mThread = 0;
  }
  ResetCounter();
  eos_static_notice("Stopping Drain for fs=%u", mFsId);
  //destroy all elements in the map

}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
/**
 * @brief resets all drain relevant counters to 0
 * 
 */
/*----------------------------------------------------------------------------*/
DrainFS::ResetCounter ()
{
  FileSystem* fs = 0;
  if (FsView::gFsView.mIdView.count(mFsId))
  {
    fs = FsView::gFsView.mIdView[mFsId];
    if (fs)
    {
      fs->OpenTransaction();
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.drainfiles", 0);
      fs->SetLongLong("stat.timeleft", 0);
      fs->SetLongLong("stat.drainprogress", 0);
      fs->SetLongLong("stat.drainretry", 0);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->CloseTransaction();
    }
  }
}

/*----------------------------------------------------------------------------*/
void*
DrainFS::StaticThreadProc (void* arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief static thread start function
 * 
 */
/*----------------------------------------------------------------------------*/
{
  return reinterpret_cast<DrainFS*> (arg)->Drain();
}

/*----------------------------------------------------------------------------*/
void
DrainFS::SetSpaceNode ()
/*----------------------------------------------------------------------------*/
/**
 * @brief set number of transfers and rate in all participating nodes
 * 
 */
/*----------------------------------------------------------------------------*/
{
  std::string SpaceNodeTransfers = "";
  std::string SpaceNodeTransferRate = "";
  FileSystem* fs = 0;

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count(mSpace))
  {
    SpaceNodeTransfers = FsView::gFsView.mSpaceView[mSpace]->GetConfigMember("drainer.node.ntx");
    SpaceNodeTransferRate = FsView::gFsView.mSpaceView[mSpace]->GetConfigMember("drainer.node.rate");
  }

  FsGroup::const_iterator git;
  if (FsView::gFsView.mGroupView.count(mGroup))
  {
    for (git = FsView::gFsView.mGroupView[mGroup]->begin(); git != FsView::gFsView.mGroupView[mGroup]->end(); git++)
    {
      if (FsView::gFsView.mIdView.count(*git))
      {
        fs = FsView::gFsView.mIdView[*git];
        if (fs)
        {
          FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];
          if (node)
          {
            // broadcast the rate & stream configuration if changed
            if (node->GetConfigMember("stat.drain.ntx") != SpaceNodeTransfers)
            {
              node->SetConfigMember("stat.drain.ntx",
                                    SpaceNodeTransfers, false, "", true);
            }
            if (node->GetConfigMember("stat.drain.rate") != SpaceNodeTransferRate)
            {
              node->SetConfigMember("stat.drain.rate",
                                    SpaceNodeTransferRate, false, "", true);
            }
          }
        }
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
void*
DrainFS::Drain (void)
/*----------------------------------------------------------------------------*/
/**
 * @brief thread function running the drain supervision
 * 
 */
/*----------------------------------------------------------------------------*/
{

  //the function should list all the files available on the given FS and create a
  // DrainTransfer object which should be responsible of the copy via thierdparty
  // and the removal of the original copy
  
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  eos::IFsView::FileList filelist;

  filelist = gOFS->eosFsView->getFileList(mFsId);

  if (filelist.size() == 0) {
      return NULL;
  }
  eos::IFsView::FileIterator fid_it = filelist.begin();

  while (fid_it != filelist.end()) {
    eos_notice("file to drain=%d ", *fid_it);
    //this should call 
    /*    bool placeRes = gGeoTreeEngine.placeNewReplicasOneGroup(
                      group, nfilesystems,
                      args->selected_filesystems,
                      args->inode,
                      args->dataproxys,
                      args->firewallentpts,
                      GeoTreeEngine::regularRW,
                      // file systems to avoid are assumed to already host a replica
                      args->alreadyused_filesystems,
                      &fsidsgeotags,
                      args->bookingsize,
                      args->plctTrgGeotag ? *args->plctTrgGeotag : "",
                      args->vid->geolocation,
                      ncollocatedfs,
                      NULL,
                      NULL,
                      NULL); */
    //testing, this should come from the GeoTreeEngine
    eos::common::FileSystem::fsid_t fsIdTarget= 5;
      
    //schedule the  job
    DrainTransferJob* job =  new DrainTransferJob (*fid_it, mFsId, fsIdTarget);
    gScheduler->Schedule((XrdJob*) job);
    fid_it++;
  }
  return 0;
}

EOSMGMNAMESPACE_END
