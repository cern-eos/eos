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
  eos_notice("waiting for join ...");
  if (mThread)
  {
    XrdSysThread::Cancel(mThread);
    if (!gOFS->Shutdown)
    {
      XrdSysThread::Join(mThread, NULL);
    }
    mThread = 0;
  }

  delete gScheduler;
  eos_notice("Stopping Drain for fs=%u", mFsId);
  
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
/**
 * @brief resets all drain relevant counters to 0
 * 
 */
/*----------------------------------------------------------------------------*/
DrainFS::SetInitialCounters()
{   
  fs = 0;
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
  //set also local var
  drainStatus= eos::common::FileSystem::kNoDrain;
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
DrainFS::GetSpaceConfiguration ()
/*----------------------------------------------------------------------------*/
/**
 * @brief get the number of transfers 
 * 
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count(mSpace))
  {
    nretries = stoi(FsView::gFsView.mSpaceView[mSpace]->GetConfigMember("drainer.nretries"));
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

  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();
  XrdSysTimer sleeper;

  eos_notice("Starting DrainFS for fs=%u",mFsId);
          
  {
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
    //set counter and status
    SetInitialCounters();
  }

 
  time_t drainstart = time(NULL);
  time_t drainperiod = 0;
  time_t drainendtime = 0;
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;

  XrdSysThread::SetCancelOff();
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = 0;
    if (FsView::gFsView.mIdView.count(mFsId))
    fs = FsView::gFsView.mIdView[mFsId];
    if (!fs) {
      XrdSysThread::SetCancelOn();
      eos_notice("Filesystem fsid=%u has been removed during drain operation",
                                                  mFsId);
      return 0;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDrainPrepare);
    //fs->SetLongLong("stat.drainretry", ntried - 1);
    drainStatus = eos::common::FileSystem::kDrainPrepare;

    fs->SnapShotFileSystem(drain_snapshot, false);
    drainperiod = fs->GetLongLong("drainperiod");
    drainendtime = drainstart + drainperiod;
    mSpace = drain_snapshot.mSpace;
    mGroup = drain_snapshot.mGroup;
  }
  
  XrdSysThread::SetCancelOn();
  // now we wait 60 seconds or the service delay time indicated by Master

  size_t kLoop = gOFS->MgmMaster.GetServiceDelay();
  if (!kLoop)
    kLoop = 60;

  for (size_t k = 0; k < kLoop; k++)
  {
    XrdSysThread::SetCancelOff();
    fs->SetLongLong("stat.timeleft", kLoop -1 - k);
    XrdSysThread::SetCancelOn();
    sleeper.Snooze(1);
    XrdSysThread::CancelPoint();
  }
  long long totalfiles = 0;
      
  //the function should list all the files available on the given FS and create a
  // DrainTransferJob object which should be responsible of the copy via thierdparty
  // and the removal of the original copy
  XrdSysThread::SetCancelOff();
  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::IFsView::FileList filelist;
    try {  
      filelist = gOFS->eosFsView->getFileList(mFsId);
    }
    catch (eos::MDException &e)
    {
     // no files to drain
    }
    if (filelist.size() == 0) {
      CompleteDrain();
      return 0;
    }  
    //set draining status
    fs->SetDrainStatus(eos::common::FileSystem::kDraining);
    drainStatus = eos::common::FileSystem::kDraining;

    eos::IFsView::FileIterator fid_it = filelist.begin();
    drainJobsMutex.Lock();
    while (fid_it != filelist.end()) {
      eos_notice("file to drain=%d ", *fid_it);
      drainJobs.push_back( shared_ptr<DrainTransferJob> (new DrainTransferJob (*fid_it, mFsId)));
      fid_it++;
    }
    drainJobsMutex.UnLock();
  }
  // set the shared object counter
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = 0;
    if (FsView::gFsView.mIdView.count(mFsId))
      fs = FsView::gFsView.mIdView[mFsId];
    if (!fs)
    {
      eos_notice("Filesystem fsid=%u has been removed during drain operation", mFsId
                        );
      XrdSysThread::SetCancelOn();
      return 0;
    }

    fs->SetLongLong("stat.drainbytesleft",
                    fs->GetLongLong("stat.statfs.usedbytes"));
    fs->SetLongLong("stat.drainfiles",
                    totalfiles);
    // ----------------------------------------------------------------------------
    // set status to 'RO'
    //----------------------------------------------------------------------------
    fs->SetConfigStatus(eos::common::FileSystem::kRO);
  }
  auto job = drainJobs.begin();

  while(job != drainJobs.end() && !drainStop) {
    eos::common::FileSystem::fsid_t fsIdTarget;

    if ((fsIdTarget= SelectTargetFS(*job->get())) != 0) {
      //testing, this should come from the GeoTreeEngine
      //fsIdTarget= 10;
      (*job)->SetTargetFS(fsIdTarget);
      gScheduler->Schedule((XrdJob*) &(*job->get()));
    } else {
      (*job)->SetStatus(DrainTransferJob::Failed);
    } 
    job++;
  }
  //TODO  add timeout
  while(gScheduler->Active() > 0) {
    //TODO update drain statistics
    eos_notice("Filesystem fsid=%u is under draining..", mFsId);
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }  

  CompleteDrain();
  return 0;
}

void
DrainFS::CompleteDrain() {

  eos_notice("Filesystem fsid=%u has been drained",mFsId);
  //----------------------------------------------------------------------------
  // set status to 'drained'
  //----------------------------------------------------------------------------
  {
   eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
   fs = 0;
   if (FsView::gFsView.mIdView.count(mFsId))
      fs = FsView::gFsView.mIdView[mFsId];
   if (!fs)
    {
      eos_notice("Filesystem fsid=%u has been removed during drain operation",
                        mFsId);
      XrdSysThread::SetCancelOn();
    }

   //fs->SetLongLong("stat.drainfiles", filesleft);
   drainStatus = eos::common::FileSystem::kDrained;
   fs->SetDrainStatus(eos::common::FileSystem::kDrained);
   fs->SetLongLong("stat.drainbytesleft", 0);
   fs->SetLongLong("stat.timeleft", 0);
   if (!gOFS->Shutdown)
   {
     //--------------------------------------------------------------------------
     // we automatically switch this filesystem to the 'empty' state - 
     // if the system is not shutting down
     //--------------------------------------------------------------------------
     fs->SetString("configstatus", "empty");
     fs->SetLongLong("stat.drainprogress", 100);
   }
  }
  //remove all jobs with OK status
  drainJobsMutex.Lock();
  auto it_jobs_ok = drainJobs.begin();
   while(it_jobs_ok != drainJobs.end()) {
     if ((*it_jobs_ok)->GetStatus() == DrainTransferJob::Status::OK) {
         drainJobs.erase(it_jobs_ok);
     }
     it_jobs_ok++;
  }
  drainJobsMutex.UnLock();
  XrdSysThread::SetCancelOn();
}

/*----------------------------------------------------------------------------*/
std::vector<shared_ptr<DrainTransferJob>>
DrainFS::GetJobs(DrainTransferJob::Status status)
/*----------------------------------------------------------------------------*/
/*
 * @brief get The jobs by Status
 *       
 */
/*----------------------------------------------------------------------------*/
{
  std::vector<shared_ptr<DrainTransferJob>> jobs;

  auto job = drainJobs.begin();
  while(job != drainJobs.end()) {
    if ((*job)->GetStatus() == status)
        jobs.push_back(*job);
    job++; 
  }
  return jobs;
  
}
/*----------------------------------------------------------------------------*/
void
DrainFS::DrainStop()
/*----------------------------------------------------------------------------*/
/*
 * @brief stop draining the current FS
 *      
 */
/*----------------------------------------------------------------------------*/
{
  drainStop = true;

  fs->OpenTransaction();
  fs->SetConfigStatus(eos::common::FileSystem::kDrainDead);
  fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
  fs->CloseTransaction();
  //set also local var
  drainStatus= eos::common::FileSystem::kNoDrain;
}
/*----------------------------------------------------------------------------*/
eos::common::FileSystem::fsid_t  
DrainFS::SelectTargetFS( DrainTransferJob &job)
/*----------------------------------------------------------------------------*/
/**
 *  * @brief select TargetFS using the GeoTreeEngine
 *    
 */    
/*----------------------------------------------------------------------------*/
{

  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex); 
   
  std::vector<FileSystem::fsid_t>* newReplicas = new std::vector<FileSystem::fsid_t>();
  std::vector<FileSystem::fsid_t> existingReplicas;


  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem* source_fs = 0;

  std::shared_ptr<eos::IFileMD> fmd =  gOFS->eosFileService->getFileMD(job.GetFileId());

  long unsigned int lid = fmd->getLayoutId();
  //TODO correctly check layout
  unsigned int nfilesystems = 1; //eos::common::LayoutId::GetStripeNumber(lid) + 1;
 
  source_fs = FsView::gFsView.mIdView[job.GetSourceFS()];
  source_fs->SnapShotFileSystem(source_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[source_snapshot.mGroup];

  unsigned int ncollocatedfs = 0;

  //check if this is needed
  /*switch (args->plctpolicy) {
    case kScattered:
      ncollocatedfs = 0;
      break;

    case kHybrid:
      switch (eos::common::LayoutId::GetLayoutType(args->lid)) {
        case eos::common::LayoutId::kPlain:
        ncollocatedfs = 1;
        break;

  case eos::common::LayoutId::kReplica:
      ncollocatedfs = nfilesystems - 1;
      break;

  default:
      ncollocatedfs = nfilesystems - eos::common::LayoutId::GetRedundancyStripeNumber(
                        args->lid);
      break;
    }

    break;

  // we only do geolocations for replica layouts
  case kGathered:
    ncollocatedfs = nfilesystems;
  }*/

  //check other replicas for the file
  std::vector<std::string> fsidsgeotags;

  existingReplicas = static_cast<std::vector<FileSystem::fsid_t>>(fmd->getLocations());

  if (!gGeoTreeEngine.getInfosFromFsIds(existingReplicas,
                                          &fsidsgeotags,
                                          0, 0)) {
      eos_notice("could not retrieve info for all avoid fsids");
      delete newReplicas;
      return 0;
    }

  auto repl = existingReplicas.begin();
  while( repl != existingReplicas.end()) {
    eos_notice("existing replicas: %d", *repl);
    repl++;
  }
  auto geo = fsidsgeotags.begin();
  while( geo != fsidsgeotags.end()) {
    eos_notice("geotags: %s", (*geo).c_str());
    geo++;
   }

  bool res = gGeoTreeEngine.placeNewReplicasOneGroup(
                      group, nfilesystems,
                      newReplicas,
                      (ino64_t) fmd->getId(),
                      NULL, //entrypoints
                      NULL, //firewall
                      GeoTreeEngine::draining,
                      &existingReplicas,
                      &fsidsgeotags,
                      fmd->getSize(),
                      "",//start from geotag
                      "",//client geo tag
                      ncollocatedfs,
                      NULL, //excludeFS
                      &fsidsgeotags, //excludeGeoTags
                      NULL); 

   if (res) {
     char buffer[1024];
     buffer[0] = 0;
     char* buf = buffer;

     for (auto it = newReplicas->begin(); it != newReplicas->end(); ++it) {
          buf += sprintf(buf, "%lu  ", (unsigned long)(*it));
     }
      
     eos_notice("GeoTree Draining Placement returned %d with fs id's -> %s",
                     (int)res, buffer);
     //return only one FS now
     eos::common::FileSystem::fsid_t targetFS = *newReplicas->begin();
     delete newReplicas;
     return targetFS;
   } else {
     eos_notice("could not place the replica");
     delete newReplicas;
     return 0;
   }
}


EOSMGMNAMESPACE_END
