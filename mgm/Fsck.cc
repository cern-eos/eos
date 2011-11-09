// ----------------------------------------------------------------------
// File: Fsck.cc
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

/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Mapping.hh"
#include "mgm/Fsck.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/

#include "XrdPosix/XrdPosixXrootd.hh"
/*----------------------------------------------------------------------------*/

#include <iostream>
#include <fstream>
#include <vector>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Fsck::Fsck() 
{
  //  mTotalErrorMap.set_empty_key("");
  //  mErrorHelp.set_empty_key("");
  //  mFsidErrorMap.set_empty_key("");
  //  mFsidErrorFidSet.set_empty_key("");

  mScanThreadInfo.set_deleted_key(0);
  mScanThreads.set_deleted_key(0);
  mScanThreadsJoin.set_deleted_key(0);
  
  mRunning = false;

  {
    XrdSysMutexHelper lock(mErrorMapMutex);
    mTotalErrorMap["totalfiles"]=0;
    mErrorNames.resize(15);
    mErrorHelp["totalfiles"] = "Total number of replicas found";
    mErrorNames[0] = "totalfiles";
    
    mTotalErrorMap["diff_mgm_disk_size"] = 0;
    mErrorHelp["diff_mgm_disk_size"] = "The size registered in the namespace differs from the size of a replica on disk";
    mErrorNames[1] = "diff_mgm_disk_size";
    
    mTotalErrorMap["diff_fst_disk_fmd_size"] = 0;
    mErrorHelp["diff_fst_disk_fmd_size"] = "The size of a replica on disk differs from the size stored in the changelog on the FST.";
    mErrorNames[2] = "diff_fst_disk_fmd_size";
    
    mTotalErrorMap["diff_mgm_disk_checksum"] = 0;
    mErrorHelp["diff_mgm_disk_checksum"] = "The checksum registered in the namespace differs from the checksum of a replica on disk";
    mErrorNames[3] = "diff_mgm_disk_checksum";
    
    mTotalErrorMap["diff_fst_disk_fmd_checksum"] = 0;
    mErrorHelp["diff_fst_disk_fmd_checksum"] = "The checksum in the chenagelog of the FST differes from the checksum stored in the extended attributes on disk";
    mErrorNames[4] = "diff_fst_disk_fmd_checksum";
    
    mTotalErrorMap["diff_file_checksum_scan"] = 0;
    mErrorHelp["diff_file_checksum_scan"] = "The computed checksum during the file scan differes from the checksum stored in the extended attributes on disk";
    mErrorNames[5] = "diff_file_checksum_scan";
    
    mTotalErrorMap["diff_block_checksum_scan"] = 0;
    mErrorHelp["diff_block_checksum_scan"] = "A block checksum errors has been detected during the file scan";
    mErrorNames[6] = "diff_block_checksum_scan";
    
    mTotalErrorMap["scanned_files"] = 0;
    mErrorHelp["scanned_files"] = "Number of files scanned by the checksum scanner";
    mErrorNames[7] = "scanned_files";
    
    mTotalErrorMap["not_scanned_files"] = 0;
    mErrorHelp["not_scanned_files"] = "Number of files without checksum scan";
    mErrorNames[8] = "not_scanned_files";
    
    mTotalErrorMap["replica_not_registered"] = 0;
    mErrorHelp["replica_not_registered"] = "Replica not registered";
    mErrorNames[9] = "replica_not_registered";
    
    mTotalErrorMap["replica_orphaned"] = 0;
    mErrorHelp["replica_orphaned"] = "There is no file name anymore connected to that replica";
    mErrorNames[10] = "replica_orphaned";
    
    mTotalErrorMap["diff_replica_layout"] = 0;
    mErrorHelp["diff_replica_layout"] = "There is a different number of replica's existing than defined by the layout";
    mErrorNames[11] = "diff_replica_layout";
    
    mTotalErrorMap["replica_offline"] = 0;
    mErrorHelp["replica_offline"] = "Not all replicas are online";
    mErrorNames[12] = "replica_offline";
    
    mTotalErrorMap["file_offline"] = 0;
    mErrorHelp["file_offline"] = "No replica is accessible";
    mErrorNames[13] = "file_offline";
    
    mTotalErrorMap["replica_missing"] = 0;
    mErrorHelp["replica_missing"] = "There is a reference to a replica in the namespace, but the replica was not seen on the storage node";
    mErrorNames[14] = "replica_missing";
    
    mParallelThreads=0;
    mThread = 0;
    n_error_file_offline = 0;
    n_error_fst_blockchecksum = 0;
    n_error_fst_disk_fmd_checksum_differ = 0;
    n_error_fst_disk_fmd_size_differ = 0;
    n_error_fst_filechecksum = 0;
    n_error_mgm_disk_checksum_differ = 0;
    n_error_mgm_disk_size_differ = 0;
    n_error_replica_layout = 0;
    n_error_replica_missing = 0;
    n_error_replica_not_registered = 0;
    n_error_replica_offline = 0;
    n_error_replica_orphaned = 0;
    nchecked=0;
    nunchecked=0;
    totalfiles=0;
    
    for (size_t i=0; i< mErrorNames.size(); i++) {
      mFsidErrorMap[mErrorNames[i]].set_deleted_key(0);
      mFsidErrorFidSet[mErrorNames[i]].set_deleted_key(0);
    }
  } 
} 

/* ------------------------------------------------------------------------- */
bool
Fsck::Start()
{
  if (!mRunning) {
    XrdSysThread::Run(&mThread, Fsck::StaticCheck, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Report Receiver Thread");
    mRunning = true;
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
bool
Fsck::Stop()
{
  if (mRunning) {
    eos_static_info("cancel fsck thread");

    // cancel the master
    XrdSysThread::Cancel(mThread);

    // kill all running eos-fst-dump commands to terminate the pending Scan threads
    system("pkill -9 eos-fst-dump");

    // we don't cancel all the still pending Scan threads since they will terminate on their own
    do {
      size_t nthreads=0;
      {
	XrdSysMutexHelper lock(mScanThreadMutex);	
	nthreads = mScanThreads.size();
      }
     
      if (!nthreads) {
        break;
      }    
      XrdSysTimer sleeper;
      sleeper.Snooze(1);
    } while(1);

    // join the master thread
    XrdSysThread::Join(mThread,NULL);
    eos_static_info("joined fsck thread");
    mRunning = false;
    Log(false,"disabled check");
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
Fsck::~Fsck() 
{
  if (mRunning)
    Stop();
}

/* ------------------------------------------------------------------------- */
void* 
Fsck::StaticCheck(void* arg){
  return reinterpret_cast<Fsck*>(arg)->Check();
}


/* ------------------------------------------------------------------------- */
void* 
Fsck::StaticScan(void* arg){
  struct ThreadInfo* tInfo = reinterpret_cast<Fsck::ThreadInfo*>(arg);

  return (tInfo->mFsck)->Scan(tInfo->mFsid, tInfo->mActive, tInfo->mPos, tInfo->mMax, tInfo->mHostPort, tInfo->mMountPoint);
}

/* ------------------------------------------------------------------------- */
void* 
Fsck::Check(void)
{
  
  mScanThreadInfo.clear();
  mScanThreadInfo.resize(0);
  mScanThreads.clear();
  mScanThreads.resize(0);

  for (size_t i=0; i< mErrorNames.size(); i++) {
    google::sparse_hash_map <eos::common::FileSystem::fsid_t, unsigned long long >::iterator itfid;

    mFsidErrorMap[mErrorNames[i]].clear();
    mFsidErrorMap[mErrorNames[i]].resize(0);
    for (itfid = mFsidErrorMap[mErrorNames[i]].begin(); itfid != mFsidErrorMap[mErrorNames[i]].end(); itfid ++) {
      mFsidErrorFidSet[mErrorNames[i]][itfid->first].clear();
      mFsidErrorFidSet[mErrorNames[i]][itfid->first].resize(0);
    }
  }


  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();
  
  XrdSysTimer sleeper;

  while (1) {
    sleeper.Snooze(1);
    eos_static_debug("Started consistency checker thread");
    ClearLog();
    Log(false,"started check");

    // run through the fsts 
    // compare files on disk with files in the namespace

    size_t pos=0;
    size_t max=0;
    {
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        max = FsView::gFsView.mIdView.size();
      }
      Log(false,"Filesystems to check: %lu", max);
      eos_static_debug("filesystems to check: %lu",max);
    }
    
    std::map<eos::common::FileSystem::fsid_t, FileSystem*>::const_iterator it;

    totalfiles=0;
    nchecked=0;
    nunchecked=0;
    n_error_replica_not_registered=0;
    n_error_replica_orphaned=0;
    
    n_error_mgm_disk_size_differ = 0;
    n_error_fst_disk_fmd_size_differ = 0;
    n_error_mgm_disk_checksum_differ = 0;
    n_error_fst_disk_fmd_checksum_differ = 0;    
    n_error_fst_filechecksum = 0;
    n_error_fst_blockchecksum = 0;
    n_error_replica_layout = 0;
    n_error_replica_offline = 0;
    n_error_file_offline = 0;
    n_error_replica_missing = 0;

    google::sparse_hash_set<eos::common::FileSystem::fsid_t> scannedfsids;

    while (pos < max) {
      {
        eos::common::FileSystem::fsid_t fsid;
        std::string hostport="";
        std::string mountpoint="";
        bool active=false;
      
        XrdSysThread::SetCancelOff();

        {
          eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
          it = FsView::gFsView.mIdView.begin();
          if (it != FsView::gFsView.mIdView.end()) {
            std::advance(it, pos);
          }

          if (it != FsView::gFsView.mIdView.end()) {
            fsid = it->first;
            hostport  = it->second->GetString("hostport");
            mountpoint = it->second->GetString("path");
            fprintf(stderr,"%s %s\n",hostport.c_str(), mountpoint.c_str());
            // check only file systems, which are broadcasting and booted
            if ( (it->second->GetActiveStatus() == eos::common::FileSystem::kOnline) && 
                 (it->second->GetStatus() == eos::common::FileSystem::kBooted ) && 
                 (it->second->GetConfigStatus() >= eos::common::FileSystem::kRO) ) {
              active = true;
            } else {
              active = false;
            }
          } else {
            fsid = 0;
          }
        }

        XrdSysThread::SetCancelOn();    

        if (fsid) {
          // this remembers which fsids have been scanned ... we use this to remove old fsids from the global map (which don't exist anymore)
          
          scannedfsids.insert(fsid);
          
          mScanThreadInfo[fsid].mFsck       = this;
          mScanThreadInfo[fsid].mFsid       = fsid;
          mScanThreadInfo[fsid].mActive     = active;
          mScanThreadInfo[fsid].mPos        = pos;
          mScanThreadInfo[fsid].mMax        = max;
          mScanThreadInfo[fsid].mHostPort   = hostport;
          mScanThreadInfo[fsid].mMountPoint = mountpoint;
        }
      }
      pos++;
      XrdSysThread::CancelPoint();
    }

    google::sparse_hash_map<eos::common::FileSystem::fsid_t, ThreadInfo>::iterator scanit;
    
    for (scanit = mScanThreadInfo.begin(); scanit != mScanThreadInfo.end(); scanit++) {
      // wait that we have less the mMaxThreads running
      size_t maxthreads = mParallelThreads;
      size_t loopcount=0;
      do {
        size_t nrunning=0;
	{
	  XrdSysMutexHelper lock(mScanThreadMutex);       
	  nrunning = mScanThreads.size();
	}
        loopcount++;
        if (nrunning < maxthreads) 
          break;
        else {
          if (!loopcount%12)
            Log(false,"=> %u/%u threads are in use", nrunning, maxthreads);
          sleeper.Snooze(5);         
        }
        
      } while(1);
      
      XrdSysThread::SetCancelOff();     
      {
	XrdSysMutexHelper lock(mScanThreadMutex);     
	mScanThreads[scanit->first] = 0;
	XrdSysThread::Run(&mScanThreads[scanit->first], Fsck::StaticScan, static_cast<void *>(&(scanit->second)), XRDSYSTHREAD_HOLD, "Fsck Scan Thread");
	mScanThreadsJoin[scanit->first] = mScanThreads[scanit->first];
      }
      XrdSysThread::SetCancelOn();      
    }


    // now wait for all threads to finish
    size_t loopcount=0;
    do {
      loopcount++;
      size_t nthreads=0;
      {
	XrdSysMutexHelper lock(mScanThreadMutex);      
	nthreads = mScanThreads.size();
      }
      if (nthreads) {
        if (!(loopcount%60))
          Log(false,"still %u threads running\n",nthreads);
      } else {
        break;
      }    
      sleeper.Snooze(1);
    } while(1);

    {
      XrdSysMutexHelper lock(mScanThreadMutex);  
      // join the slave threads
      google::sparse_hash_map<eos::common::FileSystem::fsid_t, pthread_t>::iterator tit;
      for (tit = mScanThreadsJoin.begin(); tit != mScanThreadsJoin.end(); tit++) {
        XrdSysThread::Join(tit->second,0);
      }
      mScanThreadsJoin.clear();
      mScanThreadsJoin.resize(0);     
    }

    mErrorMapMutex.Lock();



    mTotalErrorMap["totalfiles"]= totalfiles;
    mTotalErrorMap["diff_mgm_disk_size"]         = n_error_mgm_disk_size_differ;
    mTotalErrorMap["diff_fst_disk_fmd_size"]     = n_error_fst_disk_fmd_size_differ;
    mTotalErrorMap["diff_mgm_disk_checksum"]     = n_error_mgm_disk_checksum_differ;
    mTotalErrorMap["diff_fst_disk_fmd_checksum"] = n_error_fst_disk_fmd_checksum_differ;
    mTotalErrorMap["diff_file_checksum_scan"]    = n_error_fst_filechecksum;
    mTotalErrorMap["diff_block_checksum_scan"]   = n_error_fst_blockchecksum;
    mTotalErrorMap["scanned_files"]              = nchecked;
    mTotalErrorMap["not_scanned_files"]          = nunchecked;
    mTotalErrorMap["replica_not_registered"]     = n_error_replica_not_registered;
    mTotalErrorMap["replica_orphaned"]           = n_error_replica_orphaned;
    mTotalErrorMap["diff_replica_layout"]        = n_error_replica_layout;
    mTotalErrorMap["replica_offline"]            = n_error_replica_offline;
    mTotalErrorMap["file_offline"]               = n_error_file_offline;
    mTotalErrorMap["replica_missing"]            = n_error_replica_missing;

    // remove not scanned fsids

    google::sparse_hash_set<eos::common::FileSystem::fsid_t> fsidstodelete;
    google::sparse_hash_map <eos::common::FileSystem::fsid_t, unsigned long long>::const_iterator fsit;
    for (fsit = mFsidErrorMap[mErrorNames[0]].begin(); fsit != mFsidErrorMap[mErrorNames[0]].end(); fsit++) {
      if (!scannedfsids.count(fsit->first)) {
        fsidstodelete.insert(fsit->first);
      }
    }

    google::sparse_hash_set<eos::common::FileSystem::fsid_t>::const_iterator dit;
    for (dit = fsidstodelete.begin(); dit != fsidstodelete.end(); dit++) {
      for (size_t i=0; i< mErrorNames.size(); i++) {
        mFsidErrorMap[mErrorNames[i]].erase(*dit);
        mFsidErrorFidSet[mErrorNames[i]].erase(*dit);
      }
    }
    
    mErrorMapMutex.UnLock();

    Log(false,"stopping check - found %llu replicas", totalfiles);
    
    XrdSysThread::CancelPoint();
    Log(false,"=> next run in 8 hours");
    // write the report files
    XrdOucString out,err;
    XrdOucString option="e";
    XrdOucString selection="";
    Report(out, err, option, selection);

    sleeper.Snooze(8*3600);
  }

  return 0;
}

/* ------------------------------------------------------------------------- */
void 
Fsck::PrintOut(XrdOucString &out,  XrdOucString option)
{
  XrdSysMutexHelper lock(mLogMutex);
  out = mLog;
}

/* ------------------------------------------------------------------------- */
void 
Fsck::ClearLog() 
{
  XrdSysMutexHelper lock(mLogMutex);  
  mLog="";
}

/* ------------------------------------------------------------------------- */
void
Fsck::Log(bool overwrite, const char* msg, ...)
{
  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm *tm;
  
  va_list args;
  va_start (args, msg);
  char buffer[16384];
  char* ptr;
  time (&current_time);
  gettimeofday(&tv, &tz);

  tm = localtime (&current_time);
  sprintf (buffer, "%02d%02d%02d %02d:%02d:%02d %lu.%06lu ", tm->tm_year-100, tm->tm_mon+1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time, (unsigned long)tv.tv_usec);
  ptr = buffer + strlen(buffer);

  vsprintf(ptr, msg, args);
  XrdSysMutexHelper lock(mLogMutex);
  if (overwrite) {
    int spos = mLog.rfind("\n",mLog.length()-2);
    if (spos>0) {
      mLog.erase(spos+1);
    }
  }
  mLog+=buffer;
  mLog+= "\n";
  va_end(args);
}


/* ------------------------------------------------------------------------- */
bool
Fsck::Report(XrdOucString &out,  XrdOucString &err, XrdOucString option, XrdOucString selection)
{
  if ((option.find("h")!= STR_NPOS)) {
    for (size_t i=0; i< mErrorNames.size(); i++) {
      char outline[1024];
      snprintf(outline, sizeof(outline)-1,"%-32s %s\n", mErrorNames[i].c_str(), mErrorHelp[mErrorNames[i]].c_str());
      out += outline;
    }

    return true;
  }
  
  std::string sel =selection.c_str();
  
  if (selection.length()) {
    bool found=false;
    for (size_t i=0; i< mErrorNames.size(); i++) {
      if (sel == mErrorNames[i]) {
        found = true;
        break;
      }
    }
    if (!found) {
      err += "error: there is no 'tag' named '"; err += selection; err += "'\n";
      return false;
    }
  }

  XrdSysMutexHelper lock(mErrorMapMutex);  
  if ((option.find("g")!=STR_NPOS) || (option.length()==0) ) {
    // print global counts
    for (size_t i = 0; i < mErrorNames.size(); i++) {
      if ( (! sel.length()) || 
           (sel == mErrorNames[i]) ) {
        char outline[4096];
        if (option.find("m")==STR_NPOS) {
          snprintf(outline,sizeof(outline)-1,"ALL        %-32s %lld\n", mErrorNames[i].c_str(),mTotalErrorMap[mErrorNames[i]]);
        } else {
          snprintf(outline,sizeof(outline)-1,"fsck_n_%s=%lld\n",mErrorNames[i].c_str(), mTotalErrorMap[mErrorNames[i]]);
        }
        out += outline;
      }
    }
  }

  if ((option.find("e")!=STR_NPOS)) {
    // dump out everything into /var/eos/report/fsck/<unixtimestamp>/*.lfn
    for (size_t i = 0; i < mErrorNames.size(); i++) {
      char lfnfile[4096];
      snprintf(lfnfile,sizeof(lfnfile)-1,"/var/eos/report/fsck/%lu/%s.lfn", time(NULL), mErrorNames[i].c_str());
      eos::common::Path lfnpath(lfnfile);
      lfnpath.MakeParentPath(S_IRWXU);
      FILE* fout = fopen(lfnfile,"w+");
      if (fout) {
	Log(false,"created export report lfn files under %s\n", lfnfile);
      } else {
	Log(false,"creation of export report lfn files under %s failed\n", lfnfile);
      }

      if (mFsidErrorMap[mErrorNames[i]].size()) {
	google::sparse_hash_map<eos::common::FileSystem::fsid_t,unsigned long long>::const_iterator it;
	for (it = mFsidErrorMap[mErrorNames[i]].begin(); it != mFsidErrorMap[mErrorNames[i]].end(); it ++) {
	  if (it->second) {
	    google::sparse_hash_set<unsigned long long>::const_iterator fidit;
	    for (fidit = mFsidErrorFidSet[mErrorNames[i]][it->first].begin(); fidit != mFsidErrorFidSet[mErrorNames[i]][it->first].end(); fidit++) {
	      XrdOucString sizestring;
	      //-------------------------------------------
	      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
	      XrdOucString path="";
	      try {
		path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(*fidit)).c_str();
	      } catch ( eos::MDException &e ) {
		path ="EINVAL";
	      }	      
	      
	      if (path.length() && fout) {
		fprintf(fout,"%s", path.c_str());
	      }
	    }
	  }
	}
      }
      if (fout) fclose(fout);
    }
  }

  if ((option.find("a")!=STR_NPOS)) {
    // print statistic for all filesystems having errors
    for (size_t i = 0; i < mErrorNames.size(); i++) {
      if ( (! sel.length()) || 
           (sel == mErrorNames[i]) ) {
        if (mFsidErrorMap[mErrorNames[i]].size()) {
          google::sparse_hash_map<eos::common::FileSystem::fsid_t,unsigned long long>::const_iterator it;
          for (it = mFsidErrorMap[mErrorNames[i]].begin(); it != mFsidErrorMap[mErrorNames[i]].end(); it ++) {
            char outline[4096];
            if (it->second) {
              if (option.find("g")== STR_NPOS) {
                if (option.find("m")==STR_NPOS) {
                  XrdOucString sizestring;
                  snprintf(outline,sizeof(outline)-1,"%-12s%-32s %lld\n",eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long) it->first), mErrorNames[i].c_str(),it->second);
                } else {
                  snprintf(outline,sizeof(outline)-1,"fsck_fsid=%u fsck_n_%s=%lld\n",it->first, mErrorNames[i].c_str(), it->second);
                }
                out += outline;
              }
              if (option.find("i")!=STR_NPOS) {
                XrdOucString fidstring="";
                google::sparse_hash_set<unsigned long long>::const_iterator fidit;
                for (fidit = mFsidErrorFidSet[mErrorNames[i]][it->first].begin(); fidit != mFsidErrorFidSet[mErrorNames[i]][it->first].end(); fidit++) {
                  XrdOucString sizestring;
                  XrdOucString fxid="";
                  eos::common::FileId::Fid2Hex(*fidit, fxid);
                  out += "fxid=";
                  out += fxid;
                  out += " e=";
                  out += mErrorNames[i].c_str();
                  out += "\n";
                }
              }
              
              if (option.find("l")!=STR_NPOS) {
                google::sparse_hash_set<unsigned long long>::const_iterator fidit;
                for (fidit = mFsidErrorFidSet[mErrorNames[i]][it->first].begin(); fidit != mFsidErrorFidSet[mErrorNames[i]][it->first].end(); fidit++) {
                  XrdOucString sizestring;
                  XrdOucString path="";
		  {
		  //-------------------------------------------
		    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);                
		    try {
		      path = gOFS->eosView->getUri(gOFS->eosFileService->getFileMD(*fidit)).c_str();
		    } catch ( eos::MDException &e ) {
		      path ="EINVAL";
		    }                  
		    
		  }
                  if (path.length()) {
                    out += "lfn=";
                    out += path.c_str();
                    out += " e="; out += mErrorNames[i].c_str();
                    out += "\n";
                  }

                  if ( (option.find("C")!=STR_NPOS) && 
                       (mErrorNames[i] == "diff_fst_disk_fmd_checksum") ) {
                    // send a verify to that file/filesystem
                    eos::common::Mapping::VirtualIdentity vid;
                    eos::common::Mapping::Root(vid);
                    XrdOucErrInfo error;
                    int lretc = gOFS->_verifystripe(path.c_str(), error, vid, it->first, "&mgm.verify.compute.checksum=1");
                    if (!lretc) {
                      out += "success: sending verify to fsid="; out += (int)it->first; out += " for path="; out += path.c_str(); out += "\n";
                    } else {
                      err += "error: sending verify to fsid=";   err += (int)it->first; err += " failed for path="; err += path.c_str(); err += "\n";
                    }
                  }

                  if ( (option.find("U")!=STR_NPOS) &&
                       (mErrorNames[i] == "replica_not_registered")) {
                    // remove unregistered replicas
                    if (gOFS->DeleteExternal(it->first, *fidit)) {
                      char outline[1024];
                      snprintf(outline,sizeof(outline)-1, "success: send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
                      out += outline;
                    } else {
                      char errline[1024];
                      snprintf(errline,sizeof(errline)-1, "err: unable to send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
                      err += outline;
                    }
                  }
                  
                  if ( (option.find("O")!=STR_NPOS) &&
                       (mErrorNames[i] == "replica_orphaned")) {
                    // remove orphan replicas
                    if (gOFS->DeleteExternal(it->first, *fidit)) {
                      char outline[1024];
                      snprintf(outline,sizeof(outline)-1, "success: send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
                      out += outline;
                    } else {
                      char errline[1024];
                      snprintf(errline,sizeof(errline)-1, "err: unable to send unlink to fsid=%u fxid=%llx\n",it->first,*fidit);
                      err += outline;
                    }
                  }
                  

                  if ( (option.find("A")!=STR_NPOS) && 
                       (mErrorNames[i] == "diff_replica_layout") ) {
                    // execute adjust replica
                    eos::common::Mapping::VirtualIdentity vid;
                    eos::common::Mapping::Root(vid);
                    XrdOucErrInfo error;

                    // execute a proc command
                    ProcCommand Cmd;
                    XrdOucString info="mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
                    info += path.c_str();
                    info += "&mgm.format=fuse";
                    Cmd.open("/proc/user",info.c_str(), vid, &error);
                    Cmd.AddOutput(out,err);
                    out+="\n";
                    err+="\n";
                    Cmd.close();
                  }

                  if ( (option.find("D")!=STR_NPOS) && 
                       (mErrorNames[i] == "replica_missing") ) {
                    // execute adjust replica
                    eos::common::Mapping::VirtualIdentity vid;
                    eos::common::Mapping::Root(vid);
                    XrdOucErrInfo error;
                    
                    // execute a proc command
                    ProcCommand Cmd;
                    XrdOucString info="mgm.cmd=file&mgm.subcmd=drop&mgm.path=";
                    info += path.c_str();
                    info += "&mgm.file.fsid=";
                    info += (int) it->first;
                    info += "&mgm.format=fuse";
                    Cmd.open("/proc/user",info.c_str(), vid, &error);
                    Cmd.AddOutput(out,err);
                    out+="\n";
                    err+="\n";
                    Cmd.close();
                  }

                  //-------------------------------------------
                }
              }
            }
          }
        }
      }
    }
  } 
  return true;
}



void*
Fsck::Scan(eos::common::FileSystem::fsid_t fsid, bool active, size_t pos, size_t max, std::string hostport, std::string mountpoint)
{ 

  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();

  // local accounting get's copy after the full loop in to the global accounting

  google::sparse_hash_map<std::string, unsigned long long> mLocalErrorMap;
  google::sparse_hash_map<std::string, google::sparse_hash_set<unsigned long long> > mLocalErrorFidSet;
  google::sparse_hash_set<unsigned long long> mSet;
  
  // initialize local accounting
  for (size_t i=0; i< mErrorNames.size(); i++) {
    mLocalErrorMap[mErrorNames[i]] = 0;
    mLocalErrorFidSet[mErrorNames[i]] = mSet;
    mLocalErrorFidSet[mErrorNames[i]].set_deleted_key(0);
  }

  if (!active) {
    Log(false,"filesystem: fsid=%05d hostport=%20s mountpoint=%s INACTIVE", fsid, hostport.c_str(),mountpoint.c_str());
  } else {
    //    Log(false,"filesystem: fsid=%05d hostport=%20s mountpoint=%s totalfiles=%lu", fsid, hostport.c_str(),mountpoint.c_str(),totalfiles);

    // stream all the fsids in this filesystem in to the set of replica_missing, then each file found is removed from this set
    // --------------------------
    {
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      try {
	eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
	eos::FileSystemView::FileIterator it;
	for (it = filelist.begin(); it != filelist.end(); ++it) {
	  mLocalErrorFidSet["replica_missing"].insert(*it);
	}
      } catch ( eos::MDException &e ) {
	// nothing to catch
      }
    }
    // --------------------------
    
    XrdOucString url = "root://daemon@"; url += hostport.c_str(); url += "/"; url += mountpoint.c_str();
    
    XrdSysThread::SetCancelOff();

    XrdOucString dumpfile = "/tmp/eos.scan."; dumpfile += (int) fsid; dumpfile += ".dump";
    XrdOucString sysline = "touch "; sysline += dumpfile; sysline += "; chown daemon.daemon "; sysline += dumpfile; sysline +="; eos-fst-dump "; sysline += url; sysline += " >& "; sysline += dumpfile;
    
    // delete possible old dump file
    ::unlink(dumpfile.c_str());
    // runnin eos-fst-dump command
    system(sysline.c_str());

    std::ifstream inFile(dumpfile.c_str());
    std::string dumpentry;

    unsigned long long nfiles=0;

    // we do only one scan at a time to avoid to high mutex contention
    mScanMutex.Lock();

    while(std::getline(inFile, dumpentry)) {
      {
	XrdSysMutexHelper lock(mGlobalCounterLock);
	nfiles++;
	totalfiles++;
	mLocalErrorMap[mErrorNames[0]]++;
      }
      
      // decode the entry

      // the tokens are define in XrdFstOfs.cc: nextEntry as
      // [0] = fxid [1] = scan timestamp [2] = creation checksum [3] = bool:file cx error [4] = bool:block cx error 
      // [5] = phys. size [6] changelog size [7] changelog checksum [8] bool: currently open for write

      std::vector<std::string> tokens;
      std::string delimiter=":";
      std::string token = dumpentry.c_str();
      eos::common::StringConversion::Tokenize(token, tokens, delimiter);

      unsigned long long fid = strtoull(tokens[0].c_str(),0,16);

      if (tokens.size() != 9) { 
        Log(false,"Got illegal response from fst (size(tokens)=%d) => %s\n", tokens.size(), dumpentry.c_str());
        continue;
      }
      if (fid) {
        // remove this file from the replica_missing set
        mLocalErrorFidSet["replica_missing"].erase(fid);
      }

      // fid given and file is currently not written
      if (fid && (tokens[8] != "1")) {
        eos::FileMD* fmd=0;
        
        //-------------------------------------------      
	gOFS->eosViewRWMutex.LockRead();
        try {
          fmd = gOFS->eosFileService->getFileMD(fid);
        } catch ( eos::MDException &e ) {
          // nothing to catch
        }
        
        // convert size & checksum into strings
        std::string sizestring="";
        std::string mgm_size = "";
        std::string mgm_checksum = "";
        bool replicaexists = false;
        bool lfnexists = true;
        bool unlinkedlocation = false;
        bool hasfmdchecksum = false;
        
        if (fmd) {
          eos::FileMD fmdCopy(*fmd);
          fmd = &fmdCopy;
          gOFS->eosViewRWMutex.UnLockRead();
          //-------------------------------------------
          eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)fmd->getSize());
          
          mgm_size = sizestring.c_str();
          for (unsigned int i=0; i< SHA_DIGEST_LENGTH; i++) {
            unsigned int checksumtype = eos::common::LayoutId::GetChecksum(fmd->getLayoutId());
            if ( ( (checksumtype == eos::common::LayoutId::kAdler) || 
                   (checksumtype == eos::common::LayoutId::kCRC32) || 
                   (checksumtype == eos::common::LayoutId::kCRC32C) ) && (i<4) ) {
              char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[3-i]));
              mgm_checksum += hb;
              
            } else {
              char hb[3]; sprintf(hb,"%02x", (unsigned char) (fmd->getChecksum().getDataPtr()[i]));
              mgm_checksum += hb;
            }
          }
          if (fmd->hasLocation( (eos::FileMD::location_t) fsid))
            replicaexists=true;
          
          if (fmd->hasUnlinkedLocation( (eos::FileMD::location_t) fsid))
            unlinkedlocation=true;
          
          if (eos::common::LayoutId::GetChecksum(fmd->getLayoutId()) > eos::common::LayoutId::kNone) {
            hasfmdchecksum=true;
          }
          
          // check if we have != stripes than defined by the layout
          if (fmd->getNumLocation() != (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId())+1)) {
            mLocalErrorMap[mErrorNames[11]]++;
            mLocalErrorFidSet[mErrorNames[11]].insert(fid);
	    {
	      	XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_replica_layout++;
	    }
          }
          
          // check if locations are online
          eos::FileMD::LocationVector::const_iterator lociter;
          bool oneoffline=false;
          size_t nonline=0;
          
          {
            eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
            for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
              if (*lociter) {
                if (FsView::gFsView.mIdView.count(*lociter)) {
                  eos::common::FileSystem::fsstatus_t bootstatus   = (FsView::gFsView.mIdView[*lociter]->GetStatus(true));
                  eos::common::FileSystem::fsstatus_t configstatus = (FsView::gFsView.mIdView[*lociter]->GetConfigStatus());

                  bool conda = (FsView::gFsView.mIdView[*lociter]->GetActiveStatus(true) == eos::common::FileSystem::kOffline)  ;
                  bool condb = ( bootstatus != eos::common::FileSystem::kBooted) ;
                  bool condc = ( configstatus == eos::common::FileSystem::kDrainDead);

                  if ( conda || condb || condc) {
                    //              fprintf(stderr,"warning: %d %d %d %d %d %d %d %d %llx\n", (FsView::gFsView.mIdView[*lociter]->GetActiveStatus(false)), (FsView::gFsView.mIdView[*lociter]->GetActiveStatus(true)),
                    //                      FsView::gFsView.mIdView[*lociter]->GetStatus(false),FsView::gFsView.mIdView[*lociter]->GetStatus(true), conda, condb, condc, bootstatus, fid) ;
                    if (!oneoffline) {
		      {
			XrdSysMutexHelper lock(mGlobalCounterLock);
			n_error_replica_offline++;
		      }
                      oneoffline=true;
                      mLocalErrorMap[mErrorNames[12]]++;
                      mLocalErrorFidSet[mErrorNames[12]].insert(fid);
                    }
                  } else {
		    {
		      XrdSysMutexHelper lock(mGlobalCounterLock);
		      nonline++;
                    }
                  }
                }
              }
            }
          }
          if ((fmd->getNumLocation()) && (nonline < eos::common::LayoutId::GetMinOnlineReplica((fmd->getLayoutId())))) {
            mLocalErrorMap[mErrorNames[13]]++;
            mLocalErrorFidSet[mErrorNames[13]].insert(fid);
	    {
	      XrdSysMutexHelper lock(mGlobalCounterLock);
	      n_error_file_offline++;
	    }
          }
        } else {
          gOFS->eosViewRWMutex.UnLockRead();
          lfnexists=false;
          //-------------------------------------------
        }

        
        // now we have all the information and we compare all sizes, checksums and locations
        
        if (replicaexists) {
          if (mgm_size != tokens[5]) {
	    {
	      XrdSysMutexHelper lock(mGlobalCounterLock);
	      n_error_mgm_disk_size_differ++;
	    }
            mLocalErrorMap[mErrorNames[1]]++;
            mLocalErrorFidSet[mErrorNames[1]].insert(fid);
          }
          
          if (tokens[5] != tokens[6]) {
	    {
	      XrdSysMutexHelper lock(mGlobalCounterLock);
	      n_error_fst_disk_fmd_size_differ++;
	    }
            mLocalErrorMap[mErrorNames[2]]++;
            mLocalErrorFidSet[mErrorNames[2]].insert(fid);
          }
          
          if (hasfmdchecksum) {
            // we only apply this if the file is supposed to have a checksum in the namespace and the file has not zero size!
            if ( (mgm_checksum != tokens[7]) && (mgm_size != "0")) {
	      {
		XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_mgm_disk_checksum_differ++;
	      }
              mLocalErrorMap[mErrorNames[3]]++;
              mLocalErrorFidSet[mErrorNames[3]].insert(fid);
            }
            
            if (tokens[2] != tokens[7]) {
	      {
		XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_fst_disk_fmd_checksum_differ++;
	      }
              mLocalErrorMap[mErrorNames[4]]++;
              mLocalErrorFidSet[mErrorNames[4]].insert(fid);
            }
          }
          
          if (tokens[1] != "x") {
	    {
	      XrdSysMutexHelper lock(mGlobalCounterLock);
	      nchecked++;
	      mLocalErrorMap[mErrorNames[7]]++;
            }
            // don't track all the fids
            //                mLocalErrorFidSet[mErrorNames[7]].insert(fid);
            if (tokens[3] == "1") {
	      {
		XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_fst_filechecksum++;
	      }
              mLocalErrorMap[mErrorNames[5]]++;
              mLocalErrorFidSet[mErrorNames[5]].insert(fid);
            }
            if (tokens[4] == "1") {
	      {
		XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_fst_blockchecksum++;
              }
              mLocalErrorMap[mErrorNames[6]]++;
              mLocalErrorFidSet[mErrorNames[6]].insert(fid);
            }
          } else {
	    {
	      XrdSysMutexHelper lock(mGlobalCounterLock);           
	      nunchecked++;
	    }
            mLocalErrorMap[mErrorNames[8]]++;
            // don't track all the fids
            //mLocalErrorFidSet[mErrorNames[8]].insert(fid);
          }
        } else {
          if (lfnexists) {
            if (!unlinkedlocation) {
              mLocalErrorMap[mErrorNames[9]]++;
              if (!mLocalErrorFidSet[mErrorNames[9]].count(fid))
                mLocalErrorFidSet[mErrorNames[9]].insert(fid);
	      {
		XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_replica_not_registered++;
              }
            }
          } else {
            if (!unlinkedlocation) {
              mLocalErrorMap[mErrorNames[10]]++;
              mLocalErrorFidSet[mErrorNames[10]].insert(fid);
	      {
		XrdSysMutexHelper lock(mGlobalCounterLock);
		n_error_replica_orphaned++;
              }
            }
          }
        } 
      }
    }
    Log(false,"filesystem: fsid=%05d hostport=%20s mountpoint=%s totalfiles=%llu", fsid, hostport.c_str(),mountpoint.c_str(), totalfiles);
    // delete dump file
    ::unlink(dumpfile.c_str());      
    mScanMutex.UnLock();
  }
  // copy local maps to global maps

  {
    XrdSysMutexHelper lock(mErrorMapMutex);
    
    for (size_t i=0; i< mErrorNames.size(); i++) {
      mFsidErrorMap[mErrorNames[i]][fsid] = mLocalErrorMap[mErrorNames[i]];
      mFsidErrorFidSet[mErrorNames[i]][fsid].clear();
      mFsidErrorFidSet[mErrorNames[i]][fsid].resize(0);
      mFsidErrorFidSet[mErrorNames[i]][fsid] = mLocalErrorFidSet[mErrorNames[i]];
      if (mErrorNames[i] == "replica_missing") {
	// copy the missing replica counter
	{
	  XrdSysMutexHelper lock(mGlobalCounterLock);
	  n_error_replica_missing+= mLocalErrorFidSet["replica_missing"].size();
	  mFsidErrorMap[mErrorNames[i]][fsid] = mLocalErrorFidSet["replica_missing"].size();
	}
      }
    }
  }
  // remove this thread from the running thread map
 
  {
    XrdSysMutexHelper lock(mScanThreadMutex);
    mScanThreads.erase(fsid);
  }

  XrdSysThread::SetCancelOn();
  return NULL;
}

EOSMGMNAMESPACE_END
