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


#include <iostream>
#include <fstream>
#include <vector>

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
Fsck::Fsck() 
{
  mRunning = false;
}

/* ------------------------------------------------------------------------- */
bool
Fsck::Start()
{
  if (!mRunning) {
    XrdSysThread::Run(&mThread, Fsck::StaticCheck, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Fsck Thread");
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

    // join the master thread
    XrdSysThread::Detach(mThread);
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
Fsck::Check(void)
{
  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();

  XrdSysTimer sleeper;
  
  int bccount = 0;

  while (1) {
    sleeper.Snooze(1);
    eos_static_debug("Started consistency checker thread");
    ClearLog();
    Log(false,"started check");

    // run through the fsts 
    // compare files on disk with files in the namespace

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

    XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
    broadcastresponsequeue += "-fsck-";
    broadcastresponsequeue += bccount;
    XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;

    XrdOucString msgbody;
    msgbody="mgm.cmd=fsck&mgm.fsck.tags=*";
    
    XrdOucString stdOut = "";
    XrdOucString stdErr = "";

    if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue,broadcasttargetqueue, msgbody, stdOut, 10)) {
      eos_static_err("failed to broad cast and collect fsck from [%s]:[%s]", broadcastresponsequeue.c_str(),broadcasttargetqueue.c_str());
      stdErr = "error: broadcast failed\n";
    }

    ResetErrorMaps();
    
    std::vector<std::string> lines;

    // convert into a lines-wise seperated array
    eos::common::StringConversion::StringToLineVector((char*)stdOut.c_str(), lines);

    for (size_t nlines = 0; nlines <lines.size(); nlines++) {
      fprintf(stderr,"%s\n", lines[nlines].c_str());
      std::set<unsigned long long> fids;
      unsigned long fsid = 0;
      std::string errortag;
      if (eos::common::StringConversion::ParseStringIdSet((char*)lines[nlines].c_str(), errortag, fsid, fids)) {
	std::set<unsigned long long>::const_iterator it;
	if (fsid) {
	  XrdSysMutexHelper lock(eMutex);
	  for (it = fids.begin(); it != fids.end(); it++) {
	    // sort the fids into the error maps
	    eFsMap[errortag][fsid].insert(*it);
	    eMap[errortag].insert(*it);
	    eCount[errortag]++;
	  }
	}
      } else {
	eos_static_err("Can not parse fsck response: %s\n", lines[nlines].c_str());
      }
    }

    // grab all files which are damaged because filesystems are down
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      std::map<eos::common::FileSystem::fsid_t, FileSystem*>::const_iterator it;
      // loop over all filesystems and check their status
      for (it = FsView::gFsView.mIdView.begin(); it != FsView::gFsView.mIdView.end(); it++) {
	eos::common::FileSystem::fsid_t fsid = it->first;
	eos::common::FileSystem::fsactive_t fsactive = it->second->GetActiveStatus();
	eos::common::FileSystem::fsstatus_t fsconfig = it->second->GetConfigStatus();
	eos::common::FileSystem::fsstatus_t fsstatus = it->second->GetStatus();
	if ( (fsstatus       == eos::common::FileSystem::kBooted) && 
             (fsconfig       >= eos::common::FileSystem::kDrain) && 
             (fsactive ) ) { 
	  // this is healthy, don't need to do anything
	} else {
	  // this is not ok and contributes to replica offline errors
	  try {
	    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
	    eos::FileMD* fmd = 0;
	    eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
	    eos::FileSystemView::FileIterator it;
	    for (it = filelist.begin(); it != filelist.end(); ++it) {
	      fmd = gOFS->eosFileService->getFileMD(*it);
	      if (fmd) {
		eFsUnavail[fsid]++;
		eFsMap["rep_offline"][fsid].insert(*it);
		eMap["rep_offline"].insert(*it);
		eCount["rep_offline"]++;
	      }
	    }
	  } catch ( eos::MDException &e ) {
	    errno = e.getErrno();
	    eos_static_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
	  }
	}
      }
    }

    // grab all files with have no replicas at all
    {
      try {
	eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
	eos::FileMD* fmd = 0;
	eos::FileSystemView::FileList filelist = gOFS->eosFsView->getNoReplicasFileList();
	eos::FileSystemView::FileIterator it;
	for (it = filelist.begin(); it != filelist.end(); ++it) {
	  fmd = gOFS->eosFileService->getFileMD(*it);
	  if (fmd) {
	    eMap["zero_replica"].insert(*it);
	    eCount["zero_replica"]++;
	  }
	}
      } catch ( eos::MDException &e ) {
	errno = e.getErrno();
	eos_static_debug("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
      }
    }
    
    std::map<std::string, std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;
    for (emapit = eMap.begin(); emapit != eMap.end(); emapit++) {
      Log(false,"%-30s : %llu (%llu)", emapit->first.c_str(), emapit->second.size(), eCount[emapit->first]);
    }

    // look over unavailable filesystems
    std::map<eos::common::FileSystem::fsid_t,unsigned long long >::const_iterator unavailit;
    for (unavailit = eFsUnavail.begin(); unavailit != eFsUnavail.end(); unavailit++) {
      std::string host="not configured";
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      if (FsView::gFsView.mIdView.count(unavailit->first)) {
	host = FsView::gFsView.mIdView[unavailit->first]->GetString("hostport");
	
      }
      Log(false,"host=%s fsid=%lu  replica_offline=%llu ", host.c_str(), unavailit->first, unavailit->second);
    }

    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      // look for dark MD entries e.g. filesystem ids which have MD entries but have not configured file system
      eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
      size_t nfilesystems = gOFS->eosFsView->getNumFileSystems();
      for (size_t nfsid=1; nfsid <nfilesystems; nfsid++) {
	try {
	  eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(nfsid);
	  if (filelist.size()) {
	    // check if this exists in the gFsView
	    if (!FsView::gFsView.mIdView.count(nfsid)) {
	      eFsDark[nfsid]+= filelist.size();
	      Log(false,"shadow fsid=%lu shadow_entries=%llu ", nfsid, filelist.size());
	    }
	  }
	} catch ( eos::MDException &e ) {
	}
      }
    }
    
    Log(false,"stopping check");
    
    XrdSysThread::CancelPoint();
    Log(false,"=> next run in 30 minutes");

    // Write Report 
    sleeper.Snooze(1800);
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
bool 
Fsck::Report(XrdOucString &out, XrdOucString &err, XrdOucString option, XrdOucString selection)
{
  out = "option:"; out += option;
  out += " selection:"; out += selection;
  bool printfid = (option.find("i")!=STR_NPOS);
  bool printlfn = (option.find("l")!=STR_NPOS);

  if (!selection.length()) {
    if ( (option.find("json")!= STR_NPOS) || (option.find("j") != STR_NPOS) ) {
      // json output
      out += "{\n";
      if (! (option.find("a") != STR_NPOS) ) {
	// give global table
	std::map<std::string, std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;
	for (emapit = eMap.begin(); emapit != eMap.end(); emapit++) {  
	  char sn[1024];
	  snprintf(sn,sizeof(sn)-1,"%llu", (unsigned long long )emapit->second.size());
	  out += "  \""; out += emapit->first.c_str(); out += "\": {\n";
	  out += "    \"n\": "; out += sn; out += "\",\n";
	  if (printfid) {
	    out += "    \"fxid\": ["; 
	    std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
	    for (fidit = emapit->second.begin(); fidit != emapit->second.end(); fidit++) {
	      XrdOucString hexstring;
	      eos::common::FileId::Fid2Hex(*fidit,hexstring);
	      out += hexstring.c_str();
	      out += ",";
	    }
	    if (out.endswith(",")) {
	      out.erase(out.length()-1);
	    }
	    out += "]\n";
	  }
	  if (printlfn) {
	    out += "    \"lfn\": ["; 
	    std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
	    for (fidit = emapit->second.begin(); fidit != emapit->second.end(); fidit++) {
	      eos::FileMD* fmd=0;
	      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
              try {
                fmd = gOFS->eosFileService->getFileMD(*fidit);
		std::string fullpath = gOFS->eosView->getUri(fmd);
		out += "\""; out += fullpath.c_str(); out += "\"";
              } catch ( eos::MDException &e ) {
		out += "\"undefined\"";
              }
	      out += ",";
	    } 
	    if (out.endswith(",")) {
	      out.erase(out.length()-1);
	    }
	    out += "]\n";
	  }
	  out += "  },\n";
	}
	
	// list shadow filesystems
	std::map<eos::common::FileSystem::fsid_t, unsigned long long >::const_iterator fsit;
	out += "  \"shadow_fsid\": [";
	for (fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++) {
	  char sfsid[1024];
	  snprintf(sfsid,sizeof(sfsid)-1,"%lu", (unsigned long)fsit->first);
	  out += sfsid;
	  out += ",";
	}
	if (out.endswith(",")) {
	  out.erase(out.length()-1);
	}
	out += "  ]\n";
	out += "}\n";
      } else {
	// do output per filesystem

      }
    } else {
      // greppable format
      if (! (option.find("a") != STR_NPOS) ) {
	// give global table
	std::map<std::string, std::set <eos::common::FileId::fileid_t> >::const_iterator emapit;
	for (emapit = eMap.begin(); emapit != eMap.end(); emapit++) {  
	  char sn[1024];
	  snprintf(sn,sizeof(sn)-1,"%llu", (unsigned long long )emapit->second.size());
	  out += "tag=\""; out += emapit->first.c_str(); out +="\"";
	  out += " n="; out += sn; 
	  if (printfid) {
	    out += " fxid="; 
	    std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
	    for (fidit = emapit->second.begin(); fidit != emapit->second.end(); fidit++) {
	      XrdOucString hexstring;
	      eos::common::FileId::Fid2Hex(*fidit,hexstring);
	      out += hexstring.c_str();
	      out += ",";
	    }
	    if (out.endswith(",")) {
	      out.erase(out.length()-1);
	    }
	    out += "\n";
	  }
	  if (printlfn) {
	    out += " lfn="; 
	    std::set <eos::common::FileId::fileid_t>::const_iterator fidit;
	    for (fidit = emapit->second.begin(); fidit != emapit->second.end(); fidit++) {
	      eos::FileMD* fmd=0;
	      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
              try {
                fmd = gOFS->eosFileService->getFileMD(*fidit);
		std::string fullpath = gOFS->eosView->getUri(fmd);
		out += "\""; out += fullpath.c_str(); out += "\"";
              } catch ( eos::MDException &e ) {
		out += "\"undefined\"";
              }
	      out += ",";
	    } 
	    if (out.endswith(",")) {
	      out.erase(out.length()-1);
	    }
	    out += "\n";
	  }
	  
	  // list shadow filesystems
	  std::map<eos::common::FileSystem::fsid_t, unsigned long long >::const_iterator fsit;
	  out += "shadow_fsid=";
	  for (fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++) {
	    char sfsid[1024];
	    snprintf(sfsid,sizeof(sfsid)-1,"%lu", (unsigned long)fsit->first);
	    out += sfsid;
	    out += ",";
	  }
	  if (out.endswith(",")) {
	    out.erase(out.length()-1);
	  }
	  out += "\n";
	}
      } else {
	// do output per filesystem
      }
    }
  } else {
    // output the selected tag

  }
  return true;
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
EOSMGMNAMESPACE_END
