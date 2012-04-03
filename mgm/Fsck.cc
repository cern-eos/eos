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

    XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
    broadcastresponsequeue += "-fsck-";
    broadcastresponsequeue += bccount;
    XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;

    int envlen;
    XrdOucString msgbody;
    msgbody="mgm.cmd=fsck&mgm.fsck.tags=*";
    
    XrdOucString stdOut = "";
    XrdOucString stdErr = "";

    if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue,broadcasttargetqueue, msgbody, stdOut, 2)) {
      eos_static_err("failed to broad cast and collect rtlog from [%s]:[%s]", broadcastresponsequeue.c_str(),broadcasttargetqueue.c_str());
      stdErr = "error: broadcast failed\n";
    }
    
    //    if (stdOut.c_str()) {
    //      Log(false,"%s", stdOut.c_str());
    //    }
    unsigned long long totalfiles=0;
    Log(false,"stopping check - found %llu replicas", totalfiles);
    
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
