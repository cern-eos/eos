// ----------------------------------------------------------------------
// File: TransferEngine.hh
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

#ifndef __EOSMGM_TRANSFERENGINE__HH__
#define __EOSMGM_TRANSFERENGINE__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/txengine/TransferDB.hh"
#include "common/Mapping.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class TransferEngine;

extern TransferEngine gTransferEngine;

class TransferEngine {
private:
  TransferDB* xDB;
  pthread_t thread;
  pthread_t watchthread;
public:

  static const char* gConfigSchedule; //< global configuration tag if scheduling is enabled
  static const char* GetTransferState(int state) {
    if (state == kNone)      return "none";
    if (state == kInserted)  return "inserted";
    if (state == kValidated) return "validated";
    if (state == kScheduled) return "scheduled";
    if (state == kStageIn)   return "stagein";
    if (state == kRunning)   return "running";
    if (state == kStageOut)  return "stageout";
    if (state == kDone)      return "done";
    if (state == kFailed)    return "failed";
    if (state == kRetry)     return "retry";
    return "unknown";
  }

  enum eTransferState {kNone=0, kInserted, kValidated, kScheduled, kRunning, kStageIn, kStageOut, kDone, kFailed, kRetry};

  TransferEngine();
  virtual ~TransferEngine();
  bool Init(const char* connectstring = 0 );
  int Run(bool store=true);
  int Stop(bool store=true);

  static void* StaticSchedulerProc(void*);
  void* Scheduler();

  static void* StaticWatchProc(void*);
  void* Watch();

  int ApplyTransferEngineConfig();

  int Submit(XrdOucString& src, XrdOucString& dst, XrdOucString& rate, XrdOucString& streams, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid, time_t exptime=86400, XrdOucString credentials="", bool sync=false);
  int Ls(XrdOucString& id, XrdOucString& option, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Cancel(XrdOucString& id, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid );
  int Kill(XrdOucString& id, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr , eos::common::Mapping::VirtualIdentity& vid);
  int Resubmit(XrdOucString& id, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Log(XrdOucString& id, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Clear(XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Reset(XrdOucString& option, XrdOucString& sid, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  int Purge(XrdOucString& option, XrdOucString& sid, XrdOucString& group, XrdOucString& stdOut, XrdOucString& stdErr, eos::common::Mapping::VirtualIdentity& vid);
  

  bool SetState(long long id, int status)                                  { return xDB->SetState(id,status);}
  bool SetProgress(long long id, float progress)                           { return xDB->SetProgress(id,progress);}
  bool SetExecutionHost(long long id, std::string &exechost)               { return xDB->SetExecutionHost(id,exechost);}
  bool SetCredential(long long id, std::string credential, time_t exptime) { return xDB->SetCredential(id, credential, exptime);}
  bool SetLog(long long id, std::string log)                               { return xDB->SetLog(id,log);}
  TransferDB::transfer_t GetNextTransfer(int status)                       { return xDB->GetNextTransfer(status); }
  TransferDB::transfer_t GetTransfer(long long id)                         { return xDB->GetTransfer(id);}
};

EOSMGMNAMESPACE_END

#endif
