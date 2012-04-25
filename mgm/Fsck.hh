// ----------------------------------------------------------------------
// File: Fsck.hh
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

#ifndef __EOSMGM_FSCK__HH__
#define __EOSMGM_FSCK__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_map>
#include <google/sparse_hash_set>
#include <sys/types.h>
#include <string>
#include <stdarg.h>
#include <map>
#include <set>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Fsck {
  // -------------------------------------------------------------
  // ! run's a consistency check aggregation over all FST nodes against the MGM namespace
  // -------------------------------------------------------------
private:

  XrdOucString  mLog;
  XrdSysMutex mLogMutex;

  XrdOucString mEnabled;
  int mInterval;

  pthread_t mThread;
  bool mRunning;

  XrdSysMutex eMutex; //< protecting the eX... maps

  // the error detail  map storing "<error-name>"=><fsid>=>[fid1,fid2,fid3...]"
  std::map<std::string, std::map<eos::common::FileSystem::fsid_t, std::set <eos::common::FileId::fileid_t> > > eFsMap;
  
  // the error summary map storing "<error-name>"=>[fid1,fid2,fid3...]"
  std::map<std::string, std::set <eos::common::FileId::fileid_t> > eMap;
  std::map<std::string, unsigned long long > eCount;

  // unavailable filesystems map
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsUnavail;

  // dark filesystem map
  std::map<eos::common::FileSystem::fsid_t, unsigned long long > eFsDark;

  // timestamp of collection
  time_t eTimeStamp;

  void  ResetErrorMaps() {
    XrdSysMutexHelper lock(eMutex);
    eFsMap.clear(); 
    eMap.clear();
    eCount.clear();
    eFsUnavail.clear();
    eFsDark.clear();
    eTimeStamp = time(NULL);
  }

public:
  static const char* gFsckEnabled;
  static const char* gFsckInterval;

  Fsck();
  ~Fsck();
 
  bool Start(int interval=0);
  bool Stop();

  bool Usage(XrdOucString &out, XrdOucString &err);

  void PrintOut(XrdOucString &out, XrdOucString option="");
  bool Report(XrdOucString &out, XrdOucString &err, XrdOucString option="", XrdOucString selection="");
  bool Repair(XrdOucString &out, XrdOucString &err, XrdOucString option="");

  void ClearLog();
  void Log(bool overwrite, const char* msg, ...);

  void ApplyFsckConfig();
  bool StoreFsckConfig();

  static void* StaticCheck(void*);
  void* Check();
};

EOSMGMNAMESPACE_END

#endif
