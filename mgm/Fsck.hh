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
  // ! run's a consistency check over all FST nodes against the MGM namespace
  // -------------------------------------------------------------
private:

  XrdOucString  mLog;
  XrdSysMutex mLogMutex;
  XrdSysMutex mScanMutex;
  // these maps contain the snapshot state of the last fsck
  XrdSysMutex mErrorMapMutex;

  std::vector<std::string> mErrorNames;

  size_t mParallelThreads;

  // map 'error-name'-> errorcount
  google::sparse_hash_map<std::string, unsigned long long> mTotalErrorMap;
  // map 'error-name'-> error help
  google::sparse_hash_map<std::string, XrdOucString> mErrorHelp;
  // map 'error-name'-> 'fsid' -> errorcount
  google::sparse_hash_map<std::string, google::sparse_hash_map <eos::common::FileSystem::fsid_t, unsigned long long > > mFsidErrorMap;
  // map 'error-name'-> 'fsid' -> set of 'fid' 
  google::sparse_hash_map<std::string, google::sparse_hash_map <eos::common::FileSystem::fsid_t, google::sparse_hash_set<unsigned long long> > > mFsidErrorFidSet;

  XrdSysMutex mGlobalCounterLock;
  unsigned long long totalfiles;
  unsigned long long nchecked;
  unsigned long long nunchecked;
  unsigned long long n_error_replica_not_registered;
  unsigned long long n_error_replica_orphaned;
  
  unsigned long long n_error_mgm_disk_size_differ;
  unsigned long long n_error_fst_disk_fmd_size_differ;
  unsigned long long n_error_mgm_disk_checksum_differ;
  unsigned long long n_error_fst_disk_fmd_checksum_differ;    
  unsigned long long n_error_fst_filechecksum;
  unsigned long long n_error_fst_blockchecksum;
  unsigned long long n_error_replica_layout;
  unsigned long long n_error_replica_offline;
  unsigned long long n_error_file_offline;
  unsigned long long n_error_replica_missing;

public:
  struct ThreadInfo {
    Fsck* mFsck;
    eos::common::FileSystem::fsid_t mFsid;
    bool mActive;
    size_t mPos;
    size_t mMax;
    std::string mHostPort;
    std::string mMountPoint;
  };

  XrdSysMutex mScanThreadMutex;

  pthread_t mThread;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, ThreadInfo> mScanThreadInfo;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, pthread_t> mScanThreads;
  google::sparse_hash_map<eos::common::FileSystem::fsid_t, pthread_t> mScanThreadsJoin;

  bool mRunning;
  
  
  Fsck();
  ~Fsck();
 
  bool Start();
  bool Stop();

  static void* StaticScan(void*);

  void* Scan(eos::common::FileSystem::fsid_t fsid, bool active, size_t pos, size_t max, std::string hostport, std::string mountpoint);

  void PrintOut(XrdOucString &out, XrdOucString option="");
  bool Report(XrdOucString &out, XrdOucString &err, XrdOucString option="", XrdOucString selection="");

  void ClearLog();
  void Log(bool overwrite, const char* msg, ...);

  bool SetMaxThreads(size_t nthreads) {
    mParallelThreads = nthreads;
    if (mParallelThreads <1) 
      mParallelThreads = 1;
    if (mParallelThreads > 100)
      mParallelThreads = 100;
    return true;
  }

  static void* StaticCheck(void*);
  void* Check();
};

EOSMGMNAMESPACE_END

#endif
