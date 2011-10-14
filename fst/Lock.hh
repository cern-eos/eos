// ----------------------------------------------------------------------
// File: Lock.hh
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

#ifndef __XRDFSTOFS_LOCK_HH__
#define __XRDFSTOFS_LOCK_HH__

/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_set>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class LockManager {
protected:
  google::sparse_hash_set<unsigned long long> LockFid;
  XrdSysMutex LockMutex;

public:
  LockManager(){LockFid.set_deleted_key(0);}
  virtual ~LockManager(){};

  bool TryLock(unsigned long long fid) {
    if (!fid) return false;
    bool rc = false;
    LockMutex.Lock();
    if (!LockFid.count(fid)) {
      LockFid.insert(fid);
      rc = true;
    }
    LockMutex.UnLock();
    return rc;
  }
  
  bool UnLock(unsigned long long fid) {
    if (!fid) return false;
    bool rc = false;
    LockMutex.Lock();
    if (LockFid.count(fid)) {
      LockFid.erase(fid);
      LockFid.resize(0);
      rc = true;
    }
    LockMutex.UnLock();
    return rc;
  }

  bool LockTimeout(unsigned long long fid,int timeout=10) {

    for (int i=0; i< timeout; i++) {
      if (TryLock(fid)) 
        return true;
      sleep(1);
    }
    return false;
  }

  bool Lock(unsigned long long fid, int interval=100000) {
    if (!fid) return false;
    bool rc = false;
    do {
      LockMutex.Lock();
      if (!LockFid.count(fid)) {
        LockFid.insert(fid);
        rc = true;
        LockMutex.UnLock();
      } else {
        LockMutex.UnLock();
        usleep(interval);
      }
    } while (!rc);
    return rc;
  }

  bool IsLocked(unsigned long long fid) {
    if (!fid) return false;
    bool rc = false;
    LockMutex.Lock();
    if (LockFid.count(fid)) {
      rc = true;
    }
    LockMutex.UnLock();
    return rc;
  }
};

EOSFSTNAMESPACE_END

#endif
