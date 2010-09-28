#ifndef __XRDFSTOFS_LOCK_HH__
#define __XRDFSTOFS_LOCK_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLayoutId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <google/sparse_hash_set>
/*----------------------------------------------------------------------------*/

class XrdFstOfsLockManager {
protected:
  google::sparse_hash_set<unsigned long long> LockFid;
  XrdSysMutex LockMutex;

public:
  XrdFstOfsLockManager(){LockFid.set_deleted_key(0);}
  virtual ~XrdFstOfsLockManager(){};

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

#endif
