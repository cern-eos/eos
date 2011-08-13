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

  // these maps contain the snapshot state of the last fsck
  XrdSysMutex mErrorMapMutex;

  std::vector<std::string> mErrorNames;

  // map 'error-name'-> errorcount
  std::map<std::string, unsigned long long> mTotalErrorMap;
  // map 'error-name'-> error help
  std::map<std::string, std::string> mErrorHelp;
  // map 'error-name'-> 'fsid' -> errorcount
  std::map<std::string, std::map <eos::common::FileSystem::fsid_t, unsigned long long > > mFsidErrorMap;
  // map 'error-name'-> 'fsid' -> set of 'fid' 
  std::map<std::string, std::map <eos::common::FileSystem::fsid_t, std::set<unsigned long long> > > mFsidErrorFidSet;

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
  std::map<eos::common::FileSystem::fsid_t, ThreadInfo> mScanThreadInfo;
  std::map<eos::common::FileSystem::fsid_t, pthread_t> mScanThreads;

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
  
  static void* StaticCheck(void*);
  void* Check();
};

EOSMGMNAMESPACE_END

#endif
