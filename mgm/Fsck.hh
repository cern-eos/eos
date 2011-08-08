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


public:
  pthread_t mThread;
  bool mRunning;

  
  Fsck();
  ~Fsck();

  bool Start();
  bool Stop();

  void PrintOut(XrdOucString &out, XrdOucString option="");
  bool Report(XrdOucString &out, XrdOucString &err, XrdOucString option="", XrdOucString selection="");

  void ClearLog();
  void Log(bool overwrite, const char* msg, ...);
  
  static void* StaticCheck(void*);
  void* Check();
};

EOSMGMNAMESPACE_END

#endif
