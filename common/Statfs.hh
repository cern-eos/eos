#ifndef __EOSCOMMON_STATFS_HH__
#define __EOSCOMMON_STATFS_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class Statfs {
  struct statfs statFs;
  XrdOucString path;
  XrdOucString env;
  
public:
  static XrdSysMutex gMutex;
  static XrdOucHash<Statfs> gStatfs;
  static Statfs* GetStatfs(const char* path) {
    gMutex.Lock();
    Statfs* sfs = gStatfs.Find(path);
    gMutex.UnLock();
    return sfs;
  }

  struct statfs* GetStatfs() {return &statFs;}
  const char* GetEnv() {return env.c_str();}


  int DoStatfs() {
    env ="";
    int retc=::statfs(path.c_str(), (struct statfs*) &statFs);
    if (!retc) {
      char s[1024];    
      sprintf(s,"statfs.type=%ld&statfs.bsize=%ld&statfs.blocks=%ld&statfs.bfree=%ld&statfs.bavail=%ld&statfs.files=%ld&statfs.ffree=%ld", (long)statFs.f_type,(long)statFs.f_bsize,(long)statFs.f_blocks,(long)statFs.f_bfree,(long)statFs.f_bavail,(long)statFs.f_files,(long)statFs.f_ffree);
      env = s;
    }
    return retc;
  }

  
  Statfs(const char* inpath) {
    path = inpath;
    memset(&statFs, 0, sizeof(struct statfs));
  }
  
  ~Statfs(){}
  
  static Statfs* DoStatfs(const char* path) {
    Statfs* sfs = new Statfs(path);
    if (!sfs->DoStatfs()) {
      gMutex.Lock();
      gStatfs.Rep(path, sfs);
      gMutex.UnLock();
      return sfs;
    } else {
      delete sfs;
      return 0;
    }   
  }
};

EOSCOMMONNAMESPACE_END

#endif
