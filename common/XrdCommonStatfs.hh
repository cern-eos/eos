#ifndef __XRDCOMMON_STATFS_HH__
#define __XRDCOMMON_STATFS_HH__

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <sys/vfs.h>

/*----------------------------------------------------------------------------*/
class XrdCommonStatfs {
  struct statfs statFs;
  XrdOucString path;
  XrdOucString env;
  
public:
  static XrdSysMutex gMutex;
  static XrdOucHash<XrdCommonStatfs> gStatfs;
  static XrdCommonStatfs* GetStatfs(const char* path) {
    gMutex.Lock();
    XrdCommonStatfs* sfs = gStatfs.Find(path);
    gMutex.UnLock();
    return sfs;
  }

  struct statfs* GetStatfs() {return &statFs;}
  const char* GetEnv() {return env.c_str();}


  int DoStatfs() {
    env ="";
    int retc=statfs(path.c_str(), (struct statfs*) &statFs);
    if (!retc) {
      char s[1024];    
      sprintf(s,"statfs.type=%ld&statfs.bsize=%ld&statfs.blocks=%ld&statfs.bfree=%ld&statfs.bavail=%ld&statfs.files=%ld&statfs.ffree=%ld&stat.namelen=%ld", statFs.f_type,statFs.f_bsize,statFs.f_blocks,statFs.f_bfree,statFs.f_bavail,statFs.f_files,statFs.f_ffree,statFs.f_namelen);
      env = s;
    }
    return retc;
  }

  
  XrdCommonStatfs(const char* inpath) {
    path = inpath;
  }
  
  ~XrdCommonStatfs(){}
  
  static XrdCommonStatfs* DoStatfs(const char* path) {
    XrdCommonStatfs* sfs = new XrdCommonStatfs(path);
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

#endif
