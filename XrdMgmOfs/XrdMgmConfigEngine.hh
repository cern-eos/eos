#ifndef __XRDMGMOFS_CONFIGENGINE__HH__
#define __XRDMGMOFS_CONFIGENGINE__HH__

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdMgmOfs/XrdMgmFstFileSystem.hh"
#include "XrdMqOfs/XrdMqMessage.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/

#define XRDMGMCONFIGENGINE_EOS_SUFFIX ".eoscf"

class XrdMgmConfigEngine : public XrdCommonLogId {
private:
  XrdOucString configDir;
  XrdSysMutex Mutex;
  XrdOucString currentConfig;


  XrdOucHash<XrdOucString> configDefinitions;

public:

  struct PrintInfo {
    XrdOucString* out;
    XrdOucString option;
  };

  static int PrintEachConfig(const char* key, XrdOucString* def, void* Arg);

  static int DeleteConfigByMatch(const char* key, XrdOucString* def, void* Arg);


  XrdMgmConfigEngine(const char* configdir) {
    configDir = configdir;
    currentConfig = "default.eoscf";
  }

  bool LoadConfig(XrdOucEnv& env, XrdOucString &err);
  bool SaveConfig(XrdOucEnv& env, XrdOucString &err);

  // for sorted listings
  static int CompareCtime(const void* a, const void*b) {
    struct filestat {
      struct stat buf;
      char filename[1024];
    };
    return ( (((struct filestat*)a)->buf.st_mtime) - ((struct filestat*)b)->buf.st_mtime);
  }

  bool ListConfigs(XrdOucString &configlist, bool showbackups=false);

  bool DumpConfig(XrdOucString &out, XrdOucEnv &filter);

  bool BuildConfig();

  bool BroadCastConfig();

  bool ParseConfig(XrdOucString &broadcast, XrdOucString &err);

  void PrintConfig() {
    Mutex.Lock();
    configDefinitions.Apply(PrintEachConfig, NULL);
    Mutex.UnLock();
  }

  void SetFsConfig(const char* fsname, const char* def) {
    Mutex.Lock();
    XrdOucString configname = "fs:";
    XrdOucString *sdef = new XrdOucString(def);
    configname += fsname;
    configDefinitions.Rep(configname.c_str(),sdef);
    eos_static_debug("%s => %s", fsname, def);
    Mutex.UnLock();
  }

  void DeleteFsConfig(const char* fsname) {
    Mutex.Lock();
    configDefinitions.Del(fsname);
    eos_static_debug("%s", fsname);
    Mutex.UnLock();
  }

  void DeleteFsConfigByMatch(const char* match) {
    Mutex.Lock();
    XrdOucString smatch = match;
    configDefinitions.Apply(DeleteConfigByMatch, &smatch);
    Mutex.UnLock();
  }
};

#endif

