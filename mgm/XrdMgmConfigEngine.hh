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
#include <fcntl.h>

/*----------------------------------------------------------------------------*/

#define XRDMGMCONFIGENGINE_EOS_SUFFIX ".eoscf"

class XrdMgmConfigEngineChangeLog : public XrdCommonLogId {
private:
  XrdSysMutex Mutex;
  int fd;
public:
  XrdOucString configChanges;

  XrdMgmConfigEngineChangeLog();
  ~XrdMgmConfigEngineChangeLog();

  void Init(const char* changelogfile);

  bool AddEntry(const char* info);
  bool Tail(unsigned int nlines, XrdOucString &tail);
};



class XrdMgmConfigEngine : public XrdCommonLogId {
private:
  XrdOucString configDir;
  XrdSysMutex Mutex;
  XrdOucString currentConfigFile;

  XrdMgmConfigEngineChangeLog changeLog;

  XrdOucHash<XrdOucString> configDefinitionsFile;
  XrdOucHash<XrdOucString> configDefinitions;

public:

  struct PrintInfo {
    XrdOucString* out;
    XrdOucString option;
  };

  static int ApplyEachConfig(const char* key, XrdOucString* def, void* Arg);

  static int PrintEachConfig(const char* key, XrdOucString* def, void* Arg);

  static int DeleteConfigByMatch(const char* key, XrdOucString* def, void* Arg);

  XrdMgmConfigEngine(const char* configdir) {
    configDir = configdir;
    changeLog.configChanges = "";
    currentConfigFile = "default.eoscf";
    XrdOucString changeLogFile = configDir;
    changeLogFile += "/config.changelog";
    changeLog.Init(changeLogFile.c_str());
  }

  XrdMgmConfigEngineChangeLog* GetChangeLog() { return &changeLog;}

  bool LoadConfig(XrdOucEnv& env, XrdOucString &err);
  bool SaveConfig(XrdOucEnv& env, XrdOucString &err);
  void Diffs(XrdOucString &diffs) { diffs = changeLog.configChanges;   while(diffs.replace("&"," ")) {}};

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

  bool ApplyConfig(XrdOucString &err);

  void ResetConfig();

  void PrintConfig() {
    Mutex.Lock();
    configDefinitions.Apply(PrintEachConfig, NULL);
    Mutex.UnLock();
  }

  void SetConfigValue(const char* prefix, const char* fsname, const char* def, bool tochangelog = true) {
    XrdOucString cl = "set config "; cl+= prefix; cl += ":"; cl += fsname; cl+= " => "; cl += def;
    if (tochangelog)
      changeLog.AddEntry(cl.c_str());
    XrdOucString configname = prefix; configname+=":";
    XrdOucString *sdef = new XrdOucString(def);
    configname += fsname;
    configDefinitions.Rep(configname.c_str(),sdef);
    eos_static_debug("%s => %s", fsname, def);
  }

  void DeleteConfigValue(const char* prefix, const char* fsname) {
    Mutex.Lock();  

    XrdOucString configname = prefix; configname+=":"; configname += fsname;
    configDefinitions.Del(fsname);  XrdOucString currentConfig;

    eos_static_debug("%s", fsname);
    Mutex.UnLock();
  }
  
  void DeleteConfigValueByMatch(const char* prefix, const char* match) {
    Mutex.Lock();
    XrdOucString smatch = prefix; smatch += ":"; smatch += match;
    configDefinitions.Apply(DeleteConfigByMatch, &smatch);
    Mutex.UnLock();
  }
};

#endif

