#ifndef __EOSMGM_CONFIGENGINE__HH__
#define __EOSMGM_CONFIGENGINE__HH__

/*----------------------------------------------------------------------------*/
// this is needed because of some openssl definition conflict!
#undef des_set_key
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
#include <google/sparsehash/densehashtable.h>
/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "mq/XrdMqMessage.hh"
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


EOSMGMNAMESPACE_BEGIN

#define EOSMGMCONFIGENGINE_EOS_SUFFIX ".eoscf"

class ConfigEngineChangeLog : public eos::common::LogId {
private:
  XrdSysMutex Mutex;
  int fd;
public:
  XrdOucString configChanges;

  ConfigEngineChangeLog();
  ~ConfigEngineChangeLog();

  void Init(const char* changelogfile);

  bool AddEntry(const char* info);
  bool Tail(unsigned int nlines, XrdOucString &tail);
};



class ConfigEngine : public eos::common::LogId {
private:
  XrdOucString configDir;
  XrdSysMutex Mutex;
  XrdOucString currentConfigFile;

  ConfigEngineChangeLog changeLog;
  bool autosave;

public:

  std::map<std::string, std::string> configDefinitions;

  struct PrintInfo {
    XrdOucString* out;
    XrdOucString option;
  };

  int ApplyEachConfig(const char* key, std::string def, void* Arg);

  int PrintEachConfig(const char* key, std::string def, void* Arg);

  int DeleteConfigByMatch(const char* key, std::string def, void* Arg);

  ConfigEngine(const char* configdir) {
    configDir = configdir;
    changeLog.configChanges = "";
    currentConfigFile = "default";
    XrdOucString changeLogFile = configDir;
    changeLogFile += "/config.changelog";
    changeLog.Init(changeLogFile.c_str());
    autosave=false;
  }

  ConfigEngineChangeLog* GetChangeLog() { return &changeLog;}

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
    std::map<std::string,std::string>::const_iterator it;
    for (it = configDefinitions.begin(); it != configDefinitions.end(); it++) {
      PrintEachConfig(it->first.c_str(),it->second,NULL);
    }
    Mutex.UnLock();
  }

  void SetAutoSave(bool val) {autosave = val;}
  bool GetAutoSave() {return autosave;}

  void AutoSave() {
    if (autosave && currentConfigFile.length()) {
      int aspos=0;
      if ( (aspos = currentConfigFile.find(".autosave")) != STR_NPOS) {
	currentConfigFile.erase(aspos);
      }
      if ( (aspos = currentConfigFile.find(".backup")) != STR_NPOS) {
	currentConfigFile.erase(aspos);
      }
      XrdOucString envstring = "mgm.config.file=";envstring += currentConfigFile;
      envstring += "&mgm.config.force=1";
      envstring += "&mgm.config.autosave=1";
      XrdOucEnv env(envstring.c_str());
      XrdOucString err="";
      
      if (!SaveConfig(env, err)) {
	eos_static_err("%s\n", err.c_str());
      }
    }
  }
  
  void SetConfigValue(const char* prefix, const char* fsname, const char* def, bool tochangelog = true) {
    XrdOucString cl = "set config "; cl+= prefix; cl += ":"; cl += fsname; cl+= " => "; cl += def;
    if (tochangelog)
      changeLog.AddEntry(cl.c_str());
    XrdOucString configname = prefix; configname+=":";
    configname += fsname;
    Mutex.Lock();
    configDefinitions[configname.c_str()] = def;
    Mutex.UnLock();
    eos_static_debug("%s => %s", fsname, def);
    if (autosave && currentConfigFile.length()) {
      int aspos=0;
      if ( (aspos = currentConfigFile.find(".autosave")) != STR_NPOS) {
	currentConfigFile.erase(aspos);
      }
      if ( (aspos = currentConfigFile.find(".backup")) != STR_NPOS) {
	currentConfigFile.erase(aspos);
      }
      XrdOucString envstring = "mgm.config.file=";envstring += currentConfigFile;
      envstring += "&mgm.config.force=1";
      envstring += "&mgm.config.autosave=1";
      XrdOucEnv env(envstring.c_str());
      XrdOucString err="";

      if (!SaveConfig(env, err)) {
	eos_static_err("%s\n", err.c_str());
      }
    }
  }

  void DeleteConfigValue(const char* prefix, const char* fsname, bool tochangelog = true) {
    XrdOucString cl = "del config "; cl+= prefix; cl += ":"; cl += fsname; 

    XrdOucString configname = prefix; configname+=":"; configname += fsname;

    Mutex.Lock();  
    configDefinitions.erase(configname.c_str()); 
    Mutex.UnLock();

    if (tochangelog)
      changeLog.AddEntry(cl.c_str());

    if (autosave && currentConfigFile.length()) {
      int aspos=0;
      if ( (aspos = currentConfigFile.find(".autosave")) != STR_NPOS) {
	currentConfigFile.erase(aspos);
      }
      if ( (aspos = currentConfigFile.find(".backup")) != STR_NPOS) {
	currentConfigFile.erase(aspos);
      }
      XrdOucString envstring = "mgm.config.file=";envstring += currentConfigFile;
      envstring += "&mgm.config.force=1";
      envstring += "&mgm.config.autosave=1";
      XrdOucEnv env(envstring.c_str());
      XrdOucString err="";
      if (!SaveConfig(env, err)) {
	eos_static_err("%s\n", err.c_str());
      }
    }
    eos_static_debug("%s", fsname);

  }
  
  void DeleteConfigValueByMatch(const char* prefix, const char* match) {
    Mutex.Lock();
    XrdOucString smatch = prefix; smatch += ":"; smatch += match;
    std::map<std::string,std::string>::const_iterator it;
    for (it = configDefinitions.begin(); it != configDefinitions.end(); it++) {
      DeleteConfigByMatch(it->first.c_str(),it->second, (void*)match);
    }
    Mutex.UnLock();
  }

  void FlagGlobalDirty();
};

EOSMGMNAMESPACE_END

#endif

