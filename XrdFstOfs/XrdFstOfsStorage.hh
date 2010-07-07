#ifndef __XRDFSTOFS_STORAGE_HH__
#define __XRDFSTOFS_STORAGE_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonStatfs.hh"
#include "XrdCommon/XrdCommonFileSystem.hh"
#include "XrdFstOfs/XrdFstTransfer.hh"
#include "XrdFstOfs/XrdFstDeletion.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstOfsFileSystem : public XrdCommonLogId {
private:
  XrdOucString Path;
  unsigned int Id;
  XrdOucString queueName;
  XrdOucString schedulingGroup;
  XrdOucString  Env;
  int          Status;
  int          errc;
  XrdOucString errmsg;

  XrdCommonStatfs* statFs;         // the owner of the object is a global hash in XrdCommonStatfs - this are just references
  unsigned long last_blocks_free;  
  time_t last_status_broadcast;

public:
  XrdFstOfsFileSystem(const char* inpath) {Path = inpath; Id = 0; queueName =""; schedulingGroup=""; statFs = 0; last_blocks_free=0;last_status_broadcast=0;Status = XrdCommonFileSystem::kDown; errc = 0; errmsg = "";}
  ~XrdFstOfsFileSystem() {}

  void SetId(unsigned int id) {Id = id;}
  void SetSchedulingGroup(const char* inschedgroup) { schedulingGroup = inschedgroup;}
  void SetQueue(const char* inqueue) { queueName = inqueue;}
  void SetStatus(int status) { Status = status;}
  void SetError(int ec, const char* msg) { errc = ec; errmsg = msg;}

  const char* GetPath() {return Path.c_str();}
  unsigned int GetId()  {return Id;}
  XrdCommonStatfs* GetStatfs() {return statFs;}

  const char* GetEnvString() { 
    Env = "mgm.fsname="; Env += queueName; Env += "&mgm.fsschedgroup="; Env += schedulingGroup;
    Env += "&mgm.fspath="; Env += Path; Env += "&mgm.fsid="; Env += (int)Id; return Env.c_str();
  }

  void BroadcastError(const char* msg);
  void BroadcastStatus();

  XrdCommonStatfs* GetStatfs(bool &changedalot);
}; 


/*----------------------------------------------------------------------------*/
class XrdFstOfsStorage : public XrdCommonLogId {
private:
  bool zombie;

  XrdSysMutex fsMutex;
  XrdOucString metaDirectory;

  unsigned long long* scrubPattern[2];
  unsigned long long* scrubPatternVerify;
public:
  // fsstat & quota thread
  static void* StartFsQuota(void *pp); 
  static void* StartFsScrub(void * pp);
  static void* StartFsTrim(void* pp);
  static void* StartFsRemover(void* pp);
  static void* StartFsPulling(void* pp);
  static void* StartFsReport(void* pp);

  void Quota();
  void Scrub();
  void Trim();
  void Remover();
  void Pulling();
  void Report();

  XrdSysMutex transferMutex;
  std::vector <XrdFstTransfer> transfers;
  XrdSysMutex deletionsMutex;
  std::vector <XrdFstDeletion> deletions;

  XrdOucHash<XrdFstOfsFileSystem> fileSystems;
  std::vector <XrdFstOfsFileSystem*> fileSystemsVector;

  static int HasStatfsChanged(const char* key, XrdFstOfsFileSystem* filesystem, void* arg);
  int ScrubFs(const char* path, unsigned long long free, unsigned long long lbocks, unsigned long id);

  XrdFstOfsStorage(const char* metadirectory);
  ~XrdFstOfsStorage() {};

  static XrdFstOfsStorage* Create(const char* metadirectory);

  void BroadcastQuota(XrdOucString &quotareport);

  bool SetFileSystem(XrdOucEnv &env);
  bool RemoveFileSystem(XrdOucEnv &env);

  bool IsZombie() {return zombie;}

};


#endif
