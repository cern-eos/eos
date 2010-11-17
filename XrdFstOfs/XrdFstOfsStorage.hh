#ifndef __XRDFSTOFS_STORAGE_HH__
#define __XRDFSTOFS_STORAGE_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonStatfs.hh"
#include "XrdCommon/XrdCommonFileSystem.hh"
#include "XrdFstOfs/XrdFstTransfer.hh"
#include "XrdFstOfs/XrdFstDeletion.hh"
#include "XrdFstOfs/XrdFstVerify.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <list>
#include <queue>
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
  XrdOucString transactionDirectory;

  XrdCommonStatfs* statFs;         // the owner of the object is a global hash in XrdCommonStatfs - this are just references
  unsigned long last_blocks_free;  
  time_t last_status_broadcast;

public:
  XrdFstOfsFileSystem(const char* inpath) {Path = inpath; Id = 0; queueName =""; schedulingGroup=""; statFs = 0; last_blocks_free=0;last_status_broadcast=0;Status = XrdCommonFileSystem::kDown; errc = 0; errmsg = "";transactionDirectory="";}
  ~XrdFstOfsFileSystem() {}

  void SetId(unsigned int id) {Id = id;}
  void SetSchedulingGroup(const char* inschedgroup) { schedulingGroup = inschedgroup;}
  void SetQueue(const char* inqueue) { queueName = inqueue;}
  void SetStatus(int status) { Status = status;}
  void SetError(int ec, const char* msg) { errc = ec; errmsg = msg;}
  void SetTransactionDirectory(const char* tx) { transactionDirectory= tx;}

  const char* GetPath() {return Path.c_str();}
  unsigned int GetId()  {return Id;}
  XrdCommonStatfs* GetStatfs() {return statFs;}
  const char* GetTransactionDirectory() {return transactionDirectory.c_str();}

  const char* GetEnvString() { 
    Env = "mgm.fsname="; Env += queueName; Env += "&mgm.fsschedgroup="; Env += schedulingGroup;
    Env += "&mgm.fspath="; Env += Path; Env += "&mgm.fsid="; Env += (int)Id; return Env.c_str();
  }

  void BroadcastError(const char* msg);
  void BroadcastError(int errc, const char* errmsg);
  void BroadcastStatus();

  bool OpenTransaction(unsigned long long fid);
  bool CloseTransaction(unsigned long long fid);

  XrdCommonStatfs* GetStatfs(bool &changedalot);
}; 


/*----------------------------------------------------------------------------*/
class XrdFstOfsStorage : public XrdCommonLogId {
  friend class XrdFstOfsFile;

private:
  bool zombie;

  XrdOucString metaDirectory;

  unsigned long long* scrubPattern[2];
  unsigned long long* scrubPatternVerify;

protected:
  XrdSysMutex fsMutex;

public:
  // fsstat & quota thread
  static void* StartFsQuota(void *pp); 
  static void* StartFsScrub(void * pp);
  static void* StartFsTrim(void* pp);
  static void* StartFsRemover(void* pp);
  static void* StartFsPulling(void* pp);
  static void* StartFsReport(void* pp);
  static void* StartFsVerify(void* pp);

  void Quota();
  void Scrub();
  void Trim();
  void Remover();
  void Pulling();
  void Report();
  void Verify();

  XrdSysMutex transferMutex;
  std::list <XrdFstTransfer*> transfers;
  XrdFstTransfer* runningTransfer;
  XrdFstVerify* runningVerify;

  XrdSysMutex deletionsMutex;
  std::vector <XrdFstDeletion> deletions;
  
  XrdSysMutex verificationsMutex;
  std::queue <XrdFstVerify*> verifications;

  XrdOucHash<XrdFstOfsFileSystem> fileSystems;
  std::vector <XrdFstOfsFileSystem*> fileSystemsVector;
  std::map<unsigned int, XrdFstOfsFileSystem*> fileSystemsMap;

  static int HasStatfsChanged(const char* key, XrdFstOfsFileSystem* filesystem, void* arg);
  int ScrubFs(const char* path, unsigned long long free, unsigned long long lbocks, unsigned long id);

  XrdFstOfsStorage(const char* metadirectory);
  ~XrdFstOfsStorage() {};

  static XrdFstOfsStorage* Create(const char* metadirectory);

  void BroadcastQuota(XrdOucString &quotareport);

  bool SetFileSystem(XrdOucEnv &env);
  bool RemoveFileSystem(XrdOucEnv &env);

  bool IsZombie() {return zombie;}
  
  bool OpenTransaction(unsigned int fsid, unsigned long long fid);
  bool CloseTransaction(unsigned int fsid, unsigned long long fid);
};


#endif
