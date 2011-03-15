#ifndef __EOSFST_STORAGE_HH__
#define __EOSFST_STORAGE_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/Statfs.hh"
#include "common/FileSystem.hh"
#include "fst/transfer/Transfer.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
#include <list>
#include <queue>
#include <map>
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
class FileSystem : public eos::common::LogId {
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

  eos::common::Statfs* statFs;         // the owner of the object is a global hash in eos::common::Statfs - this are just references
  unsigned long last_blocks_free;  
  time_t last_status_broadcast;

public:
  FileSystem(const char* inpath) {Path = inpath; Id = 0; queueName =""; schedulingGroup=""; statFs = 0; last_blocks_free=0;last_status_broadcast=0;Status = eos::common::FileSystem::kDown; errc = 0; errmsg = "";transactionDirectory="";}
  ~FileSystem() {}

  void SetId(unsigned int id) {Id = id;}
  void SetSchedulingGroup(const char* inschedgroup) { schedulingGroup = inschedgroup;}
  void SetQueue(const char* inqueue) { queueName = inqueue;}
  void SetStatus(int status) { Status = status;}
  void SetError(int ec, const char* msg) { errc = ec; errmsg = msg;}
  void SetTransactionDirectory(const char* tx) { transactionDirectory= tx;}

  const char* GetPath() {return Path.c_str();}
  unsigned int GetId()  {return Id;}
  eos::common::Statfs* GetStatfs() {return statFs;}
  const char* GetTransactionDirectory() {return transactionDirectory.c_str();}

  const char* GetEnvString() { 
    Env = "mgm.fsname="; Env += queueName; Env += "&mgm.fsschedgroup="; Env += schedulingGroup;
    Env += "&mgm.fspath="; Env += Path; Env += "&mgm.fsid="; Env += (int)Id; return Env.c_str();
  }

  void GetLoadString(XrdOucString &loadstring) {
    loadstring  = "statfs.disk.load"; loadstring += "1.0";
    loadstring += "&statfs.disk.in";   loadstring += "100.0";
    loadstring += "&statfs.disk.out";  loadstring += "100.0"; 
    loadstring += "&statfs.net.load"; loadstring  += "1.0";
  }

  void BroadcastError(const char* msg);
  void BroadcastError(int errc, const char* errmsg);
  void BroadcastStatus();

  bool OpenTransaction(unsigned long long fid);
  bool CloseTransaction(unsigned long long fid);

  eos::common::Statfs* GetStatfs(bool &changedalot);
}; 


/*----------------------------------------------------------------------------*/
class Storage : public eos::common::LogId {
  friend class XrdFstOfsFile;
  friend class XrdFstOfs;

private:
  bool zombie;

  XrdOucString metaDirectory;

  unsigned long long* scrubPattern[2];
  unsigned long long* scrubPatternVerify;

protected:
  XrdSysMutex fsMutex;

public:
  // fsstat & quota thread
  static void* StartFsCommunicator(void *pp); 
  static void* StartFsScrub(void * pp);
  static void* StartFsTrim(void* pp);
  static void* StartFsRemover(void* pp);
  static void* StartFsPulling(void* pp);
  static void* StartFsReport(void* pp);
  static void* StartFsVerify(void* pp);

  void Scrub();
  void Trim();
  void Remover();
  void Pulling();
  void Report();
  void Verify();
  void Communicator();

  XrdSysMutex transferMutex;
  std::list <Transfer*> transfers;
  Transfer* runningTransfer;
  eos::fst::Verify* runningVerify;

  XrdSysMutex deletionsMutex;
  std::vector <Deletion> deletions;
  
  XrdSysMutex verificationsMutex;
  std::queue <eos::fst::Verify*> verifications;

  std::map<std::string, FileSystem*> fileSystems;
  std::vector <FileSystem*> fileSystemsVector;
  std::map<unsigned int, FileSystem*> fileSystemsMap;

  //  static int HasStatfsChanged(const char* key, FileSystem* filesystem, void* arg);
  int ScrubFs(const char* path, unsigned long long free, unsigned long long lbocks, unsigned long id);

  Storage(const char* metadirectory);
  ~Storage() {};

  static Storage* Create(const char* metadirectory);

  void BroadcastQuota(XrdOucString &quotareport);

  bool SetFileSystem(XrdOucEnv &env);
  bool RemoveFileSystem(XrdOucEnv &env);

  bool IsZombie() {return zombie;}
  
  bool OpenTransaction(unsigned int fsid, unsigned long long fid);
  bool CloseTransaction(unsigned int fsid, unsigned long long fid);
};

EOSFSTNAMESPACE_END

#endif
