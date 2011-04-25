#ifndef __EOSFST_STORAGE_HH__
#define __EOSFST_STORAGE_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/Statfs.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "fst/transfer/Transfer.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "fst/Load.hh"
#include "fst/ScanDir.hh"
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
class FileSystem : public eos::common::FileSystem, eos::common::LogId {
private:
  XrdOucString transactionDirectory;

  eos::common::Statfs* statFs;         // the owner of the object is a global hash in eos::common::Statfs - this are just references
  eos::fst::ScanDir*      scanDir;               // the class scanning checksum on a filesystem
  unsigned long last_blocks_free;  
  time_t        last_status_broadcast;

public:
  FileSystem(const char* queuepath, const char* queue, XrdMqSharedObjectManager* som) : eos::common::FileSystem(queuepath,queue,som) {
    last_blocks_free=0;
    last_status_broadcast=0;
    transactionDirectory="";
    statFs = 0;
    scanDir = 0;
  }

  ~FileSystem() {
    if (scanDir) {
      delete scanDir;
    }
  }

  void SetTransactionDirectory(const char* tx) { transactionDirectory= tx;}
  void RunScanner(Load* fstLoad, time_t interval);

  std::string  GetPath() {return GetString("path");}

  const char* GetTransactionDirectory() {return transactionDirectory.c_str();}

  void BroadcastError(const char* msg);
  void BroadcastError(int errc, const char* errmsg);
  void BroadcastStatus();

  bool OpenTransaction(unsigned long long fid);
  bool CloseTransaction(unsigned long long fid);

  void SetError(int errc, const char* errmsg) {
    if (errc) {
      eos_static_err("setting errc=%d errmsg=%s", errc, errmsg?errmsg:"");
    }
    if (!SetLongLong("stat.errc",errc)) {
      eos_static_err("cannot set errcode for filesystem %s", GetQueuePath().c_str());
    }
    if (!SetString("stat.errmsg",errmsg)) {
      eos_static_err("cannot set errmsg for filesystem %s", GetQueuePath().c_str());
    }
  }
  
  eos::common::Statfs* GetStatfs();
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
  eos::common::RWMutex fsMutex;

public:
  // fsstat & quota thread
  static void* StartFsCommunicator(void *pp); 
  static void* StartFsScrub(void * pp);
  static void* StartFsTrim(void* pp);
  static void* StartFsRemover(void* pp);
  static void* StartFsPulling(void* pp);
  static void* StartFsReport(void* pp);
  static void* StartFsVerify(void* pp);
  static void* StartFsPublisher(void* pp);

  void Scrub();
  void Trim();
  void Remover();
  void Pulling();
  void Report();
  void Verify();
  void Communicator();
  void Publish();

  void Boot(FileSystem* fs);

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
  std::map<eos::common::FileSystem::fsid_t , FileSystem*> fileSystemsMap;

  //  static int HasStatfsChanged(const char* key, FileSystem* filesystem, void* arg);
  int ScrubFs(const char* path, unsigned long long free, unsigned long long lbocks, unsigned long id);

  Storage(const char* metadirectory);
  ~Storage() {};
  
  Load fstLoad;

  static Storage* Create(const char* metadirectory);

  void BroadcastQuota(XrdOucString &quotareport);

  bool BootFileSystem(XrdOucEnv &env);

  bool IsZombie() {return zombie;}
  
  bool OpenTransaction (eos::common::FileSystem::fsid_t fsid, unsigned long long fid);
  bool CloseTransaction(eos::common::FileSystem::fsid_t fsid, unsigned long long fid);

  bool FsLabel(std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid);
  bool CheckLabel(std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid);
};

EOSFSTNAMESPACE_END

#endif
