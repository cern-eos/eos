// ----------------------------------------------------------------------
// File: Storage.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSFST_STORAGE_HH__
#define __EOSFST_STORAGE_HH__
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
#include "common/Logging.hh"
#include "common/Statfs.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "common/TransferQueue.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "fst/Load.hh"
#include "fst/ScanDir.hh"
#include "fst/txqueue/TransferQueue.hh"
#include "fst/txqueue/TransferMultiplexer.hh"
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
  eos::fst::ScanDir*      scanDir;     // the class scanning checksum on a filesystem
  unsigned long last_blocks_free;  
  time_t        last_status_broadcast;

  TransferQueue* mTxDrainQueue;
  TransferQueue* mTxBalanceQueue;
  TransferQueue* mTxExternQueue;

  TransferMultiplexer mTxMultiplexer;

  std::map<std::string, size_t> inconsistency_stats;
  std::map<std::string, std::set<eos::common::FileId::fileid_t> > inconsistency_sets;

public:
  FileSystem(const char* queuepath, const char* queue, XrdMqSharedObjectManager* som);

  ~FileSystem();

  void SetTransactionDirectory(const char* tx) { transactionDirectory= tx;}
  void CleanTransactions();
  void SyncTransactions();

  void RunScanner(Load* fstLoad, time_t interval);

  
  std::string  GetPath() {return GetString("path");}

  const char* GetTransactionDirectory() {return transactionDirectory.c_str();}

  TransferQueue* GetDrainQueue()   { return mTxDrainQueue;  }
  TransferQueue* GetBalanceQueue() { return mTxBalanceQueue;}
  TransferQueue* GetExternQueue()  { return mTxExternQueue; }

  XrdSysMutex InconsistencyStatsMutex; // mutex protecting inconsistency_stats
  std::map<std::string, size_t> *GetInconsistencyStats() { return &inconsistency_stats;}
  std::map<std::string, std::set<eos::common::FileId::fileid_t> > *GetInconsistencySets() { return &inconsistency_sets;}
 

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

  TransferQueue* mTxGwQueue;               // the handle to the storage queue of gw transfers
  eos::common::TransferQueue* mGwQueue;    // the handle to the low-level queue of gw transfers

protected:
  eos::common::RWMutex fsMutex;

  XrdSysMutex ThreadSetMutex;
  std::set<pthread_t> ThreadSet;

public:
  TransferMultiplexer mGwMultiplexer;      // the multiplexer for gw transfers

  // fsstat & quota thread
  static void* StartDaemonSupervisor(void *pp);
  static void* StartFsCommunicator(void *pp); 
  static void* StartFsScrub(void * pp);
  static void* StartFsTrim(void* pp);
  static void* StartFsRemover(void* pp);
  static void* StartFsReport(void* pp);
  static void* StartFsErrorReport(void* pp);
  static void* StartFsVerify(void* pp);
  static void* StartFsPublisher(void* pp);
  static void* StartFsBalancer(void* pp);
  static void* StartFsDrainer(void* pp);
  static void* StartFsCleaner(void* pp);
  static void* StartMgmSyncer(void* pp);

  struct BootThreadInfo {
    Storage*    storage;
    FileSystem* filesystem;
  };

  static void* StartBoot(void* pp);

  XrdSysMutex BootSetMutex;                          // Mutex protecting the boot set
  std::set<eos::common::FileSystem::fsid_t> BootSet; // set containing the filesystems currently booting
  bool RunBootThread(FileSystem* fs);

  void Scrub();
  void Trim();
  void Remover();
  void Report();
  void ErrorReport();
  void Verify();
  void Communicator();
  void Supervisor();
  void Publish();
  void Balancer();
  void Drainer();
  void Cleaner();
  void MgmSyncer();

  void Boot(FileSystem* fs);

  eos::fst::Verify* runningVerify;

  XrdSysMutex deletionsMutex;
  std::vector <Deletion> deletions;

  off_t deletionsSize() {
    // take the lock 'deletionsMutex' outside: 
    off_t totalsize=0;

    for (size_t i=0; i< deletions.size(); i++) {
      totalsize=deletions[i].fIdVector.size();
    }
    return totalsize;
  }
  
  XrdSysMutex verificationsMutex;
  std::queue <eos::fst::Verify*> verifications;

  std::map<std::string, FileSystem*> fileSystems;
  std::vector <FileSystem*> fileSystemsVector;
  std::map<eos::common::FileSystem::fsid_t , FileSystem*> fileSystemsMap;

  XrdSysMutex fileSystemFullMapMutex;
  std::map<eos::common::FileSystem::fsid_t , bool> fileSystemFullMap;     // map indicating if a filesystem has less than the headroom space free 
  std::map<eos::common::FileSystem::fsid_t , bool> fileSystemFullWarnMap; // map indicating if a filesystem has less than (headroom +1G) space free (disables drain + balancing)
  
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
  bool CheckLabel(std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid, bool failenoid=false, bool failnouuid=false);
  bool GetFsidFromLabel(std::string path, eos::common::FileSystem::fsid_t &fsid);
  bool GetFsidFromPath(std::string path, eos::common::FileSystem::fsid_t &fsid);
};

EOSFSTNAMESPACE_END

#endif
