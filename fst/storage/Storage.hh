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
#include "fst/Config.hh"
#include "fst/storage/FileSystem.hh"
#include "common/Logging.hh"
#include "common/Statfs.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "common/TransferQueue.hh"
#include "fst/Deletion.hh"
#include "fst/Verify.hh"
#include "fst/Load.hh"
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
class Storage : public eos::common::LogId
{
  friend class XrdFstOfsFile;
  friend class XrdFstOfs;

private:
  bool zombie;

  XrdOucString metaDirectory;

  unsigned long long* scrubPattern[2];
  unsigned long long* scrubPatternVerify;

  TransferQueue* mTxGwQueue; // the handle to the storage queue of gw transfers
  eos::common::TransferQueue* mGwQueue; // the handle to the low-level queue of gw transfers

protected:
  eos::common::RWMutex fsMutex;

  XrdSysMutex ThreadSetMutex;
  std::set<pthread_t> ThreadSet;

public:
  TransferMultiplexer mGwMultiplexer; // the multiplexer for gw transfers

  // fsstat & quota thread
  static void* StartDaemonSupervisor (void *pp);
  static void* StartFsCommunicator (void *pp);
  static void* StartFsScrub (void * pp);
  static void* StartFsTrim (void* pp);
  static void* StartFsRemover (void* pp);
  static void* StartFsReport (void* pp);
  static void* StartFsErrorReport (void* pp);
  static void* StartFsVerify (void* pp);
  static void* StartFsPublisher (void* pp);
  static void* StartFsBalancer (void* pp);
  static void* StartFsDrainer (void* pp);
  static void* StartFsCleaner (void* pp);
  static void*
  StartMgmSyncer (void* pp);

  struct BootThreadInfo
  {
    Storage* storage;
    FileSystem* filesystem;
  };

  static void* StartBoot (void* pp);

  XrdSysMutex BootSetMutex; // Mutex protecting the boot set
  std::set<eos::common::FileSystem::fsid_t> BootSet; // set containing the filesystems currently booting
  bool RunBootThread (FileSystem* fs);

  void Scrub ();
  void Trim ();
  void Remover ();
  void Report ();
  void ErrorReport ();
  void Verify ();
  void Communicator ();
  void Supervisor ();
  void Publish ();
  void Balancer ();
  void Drainer ();
  void Cleaner ();
  void MgmSyncer ();

  void Boot (FileSystem* fs);

  eos::fst::Verify* runningVerify;

  XrdSysMutex deletionsMutex;
  std::vector <Deletion> deletions;

  off_t
  deletionsSize ()
  {
    // take the lock 'deletionsMutex' outside: 
    off_t totalsize = 0;

    for (size_t i = 0; i < deletions.size(); i++)
    {
      totalsize = deletions[i].fIdVector.size();
    }
    return totalsize;
  }

  XrdSysMutex verificationsMutex;
  std::queue <eos::fst::Verify*> verifications;

  std::map<std::string, FileSystem*> fileSystems;
  std::vector <FileSystem*> fileSystemsVector;
  std::map<eos::common::FileSystem::fsid_t, FileSystem*> fileSystemsMap;

  XrdSysMutex fileSystemFullMapMutex;
  std::map<eos::common::FileSystem::fsid_t, bool> fileSystemFullMap; // map indicating if a filesystem has less than  5 GB free
  std::map<eos::common::FileSystem::fsid_t, bool> fileSystemFullWarnMap; // map indicating if a filesystem has less than (headroom) space free (disables drain + balancing)

  //  static int HasStatfsChanged(const char* key, FileSystem* filesystem, void* arg);
  int ScrubFs (const char* path, unsigned long long free, unsigned long long lbocks, unsigned long id);

  Storage (const char* metadirectory);

  ~Storage () { };

  Load fstLoad;

  static Storage* Create (const char* metadirectory);

  void BroadcastQuota (XrdOucString &quotareport);

  bool BootFileSystem (XrdOucEnv &env);

  bool
  IsZombie ()
  {
    return zombie;
  }

  bool OpenTransaction (eos::common::FileSystem::fsid_t fsid, unsigned long long fid);
  bool CloseTransaction (eos::common::FileSystem::fsid_t fsid, unsigned long long fid);

  bool FsLabel (std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid);
  bool CheckLabel (std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid, bool failenoid = false, bool failnouuid = false);
  bool GetFsidFromLabel (std::string path, eos::common::FileSystem::fsid_t &fsid);
  bool GetFsidFromPath (std::string path, eos::common::FileSystem::fsid_t &fsid);
};

EOSFSTNAMESPACE_END

#endif
