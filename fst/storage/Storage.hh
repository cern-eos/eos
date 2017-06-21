//------------------------------------------------------------------------------
// File: Storage.hh
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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
#include "fst/Health.hh"
#include "mq/XrdMqSharedObject.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <vector>
#include <list>
#include <queue>
#include <map>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Storage
//------------------------------------------------------------------------------
class Storage: public eos::common::LogId
{
  friend class XrdFstOfsFile;
  friend class XrdFstOfs;
public:
  //----------------------------------------------------------------------------
  //! Create Storage object
  //!
  //! @param metadirectory path to meta dir
  //!
  //! @return pointer to newly created storage object
  //----------------------------------------------------------------------------
  static Storage* Create(const char* metadirectory);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Storage(const char* metadirectory);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Storage();

  //----------------------------------------------------------------------------
  //! Shutdown all helper threads
  //----------------------------------------------------------------------------
  void ShutdownThreads();

  //----------------------------------------------------------------------------
  //! Add deletion object to the list of pending ones
  //!
  //! @param del deletion object
  //----------------------------------------------------------------------------
  void AddDeletion(std::unique_ptr<Deletion> del);

  //----------------------------------------------------------------------------
  //! Get deletion object removing it from the list
  //!
  //! @return get deletion object
  //----------------------------------------------------------------------------
  std::unique_ptr<Deletion> GetDeletion();

  //----------------------------------------------------------------------------
  //! Get number of pending deletions
  //!
  //! @return number of pending deletions
  //----------------------------------------------------------------------------
  size_t GetNumDeletions();

  //----------------------------------------------------------------------------
  //! Open transaction operation for file fid on filesystem fsid
  //!
  //! @param fsid filesystem id
  //! @param fid file id
  //!
  //! @return true if transaction opened successfully, otherwise false
  //----------------------------------------------------------------------------
  bool OpenTransaction(eos::common::FileSystem::fsid_t fsid,
                       unsigned long long fid);

  //----------------------------------------------------------------------------
  //! Close transaction operation for file fid on filesystem fsid
  //!
  //! @param fsid filesystem id
  //! @param fid file id
  //!
  //! @return true if transaction closed successfully, otherwise false
  //----------------------------------------------------------------------------
  bool CloseTransaction(eos::common::FileSystem::fsid_t fsid,
                        unsigned long long fid);

  //----------------------------------------------------------------------------
  //! Push new verification job to the queue if the maximum number of pending
  //! verifications is not exceeded.
  //!
  //! @param entry verification information about a file
  //----------------------------------------------------------------------------
  void PushVerification(eos::fst::Verify* entry);

  //----------------------------------------------------------------------------
  //! Wait until configuration queue is defined
  //!
  //! @param node_cfg_queue configuration queue for our FST
  //----------------------------------------------------------------------------
  void WaitConfigQueue(std::string& nodeconfigqueue);

protected:
  eos::common::RWMutex mFsMutex; ///< Mutex protecting acccess to the fs map
  std::vector <FileSystem*> mFsVect; ///< Vector of filesystems
  //! Map of filesystem id to filesystem object
  std::map<eos::common::FileSystem::fsid_t, FileSystem*> mFileSystemsMap;
  //! Map of filesystem queue to filesystem object
  std::map<std::string, FileSystem*> mQueue2FsMap;

private:
  bool mZombie; ///< State of the node
  XrdOucString mMetaDir; ///< Path to meta directory
  unsigned long long* mScrubPattern[2];
  unsigned long long* mScrubPatternVerify;
  //! Handle to the storage queue of gw transfers
  TransferQueue* mTxGwQueue;
  //! Handle to the low-level queue of gw transfers
  eos::common::TransferQueue* mGwQueue;
  //! Multiplexer for gw transfers
  TransferMultiplexer mGwMultiplexer;
  XrdSysMutex mBootingMutex; // Mutex protecting the boot set
  //! Set containing the filesystems currently booting
  std::set<eos::common::FileSystem::fsid_t> mBootingSet;
  eos::fst::Verify* mRunningVerify; ///< Currently running verification job
  XrdSysMutex mThreadsMutex; ///< Mutex protecting access to the set of threads
  std::set<pthread_t> mThreadSet; ///< Set of running helper threads
  XrdSysMutex mFsFullMapMutex; ///< Mutex protecting access to the fs full map
  //! Map indicating if a filesystem has less than  5 GB free
  std::map<eos::common::FileSystem::fsid_t, bool> mFsFullMap;
  //! Map indicating if a filesystem has less than (headroom) space free, which
  //! disables draining and balancing
  std::map<eos::common::FileSystem::fsid_t, bool> mFsFullWarnMap;
  XrdSysMutex mVerifyMutex; ///< Mutex protecting access to the verifications
  //! Queue of verification jobs pending
  std::queue <eos::fst::Verify*> mVerifications;
  XrdSysMutex mDeletionsMutex; ///< Mutex protecting the list of deletions
  std::list< std::unique_ptr<Deletion> > mListDeletions; ///< List of deletions
  Load mFstLoad; ///< Net/IO load monitor
  Health mFstHealth; ///< Local disk S.M.A.R.T monitor

  //! Struct BootThreadInfo
  struct BootThreadInfo {
    Storage* storage;
    FileSystem* filesystem;
  };

  //----------------------------------------------------------------------------
  //! Helper methods used for starting worker threads
  //----------------------------------------------------------------------------
  static void* StartVarPartitionMonitor(void* pp);
  static void* StartDaemonSupervisor(void* pp);
  static void* StartFsCommunicator(void* pp);
  static void* StartFsScrub(void* pp);
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
  static void* StartBoot(void* pp);

  //----------------------------------------------------------------------------
  //! Worker threads implementation
  //----------------------------------------------------------------------------
  void Supervisor();
  void Communicator();
  void Scrub();
  void Trim();
  void Remover();
  void Report();
  void ErrorReport();
  void Verify();
  void Publish();
  void Balancer();
  void Drainer();
  void Cleaner();
  void MgmSyncer();
  void Boot(FileSystem* fs);

  //----------------------------------------------------------------------------
  //! Scrub filesystem
  //----------------------------------------------------------------------------
  int ScrubFs(const char* path, unsigned long long free,
              unsigned long long lbocks, unsigned long id, bool direct_io);

  //----------------------------------------------------------------------------
  //! Check if node is in zombie state i.e. true if any of the helper threads
  //! was not properly started.
  //----------------------------------------------------------------------------
  inline bool
  IsZombie()
  {
    return mZombie;
  }

  //----------------------------------------------------------------------------
  //! Run boot thread for specified filesystem
  //!
  //! @param fs filesystem object
  //!
  //! @return true if boot thread started successfully, otherwise false
  //----------------------------------------------------------------------------
  bool RunBootThread(FileSystem* fs);

  //----------------------------------------------------------------------------
  //! Write file system label files (.eosid and .eosuuid) according to the
  //! configuration if they don't exist already.
  //!
  //! @param path mount point of the file system
  //! @param fsid file system id
  //! @param uuid file system uuid
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool FsLabel(std::string path, eos::common::FileSystem::fsid_t fsid,
               std::string uuid);

  //----------------------------------------------------------------------------
  //! Check that the label on the file system matches the one in the
  //! configuration.
  //!
  //! @param path mount point of the file system
  //! @param fsid file system id
  //! @param uuid file system uuid
  //! @param fail_noid when true fail if there is no .eosfsid file present
  //! @param fail_nouuid when true fail if there is no .eosfsuuid file present
  //!
  //! @return true if labels match, otherwise false
  //----------------------------------------------------------------------------
  bool CheckLabel(std::string path, eos::common::FileSystem::fsid_t fsid,
                  std::string uuid, bool fail_noid = false,
                  bool fail_nouuid = false);

  //----------------------------------------------------------------------------
  //! Balancer related methods
  //----------------------------------------------------------------------------
  XrdSysCondVar balanceJobNotification;

  void GetBalanceSlotVariables(unsigned long long& nparalleltx,
                               unsigned long long& ratex,
                               std::string configqueue);


  unsigned long long GetScheduledBalanceJobs(unsigned long long totalscheduled,
      unsigned long long& totalexecuted);

  unsigned long long WaitFreeBalanceSlot(unsigned long long& nparalleltx,
                                         unsigned long long& totalscheduled,
                                         unsigned long long& totalexecuted);

  bool GetFileSystemInBalanceMode(std::vector<unsigned int>& balancefsvector,
                                  unsigned int& cycler,
                                  unsigned long long nparalleltx,
                                  unsigned long long ratetx);

  bool GetBalanceJob(unsigned int index);

  //----------------------------------------------------------------------------
  //! Drain related methods
  //----------------------------------------------------------------------------
  XrdSysCondVar drainJobNotification;

  void GetDrainSlotVariables(unsigned long long& nparalleltx,
                             unsigned long long& ratex,
                             std::string configqueue);

  unsigned long long GetScheduledDrainJobs(unsigned long long totalscheduled,
      unsigned long long& totalexecuted);

  unsigned long long WaitFreeDrainSlot(unsigned long long& nparalleltx,
                                       unsigned long long& totalscheduled,
                                       unsigned long long& totalexecuted);

  bool GetFileSystemInDrainMode(std::vector<unsigned int>& drainfsvector,
                                unsigned int& cycler,
                                unsigned long long nparalleltx,
                                unsigned long long ratetx);

  bool GetDrainJob(unsigned int index);
};

EOSFSTNAMESPACE_END
#endif
