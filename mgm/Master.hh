// ----------------------------------------------------------------------
// File: Master.hh
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

#ifndef __EOSMGM_MASTER__HH__
#define __EOSMGM_MASTER__HH__

#include <sys/stat.h>
#include "mgm/Namespace.hh"
#include "mgm/IMaster.hh"
#include "namespace/utils/Locking.hh"
#include "XrdOuc/XrdOucString.hh"
#include <atomic>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class Master of the in-memory namespace
//------------------------------------------------------------------------------
class Master : public eos::mgm::IMaster
{
public:
  //----------------------------------------------------------------------------
  //! Running states
  //----------------------------------------------------------------------------
  struct Run {
    enum State {
      kIsNothing        = 0,
      kIsRunningMaster  = 1,
      kIsRunningSlave   = 2,
      kIsReadOnlyMaster = 3,
      kIsSecondarySlave = 4,
      kIsTransition     = 5
    };
  };

  //----------------------------------------------------------------------------
  //! Compact states
  //----------------------------------------------------------------------------
  struct Compact {
    enum State {
      kIsNotCompacting     = 0,
      kIsCompacting        = 1,
      kIsCompactingBlocked = 2
    };
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Master();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Master();

  //----------------------------------------------------------------------------
  //! Init method to determine the current master/slave state
  //----------------------------------------------------------------------------
  bool Init() override;

  //----------------------------------------------------------------------------
  //! Boot Namespace according to master slave configuration
  //----------------------------------------------------------------------------
  bool BootNamespace() override;

  //----------------------------------------------------------------------------
  //! Check if we are the master host
  //----------------------------------------------------------------------------
  bool IsMaster() override
  {
    return (fThisHost == fMasterHost);
  }

  //----------------------------------------------------------------------------
  //! Get if remove master is OK
  //!
  //! @return true if OK, otherwise false
  //----------------------------------------------------------------------------
  bool IsRemoteMasterOk() const override
  {
    return fRemoteMasterOk;
  }

  //----------------------------------------------------------------------------
  //! Return master host
  //----------------------------------------------------------------------------
  const std::string GetMasterId() const override
  {
    std::string master_id = "<none>";

    if (fMasterHost.c_str()) {
      master_id = fMasterHost.c_str();
      master_id += ":1094";
    }

    return master_id;
  }

  //----------------------------------------------------------------------------
  //! Set the new master hostname
  //!
  //! @param hostname new master hostname
  //! @param port new master port, default 1094
  //! @param err_msg error message
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetMasterId(const std::string& hostname, int port,
                   std::string& err_msg) override;

  //----------------------------------------------------------------------------
  //! Get master Log
  //----------------------------------------------------------------------------
  void GetLog(std::string& stdOut) override;

  //----------------------------------------------------------------------------
  //! Enable remote check
  //----------------------------------------------------------------------------
  bool EnableRemoteCheck();

  //----------------------------------------------------------------------------
  //! Disable remote check
  //----------------------------------------------------------------------------
  bool DisableRemoteCheck();

  //----------------------------------------------------------------------------
  //! Schedule onlinec ompacting
  //----------------------------------------------------------------------------
  bool ScheduleOnlineCompacting(time_t starttime, time_t repetitioninterval);

  //----------------------------------------------------------------------------
  //! Show the current compacting state
  //----------------------------------------------------------------------------
  void PrintOutCompacting(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Configure Online Compating Type for files and/or directories
  //!
  //! @param f mark file compacting
  //! @param d mark directory compacting
  //! @param r mark repair operation (not used)
  //----------------------------------------------------------------------------
  void SetCompactingType(bool f, bool d, bool r)
  {
    fCompactFiles = f;
    fCompactDirectories = d;
  }

  //----------------------------------------------------------------------------
  //! Start slave follower thread
  //!
  //! @param log_file changelog file path
  //----------------------------------------------------------------------------
  void StartSlaveFollower(std::string&& log_file);

  //----------------------------------------------------------------------------
  //! Shutdown slave follower thread
  //----------------------------------------------------------------------------
  void ShutdownSlaveFollower();

  //----------------------------------------------------------------------------
  //! Apply Configuration settings to the master class
  //----------------------------------------------------------------------------
  bool ApplyMasterConfig(std::string& stdOut, std::string& stdErr,
                         Transition::Type transitiontype);

  //----------------------------------------------------------------------------
  //! Show the current master/slave run configuration (used by ns stat)
  //!
  //! @return string describing the status
  //----------------------------------------------------------------------------
  std::string PrintOut() override;

  //----------------------------------------------------------------------------
  //! Return's a delay time for balancing & draining since after a transition
  //! we don't know the maps of already scheduled ID's and we have to make
  //! sure not to reissue a transfer too early!
  //----------------------------------------------------------------------------
  size_t GetServiceDelay()
  {
    time_t now = time(NULL);
    XrdSysMutexHelper lock(&f2MasterTransitionTimeMutex);
    time_t delay = 0;

    if (now > (f2MasterTransitionTime + 3600)) {
      delay = 0;
    } else {
      delay = 3600 - (now - f2MasterTransitionTime);
    }

    return delay;
  }

  //----------------------------------------------------------------------------
  //! Wait that local/remote namespace files are synced (called by slave)
  //----------------------------------------------------------------------------
  bool WaitNamespaceFilesInSync(bool wait_files, bool wait_directories,
                                unsigned int timeout = 900);

  //----------------------------------------------------------------------------
  //! Store the file inodes of the namespace file to see when a file has
  //! resynced after compactification
  //----------------------------------------------------------------------------
  void TagNamespaceInodes();

  //----------------------------------------------------------------------------
  //! Set a redirect to the remote master for everything
  //----------------------------------------------------------------------------
  void RedirectToRemoteMaster();

  //----------------------------------------------------------------------------
  //! Reboot a slave namespace
  //----------------------------------------------------------------------------
  bool RebootSlaveNamespace();

private:

  int fDevNull; ///< /dev/null filedescriptor
  Run::State fRunningState; ///< running state
  Compact::State fCompactingState; ///< compact state
  time_t fCompactingInterval; ///< compacting duration
  time_t fCompactingStart; ///< compacting start timestamp
  time_t f2MasterTransitionTime; ///< transition duration
  XrdSysMutex fCompactingMutex; ///< compacting mutex
  XrdSysMutex f2MasterTransitionTimeMutex; ///< transition time mutex
  XrdOucString fThisHost; ///< our hostname
  XrdOucString fMasterHost; ///< currently configured master host
  XrdOucString fRemoteHost; ///< hostname(+port) of the remote mgm master/slave
  XrdOucString fRemoteMq; ///< hostname(+port) of the remote mq  master/slave
  XrdOucString fThisMq; ///< hostname(+port) of the local  mq  master/slave
  //! flag indicating if the remote master is in RW = master mode or not
  bool fRemoteMasterRW;
  bool fRemoteMqOk; ///< flag indicates if the remote mq is up
  bool fCheckRemote; ///< indicate if we check the remote host status
  bool fRemoteMasterOk; ///< flag indicating if the remote master is up
  bool fCompactFiles; ///< compact the files changelog file if true
  bool fCompactDirectories; ///< compact the directories changelog file if true
  pthread_t fThread; ///< heartbeat thread id
  pthread_t fCompactingThread; ///< online compacting thread id
  //! compacting ratio for file changelog e.g. 4:1 => 4 times smaller after compaction
  double fCompactingRatio;
  //! compacting ratio for directory changelog e.g. 4:1 => 4 times smaller after compaction
  double fDirCompactingRatio;
  XrdSysLogger* fDevNullLogger; ///< /dev/null logger
  XrdSysError* fDevNullErr; ///< /dev/null error
  unsigned long long
  fFileNamespaceInode; ///< inode number of the file namespace file
  unsigned long long
  fDirNamespaceInode; ///< inode number of the dir  namespace file
  bool fHasSystemd; ///< machine has systemd (as opposed to sysv init)

  //----------------------------------------------------------------------------
  // Lock class wrapper used by the namespace
  //----------------------------------------------------------------------------
  class RWLock : public eos::LockHandler
  {
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    RWLock()
    {
      pLock = 0;
    }

    //--------------------------------------------------------------------------
    // Initializer
    //--------------------------------------------------------------------------
    void
    Init(eos::common::RWMutex* mutex)
    {
      pLock = mutex;
    }

    //------------------------------------------------------------------------
    // Destructor
    //------------------------------------------------------------------------
    virtual
    ~RWLock()
    {
      pLock = 0;
    }

    //------------------------------------------------------------------------
    // Take a read lock
    //------------------------------------------------------------------------

    virtual void
    readLock()
    {
      if (pLock) {
        pLock->LockRead();
      }
    }

    //------------------------------------------------------------------------
    // Take a write lock
    //------------------------------------------------------------------------
    virtual void
    writeLock()
    {
      if (pLock) {
        pLock->LockWrite();
      }
    }

    //------------------------------------------------------------------------
    // Unlock
    //------------------------------------------------------------------------
    virtual void
    unLock()
    {
      // Does not matter if UnLockRead or Write is called
      if (pLock) {
        pLock->UnLockRead();
      }
    }

  private:
    eos::common::RWMutex* pLock;
  };

  RWLock fNsLock;

  //----------------------------------------------------------------------------
  //! Activate the current master/slave settings = configure configuration
  //! directory and (re-)load the appropriate configuratio
  //----------------------------------------------------------------------------
  bool Activate(std::string& stdOut, std::string& stdErr, int transitiontype);

  //----------------------------------------------------------------------------
  //! Signal the remote master to reload its namespace (issued by master)
  //----------------------------------------------------------------------------
  void SignalRemoteReload(bool wait_files, bool wait_directories);

  //----------------------------------------------------------------------------
  //! Signal the remote master to bounce all requests to us (issued by master)
  //----------------------------------------------------------------------------
  void SignalRemoteBounceToMaster();

  //----------------------------------------------------------------------------
  //! Check if a remote service is reachable on a given port with timeout
  //----------------------------------------------------------------------------
  bool HostCheck(const char* hostname, int port = 1094, int timeout = 5);

  //----------------------------------------------------------------------------
  //! Do a slave=>master transition
  //----------------------------------------------------------------------------
  bool Slave2Master();

  //----------------------------------------------------------------------------
  //! Do a master=>master(ro) transition
  //----------------------------------------------------------------------------
  bool Master2MasterRO();

  //----------------------------------------------------------------------------
  //! Do a master(ro)=>slave transition = reloac the namspace on a slave from
  //! the follower file
  //----------------------------------------------------------------------------
  bool MasterRO2Slave();

  //----------------------------------------------------------------------------
  //! Check if we are currently running compacting
  //----------------------------------------------------------------------------
  bool IsCompacting();

  //----------------------------------------------------------------------------
  //! Check if we are currently blocking the compacting
  //----------------------------------------------------------------------------
  bool IsCompactingBlocked();

  //----------------------------------------------------------------------------
  //! Un/Block compacting
  //----------------------------------------------------------------------------
  void BlockCompacting();
  void UnBlockCompacting();

  //----------------------------------------------------------------------------
  //! Wait for a compacting round to finish
  //----------------------------------------------------------------------------
  void WaitCompactingFinished();

  //----------------------------------------------------------------------------
  //! Compacting thread function
  //----------------------------------------------------------------------------
  void* Compacting();

  //----------------------------------------------------------------------------
  //! Supervisor Thread Start Function
  //----------------------------------------------------------------------------
  static void* StaticSupervisor(void*);

  //----------------------------------------------------------------------------
  //! Supervisor thread function
  //----------------------------------------------------------------------------
  void* Supervisor();

  //----------------------------------------------------------------------------
  //! Online compacting thread start function
  //----------------------------------------------------------------------------
  static void* StaticOnlineCompacting(void*);
};

EOSMGMNAMESPACE_END

#endif // __EOSMGM_MASTER__HH__
