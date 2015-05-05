// ----------------------------------------------------------------------
// File: Iostat.hh
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

/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "namespace/utils/Locking.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Master : public eos::common::LogId
{
 public:

  //----------------------------------------------------------------------------
  //! Transition types
  //----------------------------------------------------------------------------
  struct Transition
  {
    enum Type
    {
      kMasterToMaster               = 0,
      kSlaveToMaster                = 1,
      kMasterToMasterRO             = 2,
      kMasterROToSlave              = 3,
      kSecondarySlaveMasterFailover = 4
    };
  };

  //----------------------------------------------------------------------------
  //! Running states
  //----------------------------------------------------------------------------
  struct Run
  {
    enum State
    {
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
  struct Compact
  {
    enum State
    {
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
  //! Supervisor Thread Start Function
  //----------------------------------------------------------------------------
  static void* StaticSupervisor(void*);

  //----------------------------------------------------------------------------
  //! Online compacting thread start function
  //----------------------------------------------------------------------------
  static void* StaticOnlineCompacting(void*);

  //----------------------------------------------------------------------------
  //! Supervisor thread function
  //----------------------------------------------------------------------------
  void* Supervisor();

  //----------------------------------------------------------------------------
  //! Show the current compacting state
  //----------------------------------------------------------------------------
  void PrintOutCompacting(XrdOucString& out);

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
  //! Configure Online Compating Type for files and/or directories
  //----------------------------------------------------------------------------
  void  SetCompactingType(bool f, bool d)
  {
    fCompactFiles = f;
    fCompactDirectories = d;
  }

  //----------------------------------------------------------------------------
  //! Boot Namespace according to master slave configuration
  //----------------------------------------------------------------------------
  bool BootNamespace();

  //----------------------------------------------------------------------------
  //! Init method to determine the current master/slave state
  //----------------------------------------------------------------------------
  bool Init();

  //----------------------------------------------------------------------------
  //! Apply Configuration settings to the master class
  //----------------------------------------------------------------------------
  bool ApplyMasterConfig(XrdOucString& stdOut,
                         XrdOucString& stdErr,
                         Transition::Type transitiontype);

  //----------------------------------------------------------------------------
  //! Activate the current master/slave settings = configure configuration
  //! directory and (re-)load the appropriate configuratio
  //----------------------------------------------------------------------------
  int Activate(XrdOucString& stdOut, XrdOucString& stdErr, int transitiontype);

  //----------------------------------------------------------------------------
  //! Set the new master host
  //----------------------------------------------------------------------------
  bool Set(XrdOucString& mastername, XrdOucString& stdout,
           XrdOucString& stdErr);

  //----------------------------------------------------------------------------
  //! Show the current master/slave run configuration (used by ns stat)
  //----------------------------------------------------------------------------
  void PrintOut(XrdOucString& out);

  //----------------------------------------------------------------------------
  //! Return master host
  //----------------------------------------------------------------------------
  const char*
  GetMasterHost()
  {
    return (fMasterHost.c_str()) ? fMasterHost.c_str() : "<none>";
  }

  //----------------------------------------------------------------------------
  //! Check if we are the master host
  //----------------------------------------------------------------------------
  bool
  IsMaster()
  {
    return (fThisHost == fMasterHost);
  }

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

    if (now > (f2MasterTransitionTime + 3600))
      delay = 0;
    else
      delay = 3600 - (now - f2MasterTransitionTime);

    return delay;
  }

  //----------------------------------------------------------------------------
  //! Reset master log
  //----------------------------------------------------------------------------
  void
  ResetLog()
  {
    fMasterLog = "";
  }

  //----------------------------------------------------------------------------
  //! Get master Log
  //----------------------------------------------------------------------------
  void
  GetLog(XrdOucString& stdOut)
  {
    stdOut = fMasterLog;
  }

  //----------------------------------------------------------------------------
  //! Add to master Log
  //----------------------------------------------------------------------------
  void
  MasterLog(const char* log)
  {
    if (log && strlen(log))
    {
      fMasterLog += log;
      fMasterLog += "\n";
    }
  }

  //----------------------------------------------------------------------------
  //! Wait that local/remote namespace files are synced (called by slave)
  //----------------------------------------------------------------------------
  bool WaitNamespaceFilesInSync(unsigned int timeout = 600);

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
  XrdOucString fMasterLog; ///< log output of master/slave interactions
  //! flag indicating if the remote master is in RW = master mode or not
  bool fRemoteMasterRW;
  bool fRemoteMqOk; ///< flag indicates if the remote mq is up
  bool fCheckRemote; ///< indicate if we check the remote host status
  bool fActivated; ///< flag indicating if the activation worked
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
  unsigned long long fFileNamespaceInode; ///< inode number of the file namespace file
  unsigned long long fDirNamespaceInode; ///< inode number of the dir  namespace file

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
      if (pLock)pLock->LockRead();
    }

    //------------------------------------------------------------------------
    // Take a write lock
    //------------------------------------------------------------------------
    virtual void
    writeLock()
    {
      if (pLock)pLock->LockWrite();
    }

    //------------------------------------------------------------------------
    // Unlock
    //------------------------------------------------------------------------
    virtual void
    unLock()
    {
      // Does not matter if UnLockRead or Write is called
      if (pLock)
        pLock->UnLockRead();
    }

   private:
    eos::common::RWMutex* pLock;
  };

  RWLock fNsLock;

  //----------------------------------------------------------------------------
  //! Signal the remote master to reload its namespace (issued by master)
  //----------------------------------------------------------------------------
  void SignalRemoteReload();

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
  //! Create a status file = touch
  //----------------------------------------------------------------------------
  bool CreateStatusFile(const char* path);

  //----------------------------------------------------------------------------
  //! Remote a status file = rm
  //----------------------------------------------------------------------------
  bool RemoveStatusFile(const char* path);

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
};

EOSMGMNAMESPACE_END

#endif // __EOSMGM_MASTER__HH__
