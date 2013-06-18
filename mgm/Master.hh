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

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN


class Master : public eos::common::LogId
{
  // -------------------------------------------------------------
  // ! steers the master/slave behaviour
  // -------------------------------------------------------------
private:
  XrdOucString fThisHost; // our hostname
  XrdOucString fMasterHost; // the currently configured master host
  XrdOucString fRemoteHost; // hostname(+port) of the remote mgm master/slave
  XrdOucString fRemoteMq; // hostname(+port) of the remote mq  master/slave
  XrdOucString fThisMq; // hostname(+port) of the local  mq  master/slave
  XrdOucString fMasterLog; // log output of master/slave interactions
  bool fActivated; // flag indicating if the activation worked
  bool fRemoteMasterOk; // indicates if the remote master is up
  bool fRemoteMasterRW; // indicates if the remote master is in RW = master mode or not
  bool fRemoteMqOk; // indicates if the remote mq is up
  pthread_t fThread; // thread id of the heart beat thread
  bool fCheckRemote; // indicate if we check the remote host status
  pthread_t fCompactingThread; // thread id of an oncline compacting thread
  double fCompactingRatio; // compacting ratio e.g. 4:1 => 4 times smaller after compaction

  int fDevNull; // /dev/null filedescriptor
  XrdSysLogger* fDevNullLogger; // /dev/null logger
  XrdSysError* fDevNullErr; // /dev/null error

  unsigned long long fFileNamespaceInode; // inode number of the file namespace file
  unsigned long long fDirNamespaceInode; // inode number of the dir  namespace file

  // --------------------------------------------
  // lock class wrapper used by the namespace
  // --------------------------------------------

  class RWLock : public eos::LockHandler
  {
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------

    RWLock ()
    {
      pLock = 0;
    }

    //--------------------------------------------------------------------------
    // Initializer
    //--------------------------------------------------------------------------

    void
    Init (eos::common::RWMutex* mutex)
    {
      pLock = mutex;
    }

    //------------------------------------------------------------------------
    // Destructor
    //------------------------------------------------------------------------

    virtual
    ~RWLock ()
    {
      pLock = 0;
    }

    //------------------------------------------------------------------------
    // Take a read lock
    //------------------------------------------------------------------------

    virtual void
    readLock ()
    {
      if (pLock)pLock->LockRead ();
    }

    //------------------------------------------------------------------------
    // Take a write lock
    //------------------------------------------------------------------------

    virtual void
    writeLock ()
    {
      if (pLock)pLock->LockWrite ();
    }

    //------------------------------------------------------------------------
    // Unlock
    //------------------------------------------------------------------------

    virtual void
    unLock ()
    {
      if (pLock)pLock->UnLockRead (); // it does not matter if UnLockRead or Write is called
    }
  private:
    eos::common::RWMutex* pLock;
  };

  RWLock fNsLock;

public:

  // transition types

  enum
  {
    kMasterToMaster = 0, kSlaveToMaster = 1, kMasterToMasterRO = 2, kMasterROToSlave = 3, kSecondarySlaveMasterFailover = 4
  };
  // running states

  enum
  {
    kIsNothing = 0, kIsRunningMaster = 1, kIsRunningSlave = 2, kIsReadOnlyMaster = 3, kIsSecondarySlave = 4, kIsTransition = 4
  };
  // compacting states

  enum
  {
    kIsNotCompacting = 0, kIsCompacting = 1, kIsCompactingBlocked = 2
  };

  int fRunningState;
  int fCompactingState;
  time_t fCompactingInterval;
  time_t fCompactingStart;
  XrdSysMutex fCompactingMutex;

  //------------------------------------------------------------------------
  // Constructor
  //------------------------------------------------------------------------
  Master ();

  //------------------------------------------------------------------------
  // Destructor
  //------------------------------------------------------------------------
  ~Master ();

  //------------------------------------------------------------------------
  // Supervisor Thread Start Function
  //------------------------------------------------------------------------
  static void* StaticSupervisor (void*);

  //------------------------------------------------------------------------
  // Online Compacting Thread Start Function
  //------------------------------------------------------------------------
  static void* StaticOnlineCompacting (void*);

  //------------------------------------------------------------------------
  // Supervisor Thread Function
  //------------------------------------------------------------------------
  void* Supervisor ();

  //------------------------------------------------------------------------
  // Compacting Thread Function
  //------------------------------------------------------------------------
  void* Compacting ();

  //------------------------------------------------------------------------
  // Wait for a compacting round to finish
  //------------------------------------------------------------------------
  void WaitCompactingFinished ();

  //------------------------------------------------------------------------
  // Un/Block compacting
  //------------------------------------------------------------------------
  void BlockCompacting ();
  void UnBlockCompacting ();

  //------------------------------------------------------------------------
  // Show the current compacting state
  //------------------------------------------------------------------------
  void PrintOutCompacting (XrdOucString &out);

  //------------------------------------------------------------------------
  // Check if we are currently running compacting
  //------------------------------------------------------------------------

  bool IsCompacting ();

  //------------------------------------------------------------------------
  // Check if we are currently blocking the compacting
  //------------------------------------------------------------------------

  bool IsCompactingBlocked ();

  //------------------------------------------------------------------------
  // Enable Remote Check
  //------------------------------------------------------------------------
  bool EnableRemoteCheck ();

  //------------------------------------------------------------------------
  // Disable Remote Check
  //------------------------------------------------------------------------
  bool DisableRemoteCheck ();

  //------------------------------------------------------------------------
  // ScheduleOnlineCompacting
  //------------------------------------------------------------------------
  bool ScheduleOnlineCompacting (time_t starttime, time_t repetitioninterval);

  //------------------------------------------------------------------------
  // Create a status file = touch
  //------------------------------------------------------------------------
  bool CreateStatusFile (const char* path);

  //------------------------------------------------------------------------
  // Remote a status file = rm
  //------------------------------------------------------------------------
  bool RemoveStatusFile (const char* path);

  //------------------------------------------------------------------------
  // Boot Namespace according to master slave configuration
  //------------------------------------------------------------------------
  bool BootNamespace ();

  //------------------------------------------------------------------------
  // Init Method to determine the current master/slave state
  //------------------------------------------------------------------------
  bool Init ();

  //------------------------------------------------------------------------
  // Apply Configuration settings to the master class
  //------------------------------------------------------------------------
  bool ApplyMasterConfig (XrdOucString &stdOut, XrdOucString &stdErr, int transitiontype);

  //------------------------------------------------------------------------
  // Activate the current master/slave settings = configure configuration directory and (re-)load the appropriate configuratio
  //------------------------------------------------------------------------
  int Activate (XrdOucString &stdOut, XrdOucString &stdErr, int transitiontype);

  //------------------------------------------------------------------------
  // Set the master host 
  //------------------------------------------------------------------------
  bool Set (XrdOucString &mastername, XrdOucString &stdout, XrdOucString &stdErr); // set's the new master hostname

  //------------------------------------------------------------------------
  // Do a slave=>master transition
  //------------------------------------------------------------------------
  bool Slave2Master ();

  //------------------------------------------------------------------------
  // Do a master=>master(ro) transition
  //------------------------------------------------------------------------
  bool Master2MasterRO ();

  //------------------------------------------------------------------------
  // Do a master(ro)=>slave transition = reloac the namspace on a slave from the follower file
  //------------------------------------------------------------------------
  bool MasterRO2Slave ();

  //------------------------------------------------------------------------
  // Show the current master/slave run configuration (used by ns stat)
  //------------------------------------------------------------------------
  void PrintOut (XrdOucString &out);

  //------------------------------------------------------------------------
  // Return master host
  //------------------------------------------------------------------------

  const char*
  GetMasterHost ()
  {
    return (fMasterHost.c_str ()) ? fMasterHost.c_str () : "<none>";
  }

  //------------------------------------------------------------------------
  // Check if we are the master host
  //------------------------------------------------------------------------

  bool
  IsMaster ()
  {
    return (fThisHost == fMasterHost);
  }

  //------------------------------------------------------------------------
  // Check if the remote is the master host
  //------------------------------------------------------------------------

  bool
  IsRemoteMaster ()
  {
    return fRemoteMasterRW;
  }

  //------------------------------------------------------------------------
  // Check if a remote service is reachable on a given port with timeout
  //------------------------------------------------------------------------
  bool HostCheck (const char* hostname, int port = 1094, int timeout = 5);

  //------------------------------------------------------------------------
  // Reset Master log
  //------------------------------------------------------------------------

  void
  ResetLog ()
  {
    fMasterLog = "";
  }

  //------------------------------------------------------------------------
  // Get Master Log
  //------------------------------------------------------------------------

  void
  GetLog (XrdOucString &stdOut)
  {
    stdOut = fMasterLog;
  }

  //------------------------------------------------------------------------
  // Add to Master Log
  //------------------------------------------------------------------------

  void
  MasterLog (const char* log)
  {
    if (log) fMasterLog += log;
    fMasterLog += "\n";
  }

  //------------------------------------------------------------------------
  // Signal the remote master to bounce all requests to us (issued by master)
  //------------------------------------------------------------------------
  void SignalRemoteBounceToMaster ();

  //------------------------------------------------------------------------
  // Signal the remote master to reload its namespace (issued by master)
  //------------------------------------------------------------------------
  void SignalRemoteReload ();

  //------------------------------------------------------------------------
  // Wait that local/remote namespace files are synced (called by slave)
  //------------------------------------------------------------------------
  bool WaitNamespaceFilesInSync (unsigned int timeout = 600);

  //------------------------------------------------------------------------
  // Store the file inodes of the namespace file to see when a file has 
  // resynced after compactification
  // -----------------------------------------------------------------------
  void TagNamespaceInodes ();

  // -----------------------------------------------------------------------
  // Set a redirect to the remote master for everything
  // -----------------------------------------------------------------------
  void RedirectToRemoteMaster ();

  // -----------------------------------------------------------------------
  // Reboot a slave namespace
  // -----------------------------------------------------------------------
  bool RebootSlaveNamespace ();
};

EOSMGMNAMESPACE_END

#endif
