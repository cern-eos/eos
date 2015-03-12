// ----------------------------------------------------------------------
// File: Master.cc
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

/*----------------------------------------------------------------------------*/
#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "mgm/Access.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/Statfs.hh"

/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNet.hh"
#include "XrdNet/XrdNetPeer.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "mq/XrdMqClient.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

// -----------------------------------------------------------------------------
// Note: the defines after have to be in agreements with the defins in XrdMqOfs.cc
//       but we don't want to create a link in the code between the two
// -----------------------------------------------------------------------------
// existance indicates that this node is to be treated as a slave
#define EOSMGMMASTER_SUBSYS_RW_LOCKFILE "/var/eos/eos.mgm.rw"
// existance indicates that the local MQ should redirect to the remote MQ
#define EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE "/var/eos/eos.mq.remote.up"

EOSMGMNAMESPACE_BEGIN

/* -------------------------------------------------------------------------- */
Master::Master ()
{
  fActivated = false;
  fRemoteMasterOk = true;
  fRemoteMqOk = true;
  fRemoteMasterRW = false;
  fThread = 0;
  fRunningState = kIsNothing;
  fCompactingState = kIsNotCompacting;
  fCompactingThread = 0;
  fCompactingStart = 0;
  fCompactingInterval = 0;
  fCompactingRatio = 0;
  fCompactFiles = false;
  fCompactDirectories = false;
  fDevNull = 0;
  fDevNullLogger = 0;
  fDevNullErr = 0;
  fCheckRemote = true;
  fFileNamespaceInode = fDirNamespaceInode = 0;
  f2MasterTransitionTime = time(NULL) - 3600; // start without service delays
}

bool
Master::HostCheck (const char* hostname, int port, int timeout)
{
  // do a quick check if a host is reachable
  XrdOucString lHostName = hostname;

  int dpos;
  if ((dpos = lHostName.find(":")) != STR_NPOS)
  {
    lHostName.erase(dpos);
  }

  XrdNetPeer peer;
  XrdNet net(fDevNullErr);
  if (net.Connect(peer, lHostName.c_str(), port, 0, timeout))
  {
    // send a handshake to avoid handshake error messages on server side
    unsigned int vshake[5];
    vshake[0] = vshake[1] = vshake[2] = 0;
    vshake[3] = htonl(4);
    vshake[4] = htonl(2012);
    ssize_t nwrite = write(peer.fd, &vshake[0], 20);
    close(peer.fd);
    if (nwrite != 20)
      return false;
    return true;
  }
  else
  {
    return false;
  }
}

/* -------------------------------------------------------------------------- */
bool
Master::Init ()
{
  // define our role master/slave
  struct stat buf;
  fThisHost = gOFS->HostName;
  fNsLock.Init(&(gOFS->eosViewRWMutex)); // fill the namespace mutex

  if ((!getenv("EOS_MGM_MASTER1")) ||
      (!getenv("EOS_MGM_MASTER2")))
  {
    eos_crit("EOS_MGM_MASTER1 and EOS_MGM_MASTER2 variables are undefined");
    return false;
  }

  if (fThisHost == getenv("EOS_MGM_MASTER1"))
  {
    fRemoteHost = getenv("EOS_MGM_MASTER2");
  }
  else
  {
    fRemoteHost = getenv("EOS_MGM_MASTER1");
  }

  // ---------------------------------------------------------------
  //! start the online compacting background thread
  // ---------------------------------------------------------------
  XrdSysThread::Run(&fCompactingThread, Master::StaticOnlineCompacting, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Master OnlineCompacting Thread");


  if (fThisHost == fRemoteHost)
  {
    // no master slave configuration ... also fine
    fMasterHost = fThisHost;
    return true;
  }

  // open a /dev/null logger/error object
  fDevNull = open("/dev/null", 0);

  fDevNullLogger = new XrdSysLogger(fDevNull);
  fDevNullErr = new XrdSysError(fDevNullLogger);

  XrdOucString lMaster1MQ;
  XrdOucString lMaster2MQ;

  // define the MQ hosts
  if (getenv("EOS_MQ_MASTER1"))
  {
    lMaster1MQ = getenv("EOS_MQ_MASTER1");
  }
  else
  {
    lMaster1MQ = getenv("EOS_MGM_MASTER1");
    int dpos = lMaster1MQ.find(":");
    if (dpos != STR_NPOS)
    {
      lMaster1MQ.erase(dpos);
    }
    lMaster1MQ += ":1097";
  }

  if (getenv("EOS_MQ_MASTER2"))
  {
    lMaster2MQ = getenv("EOS_MQ_MASTER2");
  }
  else
  {
    lMaster2MQ = getenv("EOS_MGM_MASTER2");
    int dpos = lMaster2MQ.find(":");
    if (dpos != STR_NPOS)
    {
      lMaster2MQ.erase(dpos);
    }
    lMaster2MQ += ":1097";
  }

  // define which MQ is remote
  if (lMaster1MQ.find(fThisHost) != STR_NPOS)
  {
    fRemoteMq = lMaster2MQ;
  }
  else
  {
    fRemoteMq = lMaster1MQ;
  }

  if (!::stat(EOSMGMMASTER_SUBSYS_RW_LOCKFILE, &buf))
  {
    fMasterHost = fThisHost;
  }
  else
  {
    fMasterHost = fRemoteHost;
  }


  if (fThisHost != fRemoteHost)
  {
    fCheckRemote = true;
  }
  else
  {
    fCheckRemote = false;
  }

  // start the heartbeat thread anyway
  XrdSysThread::Run(&fThread, Master::StaticSupervisor, static_cast<void *> (this), XRDSYSTHREAD_HOLD, "Master Supervisor Thread");

  // get sync up if it is not up
  int rc = system("service eos status sync || service eos start sync");
  if (WEXITSTATUS(rc))
  {
    eos_crit("failed to start sync service");
    return false;
  }

  // get eossync up if it is not up
  rc = system("service eossync status || service eossync start ");
  if (WEXITSTATUS(rc))
  {
    eos_crit("failed to start eossync service");
    return false;
  }
  return true;
}

/* -------------------------------------------------------------------------- */
bool
Master::EnableRemoteCheck ()
{
  //----------------------------------------------------------------
  //! enable the heart beat thread to do remote checks
  //----------------------------------------------------------------
  if (!fCheckRemote)
  {
    MasterLog(eos_info("remotecheck=enabled"));
    fCheckRemote = true;
    return true;
  }

  return false;
}

/* -------------------------------------------------------------------------- */
bool
Master::DisableRemoteCheck ()
{
  //----------------------------------------------------------------
  //! stop's the heart beat thread to do remote checks
  //----------------------------------------------------------------

  if (fCheckRemote)
  {
    MasterLog(eos_info("remotecheck=disabled"));
    fCheckRemote = false;
    return true;
  }

  return false;
}

/* -------------------------------------------------------------------------- */
void*
Master::StaticSupervisor (void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling Supervisor
  //----------------------------------------------------------------
  return reinterpret_cast<Master*> (arg)->Supervisor();
}

/* -------------------------------------------------------------------------- */
void*
Master::Supervisor ()
{
  // ------------------------------------------------------------------------------------
  // This thread run's in an internal loop with 1Hz and checks
  // a) if enabled a potential remote master/slave for failover
  // b) the fill state of the local disk to avoid running out of disk space
  // It then configures redirection/stalling etc. corresponding to the present situation
  // ------------------------------------------------------------------------------------

  std::string remoteMgmUrlString = "root://";
  remoteMgmUrlString += fRemoteHost.c_str();
  std::string remoteMqUrlString = "root://";
  remoteMqUrlString += fRemoteMq.c_str();

  bool lDiskFull = false;
  bool pDiskFull = false;
  std::string pStallSetting = "";

  int dpos = remoteMqUrlString.find(":", 7);
  if (dpos != STR_NPOS)
  {
    remoteMqUrlString.erase(dpos + 1);
    remoteMqUrlString += "1097";
  }

  XrdCl::URL remoteMgmUrl(remoteMgmUrlString.c_str());
  XrdCl::URL remoteMqUrl(remoteMqUrlString.c_str());

  if (!remoteMgmUrl.IsValid())
  {
    MasterLog(eos_static_crit("remote manager URL <%s> is not valid", remoteMgmUrlString.c_str()));
    fRemoteMasterOk = false;
  }

  if (!remoteMqUrl.IsValid())
  {
    MasterLog(eos_static_crit("remote mq URL <%s> is not valid", remoteMqUrlString.c_str()));
    fRemoteMqOk = false;
  }

  XrdCl::FileSystem FsMgm(remoteMgmUrl);
  XrdCl::FileSystem FsMq(remoteMqUrl);

  while (1)
  {
    XrdSysThread::SetCancelOff();


    if (fCheckRemote)
    {
      // ----------------------------------------
      // check the remote machine for it's status
      // ----------------------------------------

      // - new XrdCl -
      bool remoteMgmUp = false;
      bool remoteMqUp = false;

      // ping the two guys with short timeouts e.g. MGM & MQ
      XrdCl::XRootDStatus mgmStatus = FsMgm.Ping(1);
      XrdCl::XRootDStatus mqStatus = FsMq.Ping(1);

      if (mgmStatus.IsOK())
      {
        remoteMgmUp = true;
      }
      else
      {
        remoteMgmUp = false;
      }

      if (mqStatus.IsOK())
      {
        remoteMqUp = true;
      }
      else
      {
        remoteMqUp = false;
      }

      if (remoteMqUp)
      {
        XrdCl::StatInfo* sinfo = 0;

        if (FsMq.Stat("/eos/", sinfo, 5).IsOK())
        {
          fRemoteMqOk = true;
          CreateStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
        }
        else
        {
          fRemoteMqOk = false;
          RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
        }
        if (sinfo)
        {
          delete sinfo;
          sinfo = 0;
        }
      }
      else
      {
        fRemoteMqOk = false;
        RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
      }

      if (remoteMgmUp)
      {
        XrdCl::StatInfo* sinfo = 0;

        if (FsMgm.Stat("/", sinfo, 5).IsOK())
        {
          if (gOFS->MgmProcMasterPath.c_str())
          {
            XrdCl::StatInfo* smasterinfo = 0;

            // check if this machine is running in master mode
            if (FsMgm.Stat(gOFS->MgmProcMasterPath.c_str(), smasterinfo, 5).IsOK())
            {
              fRemoteMasterRW = true;
            }
            else
            {
              fRemoteMasterRW = false;
            }
            if (smasterinfo)
            {
              delete smasterinfo;
              smasterinfo = 0;
            }
          }
          fRemoteMasterOk = true;
        }
        else
        {
          fRemoteMasterOk = false;
          fRemoteMasterRW = false;
        }
        if (sinfo)
        {
          delete sinfo;
          sinfo = 0;
        }
      }
      else
      {
        fRemoteMasterOk = false;
        fRemoteMasterRW = false;
      }

      if (!lDiskFull)
      {
        MasterLog(eos_static_debug("ismaster=%d remote-ok=%d remote-wr=%d thishost=%s remotehost=%s masterhost=%s ", IsMaster(), fRemoteMasterOk, fRemoteMasterRW, fThisHost.c_str(), fRemoteHost.c_str(), fMasterHost.c_str()));

        eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
        if (!IsMaster())
        {
          if (fRemoteMasterOk && fRemoteMasterRW)
          {
            // set the redirect for writes to the remote master
            Access::gRedirectionRules[std::string("w:*")] = fRemoteHost.c_str();
            // set the redirect for ENOENT to the remote master
            Access::gRedirectionRules[std::string("ENOENT:*")] = fRemoteHost.c_str();
            // remove the stall
            Access::gStallRules.erase(std::string("w:*"));
            Access::gStallWrite = false;
          }
          else
          {
            // remove the redirect for writes and put a stall for writes
            Access::gRedirectionRules.erase(std::string("w:*"));
            Access::gStallRules[std::string("w:*")] = "60";
            Access::gStallWrite = true;
            Access::gRedirectionRules.erase(std::string("ENOENT:*"));
          }
        }
        else
        {
          // check if we have two master-rw
          if (fRemoteMasterOk && fRemoteMasterRW && (fThisHost != fRemoteHost))
          {
            MasterLog(eos_crit("msg=\"dual RW master setup detected\""));
            Access::gStallRules[std::string("w:*")] = "60";
            Access::gStallWrite = true;
          }
          else
          {
            // remove any redirect or stall in this case
            if (Access::gRedirectionRules.count(std::string("w:*")))
            {
              Access::gRedirectionRules.erase(std::string("w:*"));
            }
            if (Access::gStallRules.count(std::string("w:*")))
            {
              Access::gStallRules.erase(std::string("w:*"));
              Access::gStallWrite = false;
            }
            if (Access::gRedirectionRules.count(std::string("ENOENT:*")))
            {
              Access::gRedirectionRules.erase(std::string("ENOENT:*"));
            }
          }
        }
      }
    }

    // -------------------------------------------------------------------------
    // check if the local filesystem has enough space on the namespace partition
    // -------------------------------------------------------------------------
    // try to statfs the filesystem holding the namespace
    eos::common::Statfs* statfs = eos::common::Statfs::DoStatfs(gOFS->MgmMetaLogDir.c_str());

    XrdOucString sizestring;

    if (!statfs)
    {
      MasterLog(eos_err("path=%s statfs=failed", gOFS->MgmMetaLogDir.c_str()));
      // uups ... statfs failed
      lDiskFull = true;
    }
    else
    {
      // we stop if we get to < 100 MB free
      if ((statfs->GetStatfs()->f_bfree * statfs->GetStatfs()->f_bsize) < (100 * 1024 * 1024))
      {
        lDiskFull = true;
      }
      else
      {
        lDiskFull = false;
      }
      eos::common::StringConversion::GetReadableSizeString(sizestring, (statfs->GetStatfs()->f_bfree * statfs->GetStatfs()->f_bsize), "B");
    }

    if (lDiskFull != pDiskFull)
    {
      // this is a state change and we have to configure the redirection settings
      if (lDiskFull)
      {
        // the disk is full, we stall every write
        eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
        pStallSetting = Access::gStallRules[std::string("w:*")];
        Access::gStallRules[std::string("w:*")] = "60";
        Access::gStallWrite = true;
        MasterLog(eos_warning("status=\"disk space warning - stalling\"  path=%s freebytes=%s", gOFS->MgmMetaLogDir.c_str(), sizestring.c_str()));
      }
      else
      {
        MasterLog(eos_notice("status=\"disk space ok - removed stall\" path=%s freebyte=%s", gOFS->MgmMetaLogDir.c_str(), sizestring.c_str()));
        if (pStallSetting.length())
        {
          // put back the original stall setting
          Access::gStallRules[std::string("w:*")] = pStallSetting;
          if (Access::gStallRules[std::string("w:*")].length())
          {
            Access::gStallWrite = true;
          }
          else
          {
            Access::gStallWrite = false;
          }
        }
        else
        {
          // remote the stall setting
          Access::gStallRules.erase(std::string("w:*"));
          Access::gStallWrite = false;
        }
        pStallSetting = "";
      }
      pDiskFull = lDiskFull;
    }

    XrdSysThread::SetCancelOn();

    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }
  return 0;
}

/* -------------------------------------------------------------------------- */
void*
Master::StaticOnlineCompacting (void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling Compacting
  //----------------------------------------------------------------
  return reinterpret_cast<Master*> (arg)->Compacting();
}

/* -------------------------------------------------------------------------- */
bool
Master::IsCompacting ()
{
  bool retc = false;
  {
    XrdSysMutexHelper cLock(fCompactingMutex);
    retc = (fCompactingState == kIsCompacting);
  }

  return retc;
}

/* -------------------------------------------------------------------------- */
bool
Master::IsCompactingBlocked ()
{
  bool retc = false;
  {
    XrdSysMutexHelper cLock(fCompactingMutex);
    retc = (fCompactingState == kIsCompactingBlocked);
  }

  return retc;
}

/* -------------------------------------------------------------------------- */
void
Master::BlockCompacting ()
{
  XrdSysMutexHelper cLock(fCompactingMutex);
  fCompactingState = kIsCompactingBlocked;
  eos_static_info("msg=\"block compacting\"");
}

/* -------------------------------------------------------------------------- */
void
Master::UnBlockCompacting ()
{
  WaitCompactingFinished();
  {
    XrdSysMutexHelper cLock(fCompactingMutex);
    fCompactingState = kIsNotCompacting;
    eos_static_info("msg=\"unblock compacting\"");
  }
}

/* -------------------------------------------------------------------------- */
void
Master::WaitCompactingFinished ()
{
  eos_static_info("msg=\"wait for compacting to finish\"");
  do
  {
    bool isCompacting = false;
    {
      XrdSysMutexHelper cLock(fCompactingMutex);
      isCompacting = (fCompactingState == kIsCompacting);
    }
    if (isCompacting)
    {
      XrdSysTimer sleeper;
      sleeper.Wait(1000);
    }
    else
    {
      // block any further compacting
      BlockCompacting();
      break;
    }
  }
  while (1);
  eos_static_info("msg=\"waited for compacting to finish OK\"");
}

/* -------------------------------------------------------------------------- */
bool
Master::ScheduleOnlineCompacting (time_t starttime, time_t repetitioninterval)
{
  MasterLog(eos_static_info("msg=\"scheduling online compacting\" starttime=%u interval=%u", starttime, repetitioninterval));
  fCompactingStart = starttime;
  fCompactingInterval = repetitioninterval;
  return true;
}

/* -------------------------------------------------------------------------- */
void*
Master::Compacting ()
{
  do
  {
    XrdSysThread::SetCancelOff();
    time_t now = time(NULL);
    bool runcompacting = false;
    bool reschedule = false;
    {
      XrdSysMutexHelper cLock(fCompactingMutex);
      runcompacting = ((fCompactingStart) && (now >= fCompactingStart) && IsMaster());
    }

    bool isBlocked = false;
    do
    {
      // --------------------------------------------------------
      // check if we are blocked
      // --------------------------------------------------------
      {
        XrdSysMutexHelper cLock(fCompactingMutex);
        isBlocked = (fCompactingState == kIsCompactingBlocked);
      }
      // --------------------------------------------------------
      // if we are blocked we wait until we are unblocked
      // --------------------------------------------------------
      if (isBlocked)
      {
        XrdSysTimer sleeper;
        sleeper.Wait(1000);
      }
      else
      {
        if (runcompacting)
        {
          // set to compacting
          fCompactingState = kIsCompacting;
        }
        break;
      }
    }
    while (isBlocked);

    bool go = false;
    do
    {
      // --------------------------------------------------------
      // wait that the namespace is booted
      // --------------------------------------------------------
      {
        {
          XrdSysMutexHelper(gOFS->InitializationMutex);
          if (gOFS->Initialized == gOFS->kBooted)
          {
            go = true;
          }
        }
        if (!go)
        {
          XrdSysTimer sleeper;
          sleeper.Wait(1000);
        }
      }
    }
    while (!go);


    if (runcompacting)
    {
      // --------------------------------------------------------
      // run the online compacting procedure
      // --------------------------------------------------------

      {
        XrdSysMutexHelper lock(gOFS->InitializationMutex);
        gOFS->Initialized = XrdMgmOfs::kCompacting;
      }
      eos_notice("msg=\"starting online compactificiation\"");

      time_t now = time(NULL);

      // file compacting
      std::string ocfile = gOFS->MgmNsFileChangeLogFile.c_str();
      ocfile += ".oc";
      char archiveFileLogName[4096];
      snprintf(archiveFileLogName, sizeof (archiveFileLogName) - 1, "%s.%lu", gOFS->MgmNsFileChangeLogFile.c_str(), now);

      std::string archivefile = archiveFileLogName;
      if (fCompactFiles)
	MasterLog(eos_info("archive(file)=%s oc=%s", archivefile.c_str(), ocfile.c_str()));

      // directory compacting
      std::string ocdir = gOFS->MgmNsDirChangeLogFile.c_str();
      ocdir += ".oc";
      char archiveDirLogName[4096];
      snprintf(archiveDirLogName, sizeof (archiveDirLogName) - 1, "%s.%lu", gOFS->MgmNsDirChangeLogFile.c_str(), now);

      std::string archivedirfile = archiveDirLogName;
      if (fCompactDirectories)
	MasterLog(eos_info("archive(dir)=%s oc=%s", archivedirfile.c_str(), ocdir.c_str()));

      int rc = 0;
      bool CompactFiles = fCompactFiles;
      bool CompactDirectories = fCompactDirectories;
      if (CompactFiles) 
      {
	// clean-up any old .oc file
	rc = unlink(ocfile.c_str());
	if (!WEXITSTATUS(rc))
	{
	  MasterLog(eos_info("oc=%s msg=\"old online compacting file(file) unlinked\""));
	}
      }

      if (CompactDirectories)
      {
	rc = unlink(ocdir.c_str());
	if (!WEXITSTATUS(rc))
	{
	  MasterLog(eos_info("oc=%s msg=\"old online compacting file(dir) unlinked\""));
	}
      }

      bool compacted = false;
      try
      {
        void *compData = 0;
        void *compDirData = 0;
        {
          MasterLog(eos_info("msg=\"compact prepare\""));
          // require NS read lock
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
	  if (CompactFiles)
	    compData = gOFS->eosFileService->compactPrepare(ocfile);
	  if (CompactDirectories)
	    compDirData = gOFS->eosDirectoryService->compactPrepare(ocdir);
        }
        {
          MasterLog(eos_info("msg=\"compacting\""));
          // require no NS lock
	  if (CompactFiles)
	    gOFS->eosFileService->compact(compData);
	  if (CompactDirectories)
	    gOFS->eosDirectoryService->compact(compDirData);
        }
        {
          // require NS write lock
          MasterLog(eos_info("msg=\"compact commit\""));
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
	  if (CompactFiles)
	    gOFS->eosFileService->compactCommit(compData);
	  if (CompactDirectories) 
	    gOFS->eosDirectoryService->compactCommit(compDirData);
        }

        {
          XrdSysMutexHelper cLock(fCompactingMutex);
          reschedule = (fCompactingInterval != 0);
        }

        if (reschedule)
        {
          eos_notice("msg=\"rescheduling online compactificiation\" interval=%u", (unsigned int) fCompactingInterval);
          XrdSysMutexHelper cLock(fCompactingMutex);
          fCompactingStart = time(NULL) + fCompactingInterval;
        }
        else
        {
          fCompactingStart = 0;
        }

        if (fRemoteMasterOk && (fThisHost != fRemoteHost))
        {
          // if we have a remote master we have to signal it to bounce to us
          SignalRemoteBounceToMaster();
        }

	if (CompactFiles) 
	{
	  // file compaction archiving
	  if (::rename(gOFS->MgmNsFileChangeLogFile.c_str(), archivefile.c_str()))
	  {
	    MasterLog(eos_crit("failed to rename %s=>%s errno=%d", gOFS->MgmNsFileChangeLogFile.c_str(), archivefile.c_str(), errno));
	  }
	  else
	  {
	    if (::rename(ocfile.c_str(), gOFS->MgmNsFileChangeLogFile.c_str()))
	    {
	      MasterLog(eos_crit("failed to rename %s=>%s errno=%d", ocfile.c_str(), gOFS->MgmNsFileChangeLogFile.c_str(), errno));
	    }
	    else
	    {
	      // stat the sizes and set the compacting factor
	      struct stat before_compacting;
	      struct stat after_compacting;
	      fCompactingRatio = 0.0;
	      
	      if ((!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &after_compacting)) && (!::stat(archivefile.c_str(), &before_compacting)))
	      {
		if (after_compacting.st_size)
		{
		  fCompactingRatio = 1.0 * before_compacting.st_size / after_compacting.st_size;
		}
	      }
	      compacted = true;
	    }
	  }
	}

	if (CompactDirectories)
	{
	  // dir compaction archiving
	  if (::rename(gOFS->MgmNsDirChangeLogFile.c_str(), archivedirfile.c_str()))
	  {
	    MasterLog(eos_crit("failed to rename %s=>%s errno=%d", gOFS->MgmNsDirChangeLogFile.c_str(), archivedirfile.c_str(), errno));
	  }
	  else
	  {
	    if (::rename(ocdir.c_str(), gOFS->MgmNsDirChangeLogFile.c_str()))
	    {
	      MasterLog(eos_crit("failed to rename %s=>%s errno=%d", ocdir.c_str(), gOFS->MgmNsDirChangeLogFile.c_str(), errno));
	    }
	    else
	      {
		// stat the sizes and set the compacting factor
		struct stat before_compacting;
		struct stat after_compacting;
		fDirCompactingRatio = 0.0;
		
		if ((!::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &after_compacting)) && (!::stat(archivedirfile.c_str(), &before_compacting)))
		{
		  if (after_compacting.st_size)
		    {
		      fDirCompactingRatio = 1.0 * before_compacting.st_size / after_compacting.st_size;
		    }
		}
		compacted = true;
	      }
	  }
	}
      }
      catch (eos::MDException &e)
      {
	errno = e.getErrno();
	MasterLog(eos_crit("online-compacting returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
      }

      XrdSysTimer sleeper;
      sleeper.Wait(1000);

      if (compacted)
      {
        MasterLog(eos_info("msg=\"compact done\" elapsed=%lu", time(NULL) - now));

        if (fRemoteMasterOk && (fThisHost != fRemoteHost))
        {
          // if we have a remote master we have to signal it to bounce to us
          SignalRemoteReload();
        }

        // ----------------------------------------------------------------------
        // re-configure the changelog path from the .oc to the original filenames
        // - if we don't do that we cannot do a transition to RO-master state
        // ----------------------------------------------------------------------
        std::map<std::string, std::string> fileSettings;
        std::map<std::string, std::string> contSettings;

        contSettings["changelog_path"] = gOFS->MgmNsDirChangeLogFile.c_str();
        fileSettings["changelog_path"] = gOFS->MgmNsFileChangeLogFile.c_str();
        if (!IsMaster())
        {
          contSettings["slave_mode"] = "true";
          contSettings["poll_interval_us"] = "1000";
          fileSettings["slave_mode"] = "true";
          fileSettings["poll_interval_us"] = "1000";
        }

        try
        {
          gOFS->eosFileService->configure(fileSettings);
          gOFS->eosDirectoryService->configure(contSettings);
        }
        catch (eos::MDException &e)
        {
          errno = e.getErrno();
          MasterLog(eos_crit("reconfiguration returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
          exit(-1);
        }
      }
      else
      {
        MasterLog(eos_crit("failed online compactification"));
        exit(-1);
      }

      {
        // set back from kCompacting to kBooted
        XrdSysMutexHelper lock(gOFS->InitializationMutex);
        gOFS->Initialized = XrdMgmOfs::kBooted;
      }

      {
        // set to not compacting
        XrdSysMutexHelper cLock(fCompactingMutex);
        fCompactingState = kIsNotCompacting;
      }

    }

    // --------------------------------------------------------
    // check only once a minute
    // --------------------------------------------------------
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(60000);
  }
  while (1);
  return 0;
}

/* -------------------------------------------------------------------------- */
void
Master::PrintOutCompacting (XrdOucString &out)
{
  time_t now = time(NULL);
  if (IsCompacting())
  {
    out += "status=compacting";
    out += " waitstart=0";
  }
  else
  {
    if (IsCompactingBlocked())
    {
      out += "status=blocked";
      out += " waitstart=0";
    }
    else
    {
      if (fCompactingStart && IsMaster())
      {
        time_t nextrun = (fCompactingStart > now) ? (fCompactingStart - now) : 0;
        if (nextrun)
        {
          out += "status=wait";
          out += " waitstart=";
          out += (int) nextrun;
        }
        else
        {
          out += "status=starting";
          out += " waitstart=0";
        }
      }
      else
      {
        out += "status=off";
        out += " waitstart=0";
      }
    }
    out += " interval=";
    out += (int) fCompactingInterval;
  }
  char cfratio[256];
  snprintf(cfratio, sizeof (cfratio) - 1, "%.01f", fCompactingRatio);
  out += " ratio-file=";
  out += cfratio;
  out += ":1";

  snprintf(cfratio, sizeof (cfratio) - 1, "%.01f", fDirCompactingRatio);
  out += " ratio-dir=";
  out += cfratio;
  out += ":1";
}

/* -------------------------------------------------------------------------- */
void
Master::PrintOut (XrdOucString &out)
{
  if (fThisHost == fMasterHost)
  {
    out += "mode=master-rw";
  }
  else
  {
    out += "mode=slave-ro";
  }

  switch (fRunningState)
  {
    case kIsNothing:
      out += " state=invalid";
      break;
    case kIsRunningMaster:
      out += " state=master-rw";
      break;
    case kIsRunningSlave:
      out += " state=slave-ro";
      break;
    case kIsReadOnlyMaster:
      out += " state=master-ro";
      break;
  }

  out += " master=";
  out += fMasterHost;
  out += " configdir=";
  out += gOFS->MgmConfigDir.c_str();
  out += " config=";
  out += gOFS->MgmConfigAutoLoad.c_str();
  if (fActivated)
  {
    out += " active=true";
  }
  else
  {
    out += " active=false";
  }

  if (fThisHost != fRemoteHost)
  {
    // print only if we have a master slave configuration
    if (fRemoteMasterOk)
    {
      out += " mgm:";
      out += fRemoteHost;
      out += "=ok";
      if (fRemoteMasterRW)
      {
        out += " mgm:mode=master-rw";
      }
      else
      {
        out += " mgm:mode=slave-ro";
      }
    }
    else
    {
      out += " mgm:";
      out += fRemoteHost;
      out += "=down";
    }
    if (fRemoteMqOk)
    {
      out += " mq:";
      out += fRemoteMq;
      out += "=ok";
    }
    else
    {
      out += " mq:";
      out += fRemoteMq;
      out += "=down";
    }
  }
}

/* -------------------------------------------------------------------------- */
bool
Master::ApplyMasterConfig (XrdOucString &stdOut, XrdOucString &stdErr, int transitiontype)
{
  std::string enabled = "";

  if (fThisHost == fMasterHost)
  {
    gOFS->ObjectManager.EnableBroadCast(true); // we are the master and we broadcast every configuration change
    if (!CreateStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE))
      return false;
  }
  else
  {
    gOFS->ObjectManager.EnableBroadCast(false); // we are the slave and we just listen and don't broad cast anythiing
    if (!RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE))
      return false;
  }
  return Activate(stdOut, stdErr, transitiontype);
}

/* -------------------------------------------------------------------------- */
int
Master::Activate (XrdOucString &stdOut, XrdOucString &stdErr, int transitiontype)
{
  fActivated = false;
  // -----------------------------------------------------------------------
  // change the configuration directory
  // -----------------------------------------------------------------------
  if (fMasterHost == fThisHost)
  {
    gOFS->MgmConfigDir.replace(fRemoteHost, fThisHost);
    stdOut += "configdir=";
    stdOut += gOFS->MgmConfigDir.c_str();
    stdOut += " activating master=";
    stdOut += fThisHost;
    stdOut += "\n";
  }
  else
  {
    gOFS->MgmConfigDir.replace(fThisHost, fRemoteHost);
    stdOut += "configdir=";
    stdOut += gOFS->MgmConfigDir.c_str();
    stdOut += " activating master=";
    stdOut += fRemoteHost;
    stdOut += "\n";
  }

  gOFS->ConfEngine->SetConfigDir(gOFS->MgmConfigDir.c_str());

  if (transitiontype != kSlaveToMaster)
  {
    // -----------------------------------------------------------------------
    // load the master's default configuration if this is not a transition
    // -----------------------------------------------------------------------
    if ((transitiontype != kMasterToMasterRO) && (transitiontype != kMasterROToSlave))
    {
      if (gOFS->MgmConfigAutoLoad.length())
      {
        MasterLog(eos_static_info("autoload config=%s", gOFS->MgmConfigAutoLoad.c_str()));
        XrdOucString configloader = "mgm.config.file=";
        configloader += gOFS->MgmConfigAutoLoad;
        XrdOucEnv configenv(configloader.c_str());
        XrdOucString stdErr = "";
        if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr))
        {
          MasterLog(eos_static_crit("Unable to auto-load config %s - fix your configuration file!", gOFS->MgmConfigAutoLoad.c_str()));
          MasterLog(eos_static_crit("%s", stdErr.c_str()));
          return false;
        }
        else
        {
          MasterLog(eos_static_info("Successful auto-load config %s", gOFS->MgmConfigAutoLoad.c_str()));
        }
      }
    }

    // -----------------------------------------------------------------------
    // invoke a master to ro-master transition
    // -----------------------------------------------------------------------

    if (transitiontype == kMasterToMasterRO)
    {
      MasterLog(eos_static_notice("Doing Master=>Master-RO transition"));
      if (!Master2MasterRO())
      {
        return false;
      }
    }

    // -----------------------------------------------------------------------
    // invoke a ro-master to slave transition
    // -----------------------------------------------------------------------

    if (transitiontype == kMasterROToSlave)
    {
      MasterLog(eos_static_notice("Doing Master-RO=>Slave transition"));
      if (!MasterRO2Slave())
      {
        return false;
      }
    }

  }
  else
  {
    // -----------------------------------------------------------------------
    // store the current configuration to the default location
    // -----------------------------------------------------------------------
    if (!gOFS->ConfEngine->AutoSave())
    {
      return false;
    }
    // -----------------------------------------------------------------------
    // invoke a slave to master transition
    // -----------------------------------------------------------------------
    MasterLog(eos_static_notice("Doing Slave=>Master transition"));
    if (!Slave2Master())
    {
      return false;
    }
  }

  fActivated = true;
  return true;
}

/* -------------------------------------------------------------------------- */
bool
Master::Set (XrdOucString &mastername, XrdOucString &stdOut, XrdOucString &stdErr)
{
  int transitiontype = 0;

  if (fRunningState == kIsNothing)
  {
    MasterLog(eos_static_err("unable to change master/slave configuration - node is in invalid state after a failed transition"));
    stdErr += "error: unable to change master/slave configuration - node is in invalid state after a failed transition";
    return false;
  }

  if ((mastername != getenv("EOS_MGM_MASTER1")) &&
      (mastername != getenv("EOS_MGM_MASTER2")))
  {
    stdErr += "error: invalid master name specified (/etc/sysconfig/eos:EOS_MGM_MASTER1,EOS_MGM_MASTER2)\n";
    return false;
  }

  if ((fMasterHost == fThisHost))
  {
    if ((mastername != fThisHost))
    {
      if (fRunningState == kIsRunningMaster)
      {
        transitiontype = kMasterToMasterRO;
      }
      else
      {
        MasterLog(eos_static_err("invalid master/slave transition requested - we are not a running master"));
        stdErr += "invalid master/slave transition requested - we are not a running master\n";
        return false;
      }
    }
    else
    {
      transitiontype = kMasterToMaster;
      MasterLog(eos_static_err("invalid master/master transition requested - we are  a running master"));
      stdErr += "invalid master/master transition requested - we are a running master\n";
      return false;
    }
  }
  else
  {
    if (fRunningState == kIsReadOnlyMaster)
    {
      transitiontype = kMasterROToSlave;
    }
    else
    {
      if (fRunningState != kIsRunningSlave)
      {
        MasterLog(eos_static_err("invalid master/slave transition requested - we are not a running ro-master or we are already a slave"));
        stdErr += "invalid master/slave transition requested - we are not a running ro-master or we are already a slave\n";
        return false;
      }
    }
  }

  if (mastername == fThisHost)
  {
    // check if the remote machine is running as the master
    if (fRemoteMasterRW)
    {
      stdErr += "error: the remote machine <";
      stdErr += fRemoteHost;
      stdErr += "> is still running as a RW master\n";
      return false;
    }
    if (fMasterHost.length() && (fMasterHost != fThisHost))
    {
      // slave to master transition
      transitiontype = kSlaveToMaster;
    }
  }

  XrdOucString lOldMaster = fMasterHost;
  fMasterHost = mastername;

  bool arc = ApplyMasterConfig(stdOut, stdErr, transitiontype);

  // set back to the previous master
  if (!arc)
  {
    fMasterHost = lOldMaster;
    // put back the old MGM configuration status file
    if (fThisHost == fMasterHost)
    {
      gOFS->ObjectManager.EnableBroadCast(true); // we are the master and we broadcast every configuration change
      if (!CreateStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE))
        return false;
    }
    else
    {
      gOFS->ObjectManager.EnableBroadCast(false); // we are the slave and we just listen and don't broad cast anythiing
      if (!RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE))
        return false;
    }
  }
  return arc;
}

bool
Master::Slave2Master ()

{
  fRunningState = kIsTransition;
  // this will block draining/balancing for the next hour!!!
  f2MasterTransitionTime = time(NULL);

  // -----------------------------------------------------------
  // This call transforms the namespace following slave into
  // a master in RW mode
  // -----------------------------------------------------------
  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;

  std::string rfclf;
  std::string rdclf;

  contSettings["changelog_path"] = gOFS->MgmMetaLogDir.c_str();
  fileSettings["changelog_path"] = gOFS->MgmMetaLogDir.c_str();
  contSettings["changelog_path"] += "/directories.";
  fileSettings["changelog_path"] += "/files.";
  rfclf = fileSettings["changelog_path"];
  rdclf = contSettings["changelog_path"];
  contSettings["changelog_path"] += fMasterHost.c_str();
  fileSettings["changelog_path"] += fMasterHost.c_str();
  rfclf += fRemoteHost.c_str();
  rdclf += fRemoteHost.c_str();
  contSettings["changelog_path"] += ".mdlog";
  fileSettings["changelog_path"] += ".mdlog";
  rfclf += ".mdlog";
  rdclf += ".mdlog";

  // -----------------------------------------------------------
  // convert the follower namespace into a read-write namespace
  // -----------------------------------------------------------

  // -----------------------------------------------------------
  // take the sync service down
  // -----------------------------------------------------------
  int rc = system("service eos status sync && service eos stop sync");
  if (WEXITSTATUS(rc))
  {
    if (WEXITSTATUS(rc) == 2)
    {
      MasterLog(eos_warning("sync service was already stopped"));
    }
    if (WEXITSTATUS(rc) == 1)
    {
      MasterLog(eos_warning("sync service was dead"));
    }
    MasterLog(eos_crit("slave=>master transition aborted since sync was down"));
    fRunningState = kIsNothing;
    rc = system("service eos start sync");
    if (WEXITSTATUS(rc))
    {
      MasterLog(eos_warning("failed to start sync service"));
    }
    fRunningState = kIsRunningSlave;
    return false;
  }

  // -----------------------------------------------------------
  // if possible evaluate if local and remote master files are
  // in sync ...
  // -----------------------------------------------------------

  off_t size_local_file_changelog = 0;
  off_t size_local_dir_changelog = 0;
  off_t size_remote_file_changelog = 0;
  off_t size_remote_dir_changelog = 0;

  struct stat buf;

  std::string remoteSyncUrlString = "root://";
  remoteSyncUrlString += fRemoteHost.c_str();
  remoteSyncUrlString += ":1096";
  remoteSyncUrlString += "//dummy";
  std::string remoteSyncHostPort = fRemoteHost.c_str();
  remoteSyncHostPort += ":1096";

  if (!stat(gOFS->MgmNsFileChangeLogFile.c_str(), &buf))
  {
    size_local_file_changelog = buf.st_size;
  }
  else
  {
    MasterLog(eos_crit("slave=>master transition aborted since we cannot stat our own slave file-changelog-file"));
    fRunningState = kIsRunningSlave;
    return false;
  }

  if (!stat(gOFS->MgmNsDirChangeLogFile.c_str(), &buf))
  {
    size_local_dir_changelog = buf.st_size;
  }
  else
  {
    MasterLog(eos_crit("slave=>master transition aborted since we cannot stat our own slave dir-changelog-file"));
    fRunningState = kIsRunningSlave;
    return false;
  }

  bool syncok = false;

  if (HostCheck(fRemoteHost.c_str(), 1096))
  {
    MasterLog(eos_info("remote-sync host=%s:1096 is reachable", fRemoteHost.c_str()));
    syncok = true;
  }
  else
  {
    MasterLog(eos_info("remote-sync host=%s:1096 is down", fRemoteHost.c_str()));
  }

  if (syncok)
  {
    // ---------------------------------------
    // TODO: adapt for the new client      ---
    // ---------------------------------------

    XrdCl::URL remoteSyncUrl(remoteSyncUrlString.c_str());

    XrdCl::FileSystem FsSync(remoteSyncUrl);
    XrdCl::StatInfo* sinfo = 0;

    // stat the two remote changelog files
    if (FsSync.Stat(rfclf.c_str(), sinfo, 5).IsOK())
    {
      size_remote_file_changelog = sinfo->GetSize();
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
    }
    else
    {
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
    }

    if (FsSync.Stat(rdclf.c_str(), sinfo, 5).IsOK())
    {
      size_remote_dir_changelog = sinfo->GetSize();
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
    }
    else
    {
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
    }

    if (size_remote_file_changelog != size_local_file_changelog)
    {
      MasterLog(eos_crit("slave=>master transition aborted - file changelog synchronization problem found - path=%s remote-size=%llu local-size=%llu", rfclf.c_str(), size_remote_file_changelog, size_local_file_changelog));
      fRunningState = kIsRunningSlave;
      return false;
    }

    if (size_remote_dir_changelog != size_local_dir_changelog)
    {
      MasterLog(eos_crit("slave=>master transition aborted - dir changelog synchronization problem found - path=%s remote-size=%llu local-size=%llu", rdclf.c_str(), size_remote_dir_changelog, size_local_dir_changelog));
      fRunningState = kIsRunningSlave;
      return false;
    }
  }


  // -----------------------------------------------------------
  // make a backup of the new target master file
  // -----------------------------------------------------------

  XrdOucString NsFileChangeLogFileCopy = fileSettings["changelog_path"].c_str();
  NsFileChangeLogFileCopy += ".";
  NsFileChangeLogFileCopy += (int) time(NULL);
  XrdOucString NsDirChangeLogFileCopy = contSettings["changelog_path"].c_str();
  ;
  NsDirChangeLogFileCopy += ".";
  NsDirChangeLogFileCopy += (int) time(NULL);

  if (!::stat(fileSettings["changelog_path"].c_str(), &buf))
  {
    if (::rename(fileSettings["changelog_path"].c_str(), NsFileChangeLogFileCopy.c_str()))
    {
      MasterLog(eos_crit("failed to rename %s=>%s errno=%d", gOFS->MgmNsFileChangeLogFile.c_str(), NsFileChangeLogFileCopy.c_str(), errno));
      fRunningState = kIsNothing;
      return false;
    }
  }

  if (!::stat(contSettings["changelog_path"].c_str(), &buf))
  {
    if (::rename(contSettings["changelog_path"].c_str(), NsDirChangeLogFileCopy.c_str()))
    {
      MasterLog(eos_crit("failed to rename %s=>%s errno=%d", gOFS->MgmNsDirChangeLogFile.c_str(), NsDirChangeLogFileCopy.c_str(), errno));
      fRunningState = kIsNothing;
      return false;
    }
  }

  gOFS->MgmNsFileChangeLogFile = fileSettings["changelog_path"].c_str();
  gOFS->MgmNsDirChangeLogFile = contSettings["changelog_path"].c_str();

  try
  {
    MasterLog(eos_info("msg=\"invoking slave=>master transition\""));
    gOFS->eosDirectoryService->slave2Master(contSettings);
    gOFS->eosFileService->slave2Master(fileSettings);
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    MasterLog(eos_crit("slave=>master transition returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
    fRunningState = kIsNothing;

    rc = system("service eos start sync");
    if (WEXITSTATUS(rc))
    {
      MasterLog(eos_warning("slave=>master transition - sync didnt' start"));
    }
    return false;
  };
  fRunningState = kIsRunningMaster;

  rc = system("service eos start sync");
  if (WEXITSTATUS(rc))
  {
    MasterLog(eos_warning("failed to start sync service"));
    MasterLog(eos_crit("slave=>master transition aborted since sync didn't start"));
    try
    {
      gOFS->eosDirectoryService->finalize();
      gOFS->eosFileService->finalize();
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      MasterLog(eos_crit("slave=>master finalize returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
    }
    fRunningState = kIsNothing;
    return false;
  }

  UnBlockCompacting();

  // re-start the recycler thread
  gOFS->Recycler.Start();
  MasterLog(eos_notice("running in master mode"));
  return true;
}

/* -------------------------------------------------------------------------- */
bool
Master::Master2MasterRO ()
{
  fRunningState = kIsTransition;

  // -----------------------------------------------------------
  // convert the RW namespace into a read-only namespace
  // -----------------------------------------------------------

  // -----------------------------------------------------------
  // wait that compacting is finished and block any further compacting
  // -----------------------------------------------------------
  WaitCompactingFinished();

  try
  {
    gOFS->eosDirectoryService->makeReadOnly();
    gOFS->eosFileService->makeReadOnly();
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    MasterLog(eos_crit("master=>slave transition returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
    fRunningState = kIsNothing;
    return false;
  };

  // stop the recycler thread
  gOFS->Recycler.Stop();

  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  fRunningState = kIsReadOnlyMaster;
  MasterLog(eos_notice("running in RO master mode"));
  return true;
}

/* -------------------------------------------------------------------------- */
bool
Master::MasterRO2Slave ()
{
  // -----------------------------------------------------------
  // This call transforms a running ro-master into a slave following
  // a remote master
  // -----------------------------------------------------------
  fRunningState = kIsTransition;
  {
    // be aware of interference with the heart beat daemon (which does not touch a generic stall yet
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

    // remove redirects
    Access::gRedirectionRules.erase(std::string("w:*"));
    Access::gRedirectionRules.erase(std::string("ENOENT:*"));
    Access::gStallRules.erase(std::string("w:*"));
    Access::gStallWrite = false;

    // put an appropriate stall
    if (fRemoteMasterOk)
    {
      Access::gStallRules[std::string("w:*")] = "60";
      Access::gStallRules[std::string("*")] = "100";
      Access::gStallGlobal = true;
    }
    else
    {
      Access::gStallRules[std::string("w:*")] = "60";
      Access::gStallRules[std::string("*")] = "60";
      Access::gStallGlobal = true;
    }
  }

  {
    // now convert the namespace
    eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

    // take the whole namespace down
    try
    {
      if (gOFS->eosFsView)
      {
        gOFS->eosFsView->finalize();
        delete gOFS->eosFsView;
        gOFS->eosFsView = 0;
      }
      if (gOFS->eosView)
      {
        gOFS->eosView->finalize();
        delete gOFS->eosView;
        gOFS->eosView = 0;
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      MasterLog(eos_crit("master-ro=>slave namespace shutdown returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
    };

    // boot it from scratch
    if (!BootNamespace())
    {
      fRunningState = kIsNothing;
      return false;
    }
  }

  // reload the configuration to get the proper quota nodes
  if (gOFS->MgmConfigAutoLoad.length())
  {
    MasterLog(eos_static_info("autoload config=%s", gOFS->MgmConfigAutoLoad.c_str()));
    XrdOucString configloader = "mgm.config.file=";
    configloader += gOFS->MgmConfigAutoLoad;
    XrdOucEnv configenv(configloader.c_str());
    XrdOucString stdErr = "";
    if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr))
    {
      MasterLog(eos_static_crit("Unable to auto-load config %s - fix your configuration file!", gOFS->MgmConfigAutoLoad.c_str()));
      MasterLog(eos_static_crit("%s", stdErr.c_str()));
      return false;
    }
    else
    {
      MasterLog(eos_static_info("Successful auto-load config %s", gOFS->MgmConfigAutoLoad.c_str()));
    }
  }

  {
    XrdSysMutexHelper lock(gOFS->InitializationMutex);
    if (gOFS->Initialized == gOFS->kBooted)
    {
      // inform the boot thread that the stall should be removed after boot
      gOFS->RemoveStallRuleAfterBoot = true;

      // start the file view loader thread
      MasterLog(eos_info("msg=\"starting file view loader thread\""));

      pthread_t tid;
      if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView, static_cast<void *> (gOFS),
                             0, "File View Loader")))
      {
        MasterLog(eos_crit("cannot start file view loader"));
        fRunningState = kIsNothing;
        return false;
      }
    }
    else
    {
      MasterLog(eos_crit("msg=\"don't want to start file view loader for a namespace in bootfailure state\""));
      fRunningState = kIsNothing;
      return false;
    }
  }
  fRunningState = kIsRunningSlave;
  MasterLog(eos_notice("running in slave mode"));
  return true;
}

/* -------------------------------------------------------------------------- */
Master::~Master ()
{
  if (fThread)
  {
    XrdSysThread::Cancel(fThread);
    XrdSysThread::Join(fThread, 0);
    fThread = 0;
  }
  if (fCompactingThread)
  {
    XrdSysThread::Cancel(fCompactingThread);
    XrdSysThread::Join(fCompactingThread, 0);
    fCompactingThread = 0;
  }
  if (fDevNull)
  {
    close(fDevNull);
    fDevNull = 0;
  }
  if (fDevNullLogger)
  {
    delete fDevNullLogger;
    fDevNullLogger = 0;
  }
  if (fDevNullErr)
  {
    delete fDevNullErr;
    fDevNullErr = 0;
  }
}

/* -------------------------------------------------------------------------- */
bool
Master::CreateStatusFile (const char* path)
{
  struct stat buf;
  if (::stat(path, &buf))
  {
    int fd = 0;
    if ( (fd = ::creat(path, S_IRWXU | S_IRGRP | S_IROTH)) == -1)
    {
      MasterLog(eos_static_err("failed to create %s errno=%d", path, errno));
      return false;
    }
    close(fd);
  }
  return true;
}

/* -------------------------------------------------------------------------- */
bool
Master::RemoveStatusFile (const char* path)
{
  struct stat buf;
  if (!::stat(path, &buf))
  {
    if (::unlink(path))
    {
      MasterLog(eos_static_err("failed to unlink %s errno=%d", path, errno));
      return false;
    }
  }
  return true;
}

/* -------------------------------------------------------------------------- */
bool
Master::BootNamespace ()
{
  gOFS->eosDirectoryService = new eos::ChangeLogContainerMDSvc;
  gOFS->eosFileService = new eos::ChangeLogFileMDSvc;
  gOFS->eosView = new eos::HierarchicalView;
  gOFS->eosFsView = new eos::FileSystemView;

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;
  std::map<std::string, std::string> settings;

  contSettings["changelog_path"] = gOFS->MgmMetaLogDir.c_str();
  fileSettings["changelog_path"] = gOFS->MgmMetaLogDir.c_str();
  contSettings["changelog_path"] += "/directories.";
  fileSettings["changelog_path"] += "/files.";
  contSettings["changelog_path"] += fMasterHost.c_str();
  fileSettings["changelog_path"] += fMasterHost.c_str();
  contSettings["changelog_path"] += ".mdlog";
  fileSettings["changelog_path"] += ".mdlog";

  if (!IsMaster())
  {

    contSettings["slave_mode"] = "true";
    contSettings["poll_interval_us"] = "1000";
    fileSettings["slave_mode"] = "true";
    fileSettings["poll_interval_us"] = "1000";
  }

  gOFS->MgmNsFileChangeLogFile = fileSettings["changelog_path"].c_str();
  gOFS->MgmNsDirChangeLogFile = contSettings["changelog_path"].c_str();

  time_t tstart = time(0);

  //-------------------------------------------
  try
  {
    gOFS->eosFileService->configure(fileSettings);
    gOFS->eosDirectoryService->configure(contSettings);

    gOFS->eosView->setContainerMDSvc(gOFS->eosDirectoryService);
    gOFS->eosView->setFileMDSvc(gOFS->eosFileService);

    gOFS->eosFileService->setContainerService(gOFS->eosDirectoryService);

    if (!IsMaster())
    {
      // slave's need access to the namespace lock
      gOFS->eosFileService->setSlaveLock(&fNsLock);
      gOFS->eosDirectoryService->setSlaveLock(&fNsLock);
    }

    gOFS->eosView->configure(settings);

    MasterLog(eos_notice("%s", (char*) "eos directory view configure started"));

    gOFS->eosFileService->addChangeListener(gOFS->eosFsView);
    gOFS->eosFileService->setQuotaStats(gOFS->eosView->getQuotaStats());
    gOFS->eosDirectoryService->setQuotaStats(gOFS->eosView->getQuotaStats());

    gOFS->eosView->getQuotaStats()->registerSizeMapper(Quota::MapSizeCB);
    gOFS->eosView->initialize1();

    time_t tstop = time(0);
    MasterLog(eos_notice("eos directory view configure stopped after %d seconds", (tstop - tstart)));
  }
  catch (eos::MDException &e)
  {
    time_t tstop = time(0);
    MasterLog(eos_crit("eos view initialization failed after %d seconds", (tstop - tstart)));
    errno = e.getErrno();
    MasterLog(eos_crit("initialization returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
    return false;
  };

  if (!IsMaster())
  {
    fRunningState = kIsRunningSlave;
    MasterLog(eos_notice("running in slave mode"));
  }
  else
  {
    fRunningState = kIsRunningMaster;
    MasterLog(eos_notice("running in master mode"));
  }
  return true;
}

/* -------------------------------------------------------------------------- */
void
Master::SignalRemoteBounceToMaster ()
{
  //------------------------------------------------------------------------
  // Signal the remote master to bounce all requests to us
  //------------------------------------------------------------------------

  std::string remoteMgmUrlString = "root://";
  remoteMgmUrlString += fRemoteHost.c_str();
  remoteMgmUrlString += ":1094";
  remoteMgmUrlString += "//dummy";
  std::string remoteMgmHostPort = fRemoteHost.c_str();
  remoteMgmHostPort += ":1094";

  // --------------------------------------------------
  // TODO: add signals for remote slave(-only) machiens
  // --------------------------------------------------

  std::string signalbounce = "/?mgm.pcmd=mastersignalbounce";
  XrdCl::URL remoteMgmUrl(remoteMgmUrlString.c_str());
  XrdCl::FileSystem FsMgm(remoteMgmUrl);
  XrdCl::StatInfo* sinfo = 0;

  XrdCl::Buffer qbuffer;
  qbuffer.FromString(signalbounce);
  XrdCl::Buffer* rbuffer = 0;

  if (FsMgm.Query(XrdCl::QueryCode::OpaqueFile, qbuffer, rbuffer).IsOK())
  {
    MasterLog(eos_info("msg=\"signalled successfully remote master to redirect\""));
  }
  else
  {
    MasterLog(eos_warning("failed to signal remote redirect to %s", remoteMgmUrlString.c_str()));
  }
  if (rbuffer)
  {
    delete rbuffer;
    rbuffer = 0;
  }
  if (sinfo)
  {
    delete sinfo;
    sinfo = 0;
  }
}

/* -------------------------------------------------------------------------- */
void
Master::SignalRemoteReload ()
{
  //------------------------------------------------------------------------
  // Signal the remote master to reload its namespace
  //------------------------------------------------------------------------

  std::string remoteMgmUrlString = "root://";
  remoteMgmUrlString += fRemoteHost.c_str();
  remoteMgmUrlString += ":1094";
  remoteMgmUrlString += "//dummy";
  std::string remoteMgmHostPort = fRemoteHost.c_str();
  remoteMgmHostPort += ":1094";

  // --------------------------------------------------
  // TODO: add signals for remote slave(-only) machines
  // --------------------------------------------------

  std::string signalreload = "/?mgm.pcmd=mastersignalreload";

  XrdCl::URL remoteMgmUrl(remoteMgmUrlString.c_str());

  XrdCl::FileSystem FsMgm(remoteMgmUrl);
  XrdCl::StatInfo* sinfo = 0;

  XrdCl::Buffer qbuffer;
  qbuffer.FromString(signalreload);
  XrdCl::Buffer* rbuffer = 0;

  if (FsMgm.Query(XrdCl::QueryCode::OpaqueFile, qbuffer, rbuffer).IsOK())
  {
    MasterLog(eos_info("msg=\"signalled remote master to reload\""));
  }
  else
  {
    MasterLog(eos_warning("failed to signal remote reload to %s", remoteMgmUrlString.c_str()));
  }
  if (rbuffer)
  {
    delete rbuffer;
    rbuffer = 0;
  }
  if (sinfo)
  {
    delete sinfo;
    sinfo = 0;
  }
}

/* -------------------------------------------------------------------------- */
void
Master::TagNamespaceInodes ()
{
  struct stat statf;
  struct stat statd;

  MasterLog(eos_info("msg=\"tag namespace inodes\""));
  if ((!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &statf)) && (!::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &statd)))
  {
    fFileNamespaceInode = statf.st_ino;
    fDirNamespaceInode = statd.st_ino;
  }
  else
  {
    MasterLog(eos_warning("stat of namespace files failed with errno=%d", errno));
  }
}

/* -------------------------------------------------------------------------- */
bool
Master::WaitNamespaceFilesInSync (unsigned int timeout)
{
  //------------------------------------------------------------------------
  // Wait that local/remote namespace files are synced.
  // This routine is called by a slave when it got
  // signalled to reload the namespace
  //------------------------------------------------------------------------

  time_t starttime = time(NULL);

  // -----------------------------------------------------------
  // if possible evaluate if local and remote master files are
  // in sync ...
  // -----------------------------------------------------------

  MasterLog(eos_info("msg=\"check ns file synchronization\""));

  off_t size_local_file_changelog = 0;
  off_t size_local_dir_changelog = 0;
  off_t size_remote_file_changelog = 0;
  off_t size_remote_dir_changelog = 0;

  unsigned long long lFileNamespaceInode = 0;

  struct stat buf;

  std::string remoteSyncUrlString = "root://";
  remoteSyncUrlString += fRemoteHost.c_str();
  remoteSyncUrlString += ":1096";
  remoteSyncUrlString += "//dummy";
  std::string remoteSyncHostPort = fRemoteHost.c_str();
  remoteSyncHostPort += ":1096";

  std::string rfclf = gOFS->MgmMetaLogDir.c_str();
  std::string rdclf = gOFS->MgmMetaLogDir.c_str();
  rdclf += "/directories.";
  rfclf += "/files.";
  rdclf += fRemoteHost.c_str();
  rfclf += fRemoteHost.c_str();
  rdclf += ".mdlog";
  rfclf += ".mdlog";

  bool syncok = false;

  if (HostCheck(fRemoteHost.c_str(), 1096))
  {
    MasterLog(eos_info("remote-sync host=%s:1096 is reachable", fRemoteHost.c_str()));
    syncok = true;
  }
  else
  {
    MasterLog(eos_info("remote-sync host=%s:1096 is down", fRemoteHost.c_str()));
  }

  if (syncok)
  {

    // ---------------------------------------
    // TODO: adapt for the new client      ---
    // ---------------------------------------

    // ---------------------------------------
    // check once the remote size
    // ---------------------------------------

    XrdCl::URL remoteSyncUrl(remoteSyncUrlString.c_str());

    XrdCl::FileSystem FsSync(remoteSyncUrl);
    XrdCl::StatInfo* sinfo = 0;

    // stat the two remote changelog files
    if (FsSync.Stat(rfclf.c_str(), sinfo, 5).IsOK())
    {
      size_remote_file_changelog = sinfo->GetSize();
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
    }
    else
    {
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
      MasterLog(eos_crit("remote stat failed for %s", rfclf.c_str()));
      return false;
    }

    if (FsSync.Stat(rdclf.c_str(), sinfo, 5).IsOK())
    {
      size_remote_dir_changelog = sinfo->GetSize();
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
    }
    else
    {
      if (sinfo)
      {
        delete sinfo;
        sinfo = 0;
      }
      MasterLog(eos_crit("remote stat failed for %s", rdclf.c_str()));
      return false;
    }

    MasterLog(eos_info("remote files file=%llu dir=%llu", size_remote_file_changelog, size_remote_dir_changelog));

    do
    {
      // ---------------------------------------
      // wait the the inode changed and then
      // check the local size and wait,
      // that the local files is atleast as
      // big as the remote file
      // ---------------------------------------

      if (!stat(gOFS->MgmNsFileChangeLogFile.c_str(), &buf))
      {
        size_local_file_changelog = buf.st_size;
        lFileNamespaceInode = buf.st_ino;
      }
      else
      {
        MasterLog(eos_crit("local stat failed for %s", gOFS->MgmNsFileChangeLogFile.c_str()));
        return false;
      }

      if (!stat(gOFS->MgmNsDirChangeLogFile.c_str(), &buf))
      {
        size_local_dir_changelog = buf.st_size;
      }
      else
      {
        MasterLog(eos_crit("local stat failed for %s", gOFS->MgmNsDirChangeLogFile.c_str()));
        return false;
      }

      if (lFileNamespaceInode == fFileNamespaceInode)
      {
        // the inode didn't change yet
        if (time(NULL) > (starttime + timeout))
        {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }
        MasterLog(eos_info("waiting for inode change %llu=>%llu ", fFileNamespaceInode, lFileNamespaceInode));
        XrdSysTimer sleeper;
        sleeper.Wait(10000);
        continue;
      }

      if (size_remote_file_changelog > size_local_file_changelog)
      {
        if (time(NULL) > (starttime + timeout))
        {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }
        XrdSysTimer sleeper;
        sleeper.Wait(10000);
        continue;
      }

      if (size_remote_dir_changelog > size_local_dir_changelog)
      {
        if (time(NULL) > (starttime + timeout))
        {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }
        XrdSysTimer sleeper;
        sleeper.Wait(10000);
        continue;
      }
      MasterLog(eos_info("msg=\"ns files  synchronized\""));
      return true;
    }
    while (1);
  }
  else
  {
    MasterLog(eos_warning("msg=\"remote sync service is not ok\""));
    return false;
  }
}

/* -------------------------------------------------------------------------- */
void
Master::RedirectToRemoteMaster ()
{
  MasterLog(eos_info("msg=\"redirect to remote master\""));
  // push everything to the remote master
  Access::gRedirectionRules[std::string("*")] = fRemoteHost.c_str();

  try
  {
    MasterLog(eos_info("msg=\"invoking slave shutdown\""));
    gOFS->eosDirectoryService->stopSlave();
    gOFS->eosFileService->stopSlave();
    MasterLog(eos_info("msg=\"stopped namespace following\""));
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    MasterLog(eos_crit("slave shutdown returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
  }
}

/* -------------------------------------------------------------------------- */
bool
Master::RebootSlaveNamespace ()
{
  fRunningState = kIsTransition;
  {
    // now convert the namespace
    eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

    // take the whole namespace down
    try
    {
      if (gOFS->eosFsView)
      {
        gOFS->eosFsView->finalize();
        delete gOFS->eosFsView;
        gOFS->eosFsView = 0;
      }
      if (gOFS->eosView)
      {
        gOFS->eosView->finalize();
        delete gOFS->eosView;
        gOFS->eosView = 0;
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      MasterLog(eos_crit("master-ro=>slave namespace shutdown returned ec=%d %s", e.getErrno(), e.getMessage().str().c_str()));
    };

    // boot it from scratch
    if (!BootNamespace())
    {
      fRunningState = kIsNothing;
      return false;
    }

  }

  {
    XrdSysMutexHelper lock(gOFS->InitializationMutex);
    if (gOFS->Initialized == gOFS->kBooted)
    {
      // inform the boot thread that the stall should be removed after boot
      gOFS->RemoveStallRuleAfterBoot = true;

      // start the file view loader thread
      MasterLog(eos_info("msg=\"starting file view loader thread\""));

      pthread_t tid;
      if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView, static_cast<void *> (gOFS),
                             0, "File View Loader")))
      {
        MasterLog(eos_crit("cannot start file view loader"));
        fRunningState = kIsNothing;
        return false;
      }
    }
    else
    {
      MasterLog(eos_crit("msg=\"don't want to start file view loader for a namespace in bootfailure state\""));
      fRunningState = kIsNothing;
      return false;
    }

  }

  {
    // be aware of interference with the heart beat daemon
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

    // remove global redirection
    Access::gRedirectionRules.erase(std::string("*"));
  }

  fRunningState = kIsRunningSlave;
  MasterLog(eos_notice("running in slave mode"));
  return true;
}

EOSMGMNAMESPACE_END
