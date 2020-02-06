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

#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "mgm/Access.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Recycle.hh"
#include "mgm/config/IConfigEngine.hh"
#include "common/Statfs.hh"
#include "common/ShellCmd.hh"
#include "common/plugin_manager/PluginManager.hh"
#include "XrdNet/XrdNet.hh"
#include "XrdNet/XrdNetPeer.hh"
#include "namespace/interface/IChLogFileMDSvc.hh"
#include "namespace/interface/IChLogContainerMDSvc.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/INamespaceGroup.hh"
#include "namespace/ns_quarkdb/Constants.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Master::Master()
{
  fRemoteMasterOk = true;
  fRemoteMqOk = true;
  fRemoteMasterRW = false;
  fThread = 0;
  fRunningState = Run::State::kIsNothing;
  fCompactingState = Compact::State::kIsNotCompacting;
  fCompactingThread = 0;
  fCompactingStart = 0;
  fCompactingInterval = 0;
  fCompactingRatio = 0;
  fCompactFiles = false;
  fCompactDirectories = false;
  fDevNull = 0;
  fDevNullLogger = nullptr;
  fDevNullErr = nullptr;
  fCheckRemote = true;
  fFileNamespaceInode = fDirNamespaceInode = 0;
  f2MasterTransitionTime = time(nullptr) - 3600; // start without service delays
  fHasSystemd = false;
  fDirCompactingRatio = 0.0;
}

//------------------------------------------------------------------------------
// Initialize
//------------------------------------------------------------------------------
bool
Master::Init()
{
  // Check if we have systemd
  eos::common::ShellCmd
  scmd0("/usr/sbin/pidof systemd >& /dev/null");
  eos::common::cmd_status rc = scmd0.wait(30);
  fHasSystemd = (rc.exited && rc.exit_code == 0);
  eos_info("systemd found on the machine = %d", (int)fHasSystemd);
  // Define our role master/slave
  struct stat buf;
  fThisHost = gOFS->HostName;
  fNsLock.Init(&(gOFS->eosViewRWMutex)); // fill the namespace mutex

  if ((!getenv("EOS_MGM_MASTER1")) ||
      (!getenv("EOS_MGM_MASTER2"))) {
    eos_crit("EOS_MGM_MASTER1 and EOS_MGM_MASTER2 variables are undefined");
    return false;
  }

  if (fThisHost == getenv("EOS_MGM_MASTER1")) {
    fRemoteHost = getenv("EOS_MGM_MASTER2");
  } else {
    fRemoteHost = getenv("EOS_MGM_MASTER1");
  }

  // Start the online compacting background thread
  XrdSysThread::Run(&fCompactingThread, Master::StaticOnlineCompacting,
                    static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                    "Master OnlineCompacting Thread");

  if (fThisHost == fRemoteHost) {
    // No master slave configuration ... also fine
    fMasterHost = fThisHost;
    return true;
  }

  // Open a /dev/null logger/error object
  fDevNull = open("/dev/null", 0);
  fDevNullLogger = new XrdSysLogger(fDevNull);
  fDevNullErr = new XrdSysError(fDevNullLogger);
  XrdOucString lMaster1MQ;
  XrdOucString lMaster2MQ;

  // Define the MQ hosts
  if (getenv("EOS_MQ_MASTER1")) {
    lMaster1MQ = getenv("EOS_MQ_MASTER1");
  } else {
    lMaster1MQ = getenv("EOS_MGM_MASTER1");
    int dpos = lMaster1MQ.find(":");

    if (dpos != STR_NPOS) {
      lMaster1MQ.erase(dpos);
    }

    lMaster1MQ += ":1097";
  }

  if (getenv("EOS_MQ_MASTER2")) {
    lMaster2MQ = getenv("EOS_MQ_MASTER2");
  } else {
    lMaster2MQ = getenv("EOS_MGM_MASTER2");
    int dpos = lMaster2MQ.find(":");

    if (dpos != STR_NPOS) {
      lMaster2MQ.erase(dpos);
    }

    lMaster2MQ += ":1097";
  }

  // Define which MQ is remote
  if (lMaster1MQ.find(fThisHost) != STR_NPOS) {
    fRemoteMq = lMaster2MQ;
  } else {
    fRemoteMq = lMaster1MQ;
  }

  if (!::stat(EOSMGMMASTER_SUBSYS_RW_LOCKFILE, &buf)) {
    fMasterHost = fThisHost;
  } else {
    fMasterHost = fRemoteHost;
  }

  if (fThisHost != fRemoteHost) {
    fCheckRemote = true;
  } else {
    fCheckRemote = false;
  }

  // start the heartbeat thread anyway
  XrdSysThread::Run(&fThread, Master::StaticSupervisor, static_cast<void*>(this),
                    XRDSYSTHREAD_HOLD, "Master Supervisor Thread");

  // Check if we want the MGM to start sync/eossync at all
  if (!getenv("EOS_START_SYNC_SEPARATELY")) {
    // Get sync up if it is not up
    eos::common::ShellCmd
    scmd1(fHasSystemd ?
          "systemctl status eos@sync || systemctl start eos@sync" :
          "service eos status sync || service eos start sync");
    rc = scmd1.wait(30);

    if (rc.exit_code) {
      eos_crit("failed to start sync service");
      return false;
    }

    // Get eossync up if it is not up
    eos::common::ShellCmd
    scmd2(fHasSystemd ?
          "systemctl status eossync@* || systemctl start eossync" :
          "service eossync status || service eossync start ");
    rc = scmd2.wait(30);

    if (rc.exit_code) {
      eos_crit("failed to start eossync service");
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if host is reachable
//------------------------------------------------------------------------------
bool
Master::HostCheck(const char* hostname, int port, int timeout)
{
  XrdOucString lHostName = hostname;
  int dpos;

  if ((dpos = lHostName.find(":")) != STR_NPOS) {
    lHostName.erase(dpos);
  }

  XrdNetPeer peer;
  XrdNet net(fDevNullErr);

  if (net.Connect(peer, lHostName.c_str(), port, 0, timeout)) {
    // Send a handshake to avoid handshake error messages on server side
    unsigned int vshake[5];
    vshake[0] = vshake[1] = vshake[2] = 0;
    vshake[3] = htonl(4);
    vshake[4] = htonl(2012);
    ssize_t nwrite = write(peer.fd, &vshake[0], 20);
    close(peer.fd);
    return nwrite == 20;
  }

  return false;
}

//------------------------------------------------------------------------------
// Enable the heartbeat thread to do remote checks
//------------------------------------------------------------------------------
bool
Master::EnableRemoteCheck()
{
  if (!fCheckRemote) {
    MasterLog(eos_info("remotecheck=enabled"));
    fCheckRemote = true;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Stop's the heartbeat thread from doing remote checks
//------------------------------------------------------------------------------
bool
Master::DisableRemoteCheck()
{
  if (fCheckRemote) {
    MasterLog(eos_info("remotecheck=disabled"));
    fCheckRemote = false;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Static thread startup function calling Supervisor
//------------------------------------------------------------------------------
void*
Master::StaticSupervisor(void* arg)
{
  return reinterpret_cast<Master*>(arg)->Supervisor();
}

//------------------------------------------------------------------------------
// This thread runs in an internal loop with 1Hz and checks
// a) if enabled a potential remote master/slave for failover
// b) the fill state of the local disk to avoid running out of disk space
// It then configures redirection/stalling etc. corresponding to the present
// situation
//------------------------------------------------------------------------------
void*
Master::Supervisor()
{
  std::string remoteMgmUrlString = "root://";
  remoteMgmUrlString += fRemoteHost.c_str();
  std::string remoteMqUrlString = "root://";
  remoteMqUrlString += fRemoteMq.c_str();
  bool lDiskFull = false;
  bool pDiskFull = false;
  std::string pStallSetting;
  auto dpos = remoteMqUrlString.find(':', 7);

  if (dpos != std::string::npos) {
    remoteMqUrlString.erase(dpos + 1);
    remoteMqUrlString += "1097";
  }

  XrdCl::URL remoteMgmUrl(remoteMgmUrlString);
  XrdCl::URL remoteMqUrl(remoteMqUrlString);

  if (!remoteMgmUrl.IsValid()) {
    MasterLog(eos_static_crit("remote manager URL <%s> is not valid",
                              remoteMgmUrlString.c_str()));
    fRemoteMasterOk = false;
  }

  if (!remoteMqUrl.IsValid()) {
    MasterLog(eos_static_crit("remote mq URL <%s> is not valid",
                              remoteMqUrlString.c_str()));
    fRemoteMqOk = false;
  }

  XrdCl::FileSystem FsMgm(remoteMgmUrl);
  XrdCl::FileSystem FsMq(remoteMqUrl);
  XrdSysThread::SetCancelDeferred();

  while (true) {
    // Check the remote machine for its status
    if (fCheckRemote) {
      // Ping the two guys with short timeouts e.g. MGM & MQ
      XrdCl::XRootDStatus mgmStatus = FsMgm.Ping(1);
      XrdCl::XRootDStatus mqStatus = FsMq.Ping(1);
      bool remoteMgmUp = mgmStatus.IsOK();
      bool remoteMqUp = mqStatus.IsOK();

      if (remoteMqUp) {
        XrdCl::StatInfo* sinfo = 0;

        if (FsMq.Stat("/eos/", sinfo, 5).IsOK()) {
          fRemoteMqOk = true;
          CreateStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
        } else {
          fRemoteMqOk = false;
          RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
        }

        if (sinfo) {
          delete sinfo;
          sinfo = 0;
        }
      } else {
        fRemoteMqOk = false;
        RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
      }

      if (remoteMgmUp) {
        XrdCl::StatInfo* sinfo = 0;

        if (FsMgm.Stat("/", sinfo, 5).IsOK()) {
          if (gOFS->MgmProcMasterPath.c_str()) {
            XrdCl::StatInfo* smasterinfo = 0;

            // check if this machine is running in master mode
            if (FsMgm.Stat(gOFS->MgmProcMasterPath.c_str(), smasterinfo, 5).IsOK()) {
              fRemoteMasterRW = true;
            } else {
              fRemoteMasterRW = false;
            }

            if (smasterinfo) {
              delete smasterinfo;
              smasterinfo = 0;
            }
          }

          fRemoteMasterOk = true;
        } else {
          fRemoteMasterOk = false;
          fRemoteMasterRW = false;
        }

        if (sinfo) {
          delete sinfo;
          sinfo = 0;
        }
      } else {
        fRemoteMasterOk = false;
        fRemoteMasterRW = false;
      }

      if (!lDiskFull) {
        MasterLog(eos_static_debug("ismaster=%d remote-ok=%d remote-wr=%d "
                                   "thishost=%s remotehost=%s masterhost=%s ",
                                   IsMaster(), fRemoteMasterOk, fRemoteMasterRW,
                                   fThisHost.c_str(), fRemoteHost.c_str(),
                                   fMasterHost.c_str()));
        eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

        if (!IsMaster()) {
          if (fRemoteMasterOk && fRemoteMasterRW) {
            // Set the redirect for writes and ENOENT to the remote master
            Access::gRedirectionRules[std::string("w:*")] = fRemoteHost.c_str();

            // only set an ENOENT redirection, if there isn't already one
            if (!Access::gRedirectionRules.count(std::string("ENOENT:*")) ||
                ((Access::gRedirectionRules[std::string("ENOENT:*")] != fRemoteHost.c_str()) &&
                 (Access::gRedirectionRules[std::string("ENOENT:*")] != fThisHost.c_str()))) {
              // Set the redirect for ENOENT to the remote master
              Access::gRedirectionRules[std::string("ENOENT:*")] = fRemoteHost.c_str();
            }

            // Remove the stall
            Access::gStallRules.erase(std::string("w:*"));
            Access::gStallWrite = false;
          } else {
            // Remove the redirect for writes and ENOENT, if there is no external redirect
            Access::gRedirectionRules.erase(std::string("w:*"));

            if (!Access::gRedirectionRules.count(std::string("ENOENT:*")) ||
                ((Access::gRedirectionRules[std::string("ENOENT:*")] != fRemoteHost.c_str()) &&
                 (Access::gRedirectionRules[std::string("ENOENT:*")] != fThisHost.c_str()))) {
              Access::gRedirectionRules.erase(std::string("ENOENT:*"));
            }

            // Put stall for writes
            Access::gStallRules[std::string("w:*")] = "60";
            Access::gStallWrite = true;
          }
        } else {
          // Check if we have two master-rw
          if (fRemoteMasterOk && fRemoteMasterRW && (fThisHost != fRemoteHost)) {
            MasterLog(eos_crit("msg=\"dual RW master setup detected\""));
            Access::gStallRules[std::string("w:*")] = "60";
            Access::gStallWrite = true;
          } else {
            if (fRunningState == Run::State::kIsRunningMaster) {
              // Remove any redirect or stall in this case
              (void) Access::gRedirectionRules.erase(std::string("w:*"));

              if (Access::gRedirectionRules.count(std::string("ENOENT:*"))) {
                // only remove ENOENT rules if the are touching master slave redirection
                if ((Access::gRedirectionRules[std::string("ENOENT:*")] == fRemoteHost.c_str())
                    ||
                    (Access::gRedirectionRules[std::string("ENOENT:*")] == fThisHost.c_str())) {
                  Access::gRedirectionRules.erase(std::string("ENOENT:*"));
                }
              }

              if (Access::gStallRules.count(std::string("w:*"))) {
                Access::gStallRules.erase(std::string("w:*"));
                Access::gStallWrite = false;
              }
            }
          }
        }
      }
    }

    // Check if the local filesystem has enough space on the namespace partition
    XrdOucString sizestring;
    std::unique_ptr<eos::common::Statfs> statfs =
      eos::common::Statfs::DoStatfs(gOFS->MgmMetaLogDir.c_str());

    if (!statfs) {
      MasterLog(eos_err("path=%s statfs=failed", gOFS->MgmMetaLogDir.c_str()));
      // uups ... statfs failed
      lDiskFull = true;
    } else {
      // we stop if we get to < 100 MB free
      if ((statfs->GetStatfs()->f_bfree * statfs->GetStatfs()->f_bsize) <
          (100 * 1024 * 1024)) {
        lDiskFull = true;
      } else {
        lDiskFull = false;
      }

      eos::common::StringConversion::GetReadableSizeString(
        sizestring, (statfs->GetStatfs()->f_bfree * statfs->GetStatfs()->f_bsize), "B");
    }

    if (lDiskFull != pDiskFull) {
      // This is a state change and we have to configure the redirection settings
      if (lDiskFull) {
        MasterLog(eos_warning("status=\"disk space warning - stalling\" "
                              "path=%s freebytes=%s", gOFS->MgmMetaLogDir.c_str(),
                              sizestring.c_str()));
        eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
        pStallSetting = Access::gStallRules[std::string("w:*")];
        Access::gStallRules[std::string("w:*")] = "60";
        Access::gStallWrite = true;
      } else {
        MasterLog(eos_notice("status=\"disk space ok - removed stall\" "
                             "path=%s freebyte=%s", gOFS->MgmMetaLogDir.c_str(),
                             sizestring.c_str()));

        if (pStallSetting.length()) {
          // Put back the original stall setting
          Access::gStallRules[std::string("w:*")] = pStallSetting;
          Access::gStallWrite = true;
        } else {
          // Remove the stall setting
          Access::gStallRules.erase(std::string("w:*"));
          Access::gStallWrite = false;
        }

        pStallSetting = "";
      }

      pDiskFull = lDiskFull;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    XrdSysThread::CancelPoint();
  }

  XrdSysThread::SetCancelOn();
  return nullptr;
}

//------------------------------------------------------------------------------
// Static thread startup function calling Compacting
//------------------------------------------------------------------------------
void*
Master::StaticOnlineCompacting(void* arg)
{
  return reinterpret_cast<Master*>(arg)->Compacting();
}

//------------------------------------------------------------------------------
// Check if compacting is on-going
//------------------------------------------------------------------------------
bool
Master::IsCompacting()
{
  XrdSysMutexHelper cLock(fCompactingMutex);
  return (fCompactingState == Compact::State::kIsCompacting);
}

//------------------------------------------------------------------------------
// Check if compacting is blocked
//------------------------------------------------------------------------------
bool
Master::IsCompactingBlocked()
{
  XrdSysMutexHelper cLock(fCompactingMutex);
  return (fCompactingState == Compact::State::kIsCompactingBlocked);
}

//------------------------------------------------------------------------------
// Block compacting
//------------------------------------------------------------------------------
void
Master::BlockCompacting()
{
  XrdSysMutexHelper cLock(fCompactingMutex);
  fCompactingState = Compact::State::kIsCompactingBlocked;
  eos_static_info("msg=\"block compacting\"");
}

//------------------------------------------------------------------------------
// Unblock compacting
//------------------------------------------------------------------------------
void
Master::UnBlockCompacting()
{
  WaitCompactingFinished();
  XrdSysMutexHelper cLock(fCompactingMutex);
  fCompactingState = Compact::State::kIsNotCompacting;
  eos_static_info("msg=\"unblock compacting\"");
}

//------------------------------------------------------------------------------
// Wait for compacting to finish
//------------------------------------------------------------------------------
void
Master::WaitCompactingFinished()
{
  eos_static_info("msg=\"wait for compacting to finish\"");

  do {
    bool isCompacting = false;
    {
      XrdSysMutexHelper cLock(fCompactingMutex);
      isCompacting = (fCompactingState == Compact::State::kIsCompacting);
    }

    if (isCompacting) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      // Block any further compacting
      BlockCompacting();
      break;
    }
  } while (true);

  eos_static_info("msg=\"waited for compacting to finish OK\"");
}

//------------------------------------------------------------------------------
// Schedule online compacting
//------------------------------------------------------------------------------
bool
Master::ScheduleOnlineCompacting(time_t starttime, time_t repetitioninterval)
{
  MasterLog(eos_static_info("msg=\"scheduling online compacting\" starttime=%u "
                            "interval=%u", starttime, repetitioninterval));
  fCompactingStart = starttime;
  fCompactingInterval = repetitioninterval;
  return true;
}

//------------------------------------------------------------------------------
// Do compacting
//------------------------------------------------------------------------------
void*
Master::Compacting()
{
  XrdSysThread::SetCancelDeferred();

  do {
    time_t now = time(nullptr);
    bool runcompacting = false;
    bool reschedule = false;
    {
      XrdSysMutexHelper cLock(fCompactingMutex);
      runcompacting = ((fCompactingStart) && (now >= fCompactingStart) && IsMaster());
    }
    bool isBlocked = false;

    do {
      // Check if we are blocked
      {
        XrdSysMutexHelper cLock(fCompactingMutex);
        isBlocked = (fCompactingState == Compact::State::kIsCompactingBlocked);
      }

      // If we are blocked we wait until we are unblocked
      if (isBlocked) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      } else {
        // Set to compacting
        if (runcompacting) {
          fCompactingState = Compact::State::kIsCompacting;
        }

        break;
      }
    } while (isBlocked);

    gOFS->WaitUntilNamespaceIsBooted();

    if (!gOFS->eosFileService || !gOFS->eosDirectoryService) {
      eos_notice("file/directory metadata service is not available");
      XrdSysThread::SetCancelOn();
      return nullptr;
    }

    auto* eos_chlog_filesvc =
      dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);
    auto* eos_chlog_dirsvc =
      dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);

    // Check if namespace supports compacting
    if (!eos_chlog_filesvc || !eos_chlog_dirsvc) {
      eos_notice("msg=\"namespace does not support compacting - disable it\"");
      XrdSysThread::SetCancelOn();
      return nullptr;
    }

    if (runcompacting) {
      // Run the online compacting procedure
      eos_alert("msg=\"online-compacting running\"");
      gOFS->mNamespaceState = NamespaceState::kCompacting;
      eos_notice("msg=\"starting online compaction\"");
      time_t now = time(nullptr);
      // File compacting
      std::string ocfile = gOFS->MgmNsFileChangeLogFile.c_str();
      ocfile += ".oc";
      char archiveFileLogName[4096];
      snprintf(archiveFileLogName, sizeof(archiveFileLogName) - 1, "%s.%lu",
               gOFS->MgmNsFileChangeLogFile.c_str(), now);
      std::string archivefile = archiveFileLogName;

      if (fCompactFiles)
        MasterLog(eos_info("archive(file)=%s oc=%s", archivefile.c_str(),
                           ocfile.c_str()));

      // Directory compacting
      std::string ocdir = gOFS->MgmNsDirChangeLogFile.c_str();
      ocdir += ".oc";
      char archiveDirLogName[4096];
      snprintf(archiveDirLogName, sizeof(archiveDirLogName) - 1, "%s.%lu",
               gOFS->MgmNsDirChangeLogFile.c_str(), now);
      std::string archivedirfile = archiveDirLogName;

      if (fCompactDirectories)
        MasterLog(eos_info("archive(dir)=%s oc=%s", archivedirfile.c_str(),
                           ocdir.c_str()));

      int rc = 0;
      bool CompactFiles = fCompactFiles;
      bool CompactDirectories = fCompactDirectories;

      if (CompactFiles) {
        // Clean-up any old .oc file
        rc = unlink(ocfile.c_str());

        if (!rc) {
          MasterLog(eos_info("oc=%s msg=\"old online compacting file(file) unlinked\""));
        }
      }

      if (CompactDirectories) {
        rc = unlink(ocdir.c_str());

        if (!rc) {
          MasterLog(eos_info("oc=%s msg=\"old online compacting file(dir) unlinked\""));
        }
      }

      bool compacted = false;

      try {
        void* compData = nullptr;
        void* compDirData = nullptr;
        {
          MasterLog(eos_info("msg=\"compact prepare\""));
          // Require NS read lock
          eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

          if (CompactFiles) {
            compData = eos_chlog_filesvc->compactPrepare(ocfile);
          }

          if (CompactDirectories) {
            compDirData = eos_chlog_dirsvc->compactPrepare(ocdir);
          }
        }
        {
          MasterLog(eos_info("msg=\"compacting\""));

          // Does not require namespace lock
          if (CompactFiles) {
            eos_chlog_filesvc->compact(compData);
          }

          if (CompactDirectories) {
            eos_chlog_dirsvc->compact(compDirData);
          }
        }
        {
          // Requires namespace write lock
          MasterLog(eos_info("msg=\"compact commit\""));
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

          if (CompactFiles) {
            eos_chlog_filesvc->compactCommit(compData);
          }

          if (CompactDirectories) {
            eos_chlog_dirsvc->compactCommit(compDirData);
          }
        }
        {
          XrdSysMutexHelper cLock(fCompactingMutex);
          reschedule = (fCompactingInterval != 0);
        }

        if (reschedule) {
          eos_notice("msg=\"rescheduling online compactification\" interval=%u",
                     (unsigned int) fCompactingInterval);
          XrdSysMutexHelper cLock(fCompactingMutex);
          fCompactingStart = time(nullptr) + fCompactingInterval;
        } else {
          fCompactingStart = 0;
        }

        // If we have a remote master we have to signal it to bounce to us
        if (fRemoteMasterOk && (fThisHost != fRemoteHost)) {
          SignalRemoteBounceToMaster();
        }

        if (CompactFiles) {
          // File compaction archiving
          if (::rename(gOFS->MgmNsFileChangeLogFile.c_str(), archivefile.c_str())) {
            MasterLog(eos_crit("failed to rename %s=>%s errno=%d",
                               gOFS->MgmNsFileChangeLogFile.c_str(),
                               archivefile.c_str(), errno));
          } else {
            if (::rename(ocfile.c_str(), gOFS->MgmNsFileChangeLogFile.c_str())) {
              MasterLog(eos_crit("failed to rename %s=>%s errno=%d", ocfile.c_str(),
                                 gOFS->MgmNsFileChangeLogFile.c_str(), errno));
            } else {
              // Stat the sizes and set the compacting factor
              struct stat before_compacting;
              struct stat after_compacting;
              fCompactingRatio = 0.0;

              if ((!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &after_compacting)) &&
                  (!::stat(archivefile.c_str(), &before_compacting))) {
                if (after_compacting.st_size) {
                  fCompactingRatio = 1.0 * before_compacting.st_size / after_compacting.st_size;
                }
              }

              compacted = true;
            }
          }
        }

        if (CompactDirectories) {
          // Dir compaction archiving
          if (::rename(gOFS->MgmNsDirChangeLogFile.c_str(), archivedirfile.c_str())) {
            MasterLog(eos_crit("failed to rename %s=>%s errno=%d",
                               gOFS->MgmNsDirChangeLogFile.c_str(),
                               archivedirfile.c_str(), errno));
          } else {
            if (::rename(ocdir.c_str(), gOFS->MgmNsDirChangeLogFile.c_str())) {
              MasterLog(eos_crit("failed to rename %s=>%s errno=%d", ocdir.c_str(),
                                 gOFS->MgmNsDirChangeLogFile.c_str(), errno));
            } else {
              // Stat the sizes and set the compacting factor
              struct stat before_compacting;
              struct stat after_compacting;
              fDirCompactingRatio = 0.0;

              if ((!::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &after_compacting)) &&
                  (!::stat(archivedirfile.c_str(), &before_compacting))) {
                if (after_compacting.st_size)
                  fDirCompactingRatio = 1.0 * before_compacting.st_size /
                                        after_compacting.st_size;
              }

              compacted = true;
            }
          }
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        MasterLog(eos_crit("online-compacting returned ec=%d %s", e.getErrno(),
                           e.getMessage().str().c_str()));
      }

      std::this_thread::sleep_for(std::chrono::seconds(1));

      if (compacted) {
        eos_alert("msg=\"compact done\"");
        MasterLog(eos_info("msg=\"compact done\" elapsed=%lu", time(nullptr) - now));

        if (fRemoteMasterOk && (fThisHost != fRemoteHost)) {
          // if we have a remote master we have to signal it to bounce to us
          SignalRemoteReload(CompactFiles, CompactDirectories);
        }

        // Re-configure the changelog path from the .oc to the original filenames
        // - if we don't do that we cannot do a transition to RO-master state
        std::map<std::string, std::string> fileSettings;
        std::map<std::string, std::string> contSettings;
        contSettings["changelog_path"] = gOFS->MgmNsDirChangeLogFile.c_str();
        fileSettings["changelog_path"] = gOFS->MgmNsFileChangeLogFile.c_str();

        if (!IsMaster()) {
          contSettings["slave_mode"] = "true";
          contSettings["poll_interval_us"] = "1000";
          contSettings["auto_repair"] = "true";
          fileSettings["slave_mode"] = "true";
          fileSettings["poll_interval_us"] = "1000";
          fileSettings["auto_repair"] = "true";
        }

        try {
          gOFS->eosFileService->configure(fileSettings);
          gOFS->eosDirectoryService->configure(contSettings);
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          MasterLog(eos_crit("reconfiguration returned ec=%d %s", e.getErrno(),
                             e.getMessage().str().c_str()));
          exit(-1);
        }
      } else {
        MasterLog(eos_crit("failed online compactification"));
        exit(-1);
      }

      gOFS->mNamespaceState = NamespaceState::kBooted;
      {
        // Set to not compacting
        XrdSysMutexHelper cLock(fCompactingMutex);
        fCompactingState = Compact::State::kIsNotCompacting;
      }
    }

    // Check only once a minute
    XrdSysThread::CancelPoint();
    std::this_thread::sleep_for(std::chrono::seconds(60));
  } while (true);

  XrdSysThread::SetCancelOn();
  return nullptr;
}

//------------------------------------------------------------------------------
// Print out compacting status
//------------------------------------------------------------------------------
void
Master::PrintOutCompacting(XrdOucString& out)
{
  time_t now = time(nullptr);

  if (IsCompacting()) {
    out += "status=compacting";
    out += " waitstart=0";
  } else {
    if (IsCompactingBlocked()) {
      out += "status=blocked";
      out += " waitstart=0";
    } else {
      if (fCompactingStart && IsMaster()) {
        time_t nextrun = (fCompactingStart > now) ? (fCompactingStart - now) : 0;

        if (nextrun) {
          out += "status=wait";
          out += " waitstart=";
          out += (int) nextrun;
        } else {
          out += "status=starting";
          out += " waitstart=0";
        }
      } else {
        out += "status=off";
        out += " waitstart=0";
      }
    }

    out += " interval=";
    out += (int) fCompactingInterval;
  }

  char cfratio[256];
  snprintf(cfratio, sizeof(cfratio) - 1, "%.01f", fCompactingRatio);
  out += " ratio-file=";
  out += cfratio;
  out += ":1";
  snprintf(cfratio, sizeof(cfratio) - 1, "%.01f", fDirCompactingRatio);
  out += " ratio-dir=";
  out += cfratio;
  out += ":1";
}

//------------------------------------------------------------------------------
// Print out instance information
//------------------------------------------------------------------------------
std::string
Master::PrintOut()
{
  std::string out;

  if (fThisHost == fMasterHost) {
    out += "mode=master-rw";
  } else {
    out += "mode=slave-ro";
  }

  switch (fRunningState) {
  case Run::State::kIsNothing:
    out += " state=invalid";
    break;

  case Run::State::kIsRunningMaster:
    out += " state=master-rw";
    break;

  case Run::State::kIsRunningSlave:
    out += " state=slave-ro";
    break;

  case Run::State::kIsReadOnlyMaster:
    out += " state=master-ro";
    break;

  default:
    break;
  }

  out += " master=";
  out += fMasterHost.c_str();
  out += " configdir=";
  out += gOFS->MgmConfigDir.c_str();
  out += " config=";
  out += gOFS->MgmConfigAutoLoad.c_str();

  if (fThisHost != fRemoteHost) {
    // print only if we have a master slave configuration
    if (fRemoteMasterOk) {
      out += " mgm:";
      out += fRemoteHost.c_str();
      out += "=ok";

      if (fRemoteMasterRW) {
        out += " mgm:mode=master-rw";
      } else {
        out += " mgm:mode=slave-ro";
      }
    } else {
      out += " mgm:";
      out += fRemoteHost.c_str();
      out += "=down";
    }

    if (fRemoteMqOk) {
      out += " mq:";
      out += fRemoteMq.c_str();
      out += "=ok";
    } else {
      out += " mq:";
      out += fRemoteMq.c_str();
      out += "=down";
    }
  }

  return out;
}

//------------------------------------------------------------------------------
// Apply master configuration
//------------------------------------------------------------------------------
bool
Master::ApplyMasterConfig(std::string& stdOut, std::string& stdErr,
                          Transition::Type transitiontype)
{
  if (fThisHost == fMasterHost) {
    // We are the master and we broadcast every configuration change
    gOFS->ObjectManager.EnableBroadCast(true);

    if (!CreateStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE)) {
      return false;
    }
  } else {
    // We are the slave and we just listen and don't broad cast anythiing
    gOFS->ObjectManager.EnableBroadCast(false);

    if (!RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE)) {
      return false;
    }
  }

  return Activate(stdOut, stdErr, transitiontype);
}

//------------------------------------------------------------------------------
// Activate
//------------------------------------------------------------------------------
bool
Master::Activate(std::string& stdOut, std::string& stdErr, int transitiontype)
{
  // Change the configuration directory
  if (fMasterHost == fThisHost) {
    gOFS->MgmConfigDir.replace(fRemoteHost, fThisHost);
    stdOut += "configdir=";
    stdOut += gOFS->MgmConfigDir.c_str();
    stdOut += " activating master=";
    stdOut += fThisHost.c_str();
  } else {
    gOFS->MgmConfigDir.replace(fThisHost, fRemoteHost);
    stdOut += "configdir=";
    stdOut += gOFS->MgmConfigDir.c_str();
    stdOut += " activating master=";
    stdOut += fRemoteHost.c_str();
  }

  MasterLog(eos_static_notice(stdOut.c_str()));
  gOFS->ConfEngine->SetConfigDir(gOFS->MgmConfigDir.c_str());

  if (transitiontype != Transition::Type::kSlaveToMaster) {
    // Load the master's default configuration if this is not a transition
    if ((transitiontype != Transition::Type::kMasterToMasterRO)
        && (transitiontype != Transition::Type::kMasterROToSlave)) {
      if (gOFS->MgmConfigAutoLoad.length()) {
        MasterLog(eos_static_info("autoload config=%s",
                                  gOFS->MgmConfigAutoLoad.c_str()));
        std::string configenv = gOFS->MgmConfigAutoLoad.c_str();
        XrdOucString stdErr = "";
        // Take care of setting the config engine for FsView to null while
        // applying the config otherwise we deadlock since the FsView will
        // try to set config keys
        eos::mgm::ConfigResetMonitor fsview_cfg_reset_monitor;

        if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr)) {
          MasterLog(eos_static_crit("Unable to auto-load config %s - fix your "
                                    "configuration file!", gOFS->MgmConfigAutoLoad.c_str()));
          MasterLog(eos_static_crit("%s", stdErr.c_str()));
          return false;
        } else {
          MasterLog(eos_static_info("Successful auto-load config %s",
                                    gOFS->MgmConfigAutoLoad.c_str()));
        }
      }
    }

    // Invoke master to ro-master transition
    if (transitiontype == Transition::Type::kMasterToMasterRO) {
      MasterLog(eos_static_notice("Doing Master=>Master-RO transition"));

      if (!Master2MasterRO()) {
        return false;
      }
    }

    // Invoke ro-master to slave transition
    if (transitiontype == Transition::Type::kMasterROToSlave) {
      MasterLog(eos_static_notice("Doing Master-RO=>Slave transition"));

      if (!MasterRO2Slave()) {
        return false;
      }
    }
  } else {
    // Store the current configuration to the default location
    if (!gOFS->ConfEngine->AutoSave()) {
      return false;
    }

    // Invoke a slave to master transition
    MasterLog(eos_static_notice("Doing Slave=>Master transition"));

    if (!Slave2Master()) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Set transition for instance
//------------------------------------------------------------------------------
bool
Master::SetMasterId(const std::string& hostname, int port,
                    std::string& err_msg)
{
  Transition::Type transitiontype = Transition::Type::kMasterToMaster;

  if (fRunningState == Run::State::kIsNothing) {
    MasterLog(eos_static_err("unable to change master/slave configuration - "
                             "node is in invalid state after a failed transition"));
    err_msg += "error: unable to change master/slave configuration - node is "
               "in invalid state after a failed transition";
    return false;
  }

  if ((hostname != getenv("EOS_MGM_MASTER1")) &&
      (hostname != getenv("EOS_MGM_MASTER2"))) {
    err_msg += "error: invalid master name specified (/etc/sysconfig/eos:"
               "EOS_MGM_MASTER1,EOS_MGM_MASTER2)\n";
    return false;
  }

  if ((fMasterHost == fThisHost)) {
    if ((hostname != fThisHost.c_str())) {
      if (fRunningState == Run::State::kIsRunningMaster) {
        transitiontype = Transition::Type::kMasterToMasterRO;
      } else {
        MasterLog(eos_static_err("invalid master/slave transition requested - "
                                 "we are not a running master"));
        err_msg += "invalid master/slave transition requested - "
                   "we are not a running master\n";
        return false;
      }
    } else {
      transitiontype = Transition::Type::kMasterToMaster;
      MasterLog(eos_static_err("invalid master/master transition requested - "
                               "we are  a running master"));
      err_msg +=
        "invalid master/master transition requested - we are a running master\n";
      return false;
    }
  } else {
    if (fRunningState == Run::State::kIsReadOnlyMaster) {
      transitiontype = Transition::Type::kMasterROToSlave;
    } else {
      if (fRunningState != Run::State::kIsRunningSlave) {
        MasterLog(eos_static_err("invalid master/slave transition requested - "
                                 "we are not a running ro-master or we are already a slave"));
        err_msg += "invalid master/slave transition requested - we are not a "
                   "running ro-master or we are already a slave\n";
        return false;
      }
    }
  }

  if (hostname == fThisHost.c_str()) {
    // Check if the remote machine is running as the master
    if (fRemoteMasterRW) {
      err_msg += "error: the remote machine <";
      err_msg += fRemoteHost.c_str();
      err_msg += "> is still running as a RW master\n";
      return false;
    }

    if (fMasterHost.length() && (fMasterHost != fThisHost)) {
      // Slave to master transition
      transitiontype = Transition::Type::kSlaveToMaster;
    }
  }

  XrdOucString lOldMaster = fMasterHost;
  std::string out_msg;
  fMasterHost = hostname.c_str();
  bool arc = ApplyMasterConfig(out_msg, err_msg, transitiontype);

  // Set back to the previous master
  if (!arc) {
    fMasterHost = lOldMaster;

    // Put back the old MGM configuration status file
    if (fThisHost == fMasterHost) {
      // We are the master and we broadcast every configuration change
      gOFS->ObjectManager.EnableBroadCast(true);

      if (!CreateStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE)) {
        return false;
      }
    } else {
      // We are the slave and we just listen and don't broad cast anythiing
      gOFS->ObjectManager.EnableBroadCast(false);

      if (!RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE)) {
        return false;
      }
    }
  }

  return arc;
}

//------------------------------------------------------------------------------
// Do slave to master transition
//------------------------------------------------------------------------------
bool
Master::Slave2Master()
{
  eos_alert("msg=\"slave to master transition\"");
  fRunningState = Run::State::kIsTransition;
  // This will block draining/balancing for the next hour!!!
  f2MasterTransitionTime = time(nullptr);
  // This call transforms the namespace following slave into a master in RW mode
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
  eos::common::ShellCmd
  scmd1(fHasSystemd ?
        "systemctl status eos@sync && systemctl stop eos@sync"
        :
        "service eos status sync && service eos stop sync");
  eos::common::cmd_status rc = scmd1.wait(30);

  if (rc.exit_code) {
    if (rc.exit_code == -1) {
      MasterLog(
        eos_warning("system command failed due to memory pressure - cannot check the sync service"));
    }

    if (rc.exit_code == 2) {
      MasterLog(eos_warning("sync service was already stopped"));
    }

    if (rc.exit_code == 1) {
      MasterLog(eos_warning("sync service was dead"));
    }

    MasterLog(eos_crit("slave=>master transition aborted since sync was down"));
    fRunningState = Run::State::kIsNothing;
    eos::common::ShellCmd
    scmd2(fHasSystemd ?
          "systemctl start eos@sync"
          :
          "service eos start sync");
    rc = scmd2.wait(30);

    if (rc.exit_code) {
      MasterLog(eos_warning("failed to start sync service"));
    }

    fRunningState = Run::State::kIsRunningSlave;
    return false;
  }

  // If possible evaluate if local and remote master files are in sync ...
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

  if (!stat(gOFS->MgmNsFileChangeLogFile.c_str(), &buf)) {
    size_local_file_changelog = buf.st_size;
  } else {
    MasterLog(eos_crit("slave=>master transition aborted since we cannot stat "
                       "our own slave file-changelog-file"));
    fRunningState = Run::State::kIsRunningSlave;
    return false;
  }

  if (!stat(gOFS->MgmNsDirChangeLogFile.c_str(), &buf)) {
    size_local_dir_changelog = buf.st_size;
  } else {
    MasterLog(eos_crit("slave=>master transition aborted since we cannot stat "
                       "our own slave dir-changelog-file"));
    fRunningState = Run::State::kIsRunningSlave;
    return false;
  }

  size_t n_wait = 0;
  // Wait that the follower reaches the offset seen now
  auto chlog_file_svc = dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

  if (chlog_file_svc) {
    while (chlog_file_svc->getFollowOffset() <
           (uint64_t)size_local_file_changelog) {
      std::this_thread::sleep_for(std::chrono::seconds(5));
      eos_static_info("msg=\"waiting for the namespace to reach the follow "
                      "point\" is-offset=%llu follow-offset=%llu",
                      chlog_file_svc->getFollowOffset(),
                      (uint64_t) size_local_file_changelog);

      if (n_wait > 12) {
        MasterLog(eos_crit("slave=>master transition aborted since we didn't "
                           "reach the follow point in 60 seconds - you may retry"));
        fRunningState = Run::State::kIsRunningSlave;
        return false;
      }

      n_wait++;
    }
  }

  bool syncok = false;

  if (HostCheck(fRemoteHost.c_str(), 1096)) {
    MasterLog(eos_info("remote-sync host=%s:1096 is reachable",
                       fRemoteHost.c_str()));
    syncok = true;
  } else {
    MasterLog(eos_info("remote-sync host=%s:1096 is down", fRemoteHost.c_str()));
  }

  if (syncok) {
    XrdCl::URL remoteSyncUrl(remoteSyncUrlString);
    XrdCl::FileSystem FsSync(remoteSyncUrl);
    XrdCl::StatInfo* sinfo = nullptr;

    // Stat the two remote changelog files
    if (FsSync.Stat(rfclf, sinfo, 5).IsOK()) {
      size_remote_file_changelog = sinfo->GetSize();
      delete sinfo;
      sinfo = 0;
    } else {
      if (sinfo) {
        delete sinfo;
        sinfo = 0;
      }
    }

    if (FsSync.Stat(rdclf, sinfo, 5).IsOK()) {
      size_remote_dir_changelog = sinfo->GetSize();
      delete sinfo;
      sinfo = nullptr;
    } else {
      if (sinfo) {
        delete sinfo;
        sinfo = nullptr;
      }
    }

    if (size_remote_file_changelog != size_local_file_changelog) {
      MasterLog(eos_crit("slave=>master transition aborted - file changelog "
                         "synchronization problem found - path=%s "
                         "remote-size=%llu local-size=%llu", rfclf.c_str(),
                         size_remote_file_changelog, size_local_file_changelog));
      fRunningState = Run::State::kIsRunningSlave;
      return false;
    }

    if (size_remote_dir_changelog != size_local_dir_changelog) {
      MasterLog(eos_crit("slave=>master transition aborted - dir changelog "
                         "synchronization problem found - path=%s "
                         "remote-size=%llu local-size=%llu", rdclf.c_str(),
                         size_remote_dir_changelog, size_local_dir_changelog));
      fRunningState = Run::State::kIsRunningSlave;
      return false;
    }
  }

  // Make a backup of the new target master file
  XrdOucString NsFileChangeLogFileCopy = fileSettings["changelog_path"].c_str();
  NsFileChangeLogFileCopy += ".";
  NsFileChangeLogFileCopy += (int) time(nullptr);
  XrdOucString NsDirChangeLogFileCopy = contSettings["changelog_path"].c_str();
  NsDirChangeLogFileCopy += ".";
  NsDirChangeLogFileCopy += (int) time(nullptr);

  if (!::stat(fileSettings["changelog_path"].c_str(), &buf)) {
    if (::rename(fileSettings["changelog_path"].c_str(),
                 NsFileChangeLogFileCopy.c_str())) {
      MasterLog(eos_crit("failed to rename %s=>%s errno=%d",
                         gOFS->MgmNsFileChangeLogFile.c_str(),
                         NsFileChangeLogFileCopy.c_str(), errno));
      fRunningState = Run::State::kIsNothing;
      return false;
    }
  }

  if (!::stat(contSettings["changelog_path"].c_str(), &buf)) {
    if (::rename(contSettings["changelog_path"].c_str(),
                 NsDirChangeLogFileCopy.c_str())) {
      MasterLog(eos_crit("failed to rename %s=>%s errno=%d",
                         gOFS->MgmNsDirChangeLogFile.c_str(),
                         NsDirChangeLogFileCopy.c_str(), errno));
      fRunningState = Run::State::kIsNothing;
      return false;
    }
  }

  gOFS->MgmNsFileChangeLogFile = fileSettings["changelog_path"].c_str();
  gOFS->MgmNsDirChangeLogFile = contSettings["changelog_path"].c_str();

  try {
    MasterLog(eos_info("msg=\"invoking slave=>master transition\""));
    auto* eos_chlog_dirsvc =
      dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);

    if (eos_chlog_dirsvc) {
      eos_chlog_dirsvc->slave2Master(contSettings);
    }

    auto* eos_chlog_filesvc =
      dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

    if (eos_chlog_filesvc) {
      eos_chlog_filesvc->slave2Master(fileSettings);
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    MasterLog(eos_crit("slave=>master transition returned ec=%d %s",
                       e.getErrno(), e.getMessage().str().c_str()));
    fRunningState = Run::State::kIsNothing;
    eos::common::ShellCmd
    scmd3(fHasSystemd ? "systemctl start eos@sync" :
          "service eos start sync");
    rc = scmd3.wait(30);

    if (rc.exit_code) {
      MasterLog(eos_warning("slave=>master transition - sync didnt' start"));
    }

    return false;
  }

  fRunningState = Run::State::kIsRunningMaster;
  eos::common::ShellCmd
  scmd3(fHasSystemd ? "systemctl start eos@sync" :
        "service eos start sync");
  rc = scmd3.wait(30);

  if (rc.exit_code) {
    MasterLog(eos_warning("failed to start sync service - %d", rc.exit_code));
    MasterLog(eos_crit("slave=>master transition aborted since sync didn't start"));

    try {
      gOFS->eosDirectoryService->finalize();
      gOFS->eosFileService->finalize();
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      MasterLog(eos_crit("slave=>master finalize returned ec=%d %s",
                         e.getErrno(), e.getMessage().str().c_str()));
    }

    fRunningState = Run::State::kIsNothing;
    return false;
  }

  // get eossync up if it is not up
  eos::common::ShellCmd
  scmd4(". /etc/sysconfig/eos; service eossync status || service eossync start ");
  rc = scmd4.wait(30);

  if (rc.exit_code) {
    MasterLog(eos_warning("failed to start eossync services - %d", rc.exit_code));
  }

  UnBlockCompacting();
  // Broadcast the new manager node variable
  MasterLog(eos_info("msg=\"registering new manager to nodes\""));
  FsView::gFsView.BroadcastMasterId(GetMasterId());
  // Re-start the recycler thread
  gOFS->Recycler->Start();
  eos_alert("msg=\"running as master-rw\"");
  MasterLog(eos_notice("running in master mode"));
  return true;
}

//------------------------------------------------------------------------------
// Master to ro-master transition
//------------------------------------------------------------------------------
bool
Master::Master2MasterRO()
{
  eos_alert("msg=\"rw-master to ro-master transition\"");
  fRunningState = Run::State::kIsTransition;
  // Convert the RW namespace into a read-only namespace
  // Wait that compacting is finished and block any further compacting
  WaitCompactingFinished();
  auto* eos_chlog_dirsvc =
    dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);
  auto* eos_chlog_filesvc =
    dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

  if (eos_chlog_dirsvc && eos_chlog_filesvc) {
    try {
      eos_chlog_dirsvc->makeReadOnly();
      eos_chlog_filesvc->makeReadOnly();
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      MasterLog(eos_crit("master=>slave transition returned ec=%d %s",
                         e.getErrno(), e.getMessage().str().c_str()));
      fRunningState = Run::State::kIsNothing;
      return false;
    }
  }

  // Stop the recycler thread
  gOFS->Recycler->Stop();
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  fRunningState = Run::State::kIsReadOnlyMaster;
  eos_alert("msg=\"running as master-ro\"");
  MasterLog(eos_notice("running in RO master mode"));
  return true;
}

//------------------------------------------------------------------------------
// Transform a running ro-master into a slave following a remote master
//------------------------------------------------------------------------------
bool
Master::MasterRO2Slave()
{
  eos_alert("msg=\"ro-master to slave transition\"");
  // This call transforms a running ro-master into a slave following
  // a remote master
  fRunningState = Run::State::kIsTransition;
  {
    // Be aware of interference with the heart beat daemon (which does not
    // touch a generic stall yet)
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    // Remove redirects
    Access::gRedirectionRules.erase(std::string("w:*"));
    Access::gRedirectionRules.erase(std::string("ENOENT:*"));
    Access::gStallRules.erase(std::string("w:*"));
    Access::gStallWrite = false;

    // Put an appropriate stall
    if (fRemoteMasterOk) {
      Access::gStallRules[std::string("w:*")] = "60";
      Access::gStallRules[std::string("*")] = "100";
      Access::gStallGlobal = true;
    } else {
      Access::gStallRules[std::string("w:*")] = "60";
      Access::gStallRules[std::string("*")] = "60";
      Access::gStallGlobal = true;
    }
  }
  {
    // Convert the namespace
    eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

    // Take the whole namespace down
    try {
      if (gOFS->eosFsView) {
        gOFS->eosFsView->finalize();
        gOFS->eosFsView = nullptr;
      }

      if (gOFS->eosContainerAccounting) {
        gOFS->eosContainerAccounting = nullptr;
      }

      if (gOFS->eosSyncTimeAccounting) {
        gOFS->eosSyncTimeAccounting = nullptr;
      }

      if (gOFS->eosView) {
        gOFS->eosView->finalize();
        gOFS->eosView = nullptr;
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      MasterLog(eos_crit("master-ro=>slave namespace shutdown returned ec=%d %s",
                         e.getErrno(), e.getMessage().str().c_str()));
    };

    // Boot it from scratch
    if (!BootNamespace()) {
      fRunningState = Run::State::kIsNothing;
      return false;
    }
  }

  // Reload the configuration to get the proper quota nodes
  if (gOFS->MgmConfigAutoLoad.length()) {
    MasterLog(eos_static_info("autoload config=%s",
                              gOFS->MgmConfigAutoLoad.c_str()));
    std::string configenv = gOFS->MgmConfigAutoLoad.c_str();
    XrdOucString stdErr = "";

    if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr)) {
      MasterLog(eos_static_crit("Unable to auto-load config %s - fix your "
                                "configuration file!", gOFS->MgmConfigAutoLoad.c_str()));
      MasterLog(eos_static_crit("%s", stdErr.c_str()));
      return false;
    } else {
      MasterLog(eos_static_info("Successful auto-load config %s",
                                gOFS->MgmConfigAutoLoad.c_str()));
    }
  }

  if (gOFS->mNamespaceState == NamespaceState::kBooted) {
    // Start the file view loader thread
    MasterLog(eos_info("msg=\"starting file view loader thread\""));
    pthread_t tid;

    if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView,
                           static_cast<void*>(gOFS), 0, "File View Loader"))) {
      MasterLog(eos_crit("cannot start file view loader"));
      fRunningState = Run::State::kIsNothing;
      return false;
    }
  } else {
    MasterLog(eos_crit("msg=\"don't want to start file view loader for a "
                       "namespace in bootfailure state\""));
    fRunningState = Run::State::kIsNothing;
    return false;
  }

  fRunningState = Run::State::kIsRunningSlave;
  eos_alert("msg=\"running as slave\"");
  MasterLog(eos_notice("running in slave mode"));
  return true;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Master::~Master()
{
  if (fThread) {
    XrdSysThread::Cancel(fThread);
    XrdSysThread::Join(fThread, nullptr);
    fThread = 0;
  }

  if (fCompactingThread) {
    XrdSysThread::Cancel(fCompactingThread);
    XrdSysThread::Join(fCompactingThread, nullptr);
    fCompactingThread = 0;
  }

  if (fDevNull) {
    close(fDevNull);
    fDevNull = 0;
  }

  if (fDevNullLogger) {
    delete fDevNullLogger;
    fDevNullLogger = nullptr;
  }

  if (fDevNullErr) {
    delete fDevNullErr;
    fDevNullErr = nullptr;
  }
}

//------------------------------------------------------------------------------
// Boot namespace
//------------------------------------------------------------------------------
bool
Master::BootNamespace()
{
  using eos::common::PluginManager;

  if (IsMaster()) {
    eos_alert("msg=\"running boot sequence (as master)\"");
  } else {
    eos_alert("msg=\"running boot sequence (as slave)\"");
  }

  PluginManager& pm = PluginManager::GetInstance();
  PF_PlatformServices& pm_svc = pm.GetPlatformServices();
  pm_svc.invokeService = &XrdMgmOfs::DiscoverPlatformServices;
  gOFS->namespaceGroup.reset(static_cast<INamespaceGroup*>
                             (pm.CreateObject("NamespaceGroup")));
  gOFS->NsInQDB = !gOFS->namespaceGroup->isInMemory();
  //----------------------------------------------------------------------------
  // Collect namespace options, and initialize namespace group
  //----------------------------------------------------------------------------
  std::map<std::string, std::string> namespaceConfig;
  std::string err;

  if (gOFS->NsInQDB) {
    std::string instance_id =
      SSTR(gOFS->MgmOfsInstanceName << ":" << gOFS->ManagerPort);
    namespaceConfig["queue_path"] = "/var/eos/ns-queue/";
    namespaceConfig["qdb_cluster"] = gOFS->mQdbCluster;
    namespaceConfig["qdb_password"] = gOFS->mQdbPassword;
    namespaceConfig["qdb_flusher_md"] = SSTR(instance_id << "_md");
    namespaceConfig["qdb_flusher_quota"] = SSTR(instance_id << "_quota");

    // Forbit running as slave with the QDB namespace when the legacy master-
    // slave setup is still enabled
    if (IsMaster() == false) {
      eos_crit("%s", "msg=\"not allowed to run as slave with QDB namespace "
               "while the legacy HA setup is still enabled\"");
      return false;
    }
  }

  if (!gOFS->namespaceGroup->initialize(&gOFS->eosViewRWMutex,
                                        namespaceConfig, err)) {
    eos_err("msg=\"could not initialize namespace group, err: %s\"", err.c_str());
    return false;
  }

  //----------------------------------------------------------------------------
  // Fetch all required services out of namespace group
  //----------------------------------------------------------------------------
  gOFS->eosDirectoryService = gOFS->namespaceGroup->getContainerService();
  gOFS->eosFileService = gOFS->namespaceGroup->getFileService();
  gOFS->eosView = gOFS->namespaceGroup->getHierarchicalView();
  gOFS->eosFsView = gOFS->namespaceGroup->getFilesystemView();

  if (!gOFS->eosDirectoryService || ! gOFS->eosFileService ||
      !gOFS->eosView || !gOFS->eosFsView) {
    MasterLog(eos_err("namespace implementation could not be loaded using "
                      "the provided library plugin"));
    return false;
  }

  // For qdb namespace enable by default all the views
  if (gOFS->NsInQDB ||
      (getenv("EOS_NS_ACCOUNTING") &&
       ((std::string(getenv("EOS_NS_ACCOUNTING")) == "1") ||
        (std::string(getenv("EOS_NS_ACCOUNTING")) == "yes")))) {
    eos_alert("msg=\"enabling recursive size accounting ...\"");
    gOFS->eosContainerAccounting =
      gOFS->namespaceGroup->getContainerAccountingView();

    if (!gOFS->eosContainerAccounting) {
      eos_err("msg=\"namespace implemetation does not provide ContainerAccounting"
              " class\"");
      return false;
    }
  }

  if (gOFS->NsInQDB ||
      (getenv("EOS_SYNCTIME_ACCOUNTING") &&
       ((std::string(getenv("EOS_SYNCTIME_ACCOUNTING")) == "1") ||
        (std::string(getenv("EOS_SYNCTIME_ACCOUNTING")) == "yes")))) {
    eos_alert("msg=\"enabling sync time propagation ...\"");
    gOFS->eosSyncTimeAccounting = gOFS->namespaceGroup->getSyncTimeAccountingView();

    if (!gOFS->eosSyncTimeAccounting) {
      eos_err("msg=\"namespace implemetation does not provide SyncTimeAccounting"
              " class\"");
      return false;
    }
  }

  std::map<std::string, std::string> fileSettings;
  std::map<std::string, std::string> contSettings;

  if (!IsMaster()) {
    contSettings["slave_mode"] = "true";
    contSettings["poll_interval_us"] = "1000";
    contSettings["auto_repair"] = "false";
    fileSettings["slave_mode"] = "true";
    fileSettings["poll_interval_us"] = "1000";
    fileSettings["auto_repair"] = "false";
  }

  if (!gOFS->NsInQDB) {
    // Build local path of the file and directory changelogs
    std::ostringstream oss;
    oss << gOFS->MgmMetaLogDir.c_str() << "/directories."
        << fMasterHost.c_str() << ".mdlog";
    contSettings["changelog_path"] = oss.str();
    gOFS->MgmNsDirChangeLogFile = oss.str().c_str();
    oss.str("");
    oss << gOFS->MgmMetaLogDir.c_str() << "/files."
        << fMasterHost.c_str() << ".mdlog";
    fileSettings["changelog_path"] = oss.str();
    gOFS->MgmNsFileChangeLogFile = oss.str().c_str();
    // Clear the qdb cluster name for safety, since it's used in the code as
    // a switch for in-memory or qdb namespace implementation
    gOFS->mQdbCluster.clear();
  } else {
    if (gOFS->mQdbCluster.empty()) {
      eos_alert("msg=\"mgmofs.qdbcluster configuration is missing\"");
      MasterLog(eos_err("msg=\"mgmofs.qdbcluster configuration is missing\""));
      return false;
    } else {
      contSettings = namespaceConfig;
      fileSettings = namespaceConfig;
    }
  }

  time_t tstart = time(nullptr);

  try {
    gOFS->eosDirectoryService->configure(contSettings);
    gOFS->eosFileService->configure(fileSettings);
    gOFS->eosFsView->configure(fileSettings);
    gOFS->eosView->configure(contSettings);

    if (IsMaster()) {
      MasterLog(eos_notice("eos directory view configure started as master"));
    } else {
      MasterLog(eos_notice("eos directory view configure started as slave"));
    }

    // This is only done for the ChangeLog implementation
    auto* eos_chlog_dirsvc =
      dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);
    auto* eos_chlog_filesvc =
      dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

    if (eos_chlog_filesvc && eos_chlog_dirsvc) {
      if (!IsMaster()) {
        // slave needs access to the namespace lock
        eos_chlog_filesvc->setSlaveLock(&fNsLock);
        eos_chlog_dirsvc->setSlaveLock(&fNsLock);
      }

      eos_chlog_filesvc->clearWarningMessages();
      eos_chlog_dirsvc->clearWarningMessages();
    }

    gOFS->eosFileService->setQuotaStats(gOFS->eosView->getQuotaStats());
    gOFS->eosDirectoryService->setQuotaStats(gOFS->eosView->getQuotaStats());
    gOFS->eosView->getQuotaStats()->registerSizeMapper(Quota::MapSizeCB);
    gOFS->eosView->initialize1();
    time_t tstop = time(nullptr);
    // Add boot errors to the master log
    std::string out;
    GetLog(out);
    gOFS->mBootContainerId = gOFS->eosDirectoryService->getFirstFreeId();
    MasterLog(eos_notice("eos directory view configure stopped after %d seconds",
                         (tstop - tstart)));

    gOFS->namespaceGroup->startCacheRefreshListener();

    if (!IsMaster()) {
      fRunningState = Run::State::kIsRunningSlave;
      MasterLog(eos_notice("running in slave mode"));
    } else {
      fRunningState = Run::State::kIsRunningMaster;
      MasterLog(eos_notice("running in master mode"));
    }

    return true;
  } catch (eos::MDException& e) {
    time_t tstop = time(nullptr);
    // Add boot errors to the master log
    std::string out;
    GetLog(out);
    MasterLog(eos_crit("eos view initialization failed after %d seconds",
                       (tstop - tstart)));
    errno = e.getErrno();
    MasterLog(eos_crit("initialization returned ec=%d %s", e.getErrno(),
                       e.getMessage().str().c_str()));
    return false;
  } catch (const std::runtime_error& qdb_err) {
    MasterLog(eos_crit("eos view initialization failed, unable to connect to "
                       "QuarkDB cluster, reason: %s", qdb_err.what()));
    return false;
  }
}

//------------------------------------------------------------------------------
// Signal the remote master to bounce all requests to us
//------------------------------------------------------------------------------
void
Master::SignalRemoteBounceToMaster()
{
  std::string remoteMgmUrlString = "root://";
  remoteMgmUrlString += fRemoteHost.c_str();
  remoteMgmUrlString += ":1094";
  remoteMgmUrlString += "//dummy";
  std::string remoteMgmHostPort = fRemoteHost.c_str();
  remoteMgmHostPort += ":1094";
  // TODO: add signals for remote slave(-only) machiens
  std::string signalbounce = "/?mgm.pcmd=mastersignalbounce";
  XrdCl::URL remoteMgmUrl(remoteMgmUrlString);
  XrdCl::FileSystem FsMgm(remoteMgmUrl);
  XrdCl::Buffer qbuffer;
  qbuffer.FromString(signalbounce);
  XrdCl::Buffer* rbuffer = nullptr;

  if (FsMgm.Query(XrdCl::QueryCode::OpaqueFile, qbuffer, rbuffer).IsOK()) {
    MasterLog(
      eos_info("msg=\"signalled successfully remote master to redirect\""));
  } else
    MasterLog(eos_warning("failed to signal remote redirect to %s",
                          remoteMgmUrlString.c_str()));

  if (rbuffer) {
    delete rbuffer;
    rbuffer = nullptr;
  }
}

//------------------------------------------------------------------------------
// Signal the remote master to reload its namespace
//------------------------------------------------------------------------------
void
Master::SignalRemoteReload(bool compact_files, bool compact_directories)
{
  std::string remoteMgmUrlString = "root://";
  remoteMgmUrlString += fRemoteHost.c_str();
  remoteMgmUrlString += ":1094";
  remoteMgmUrlString += "//dummy";
  std::string remoteMgmHostPort = fRemoteHost.c_str();
  remoteMgmHostPort += ":1094";
  // TODO: add signals for remote slave(-only) machines
  std::string signalreload = "/?mgm.pcmd=mastersignalreload";

  if (compact_files) {
    signalreload += "&compact.files=1";
  } else if (compact_directories) {
    signalreload += "&compact.directories=1";
  }

  XrdCl::URL remoteMgmUrl(remoteMgmUrlString);
  XrdCl::FileSystem FsMgm(remoteMgmUrl);
  XrdCl::Buffer qbuffer;
  qbuffer.FromString(signalreload);
  XrdCl::Buffer* rbuffer = nullptr;

  if (FsMgm.Query(XrdCl::QueryCode::OpaqueFile, qbuffer, rbuffer).IsOK()) {
    MasterLog(eos_info("msg=\"signalled remote master to reload\""));
  } else
    MasterLog(eos_warning("failed to signal remote reload to %s",
                          remoteMgmUrlString.c_str()));

  if (rbuffer) {
    delete rbuffer;
    rbuffer = nullptr;
  }
}

//------------------------------------------------------------------------------
// Tag namespace inodes
//------------------------------------------------------------------------------
void
Master::TagNamespaceInodes()
{
  struct stat statf;
  struct stat statd;
  MasterLog(eos_info("msg=\"tag namespace inodes\""));

  if ((!::stat(gOFS->MgmNsFileChangeLogFile.c_str(), &statf)) &&
      (!::stat(gOFS->MgmNsDirChangeLogFile.c_str(), &statd))) {
    fFileNamespaceInode = statf.st_ino;
    fDirNamespaceInode = statd.st_ino;
  } else {
    MasterLog(eos_warning("stat of namespace files failed with errno=%d", errno));
  }
}

//------------------------------------------------------------------------------
// Wait that local/remote namespace files are synced. This routine is called
// by a slave when it got signalled to reload the namespace.
//------------------------------------------------------------------------------
bool
Master::WaitNamespaceFilesInSync(bool wait_files, bool wait_directories,
                                 unsigned int timeout)
{
  time_t starttime = time(nullptr);
  // If possible evaluate if local and remote master files are in sync ...
  MasterLog(eos_info("msg=\"check ns file synchronization\""));
  off_t size_local_file_changelog = 0;
  off_t size_local_dir_changelog = 0;
  off_t size_remote_file_changelog = 0;
  off_t size_remote_dir_changelog = 0;
  unsigned long long lFileNamespaceInode = 0;
  unsigned long long lDirNamespaceInode = 0;
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

  if (HostCheck(fRemoteHost.c_str(), 1096)) {
    syncok = true;
    MasterLog(eos_info("remote-sync host=%s:1096 is reachable",
                       fRemoteHost.c_str()));
  } else {
    MasterLog(eos_info("remote-sync host=%s:1096 is down", fRemoteHost.c_str()));
  }

  if (syncok) {
    // Check once the remote size
    XrdCl::URL remoteSyncUrl(remoteSyncUrlString);
    XrdCl::FileSystem FsSync(remoteSyncUrl);
    XrdCl::StatInfo* sinfo = nullptr;

    // stat the two remote changelog files
    if (FsSync.Stat(rfclf, sinfo, 5).IsOK()) {
      if (sinfo) {
        size_remote_file_changelog = sinfo->GetSize();
        delete sinfo;
        sinfo = nullptr;
      }
    } else {
      if (sinfo) {
        delete sinfo;
        sinfo = nullptr;
      }

      MasterLog(eos_crit("remote stat failed for %s", rfclf.c_str()));
      return false;
    }

    if (FsSync.Stat(rdclf, sinfo, 5).IsOK()) {
      if (sinfo) {
        size_remote_dir_changelog = sinfo->GetSize();
        delete sinfo;
        sinfo = nullptr;
      }
    } else {
      if (sinfo) {
        delete sinfo;
        sinfo = nullptr;
      }

      MasterLog(eos_crit("remote stat failed for %s", rdclf.c_str()));
      return false;
    }

    MasterLog(eos_info("remote files file=%llu dir=%llu",
                       size_remote_file_changelog,
                       size_remote_dir_changelog));

    do {
      // Wait that the inode changed and then check the local size and wait,
      // that the local files is at least as big as the remote file
      if (!stat(gOFS->MgmNsFileChangeLogFile.c_str(), &buf)) {
        size_local_file_changelog = buf.st_size;
        lFileNamespaceInode = buf.st_ino;
      } else {
        MasterLog(eos_crit("local stat failed for %s",
                           gOFS->MgmNsFileChangeLogFile.c_str()));
        return false;
      }

      if (!stat(gOFS->MgmNsDirChangeLogFile.c_str(), &buf)) {
        size_local_dir_changelog = buf.st_size;
        lDirNamespaceInode = buf.st_ino;
      } else {
        MasterLog(eos_crit("local stat failed for %s",
                           gOFS->MgmNsDirChangeLogFile.c_str()));
        return false;
      }

      if ((wait_directories) && (lDirNamespaceInode == fDirNamespaceInode)) {
        // the inode didn't change yet
        if (time(nullptr) > (starttime + timeout)) {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }

        MasterLog(eos_info("waiting for 'directories' inode change %llu=>%llu ",
                           fDirNamespaceInode, lDirNamespaceInode));
        std::this_thread::sleep_for(std::chrono::seconds(10));
        continue;
      }

      if ((wait_files) && (lFileNamespaceInode == fFileNamespaceInode)) {
        // the inode didn't change yet
        if (time(nullptr) > (starttime + timeout)) {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }

        MasterLog(eos_info("waiting for 'files' inode change %llu=>%llu ",
                           fFileNamespaceInode, lFileNamespaceInode));
        std::this_thread::sleep_for(std::chrono::seconds(10));
        continue;
      }

      if (size_remote_file_changelog > size_local_file_changelog) {
        if (time(nullptr) > (starttime + timeout)) {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));
        continue;
      }

      if (size_remote_dir_changelog > size_local_dir_changelog) {
        if (time(nullptr) > (starttime + timeout)) {
          MasterLog(eos_warning("timeout occured after %u seconds", timeout));
          return false;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));
        continue;
      }

      MasterLog(eos_info("msg=\"ns files  synchronized\""));
      return true;
    } while (true);
  } else {
    MasterLog(eos_warning("msg=\"remote sync service is not ok\""));
    return false;
  }
}

//------------------------------------------------------------------------------
// Push everything to the remote master
//------------------------------------------------------------------------------
void
Master::RedirectToRemoteMaster()
{
  MasterLog(eos_info("msg=\"redirect to remote master\""));
  Access::gRedirectionRules[std::string("*")] = fRemoteHost.c_str();
  auto* eos_chlog_dirsvc =
    dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);
  auto* eos_chlog_filesvc =
    dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

  if (eos_chlog_dirsvc && eos_chlog_filesvc) {
    try {
      MasterLog(eos_info("msg=\"invoking slave shutdown\""));
      eos_chlog_dirsvc->stopSlave();
      eos_chlog_filesvc->stopSlave();
      MasterLog(eos_info("msg=\"stopped namespace following\""));
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      MasterLog(eos_crit("slave shutdown returned ec=%d %s", e.getErrno(),
                         e.getMessage().str().c_str()));
    }
  }
}

//------------------------------------------------------------------------------
// Reboot slave namespace
//------------------------------------------------------------------------------
bool
Master::RebootSlaveNamespace()
{
  fRunningState = Run::State::kIsTransition;
  gOFS->mNamespaceState = NamespaceState::kBooting;
  {
    // now convert the namespace
    eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);

    // Take the whole namespace down
    try {
      if (gOFS->eosFsView) {
        gOFS->eosFsView->finalize();
        gOFS->eosFsView = nullptr;
      }

      if (gOFS->eosContainerAccounting) {
        gOFS->eosContainerAccounting = nullptr;
      }

      if (gOFS->eosSyncTimeAccounting) {
        gOFS->eosSyncTimeAccounting = nullptr;
      }

      if (gOFS->eosView) {
        gOFS->eosView->finalize();
        gOFS->eosView = nullptr;
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      MasterLog(eos_crit("master-ro=>slave namespace shutdown returned ec=%d %s",
                         e.getErrno(), e.getMessage().str().c_str()));
    }

    // Boot it from scratch
    if (!BootNamespace()) {
      fRunningState = Run::State::kIsNothing;
      gOFS->mNamespaceState = NamespaceState::kFailed;
      return false;
    }

    gOFS->mNamespaceState = NamespaceState::kBooted;
  }

  if (gOFS->mNamespaceState == NamespaceState::kBooted) {
    // Start the file view loader thread
    MasterLog(eos_info("msg=\"starting file view loader thread\""));
    pthread_t tid;

    if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView,
                           static_cast<void*>(gOFS), 0, "File View Loader"))) {
      MasterLog(eos_crit("cannot start file view loader"));
      fRunningState = Run::State::kIsNothing;
      return false;
    }
  } else {
    MasterLog(eos_crit("msg=\"don't want to start file view loader for a "
                       "namespace in bootfailure state\""));
    fRunningState = Run::State::kIsNothing;
    return false;
  }

  {
    // Be aware of interference with the heart beat daemon
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    // Remove global redirection
    Access::gRedirectionRules.erase(std::string("*"));
  }

  fRunningState = Run::State::kIsRunningSlave;
  MasterLog(eos_notice("running in slave mode"));
  return true;
}

//------------------------------------------------------------------------------
// Start slave follower thread
//------------------------------------------------------------------------------
void
Master::StartSlaveFollower(std::string&& log_file)
{
  auto* eos_chlog_dirsvc =
    dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);
  auto* eos_chlog_filesvc =
    dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

  if (eos_chlog_dirsvc && eos_chlog_filesvc) {
    // Get change log file size
    struct stat buf;
    int retc = stat(log_file.c_str(), &buf);

    if (!retc) {
      eos_err("failed stat for file=%s - abort slave start", log_file.c_str());
      return;
    }

    eos_chlog_filesvc->startSlave();
    eos_chlog_dirsvc->startSlave();

    // wait that the follower reaches the offset seen now
    while (eos_chlog_filesvc->getFollowOffset() < (uint64_t) buf.st_size) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      eos_static_debug("msg=\"waiting for the namespace to reach the follow "
                       "point\" is-offset=%llu follow-offset=%llu",
                       eos_chlog_filesvc->getFollowOffset(), (uint64_t) buf.st_size);
    }
  }
}

//------------------------------------------------------------------------------
// Shutdown slave follower thread
//------------------------------------------------------------------------------
void
Master::ShutdownSlaveFollower()
{
  if (!gOFS->mMaster->IsMaster()) {
    // Stop the follower thread ...
    if (gOFS->eosFileService) {
      auto* eos_chlog_filesvc =
        dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

      if (eos_chlog_filesvc) {
        eos_chlog_filesvc->stopSlave();
      }
    }

    if (gOFS->eosDirectoryService) {
      auto* eos_chlog_dirsvc =
        dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);

      if (eos_chlog_dirsvc) {
        eos_chlog_dirsvc->stopSlave();
      }
    }
  }
}

//------------------------------------------------------------------------------
// Post the namespace record errors to the master changelog
//------------------------------------------------------------------------------
void
Master::GetLog(std::string& stdOut)
{
  auto* eos_chlog_dirsvc =
    dynamic_cast<eos::IChLogContainerMDSvc*>(gOFS->eosDirectoryService);
  auto* eos_chlog_filesvc =
    dynamic_cast<eos::IChLogFileMDSvc*>(gOFS->eosFileService);

  if (eos_chlog_filesvc && eos_chlog_dirsvc) {
    std::vector<std::string> file_warn = eos_chlog_filesvc->getWarningMessages();
    std::vector<std::string> directory_warn =
      eos_chlog_dirsvc->getWarningMessages();

    for (const auto& fw : file_warn) {
      MasterLog(eos_err(fw.c_str()));
    }

    for (const auto& dw : directory_warn) {
      MasterLog(eos_err(dw.c_str()));
    }

    eos_chlog_filesvc->clearWarningMessages();
    eos_chlog_dirsvc->clearWarningMessages();
  }

  stdOut = mLog;
}

EOSMGMNAMESPACE_END
