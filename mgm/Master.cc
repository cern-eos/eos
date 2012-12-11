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

/*----------------------------------------------------------------------------*/
#include "XrdNet/XrdNet.hh"
#include "XrdNet/XrdNetPeer.hh"
#ifdef NEWXRDCL
#include "XrdCl/XrdCl.hh"
#else
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdClient/XrdClientDebug.hh"
#include "mq/XrdMqClient.hh"
#endif
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

/* ------------------------------------------------------------------------- */
Master::Master() 
{
  fActivated      = false;
  fRemoteMasterOk = true;
  fRemoteMqOk     = true;
  fRemoteMasterRW = false;
  fThread         = 0;
  fRunningState   = kIsNothing;
}


bool 
Master::HostCheck(const char* hostname, int port, int timeout)
{
  // do a quick check if a host is reachable
  XrdOucString lHostName = hostname;

  int dpos;
  if ( (dpos = lHostName.find(":")) != STR_NPOS) {
    lHostName.erase(dpos);
  }

  XrdNetPeer peer;
  XrdNet net(XrdMgmOfs::eDest);
  if (net.Connect(peer, lHostName.c_str(), port,0,timeout)) {
    // send a handshake to avoid handshake error messages on server side
    unsigned int vshake[5];
    vshake[0]=vshake[1]=vshake[2]=0;
    vshake[3]=htonl(4);
    vshake[4]=htonl(2012);
    write(peer.fd,&vshake[0], 20);
    close(peer.fd);
    return true;
  } else {
    return false;
  }
}

/* ------------------------------------------------------------------------- */
bool
Master::Init()
{
  // define our role master/slave
  struct stat buf;
  fThisHost = gOFS->HostName;
  fNsLock.Init(&(gOFS->eosViewRWMutex)); // fill the namespace mutex
  
  if ( (!getenv("EOS_MGM_MASTER1")) ||
       (!getenv("EOS_MGM_MASTER2")) ) {
    eos_crit("EOS_MGM_MASTER1 and EOS_MGM_MASTER2 variables are undefined");
    return false;
  }

  if (fThisHost == getenv("EOS_MGM_MASTER1")) {
    fRemoteHost = getenv("EOS_MGM_MASTER2");
  } else {
    fRemoteHost = getenv("EOS_MGM_MASTER1");
  }

  if (fThisHost == fRemoteHost) {
    // no master slave configuration ... also fine
    return true;
  }

  XrdOucString lMaster1MQ;
  XrdOucString lMaster2MQ;
  
  // define the MQ hosts
  if (getenv("EOS_MQ_MASTER1")) {
    lMaster1MQ = getenv("EOS_MQ_MASTER1");
  } else {
    lMaster1MQ = getenv("EOS_MGM_MASTER1");
    int dpos = lMaster1MQ.find(":");
    if (dpos != STR_NPOS) {
      lMaster1MQ.erase(dpos);
    }
    lMaster1MQ+= ":1097";
  }

  if (getenv("EOS_MQ_MASTER2")) {
    lMaster2MQ = getenv("EOS_MQ_MASTER2");
  } else {
    lMaster2MQ = getenv("EOS_MGM_MASTER2");
    int dpos = lMaster2MQ.find(":");
    if (dpos != STR_NPOS) {
      lMaster2MQ.erase(dpos);
    }
    lMaster2MQ+= ":1097";
  }
  
  // define which MQ is remote
  if (lMaster1MQ.find(fThisHost)!= STR_NPOS) {
    fRemoteMq = lMaster2MQ;
  } else {
    fRemoteMq = lMaster1MQ;
  }

  if (!::stat(EOSMGMMASTER_SUBSYS_RW_LOCKFILE, &buf)) {
    fMasterHost = fThisHost;
  } else {
    fMasterHost = fRemoteHost;
  }
  
  // start the heartbeat thread ...
  XrdSysThread::Run(&fThread, Master::StaticHeartBeat, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Master HeartBeat Thread");

  // get sync up if it is not up
  int rc = system("service eos status sync || service eos start sync");
  if (WEXITSTATUS(rc)) {
    eos_crit("failed to start sync service");
    return false;
  }
  return true;
}  

bool 
Master::EnableHeartBeat()
{
  //----------------------------------------------------------------
  //! start's the heart beat thread if not already running
  //----------------------------------------------------------------
  XrdSysTimer sleeper;
  sleeper.Snooze(1);

  if (!fThread) {
    XrdSysThread::Run(&fThread, Master::StaticHeartBeat, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "Master HeartBeat Thread");
    return true;
  }
  return false;
}

bool 
Master::DisableHeartBeat()
{
  //----------------------------------------------------------------
  //! stop's the heart beat thread if running
  //----------------------------------------------------------------

  if (fThread) {
    XrdSysThread::Cancel(fThread);
    XrdSysThread::Join(fThread,0);
    fThread = 0;
    return true;
  }
  return false;
}

/* ------------------------------------------------------------------------- */
void* 
Master::StaticHeartBeat(void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling HeartBeat
  //----------------------------------------------------------------
  return reinterpret_cast<Master*>(arg)->HeartBeat();
}

/* ------------------------------------------------------------------------- */
void*
Master::HeartBeat()
{
  std::string remoteMgmUrlString = "root://"; remoteMgmUrlString += fRemoteHost.c_str();
  std::string remoteMqUrlString  = "root://"; remoteMqUrlString  += fRemoteMq.c_str();

  int dpos = remoteMqUrlString.find(":",7);
  if (dpos != STR_NPOS) {
    remoteMqUrlString.erase(dpos+1);
    remoteMqUrlString += "1097";
  }

#ifdef NEWXRDCL
  XrdCl::URL remoteMgmUrl(remoteMgmUrlString.c_str());
  XrdCl::URL remoteMqUrl(remoteMqUrlString.c_str());

  if (! remoteMgmUrl.IsValid()) {
    MasterLog(eos_static_crit("remote manager URL <%s> is not valid", remoteMgmUrl.c_str()));
    fRemoteMasterOk=false;
    return;
  }

  if (! remoteMqUrl.IsValid()) {
    MasterLog(eos_static_crit("remote mq URL <%s> is not valid", remoteMqUrl.c_str()));
    fRemoteMqOk=false;
    return;
  }

  XrdCl::FileSystem FsMgm ( remoteMgmUrl );
  XrdCl::FileSystem FsMq  ( remoteMqUrl  );
#else
  XrdMqClient::SetXrootVariables();
  remoteMgmUrlString += "//dummy";
  remoteMqUrlString  += "//dummy";
#endif

  while (1) {
    XrdSysThread::SetCancelOff();

    // - new XrdCl - 
#ifdef NEWXRDCL

    // ---------------------------------------
    // ---- CAREFUL this is incomplete now ---
    // ---------------------------------------

    // ping the two guys e.g. MGM & MQ
    XrdCl::XRootDStatus mgmStatus = FsMgm.Ping(1);
    XrdCl::XRootDStatus mqStatus  = FsMgm.Ping(1);
    
    if (mgmStatus.IsOK()) {
      fRemoteMasterOk = true;
    } else {
      fRemoteMasterOk = false;
    }
    
    if (mqStatus.IsOK()) {
      fRemoteMqOk = true;
      CreateStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
    } else {
      fRemoteMqOk = false;
      RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
    }
    // need to add the stat for the master proc entry
#else

    XrdClientAdmin* MgmAdmin = new XrdClientAdmin(remoteMgmUrlString.c_str());
    XrdClientAdmin* MqAdmin  = new XrdClientAdmin(remoteMqUrlString.c_str());
    
    bool remoteMgmUp=false;
    bool remoteMqUp=false;

    if (HostCheck(fRemoteHost.c_str())) {
      remoteMgmUp = true;
    }

    if (HostCheck(fRemoteMq.c_str(),1097)) {
      remoteMqUp = true;
    }

    // - old XrdClient - 
    // is there a difference? ;-)
    if (remoteMqUp) {
      // check the MQ service
      MqAdmin->Connect();
      MqAdmin->GetClientConn()->ClearLastServerError();
      MqAdmin->GetClientConn()->SetOpTimeLimit(2);
      long id=0;
      long long size = 0;
      long flags=0;
      long modtime =0;
      
      if (MqAdmin->Stat("/eos/",id, size, flags,modtime)) {
	fRemoteMqOk = true;
	CreateStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
      } else {
	fRemoteMqOk = false;
	RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
      }
    } else {
      fRemoteMqOk = false;
      RemoveStatusFile(EOSMQMASTER_SUBSYS_REMOTE_LOCKFILE);
    }

    if (remoteMgmUp) {
      // check the MGM service
      MgmAdmin->Connect();
      MgmAdmin->GetClientConn()->ClearLastServerError();
      MgmAdmin->GetClientConn()->SetOpTimeLimit(2);
      long id=0;
      long long size = 0;
      long flags=0;
      long modtime =0;

      // --------------------------------------------
      // this really sucks ...
      // --------------------------------------------
      int olddbg=DebugLevel();
      DebugSetLevel(-1);
      // --------------------------------------------
      if (MgmAdmin->Stat("/",id, size, flags,modtime)) {
	if (gOFS->MgmProcMasterPath.c_str()) {
	  
	  // check if this machine is running in master mode
	  if (MgmAdmin->Stat(gOFS->MgmProcMasterPath.c_str(),id, size, flags,modtime)) {
	    fRemoteMasterRW = true;
	  } else {
	    fRemoteMasterRW = false;
	  }
	}
	fRemoteMasterOk = true;
      } else {
	fRemoteMasterOk = false;
	fRemoteMasterRW = false;
      }
      DebugSetLevel(olddbg);
    } else {
      fRemoteMasterOk = false;
      fRemoteMasterRW = false;
    }
    delete MgmAdmin;
    delete MqAdmin;

#endif
    
    {      
      eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
      if (!IsMaster() ) {
	if (fRemoteMasterOk && fRemoteMasterRW) {
	  // set the redirect for writes to the remote master
	  Access::gRedirectionRules[std::string("w:*")] = fRemoteHost.c_str();
	  // set the redirect for ENOENT to the remote master
	  Access::gRedirectionRules[std::string("ENOENT:*")] = fRemoteHost.c_str();
	  // remove the stall 
	  Access::gStallRules.erase(std::string("w:*"));
	} else {
	  // remove the redirect for writes and put a stall for writes
	  Access::gRedirectionRules.erase(std::string("w:*"));
	  Access::gStallRules[std::string("w:*")] = "60";
	  Access::gRedirectionRules.erase(std::string("ENOENT:*"));
	}
      } else {
	// remove any redirect or stall in this case
	if (Access::gRedirectionRules.count(std::string("w:*"))) {
	  Access::gRedirectionRules.erase(std::string("w:*"));
	}
	if (Access::gStallRules.count(std::string("w:*"))) {
	  Access::gStallRules.erase(std::string("w:*"));
	}
	if (Access::gStallRules.count(std::string("ENOENT:*"))) {
	  Access::gRedirectionRules.erase(std::string("ENOENT:*"));
	}
      }
    }
    
    XrdSysThread::SetCancelOn();

    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }
  return 0;
}

/* ------------------------------------------------------------------------- */
void
Master::PrintOut(XrdOucString &out)
{
  if (fThisHost == fMasterHost) {
    out += "mode=master-rw"; 
  } else {
    out += "mode=slave-ro";
  }
  
  switch (fRunningState) {
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
  
  out+=" master=";out += fMasterHost; out += " configdir="; out += gOFS->MgmConfigDir.c_str(); out += " config="; out += gOFS->MgmConfigAutoLoad.c_str();
  if (fActivated) {
    out += " active=true";
  } else {
    out += " active=false";
  }

  if (fThisHost != fRemoteHost) {
    // print only if we have a master slave configuration
    if (fRemoteMasterOk) {
      out += " mgm:"; out += fRemoteHost; out += "=ok";
      if (fRemoteMasterRW) {
	out += " mgm:mode=rw-master";
      } else {
	out += " mgm:mode=ro-slave";
      }
    } else {
      out += " mgm:"; out += fRemoteHost; out += "=down";
    }
    if (fRemoteMqOk) {
      out += " mq:"; out += fRemoteMq; out += "=ok";
    } else {
    out += " mq:"; out += fRemoteMq; out += "=down";
    }
  }
}

/* ------------------------------------------------------------------------- */
bool
Master::ApplyMasterConfig(XrdOucString &stdOut, XrdOucString &stdErr, int transitiontype)
{
  std::string enabled="";
  
  if (fThisHost == fMasterHost) {
    gOFS->ObjectManager.EnableBroadCast(true); // we are the master and we broadcast every configuration change
    if (!CreateStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE)) 
      return false;
  } else {
    gOFS->ObjectManager.EnableBroadCast(false); // we are the slave and we just listen and don't broad cast anythiing
    if (!RemoveStatusFile(EOSMGMMASTER_SUBSYS_RW_LOCKFILE))
      return false;
  }
  return Activate(stdOut, stdErr,transitiontype);
}

/* ------------------------------------------------------------------------- */
int
Master::Activate(XrdOucString &stdOut, XrdOucString &stdErr, int transitiontype)
{
  fActivated = false;
  // -----------------------------------------------------------------------
  // change the configuration directory
  // -----------------------------------------------------------------------
  if (fMasterHost == fThisHost) {
    gOFS->MgmConfigDir.replace(fRemoteHost, fThisHost);
    stdOut += "configdir="; stdOut += gOFS->MgmConfigDir.c_str(); stdOut += " activating master="; stdOut += fThisHost; stdOut += "\n";
  } else {
    gOFS->MgmConfigDir.replace(fThisHost, fRemoteHost);
    stdOut += "configdir="; stdOut += gOFS->MgmConfigDir.c_str(); stdOut += " activating master="; stdOut += fRemoteHost; stdOut += "\n";
  }
  
  gOFS->ConfEngine->SetConfigDir(gOFS->MgmConfigDir.c_str());

  if (transitiontype != kSlaveToMaster) {
    // -----------------------------------------------------------------------
    // load the master's default configuration if this is not a transition
    // -----------------------------------------------------------------------
    if ( (transitiontype != kMasterToMasterRO) && (transitiontype != kMasterROToSlave) ) {
      if (gOFS->MgmConfigAutoLoad.length()) {
	MasterLog(eos_static_info("autoload config=%s", gOFS->MgmConfigAutoLoad.c_str()));
	XrdOucString configloader = "mgm.config.file=";
	configloader += gOFS->MgmConfigAutoLoad;
	XrdOucEnv configenv(configloader.c_str());
	XrdOucString stdErr="";
	if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr)) {
	  MasterLog(eos_static_crit("Unable to auto-load config %s - fix your configuration file!", gOFS->MgmConfigAutoLoad.c_str()));
	  MasterLog(eos_static_crit("%s\n", stdErr.c_str()));
	return false;
	} else {
	  MasterLog(eos_static_info("Successful auto-load config %s", gOFS->MgmConfigAutoLoad.c_str()));
	}
      } 
    }
    
    // -----------------------------------------------------------------------
    // invoke a master to ro-master transition
    // -----------------------------------------------------------------------
    
    if (transitiontype == kMasterToMasterRO) {
      MasterLog(eos_static_notice("Doing Master=>Master-RO transition"));
      if (!Master2MasterRO()) {
	return false;
      }
    }

    // -----------------------------------------------------------------------
    // invoke a ro-master to slave transition
    // -----------------------------------------------------------------------
	
    if (transitiontype == kMasterROToSlave) {
      MasterLog(eos_static_notice("Doing Master-RO=>Slave transition"));
      if (!MasterRO2Slave()) {
	return false;
      }
    }

  } else {
    // -----------------------------------------------------------------------
    // store the current configuration to the default location
    // -----------------------------------------------------------------------
    if (!gOFS->ConfEngine->AutoSave()) {
      return false;
    }
    // -----------------------------------------------------------------------
    // invoke a slave to master transition
    // ----------------------------------------------------------------------- 
    MasterLog(eos_static_notice("Doing Slave=>Master transition"));
    if (!Slave2Master()) {
      return false;
    }
  }

  fActivated = true;
  return true;
}

/* ------------------------------------------------------------------------- */
bool 
Master::Set(XrdOucString &mastername, XrdOucString &stdOut, XrdOucString &stdErr)
{
  int transitiontype = 0;

  if (fRunningState == kIsNothing) {
    MasterLog(eos_static_err("unable to change master/slave configuration - node is in invalid state after a failed transition"));
    stdErr += "error: unable to change master/slave configuration - node is in invalid state after a failed transition";
    return false;
  }

  if ( (mastername != getenv("EOS_MGM_MASTER1")) &&
       (mastername != getenv("EOS_MGM_MASTER2")) ) {
    stdErr += "error: invalid master name specified (/etc/sysconfig/eos:EOS_MGM_MASTER1,EOS_MGM_MASTER2)\n";
    return false;
  }

  if ( (fMasterHost == fThisHost) ) {
    if ((mastername != fThisHost) ) {
      if (fRunningState == kIsRunningMaster) {
	transitiontype = kMasterToMasterRO;
      } else {
	MasterLog(eos_static_err("invalid master/slave transition requested - we are not a running master"));
	stdErr += "invalid master/slave transition requested - we are not a running master\n";
	return false;
      }
    } else {
      transitiontype = kMasterToMaster;
      MasterLog(eos_static_err("invalid master/master transition requested - we are  a running master"));
      stdErr += "invalid master/master transition requested - we are a running master\n";
      return false;
    }
  } else {
    if (fRunningState == kIsReadOnlyMaster) {
      transitiontype = kMasterROToSlave;
    } else {
      if (fRunningState != kIsRunningSlave) {
	MasterLog(eos_static_err("invalid master/slave transition requested - we are not a running ro-master or we are already a slave"));
	stdErr += "invalid master/slave transition requested - we are not a running ro-master or we are already a slave\n";
	return false;
      } 
    }
  }

  if (mastername == fThisHost) {
    // check if the remote machine is running as the master
    if (fRemoteMasterRW) {
      stdErr += "error: the remote machine <"; stdErr += fRemoteHost; stdErr += "> is still running as a RW master\n";
      return false;
    }
    if (fMasterHost.length() && ( fMasterHost != fThisHost)) {
      // slave to master transition
      transitiontype = kSlaveToMaster;
    }
  }

  XrdOucString lOldMaster = fMasterHost;
  fMasterHost = mastername;
  
  bool arc= ApplyMasterConfig(stdOut, stdErr, transitiontype);

  // set back to the previous master
  if (!arc) 
    fMasterHost = lOldMaster;
  return arc;
}

bool 
Master::Slave2Master()

{
  fRunningState = kIsTransition;

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
  if (WEXITSTATUS(rc)) {
    if (WEXITSTATUS(rc) == 2) {
      MasterLog(eos_warning("sync service was already stopped"));
    }
    if (WEXITSTATUS(rc) == 1) {
      MasterLog(eos_warning("sync service was dead"));
    }
    MasterLog(eos_crit("slave=>master transition aborted since sync was down"));
    fRunningState = kIsNothing;
    rc = system("service eos sync start");
    if (WEXITSTATUS(rc)) {
      MasterLog(eos_warning("failed to start sync service"));
    }
    fRunningState = kIsRunningSlave;
    return false;
  }

  // -----------------------------------------------------------
  // if possible evaluate if local and remote master files are 
  // in sync ...
  // -----------------------------------------------------------

  off_t size_local_file_changelog  = 0;
  off_t size_local_dir_changelog   = 0;
  off_t size_remote_file_changelog = 0;
  off_t size_remote_dir_changelog  = 0;

  struct stat buf;

  std::string remoteSyncUrlString = "root://"; remoteSyncUrlString += fRemoteHost.c_str(); remoteSyncUrlString += ":1096"; remoteSyncUrlString += "//dummy";
  std::string remoteSyncHostPort =  fRemoteHost.c_str(); remoteSyncHostPort += ":1096";

  if (!stat(gOFS->MgmNsFileChangeLogFile.c_str(),&buf)) {
    size_local_file_changelog = buf.st_size;
  } else {
    MasterLog(eos_crit("slave=>master transition aborted since we cannot stat our own slave file-changelog-file"));
    fRunningState = kIsRunningSlave;
    return false;
  }

  if (!stat(gOFS->MgmNsDirChangeLogFile.c_str(),&buf)) {
    size_local_dir_changelog = buf.st_size;
  } else {
    MasterLog(eos_crit("slave=>master transition aborted since we cannot stat our own slave dir-changelog-file"));
    fRunningState = kIsRunningSlave;
    return false;
  }
  
  bool syncok=false;
  
  if (HostCheck(fRemoteHost.c_str(), 1096)) {
    MasterLog(eos_info("remote-sync host=%s:1096 is reachable", fRemoteHost.c_str()));
    syncok = true;
  } else {
    MasterLog(eos_info("remote-sync host=%s:1096 is down",fRemoteHost.c_str()));
  }
  
  if (syncok) {
    XrdClientAdmin* SyncAdmin = new XrdClientAdmin(remoteSyncUrlString.c_str());
    if (SyncAdmin) {
      SyncAdmin->Connect();
      SyncAdmin->GetClientConn()->ClearLastServerError();
      SyncAdmin->GetClientConn()->SetOpTimeLimit(2);
      long id=0;
      long long size = 0;
      long flags=0;
      long modtime =0;

      // stat the two remote changelog files
      if (SyncAdmin->Stat(rfclf.c_str(),id, size, flags, modtime)) {
	size_remote_file_changelog = size;
      }
      if (SyncAdmin->Stat(rdclf.c_str(),id, size, flags, modtime)) {
	size_remote_dir_changelog = size;
      }
      delete SyncAdmin;
    }
    
    if (size_remote_file_changelog != size_local_file_changelog) {
      MasterLog(eos_crit("slave=>master transition aborted - file changelog synchronization problem found - path=%s remote-size=%llu local-size=%llu",rfclf.c_str(), size_remote_file_changelog, size_local_file_changelog));
      fRunningState = kIsRunningSlave;
      return false;
    }

    if (size_remote_dir_changelog != size_local_dir_changelog) {
      MasterLog(eos_crit("slave=>master transition aborted - dir changelog synchronization problem found - path=%s remote-size=%llu local-size=%llu",rdclf.c_str(), size_remote_dir_changelog, size_local_dir_changelog));
      fRunningState = kIsRunningSlave;
      return false;
    }
  }


  // -----------------------------------------------------------
  // make a backup of the new target master file
  // -----------------------------------------------------------

  XrdOucString NsFileChangeLogFileCopy = fileSettings["changelog_path"].c_str(); NsFileChangeLogFileCopy += "."; NsFileChangeLogFileCopy += (int) time(NULL);
  XrdOucString NsDirChangeLogFileCopy  = contSettings["changelog_path"].c_str();;  NsDirChangeLogFileCopy  += "."; NsDirChangeLogFileCopy += (int) time(NULL);

  if (!::stat(fileSettings["changelog_path"].c_str(),&buf)) {
    if (::rename(fileSettings["changelog_path"].c_str(), NsFileChangeLogFileCopy.c_str())) {
      MasterLog(eos_crit("failed to rename %s=>%s errno=%d", gOFS->MgmNsFileChangeLogFile.c_str(), NsFileChangeLogFileCopy.c_str(),errno));
      fRunningState = kIsNothing;
      return false;
    }
  }

  if (!::stat(contSettings["changelog_path"].c_str(),&buf)) {
    if (::rename(contSettings["changelog_path"].c_str(), NsDirChangeLogFileCopy.c_str())) {
      MasterLog(eos_crit("failed to rename %s=>%s errno=%d", gOFS->MgmNsDirChangeLogFile.c_str(), NsDirChangeLogFileCopy.c_str(),errno));
      fRunningState = kIsNothing;
      return false;
    }
  }

  gOFS->MgmNsFileChangeLogFile = fileSettings["changelog_path"].c_str();
  gOFS->MgmNsDirChangeLogFile  = contSettings["changelog_path"].c_str();  

  try {
    MasterLog(eos_info("msg=\"invoking slave=>master transition\""));
    gOFS->eosDirectoryService->slave2master(contSettings);
    gOFS->eosFileService->slave2master(fileSettings);
  } catch ( eos::MDException &e ) {
    errno = e.getErrno();
    MasterLog(eos_crit("slave=>master transition returned ec=%d %s\n", e.getErrno(),e.getMessage().str().c_str()));
    fRunningState = kIsNothing;
    
    rc = system("service eos sync start");
    if (WEXITSTATUS(rc)) {
      MasterLog(eos_warning("slave=>master transition - sync didnt' start"));
    }
    return false;
  };
  fRunningState = kIsRunningMaster;
  
  rc = system("service eos sync start");
  if (WEXITSTATUS(rc)) {
    MasterLog(eos_warning("failed to start sync service"));
    MasterLog(eos_crit("slave=>master transition aborted since sync didn't start"));
    try {
      gOFS->eosDirectoryService->finalize();
      gOFS->eosFileService->finalize();
    } catch ( eos::MDException &e ) {
      errno = e.getErrno();
      MasterLog(eos_crit("slave=>master finalize returned ec=%d %s\n", e.getErrno(),e.getMessage().str().c_str()));
    }
    fRunningState = kIsNothing;
    return false;
  }
  MasterLog(eos_notice("running in master mode"));
  return true;
}


bool
Master::Master2MasterRO()
{
  fRunningState = kIsTransition;
  // -----------------------------------------------------------
  // convert the RW namespace into a read-only namespace
  // -----------------------------------------------------------
  try {
    gOFS->eosDirectoryService->makeReadOnly();
    gOFS->eosFileService->makeReadOnly();
  } catch ( eos::MDException &e ) {
    errno = e.getErrno();
    MasterLog(eos_crit("master=>slave transition returned ec=%d %s\n", e.getErrno(),e.getMessage().str().c_str()));
    fRunningState = kIsNothing;
    return false;
  };
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  fRunningState = kIsReadOnlyMaster;
  MasterLog(eos_notice("running in RO master mode"));
  return true;
}

bool
Master::MasterRO2Slave()
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
    
    // put an appropriate stall
    if (fRemoteMasterOk) {
      Access::gStallRules[std::string("*")] = "100";
      
    } else {
      Access::gStallRules[std::string("*")] = "60";
      
    }
  }
  
  {
    // now convert the namespace
    eos::common::RWMutexWriteLock nsLock(gOFS->eosViewRWMutex);
    
    // take the whole namespace down
    try {
      if (gOFS->eosFsView)           { gOFS->eosFsView->finalize();           delete gOFS->eosFsView; gOFS->eosFsView=0; }
      if (gOFS->eosView)             { gOFS->eosView->finalize();             delete gOFS->eosView;   gOFS->eosView=0;   }
    } catch ( eos::MDException &e ) {
      errno = e.getErrno();
      MasterLog(eos_crit("master-ro=>slave namespace shutdown returned ec=%d %s\n", e.getErrno(),e.getMessage().str().c_str()));
    };

    // boot it from scratch
    if (!BootNamespace()) {
      fRunningState = kIsNothing;
      return false;
    }
  }
  
  // reload the configuration to get the proper quota nodes
  if (gOFS->MgmConfigAutoLoad.length()) {
    MasterLog(eos_static_info("autoload config=%s", gOFS->MgmConfigAutoLoad.c_str()));
    XrdOucString configloader = "mgm.config.file=";
    configloader += gOFS->MgmConfigAutoLoad;
    XrdOucEnv configenv(configloader.c_str());
    XrdOucString stdErr="";
    if (!gOFS->ConfEngine->LoadConfig(configenv, stdErr)) {
      MasterLog(eos_static_crit("Unable to auto-load config %s - fix your configuration file!", gOFS->MgmConfigAutoLoad.c_str()));
      MasterLog(eos_static_crit("%s\n", stdErr.c_str()));
      return false;
    } else {
      MasterLog(eos_static_info("Successful auto-load config %s", gOFS->MgmConfigAutoLoad.c_str()));
    }
  } 
  
  {
    XrdSysMutexHelper lock(gOFS->InitializationMutex);
    if (gOFS->Initialized== gOFS->kBooted) {
      // inform the boot thread that the stall should be removed after boot
      gOFS->RemoveStallRuleAfterBoot = true;

      // start the file view loader thread
      MasterLog(eos_info("msg=\"starting file view loader thread\""));

      pthread_t tid;
      if ((XrdSysThread::Run(&tid, XrdMgmOfs::StaticInitializeFileView, static_cast<void *>(gOFS),
			     0, "File View Loader"))) {
	MasterLog(eos_crit("cannot start file view loader"));
	fRunningState = kIsNothing;
	return false;
      }
    } else {
      MasterLog(eos_crit("msg=\"don't want to start file view loader for a namespace in bootfailure state\""));
      fRunningState = kIsNothing;
      return false;
    }
  }
  fRunningState = kIsRunningSlave;
  MasterLog(eos_notice("running in slave mode"));
  return true;
}
/* ------------------------------------------------------------------------- */
Master::~Master() 
{
  if (fThread) {
    XrdSysThread::Cancel(fThread);
    XrdSysThread::Join(fThread,0);
    fThread = 0;
  }
}

/* ------------------------------------------------------------------------- */
bool
Master::CreateStatusFile(const char* path)
{
  struct stat buf;
  if (::stat(path, &buf)) {
    if (::creat(path, S_IRWXU | S_IRGRP | S_IROTH) == -1) {
      MasterLog(eos_static_err("failed to create %s errno=%d", path, errno));
      return false;
    }
  }
  return true;
}

/* ------------------------------------------------------------------------- */
bool
Master::RemoveStatusFile(const char* path)
{
  struct stat buf;
  if (!::stat(path, &buf)) {
    if (::unlink(path)) {
      MasterLog(eos_static_err("failed to unlink %s errno=%d", path,errno));
      return false;
    }
  }
  return true;
}

/* ------------------------------------------------------------------------- */
bool 
Master::BootNamespace()
{
  gOFS->eosDirectoryService = new eos::ChangeLogContainerMDSvc;
  gOFS->eosFileService      = new eos::ChangeLogFileMDSvc;
  gOFS->eosView             = new eos::HierarchicalView;
  gOFS->eosFsView           = new eos::FileSystemView;

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

  if (!IsMaster()) {
    
    contSettings["slave_mode"]       = "true";
    contSettings["poll_interval_us"] = "1000";
    fileSettings["slave_mode"]       = "true";
    fileSettings["poll_interval_us"] = "1000";
  }

  gOFS->MgmNsFileChangeLogFile = fileSettings["changelog_path"].c_str();
  gOFS->MgmNsDirChangeLogFile  = contSettings["changelog_path"].c_str();

  time_t tstart = time(0);
  
  //-------------------------------------------
  try {
    gOFS->eosFileService->configure( fileSettings );
    gOFS->eosDirectoryService->configure( contSettings );
    
    gOFS->eosView->setContainerMDSvc( gOFS->eosDirectoryService );
    gOFS->eosView->setFileMDSvc ( gOFS->eosFileService );
    
    gOFS->eosFileService->setContainerService( gOFS->eosDirectoryService );

    if (!IsMaster()) {
      // slave's need access to the namespace lock
      gOFS->eosFileService->setSlaveLock(&fNsLock);
      gOFS->eosDirectoryService->setSlaveLock(&fNsLock);
    }

    gOFS->eosView->configure ( settings );
    
    MasterLog(eos_notice("%s",(char*)"eos directory view configure started"));

    gOFS->eosFileService->addChangeListener( gOFS->eosFsView );
    gOFS->eosFileService->setQuotaStats( gOFS->eosView->getQuotaStats() );

    gOFS->eosView->getQuotaStats()->registerSizeMapper( Quota::MapSizeCB );
    gOFS->eosView->initialize1();

    time_t tstop  = time(0);
    MasterLog(eos_notice("eos directory view configure stopped after %d seconds", (tstop-tstart)));
  } catch ( eos::MDException &e ) {
    time_t tstop  = time(0);
    MasterLog(eos_crit("eos view initialization failed after %d seconds", (tstop-tstart)));
    errno = e.getErrno();
    MasterLog(eos_crit("initialization returned ec=%d %s\n", e.getErrno(),e.getMessage().str().c_str()));
    return false;
  };

  if (!IsMaster()) {
    fRunningState = kIsRunningSlave;
    MasterLog(eos_notice("running in slave mode"));
  } else {
    fRunningState = kIsRunningMaster;
    MasterLog(eos_notice("running in master mode"));
  }
  return true;
}

EOSMGMNAMESPACE_END
