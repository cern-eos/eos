/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsConfig.hh"
#include "XrdFstOfs/XrdFstOfsStorage.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
#include <google/dense_hash_map>
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdCommon/XrdCommonFileSystem.hh"
#include "XrdMqOfs/XrdMqMessaging.hh"

/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
void
XrdFstOfsFileSystem::BroadcastError(const char* msg) 
{
  XrdMqMessage message("fst");
  XrdOucString msgbody;
  XrdOucEnv env(GetEnvString());
  XrdCommonFileSystem::GetBootReplyString(msgbody, env, XrdCommonFileSystem::kOpsError);
  
  SetStatus(XrdCommonFileSystem::kOpsError);

  XrdOucString response = response += msg; response += " "; response += Path;; response += " ["; response += strerror(errno); response += "]";

  msgbody += "errmsg=";
  msgbody += response;
  msgbody += "&errc="; 
  msgbody += errno; 

  SetError(errno,response.c_str());
  
  message.SetBody(msgbody.c_str());

  eos_debug("broadcasting error message: %s", msgbody.c_str());

  if (!XrdMqMessaging::gMessageClient.SendMessage(message)) {
    // display communication error
    eos_err("cannot send error broadcast");
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsFileSystem::BroadcastStatus()
{
  XrdMqMessage message("fst");
  XrdOucString msgbody;
  XrdOucEnv env(GetEnvString());

  XrdCommonFileSystem::GetBootReplyString(msgbody, env, Status);

  XrdOucString response = statFs->GetEnv();

  msgbody += response;
  if (errc) {
    msgbody += "&errmsg=";
    msgbody += errmsg;
    msgbody += "&errc=";
    msgbody += errc;
  }
  
  message.SetBody(msgbody.c_str());

  eos_debug("broadcasting status message: %s", msgbody.c_str());

  if (!XrdMqMessaging::gMessageClient.SendMessage(message)) {
    // display communication error
    eos_err("cannot send status broadcast");
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::BroadcastQuota(XrdOucString &quotastring)
{
  XrdMqMessage message("fst");
  XrdOucString msgbody;
  XrdCommonFileSystem::GetQuotaReportString(msgbody);

  msgbody += quotastring;
  message.SetBody(msgbody.c_str());

  eos_debug("broadcasting quota message: %s", msgbody.c_str());

  if (!XrdMqMessaging::gMessageClient.SendMessage(message)) {
    // display communication error
    eos_err("cannot send status broadcast");
  }
}

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
XrdCommonStatfs*
XrdFstOfsFileSystem::GetStatfs(bool &changedalot) 
{ 

  statFs = XrdCommonStatfs::DoStatfs(Path.c_str());
  if (!statFs) {
    BroadcastError("cannot statfs");
    return 0;
  }

  if (!last_blocks_free) last_blocks_free = statFs->GetStatfs()->f_bfree;

  eos_debug("statfs on filesystem %s id %d - %lu => %lu", queueName.c_str(),Id,last_blocks_free,statFs->GetStatfs()->f_bfree  );
  // define significant change here as 1GB change
  if ( llabs((last_blocks_free - statFs->GetStatfs()->f_bfree)*4096ll) > (1024ll*1024ll*1024ll)) {
    eos_debug("significant filesystem change on filesystem %s id %d", queueName.c_str(), Id);
    changedalot = true;
    last_blocks_free = statFs->GetStatfs()->f_bfree;
    // since it changed a lot we broadcast the information to mgm's
    BroadcastStatus();
    last_status_broadcast = time(0);
    return statFs;
  }

  // within the quota report interval we send a broad cast in any case!
  if ((time(0) - last_status_broadcast) > XrdFstOfsConfig::gConfig.FstQuotaReportInterval) 
    BroadcastStatus();

  return statFs;
}

/*----------------------------------------------------------------------------*/
XrdFstOfsStorage::XrdFstOfsStorage(const char* metadirectory)
{


  SetLogId("FstOfsStorage");

  // make metadir
  XrdOucString mkmetalogdir = "mkdir -p "; mkmetalogdir += metadirectory;; mkmetalogdir += " >& /dev/null";
  system(mkmetalogdir.c_str());
  metaDirectory = metadirectory;
  
  // check if the meta directory is accessible
  if (access(metadirectory,R_OK|W_OK|X_OK)) {
    eos_crit("cannot access meta data directory %s", metadirectory);
    zombie = true;
  }

  zombie = false;
  // start threads
  pthread_t tid;
  int rc;

  eos_info("starting quota thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsQuota, static_cast<void *>(this),
                              0, "Quota Report"))) {
    eos_crit("cannot start quota report thread");
    zombie = true;
  }

  eos_info("starting scrubbing thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsScrub, static_cast<void *>(this),
                              0, "Scrubber"))) {
    eos_crit("cannot start scrubber thread");
    zombie = true;
  }

  eos_info("starting trim thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsTrim, static_cast<void *>(this),
                              0, "Meta Store Trim"))) {
    eos_crit("cannot start trimming theread");
    zombie = true;
  }

  eos_info("starting deletion thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsRemover, static_cast<void *>(this),
                              0, "Data Store Remover"))) {
    eos_crit("cannot start deletion theread");
    zombie = true;
  }

  eos_info("starting replication thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsPulling, static_cast<void *>(this),
                              0, "Data Pulling Thread"))) {
    eos_crit("cannot start pulling thread");
    zombie = true;
  }
}


/*----------------------------------------------------------------------------*/
XrdFstOfsStorage*
XrdFstOfsStorage::Create(const char* metadirectory)
{
  XrdFstOfsStorage* storage = new XrdFstOfsStorage(metadirectory);
  if (storage->IsZombie()) {
    delete storage;
    return 0;
  } 
  return storage;
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsStorage::HasStatfsChangedalot(const char* key, XrdFstOfsFileSystem* filesystem, void* arg) 
{
  bool* changedalot = (bool*) arg;
  // get a fresh statfs
  filesystem->GetStatfs(*changedalot);
  return 0;
}

/*----------------------------------------------------------------------------*/
bool 
XrdFstOfsStorage::SetFileSystem(XrdOucEnv& env)
{
  fsMutex.Lock();
  XrdFstOfsFileSystem* fs=0;
  const char* path = env.Get("mgm.fspath");
  const char* sfsid = env.Get("mgm.fsid");
  if ( (!path) || (!sfsid)) {
    fsMutex.UnLock();
    return false;
  }

  unsigned int fsid = atoi(sfsid);

  if (!(fs = fileSystems.Find(path)) ) {
    fs = new XrdFstOfsFileSystem(path);
    fileSystems.Add(path,fs);
  }

  fs->SetId(fsid);
  const char* val=0;

  if (( val = env.Get("mgm.fsschedgroup"))) {
    fs->SetSchedulingGroup(val);
  }
  
  if (( val = env.Get("mgm.fsname"))) {
    fs->SetQueue(val);
  }

  if (!gFmdHandler.AttachLatestChangeLogFile(metaDirectory.c_str(), fsid)) {
    fs->SetStatus(XrdCommonFileSystem::kBootFailure);
    fs->SetError(EFAULT,"cannot attach to latest change log file - see the fst logfile for details");
    fsMutex.UnLock();
    return false;
  }

  fs->SetStatus(XrdCommonFileSystem::kBooted);
  fs->SetError(0,0);
  fsMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
XrdFstOfsStorage::RemoveFileSystem(XrdOucEnv& env)
{
  fsMutex.Lock();
  XrdFstOfsFileSystem* fs=0;
  const char* path = env.Get("mgm.fspath");

  if ( (!path) ) 
    return false;
  
  if ( (fs = fileSystems.Find(path)) ) {
    fileSystems.Del(path);
    fsMutex.UnLock();
    return true;
  } 

  fsMutex.UnLock();
  return false;
}

/*----------------------------------------------------------------------------*/
void*
XrdFstOfsStorage::StartFsQuota(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Quota();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
XrdFstOfsStorage::StartFsScrub(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Scrub();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
XrdFstOfsStorage::StartFsTrim(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Trim();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
XrdFstOfsStorage::StartFsRemover(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Remover();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
XrdFstOfsStorage::StartFsPulling(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Pulling();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Quota() 
{
  
  while(1) {
    // check statfs for each filesystem - if there was a significant change, broadcast
    // the statfs information and quota table
    bool changedalot = false;
    fsMutex.Lock();
    // update the filesystem statfs
    fileSystems.Apply(HasStatfsChangedalot, &changedalot);

    fsMutex.UnLock();

    gFmdHandler.Mutex.Lock();
    google::dense_hash_map<long long, unsigned long long>::const_iterator it;

    XrdOucString fullreport="";
    XrdOucString quotareport="";

    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.userbytes", quotareport);
    
    for(it = gFmdHandler.UserBytes.begin(); it != gFmdHandler.UserBytes.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("USER  BYTES : uid %lld volume=%llu", it->first, it->second);
    }
    
    fullreport += quotareport; fullreport +="&";

    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.groupbytes", quotareport);
    
    for(it = gFmdHandler.GroupBytes.begin(); it != gFmdHandler.GroupBytes.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("GROUP BYTES : uid %lld volume=%llu", it->first, it->second);
    }
    
    fullreport += quotareport; fullreport +="&";
    
    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.userfiles", quotareport);

    for(it = gFmdHandler.UserFiles.begin(); it != gFmdHandler.UserFiles.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("USER  FILES : uid %lld  files=%llu", it->first, it->second);
    }

    fullreport += quotareport; fullreport +="&";

    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.groupfiles", quotareport);

    for(it = gFmdHandler.GroupFiles.begin(); it != gFmdHandler.GroupFiles.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("GROUP FILES : uid %lld  files=%llu", it->first, it->second);
    }

    fullreport += quotareport;

    // broadcast the quota table
    if (changedalot)
      BroadcastQuota(fullreport);
 
    gFmdHandler.Mutex.UnLock();
    sleep(XrdFstOfsConfig::gConfig.FstQuotaReportInterval);
    

  }
} 

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Scrub()
{
  // this thread reads the oldest files and checks their integrity
  while(1) {
    sleep(1);
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Trim()
{
  // this thread supervises the changelogfile and trims them from time to time to shrink their size
  while(1) {
    sleep(30);
    google::sparse_hash_map<unsigned long long, google::dense_hash_map<unsigned long long, unsigned long long> >::const_iterator it;
    eos_static_info("Trimming Size  %u", gFmdHandler.Fmd.size());
    for ( it = gFmdHandler.Fmd.begin(); it != gFmdHandler.Fmd.end(); ++it) {
      eos_static_info("Trimming fsid=%llu ",it->first);
      if (!gFmdHandler.TrimLogFile(it->first)) {
      	eos_static_err("Trimming failed on fsid=%llu",it->first);
      }
    }
    sleep(80000);
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Remover()
{
  // this thread unlinks stored files
  while(1) {
    sleep(1);
    deletionsMutex.Lock();
    if (deletions.size()) 
      eos_static_debug("%u files to delete",deletions.size());
    for (unsigned int i=0; i< deletions.size(); i++) {
      for (unsigned int j=0; j< deletions[i].fIdVector.size(); j++) {
	eos_static_debug("Deleting File Id=%llu on Fs=%u", deletions[i].fIdVector[j], deletions[i].fsId);
	// delete the file
	XrdOucString hexstring="";
	XrdCommonFileId::Fid2Hex(deletions[i].fIdVector[j],hexstring);
	XrdOucErrInfo error;

	XrdOucString capOpaqueString="/?mgm.pcmd=drop";
	XrdOucString OpaqueString = "";
	OpaqueString+="&mgm.fsid="; OpaqueString += (int)deletions[i].fsId;
	OpaqueString+="&mgm.fid=";  OpaqueString += hexstring;
	OpaqueString+="&mgm.localprefix="; OpaqueString += deletions[i].localPrefix;
	XrdOucEnv Opaque(OpaqueString.c_str());
	capOpaqueString += OpaqueString;
	
	if ( (gOFS._rem("/DELETION",error, (const XrdSecEntity*)0, &Opaque)!= SFS_OK)) {
	  eos_static_err("unable to remove fid %s fsid %lu localprefix=%s",hexstring.c_str(), deletions[i].fsId, deletions[i].localPrefix.c_str());
	} 

	// update the manager
	int rc = gOFS.CallManager(0, 0, deletions[i].managerId.c_str(), capOpaqueString);
	if (rc) {
	  eos_static_err("unable to drop file id %s fsid %u at manager %s",hexstring.c_str(), deletions[i].fsId, deletions[i].managerId.c_str()); 
	}
      }
    }

    deletions.clear();
    deletionsMutex.UnLock();
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Pulling()
{
  // this thread pulls files from other nodes
  while(1) {
    sleep(1);
    transferMutex.Lock();
    if (transfers.size()) 
      eos_static_debug("%u files to delete",transfers.size());
    
    while (transfers.size()) {
      int retc=0;
      transfers[0].Debug();
      retc = transfers[0].Do();
      transferMutex.UnLock();
      // try the transfer here
      
      
      transferMutex.Lock();
      if (!retc) {
	// remove the entry if successfull
	if (transfers.size()) transfers.erase(transfers.begin());
      }
      
    }
    transferMutex.UnLock();
  }
}
