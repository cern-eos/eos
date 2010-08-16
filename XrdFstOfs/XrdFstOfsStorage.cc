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
XrdFstOfsFileSystem::BroadcastError(int errc, const char* errmsg) 
{
  XrdMqMessage message("fst");
  XrdOucString msgbody;
  XrdOucEnv env(GetEnvString());
  XrdCommonFileSystem::GetBootReplyString(msgbody, env, XrdCommonFileSystem::kOpsError);
  
  SetStatus(XrdCommonFileSystem::kOpsError);

  XrdOucString response = response += errmsg; response += " "; response += Path;; 

  msgbody += "errmsg=";
  msgbody += errmsg;
  msgbody += "&errc="; 
  msgbody += errc; 

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

  XrdOucString rwstatus="";
  gOFS.OpenFidString(Id, rwstatus);
  msgbody += rwstatus;
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
XrdFstOfsFileSystem::GetStatfs(bool &changed) 
{ 

  statFs = XrdCommonStatfs::DoStatfs(Path.c_str());
  if (!statFs) {
    BroadcastError("cannot statfs");
    return 0;
  }

  //  if (!last_blocks_free) last_blocks_free = statFs->GetStatfs()->f_bfree;

  eos_debug("statfs on filesystem %s id %d - %lu => %lu", queueName.c_str(),Id,last_blocks_free,statFs->GetStatfs()->f_bfree  );
  // define significant change here as 1GB change
  if ( (last_blocks_free == 0) || ( last_blocks_free != statFs->GetStatfs()->f_bfree) ) {
    eos_debug("filesystem change on filesystem %s id %d", queueName.c_str(), Id);
    changed = true;
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

  // we need page aligned addresses for direct IO
  if (posix_memalign((void**)&scrubPattern[0], sysconf(_SC_PAGESIZE),1024*1024) ||
      posix_memalign((void**)&scrubPattern[1], sysconf(_SC_PAGESIZE),1024*1024) ||
      posix_memalign((void**)&scrubPatternVerify, sysconf(_SC_PAGESIZE),1024*1024) ) {
    eos_crit("cannot allocate memory aligned scrub buffer");
    exit(-1);
  }

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

  eos_info("starting report thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsReport, static_cast<void *>(this),
                              0, "Report Thread"))) {
    eos_crit("cannot start report thread");
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
XrdFstOfsStorage::HasStatfsChanged(const char* key, XrdFstOfsFileSystem* filesystem, void* arg) 
{
  bool* changed = (bool*) arg;
  // get a fresh statfs
  filesystem->GetStatfs(*changed);
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
    fileSystemsVector.push_back(fs);
  }

  fs->SetId(fsid);
  const char* val=0;
  if (!gOFS.ROpenFid.count(fsid)) {
    gOFS.ROpenFid[fsid].set_deleted_key(0);
  }
  if (!gOFS.WOpenFid.count(fsid)) {
    gOFS.WOpenFid[fsid].set_deleted_key(0);
  }

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
    std::vector<XrdFstOfsFileSystem*>::iterator it;
    for (it = fileSystemsVector.begin(); it != fileSystemsVector.end(); ++it) {
      if (*it == fs) {
	fileSystemsVector.erase(it);
	break;
      }
    }

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
void*
XrdFstOfsStorage::StartFsReport(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Report();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Quota() 
{
  while(1) {
    // check statfs for each filesystem - if there was a significant change, broadcast
    // the statfs information and quota table
    bool changed = false;
    fsMutex.Lock();
    // update the filesystem statfs
    fileSystems.Apply(HasStatfsChanged, &changed);

    fsMutex.UnLock();

    gFmdHandler.Mutex.Lock();
    google::dense_hash_map<long long, long long>::const_iterator it;

    XrdOucString fullreport="";
    XrdOucString quotareport="";

    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.userbytes", quotareport);
    
    for(it = gFmdHandler.UserBytes.begin(); it != gFmdHandler.UserBytes.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("USER  BYTES : uid %lld volume=%lld", it->first, it->second);
    }
    
    fullreport += quotareport; fullreport +="&";

    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.groupbytes", quotareport);
    
    for(it = gFmdHandler.GroupBytes.begin(); it != gFmdHandler.GroupBytes.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("GROUP BYTES : uid %lld volume=%lld", it->first, it->second);
    }
    
    fullreport += quotareport; fullreport +="&";
    
    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.userfiles", quotareport);

    for(it = gFmdHandler.UserFiles.begin(); it != gFmdHandler.UserFiles.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("USER  FILES : uid %lld  files=%lld", it->first, it->second);
    }

    fullreport += quotareport; fullreport +="&";

    XrdCommonFileSystem::CreateQuotaReportString("fst.quota.groupfiles", quotareport);

    for(it = gFmdHandler.GroupFiles.begin(); it != gFmdHandler.GroupFiles.end(); it++) {
      XrdCommonFileSystem::AddQuotaReportString((unsigned long) it->first, it->second, quotareport);
      eos_debug("GROUP FILES : uid %lld  files=%lld", it->first, it->second);
    }

    fullreport += quotareport;

    // broadcast the quota table
    if (changed) {
      BroadcastQuota(fullreport);
    }
 
    gFmdHandler.Mutex.UnLock();
    sleep(XrdFstOfsConfig::gConfig.FstQuotaReportInterval);
    

  }
} 

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Scrub()
{
  // create a 1M pattern
  eos_static_info("Creating Scrubbing pattern ...");
  for (int i=0;i< 1024*1024/16; i+=2) {
    scrubPattern[0][i]=0xaaaa5555aaaa5555;
    scrubPattern[0][i+1]=0x5555aaaa5555aaaa;
    scrubPattern[1][i]=0x5555aaaa5555aaaa;
    scrubPattern[1][i+1]=0xaaaa5555aaaa5555;
  }


  eos_static_info("Start Scrubbing ...");

  // this thread reads the oldest files and checks their integrity
  while(1) {
    time_t start = time(0);
    unsigned int nfs=0;
    fsMutex.Lock();
    nfs = fileSystemsVector.size();
    eos_static_info("FileSystem Vector %u",nfs);
    fsMutex.UnLock();
    
    for (unsigned int i=0; i< nfs; i++) {
      fsMutex.Lock();
      if (i< fileSystemsVector.size()) {
	XrdOucString path = fileSystemsVector[i]->GetPath();
	if (!fileSystemsVector[i]->GetStatfs()) {
	  fsMutex.UnLock();
	  continue;
	}

	unsigned long long free   = fileSystemsVector[i]->GetStatfs()->GetStatfs()->f_bfree;
	unsigned long long blocks = fileSystemsVector[i]->GetStatfs()->GetStatfs()->f_blocks;
	unsigned long id = fileSystemsVector[i]->GetId();
	fsMutex.UnLock();
	
	if (ScrubFs(path.c_str(),free,blocks,id)) {
	  // filesystem has errors!
	  fsMutex.Lock();
	  if (fileSystemsVector[i]) {
	    fileSystemsVector[i]->BroadcastError(EIO,"filesystem probe error detected");
	  }
	  fsMutex.UnLock();
	}
      } else {
	fsMutex.UnLock();
      }
    }
    time_t stop = time(0);

    int nsleep = ( (4*3600)-(stop-start));
    eos_static_info("Scrubber will pause for %u seconds",nsleep);
    sleep(nsleep);
  }
}

/*----------------------------------------------------------------------------*/
int
XrdFstOfsStorage::ScrubFs(const char* path, unsigned long long free, unsigned long long blocks, unsigned long id) 
{
  int MB = 100; // the test files have 100 MB

  int index = 10 - (int) (10.0 * free / blocks);

  eos_static_info("Running Scrubber on filesystem path=%s id=%u free=%llu blocks=%llu index=%d", path, id, free,blocks,index);

  int fserrors=0;
  
  for ( int fs=1; fs<= index; fs++ ) {
    // check if test file exists, if not, write it
    XrdOucString scrubfile[2];
    scrubfile[0] = path;
    scrubfile[1] = path;
    scrubfile[0] += "/scrub.write-once."; scrubfile[0] += fs;
    scrubfile[1] += "/scrub.re-write."; scrubfile[1] += fs;
    struct stat buf;

    for (int k = 0; k< 2; k++) {
      eos_static_info("Scrubbing file %s", scrubfile[k].c_str());
      if ( ((k==0) && stat(scrubfile[k].c_str(),&buf)) || ((k==0) && (buf.st_size!=(MB*1024*1024))) || ((k==1))) {
	// ok, create this file once
	int ff=0;
	if (k==0)
	  ff = open(scrubfile[k].c_str(),O_CREAT|O_TRUNC|O_WRONLY|O_DIRECT, S_IRWXU);
	else
	  ff = open(scrubfile[k].c_str(),O_CREAT|O_WRONLY|O_DIRECT, S_IRWXU);

	if (ff<0) {
	  eos_static_crit("Unable to create/wopen scrubfile %s", scrubfile[k].c_str());
	  break;
	}
	// select the pattern randomly
	int rshift = (int) ( (1.0 *rand()/RAND_MAX)+ 0.5);
	eos_static_info("rshift is %d", rshift);
	for (int i=0; i< MB; i++) {
	  int nwrite = write(ff, scrubPattern[rshift], 1024 * 1024);
	  if (nwrite != (1024*1024)) {
	    eos_static_crit("Unable to write all needed bytes for scrubfile %s", scrubfile[k].c_str());
	    break;
	  }
	  if (k!=0) {
	    usleep(100000);
	  }
	}
	close(ff);
      }

      // do a read verify
      int ff = open(scrubfile[k].c_str(),O_DIRECT|O_RDONLY);
      if (ff<0) {
	eos_static_crit("Unable to open static scrubfile %s", scrubfile[k].c_str());
      }

      int eberrors=0;

      for (int i=0; i< MB; i++) {
	int nread = read(ff, scrubPatternVerify, 1024 * 1024);
	if (nread != (1024*1024)) {
	  eos_static_crit("Unable to read all needed bytes from scrubfile %s", scrubfile[k].c_str());
	  break;
	}
	unsigned long long* ref = (unsigned long long*)scrubPattern[0];
	unsigned long long* cmp = (unsigned long long*)scrubPatternVerify;
	// do a quick check
	for (int b=0; b< MB*1024/8; b++) {
	  if ( (*ref != *cmp) ) {
	    ref = (unsigned long long*)scrubPattern[1];
	    if (*(ref) == *cmp) {
	      // ok - pattern shifted
	    } else {
	      // this is real fatal error 
	      eberrors++;
	    }
	  }
	}
	usleep(100000);
      }
      if (eberrors) {
	eos_static_alert("%d block errors on filesystem %lu scrubfile %s",id, scrubfile[k].c_str());
	fserrors++;
      }
      close(ff);
    }
  }

  if (fserrors) {
    return 1;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Trim()
{
  // this thread supervises the changelogfile and trims them from time to time to shrink their size
  while(1) {
    sleep(10);
    google::sparse_hash_map<unsigned long long, google::dense_hash_map<unsigned long long, unsigned long long> >::const_iterator it;
    eos_static_info("Trimming Size  %u", gFmdHandler.Fmd.size());
    for ( it = gFmdHandler.Fmd.begin(); it != gFmdHandler.Fmd.end(); ++it) {
      eos_static_info("Trimming fsid=%llu ",it->first);
      // stat the size of this logfile
      struct stat buf;
      if (fstat(gFmdHandler.fdChangeLogRead[it->first],&buf)) {
	eos_static_err("Cannot stat the changelog file for fsid=%llu for", it->first);
      } else {
	// we trim only if the file reached 1 GB
	if (buf.st_size > (1000l * 1024 * 1024)) {
	  if (!gFmdHandler.TrimLogFile(it->first)) {
	    eos_static_err("Trimming failed on fsid=%llu",it->first);
	  }
	} else {
	  eos_static_info("Trimming skipped ... changelog is < 1GB");
	}
      }
    }
    // check once per day only 
    sleep(86400);
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

    bool more = false;

    std::vector<XrdFstTransfer>::iterator it;
    do {
      more = false;
      for ( it = transfers.begin(); it != transfers.end(); ++it) {
	int retc=0;
	it->Debug();
	if ( it->ShouldRun() ) {
	  more = true;
	  transferMutex.UnLock();
	  retc = it->Do();
	  
	  // try the transfer here
	  transferMutex.Lock();
	  if (retc) {
	    // reschedule
	    it->Reschedule(300);
	  } else {
	    // we have to leave the loop because the iterator get's invalid
	    transfers.erase(it);
	    break;
	  }
	}
      }
    } while (more);

    transferMutex.UnLock();
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Report()
{
  // this thread send's report messages from the report queue
  bool failure;

  XrdOucString monitorReceiver = XrdFstOfsConfig::gConfig.FstDefaultReceiverQueue;
  monitorReceiver.replace("*/mgm", "*/report");

  while(1) {
    failure = false;

    gOFS.ReportQueueMutex.Lock();
    while ( gOFS.ReportQueue.size()>0) {
      // send all reports away and dump them into the log
      XrdOucString report = gOFS.ReportQueue.front();
      eos_static_info(report.c_str());

      // this type of messages can have no receiver
      XrdMqMessage message("report");
      message.MarkAsMonitor();

      XrdOucString msgbody;
      message.SetBody(report.c_str());
      
      eos_debug("broadcasting report message: %s", msgbody.c_str());
      

      if (!XrdMqMessaging::gMessageClient.SendMessage(message, monitorReceiver.c_str())) {
	// display communication error
	eos_err("cannot send report broadcast");
	failure = true;
	break;
      }
      gOFS.ReportQueue.pop();
    }
    gOFS.ReportQueueMutex.UnLock();
    if (failure) 
      sleep(10);
    else 
      sleep(1);
  }
}
