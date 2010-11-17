/*----------------------------------------------------------------------------*/
#include "XrdFstOfs/XrdFstOfsConfig.hh"
#include "XrdFstOfs/XrdFstOfsStorage.hh"
#include "XrdFstOfs/XrdFstOfs.hh"
#include <google/dense_hash_map>
#include "XrdCommon/XrdCommonFmd.hh"
#include "XrdCommon/XrdCommonFileSystem.hh"
#include "XrdCommon/XrdCommonPath.hh"
#include "XrdMqOfs/XrdMqMessaging.hh"

/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"

extern XrdOssSys  *XrdOfsOss;

/*----------------------------------------------------------------------------*/
void
XrdFstOfsFileSystem::BroadcastError(const char* msg) 
{
  XrdMqMessage message("fst");
  XrdOucString msgbody;
  XrdOucEnv env(GetEnvString());
  XrdCommonFileSystem::GetBootReplyString(msgbody, env, XrdCommonFileSystem::kOpsError);
  
  SetStatus(XrdCommonFileSystem::kOpsError);

  XrdOucString response;
  response = msg; response += " "; response += Path;; response += " ["; response += strerror(errno); response += "]";

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

  XrdOucString response;
  response = errmsg; response += " "; response += Path;; 

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
bool 
XrdFstOfsFileSystem::OpenTransaction(unsigned long long fid) {
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  XrdOucString hexstring="";
  XrdCommonFileId::Fid2Hex(fid, hexstring);
  tagfile += hexstring;
  int fd = open(tagfile.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH | S_IRGRP);
  if (fd > 0) {
    close (fd);
    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool 
XrdFstOfsFileSystem::CloseTransaction(unsigned long long fid) {
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  XrdOucString hexstring="";
  XrdCommonFileId::Fid2Hex(fid, hexstring);
  tagfile += hexstring;
  if (unlink (tagfile.c_str())) 
    return false;
  return true;
}



/*----------------------------------------------------------------------------*/
XrdFstOfsStorage::XrdFstOfsStorage(const char* metadirectory)
{


  SetLogId("FstOfsStorage");

  runningTransfer = 0;

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
  long pageval = sysconf(_SC_PAGESIZE);
  if (pageval<0) {
    eos_crit("cannot get page size");
    exit(-1);
  }

  if (posix_memalign((void**)&scrubPattern[0], pageval,1024*1024) ||
      posix_memalign((void**)&scrubPattern[1], pageval,1024*1024) ||
      posix_memalign((void**)&scrubPatternVerify, pageval,1024*1024) ) {
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

  eos_info("starting verification thread");
  if ((rc = XrdSysThread::Run(&tid, XrdFstOfsStorage::StartFsVerify, static_cast<void *>(this),
			      0, "Verify Thread"))) {
    eos_crit("cannot start verify thread");
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
    fileSystemsMap[fsid] = fs;
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

  // write FS tag file
  XrdOucString tagfile = fs->GetPath();
  tagfile += "/.eosfsid";
  int fd = open(tagfile.c_str(),O_TRUNC|O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    fs->SetStatus(XrdCommonFileSystem::kBootFailure);
    fs->SetError(errno,"cannot open fs tagfile");
    fsMutex.UnLock();
    return false;
  } else {
    char ssfid[32];
    snprintf(ssfid,32,"%u", fsid);
    if ( (write(fd,ssfid,strlen(ssfid))) != (int)strlen(ssfid) ) {
      fs->SetStatus(XrdCommonFileSystem::kBootFailure);
      fs->SetError(errno,"cannot write fs tagfile");
      fsMutex.UnLock();
      close(fd);
      return false;
    }
  }

  close(fd);

  // create FS transaction directory
  
  XrdOucString transactionDirectory="";
  transactionDirectory = fs->GetPath();
  transactionDirectory += "/.eostransaction";
  
  if (mkdir(transactionDirectory.c_str(), S_IRWXU | S_IRGRP| S_IXGRP | S_IROTH | S_IXOTH)) {
    if (errno != EEXIST) {
      fs->SetStatus(XrdCommonFileSystem::kBootFailure);
      fs->SetError(errno,"cannot create transactiondirectory");
      fsMutex.UnLock();
      return false;
    }
  }

  fs->SetTransactionDirectory(transactionDirectory.c_str());

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

  if ( (!path) ) {
    fsMutex.UnLock();
    return false;
  }
  
  if ( (fs = fileSystems.Find(path)) ) {
    fileSystems.Del(path);
    std::vector<XrdFstOfsFileSystem*>::iterator it;
    for (it = fileSystemsVector.begin(); it != fileSystemsVector.end(); ++it) {
      if (*it == fs) {
	fileSystemsMap.erase((*it)->GetId());
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
void*
XrdFstOfsStorage::StartFsVerify(void * pp)
{
  XrdFstOfsStorage* storage = (XrdFstOfsStorage*)pp;
  storage->Verify();
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
	  fserrors = 1;
	  break;
	}
	// select the pattern randomly
	int rshift = (int) ( (1.0 *rand()/RAND_MAX)+ 0.5);
	eos_static_info("rshift is %d", rshift);
	for (int i=0; i< MB; i++) {
	  int nwrite = write(ff, scrubPattern[rshift], 1024 * 1024);
	  if (nwrite != (1024*1024)) {
	    eos_static_crit("Unable to write all needed bytes for scrubfile %s", scrubfile[k].c_str());
	    fserrors = 1;
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
	return 1;
      }

      int eberrors=0;

      for (int i=0; i< MB; i++) {
	int nread = read(ff, scrubPatternVerify, 1024 * 1024);
	if (nread != (1024*1024)) {
	  eos_static_crit("Unable to read all needed bytes from scrubfile %s", scrubfile[k].c_str());
	  fserrors = 1;
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
	// we trim only if the file reached 6 GB
	if (buf.st_size > (6000l * 1024 * 1024)) {
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
      eos_static_debug("%u files to transfer",transfers.size());

    bool more = false;

    std::list<XrdFstTransfer*>::iterator it;
    do {
      more = false;
      for ( it = transfers.begin(); it != transfers.end(); ++it) {
	int retc=0;
	(*it)->Debug();
	if ( (*it)->ShouldRun() ) {
	  XrdFstTransfer* transfer=*it;
	  more = true;
	  // remove it from the list
	  runningTransfer = transfer;
	  transfers.erase(it);
	  transferMutex.UnLock();
	  retc = transfer->Do();
	  
	  // try the transfer here
	  transferMutex.Lock();
	  runningTransfer = 0;
	  if (retc) {
	    if (transfer->ShouldRetry()) {
	      // reschedule
	      transfer->Reschedule(300);
	      // push it back on the list
	      transfers.push_back(transfer);
	    }
	    break;
	  }  else {
	    // delete the transfer object
	    delete transfer;
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
      gOFS.ReportQueueMutex.UnLock();

      gOFS.ReportQueueMutex.Lock();
      // send all reports away and dump them into the log
      XrdOucString report = gOFS.ReportQueue.front();
      gOFS.ReportQueueMutex.UnLock();
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
	gOFS.ReportQueueMutex.Lock();
	break;
      }
      gOFS.ReportQueueMutex.Lock();
      gOFS.ReportQueue.pop();
    }
    gOFS.ReportQueueMutex.UnLock();

    if (failure) 
      sleep(10);
    else 
      sleep(1);
  }
}

/*----------------------------------------------------------------------------*/
void
XrdFstOfsStorage::Verify()
{
  // this thread unlinks stored files
  fprintf(stderr,"Starting Verify thread\n");
  while(1) {
     verificationsMutex.Lock();
    if (!verifications.size()) {
      verificationsMutex.UnLock();
      sleep(1);
      continue;
    }
    
    XrdFstVerify* verifyfile = verifications.front();
    if (verifyfile) {
      eos_static_debug("got %llu\n", (unsigned long long) verifyfile);
      verifications.pop();
      runningVerify=verifyfile;

      // try to lock this file
      if (!gOFS.LockManager.TryLock(verifyfile->fId)) {
	eos_static_info("verifying File Id=%llu on Fs=%u postponed - file is currently open for writing");
	verifications.push(verifyfile);
	continue;
      }
    } else {
      eos_static_debug("got nothing");
      verificationsMutex.UnLock();
      runningVerify=0;
      continue;
    }
    verificationsMutex.UnLock();

    eos_static_debug("verifying File Id=%llu on Fs=%u", verifyfile->fId, verifyfile->fsId);
    // verify the file
    XrdOucString hexfid="";
    XrdCommonFileId::Fid2Hex(verifyfile->fId,hexfid);
    XrdOucErrInfo error;
    
    XrdOucString fstPath = "";
    
    XrdCommonFileId::FidPrefix2FullPath(hexfid.c_str(), verifyfile->localPrefix.c_str(),fstPath);
    
    // get current size on disk
    struct stat statinfo;
    if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
      eos_static_err("unable to verify file id=%llu on fs=%u path=%s - stat on local disk failed", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
    } else {
      // attach meta data
      XrdCommonFmd* fMd = 0;
      fMd = gFmdHandler.GetFmd(verifyfile->fId, verifyfile->fsId, 0, 0, 0, 0);
      bool localUpdate = false;
      if (!fMd) {
	eos_static_err("unable to verify id=%llu on fs=%u path=%s - no local MD stored", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
      } else {
	if ( fMd->fMd.size != (unsigned long long)statinfo.st_size) {
	  eos_static_err("updating file size: path=%s fid=%s changelog value %llu - fs value %llu",verifyfile->path.c_str(),hexfid.c_str(), statinfo.st_size, fMd->fMd.size);
	  localUpdate = true;
	}
	
	if ( fMd->fMd.lid != verifyfile->lId) {
	  eos_static_err("updating layout id: path=%s fid=%s central value %u - changelog value %u", verifyfile->path.c_str(),hexfid.c_str(),verifyfile->lId, fMd->fMd.lid);
	  localUpdate = true;
	}

	if ( fMd->fMd.cid != verifyfile->cId) {
	  eos_static_err("updating container: path=%s fid=%s central value %llu - changelog value %llu", verifyfile->path.c_str(),hexfid.c_str(),verifyfile->cId, fMd->fMd.cid);
	  localUpdate = true;
	}

	// update size
	fMd->fMd.size     = statinfo.st_size;
	fMd->fMd.lid      = verifyfile->lId;
	fMd->fMd.cid      = verifyfile->cId;
	
	// if set recalculate the checksum
	XrdFstOfsChecksum* checksummer = XrdFstOfsChecksumPlugins::GetChecksumObject(fMd->fMd.lid);
	
	unsigned long long scansize=0;
	float scantime = 0; // is ms
	
	if ((checksummer) && verifyfile->computeChecksum && (!checksummer->ScanFile(fstPath.c_str(), scansize, scantime, verifyfile->verifyRate))) {
	  eos_static_crit("cannot scan file to recalculate the checksum id=%llu on fs=%u path=%s",verifyfile->fId, verifyfile->fsId, fstPath.c_str());
	} else {
	  XrdOucString sizestring;
	  if (checksummer && verifyfile->computeChecksum) 
	    eos_static_info("rescanned checksum - size=%s time=%.02fms rate=%.02f MB/s limit=%d MB/s", XrdCommonFileSystem::GetReadableSizeString(sizestring, scansize, "B"), scantime, 1.0*scansize/1000/(scantime?scantime:99999999999999), verifyfile->verifyRate);

	  if (checksummer && verifyfile->computeChecksum) { 
	    int checksumlen=0;
	    checksummer->GetBinChecksum(checksumlen);
	    
	    // check if the computed checksum differs from the one in the change log
	    bool cxError=false;
	    for (int i=0 ; i< checksumlen; i++) {
	      if (fMd->fMd.checksum[i] != checksummer->GetBinChecksum(checksumlen)[i])
		cxError=true;
	    }
	    
	    if (cxError) {
	      eos_static_err("checksum invalid   : path=%s fid=%s checksum=%s", verifyfile->path.c_str(),hexfid.c_str(), checksummer->GetHexChecksum());
	      memset(fMd->fMd.checksum,0,sizeof(fMd->fMd.checksum));
	      // copy checksum into meta data
	      memcpy(fMd->fMd.checksum, checksummer->GetBinChecksum(checksumlen),checksumlen);
	      localUpdate =true;
	    } else {
	      eos_static_info("checksum OK        : path=%s fid=%s checksum=%s", verifyfile->path.c_str(),hexfid.c_str(), checksummer->GetHexChecksum());
	    }
	  }

	  XrdCommonPath cPath(verifyfile->path.c_str());
	  if (cPath.GetName())strncpy(fMd->fMd.name,cPath.GetName(),255);
	  if (verifyfile->container.length()) 
	    strncpy(fMd->fMd.container,verifyfile->container.c_str(),255);
	  
	  // commit local
	  if (localUpdate && (!gFmdHandler.Commit(fMd))) {
	    eos_static_err("unable to verify file id=%llu on fs=%u path=%s - commit to local MD storage failed", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
	  } else {
	    if (localUpdate) eos_static_info("commited verified meta data locally id=%llu on fs=%u path=%s",verifyfile->fId, verifyfile->fsId, fstPath.c_str());

	    // commit to central mgm cache, only if commitSize or commitChecksum is set
	    XrdOucString capOpaqueFile="";
	    XrdOucString mTimeString="";
	    capOpaqueFile += "/?";
	    capOpaqueFile += "&mgm.pcmd=commit";
	    capOpaqueFile += "&mgm.verify.checksum=1";
	    capOpaqueFile += "&mgm.size=";
	    char filesize[1024]; sprintf(filesize,"%llu", fMd->fMd.size);
	    capOpaqueFile += filesize;
	    capOpaqueFile += "&mgm.fid=";
	    capOpaqueFile += hexfid;
	    capOpaqueFile += "&mgm.path=";
	    capOpaqueFile += verifyfile->path.c_str();

	    if (checksummer && verifyfile->computeChecksum) {
	      capOpaqueFile += "&mgm.checksum=";
	      capOpaqueFile += checksummer->GetHexChecksum();
	      if (verifyfile->commitChecksum) {
		capOpaqueFile += "&mgm.commit.checksum=1";
	      }
	    }

	    if (verifyfile->commitSize) {
	      capOpaqueFile += "&mgm.commit.size=1";
	    }
	    
	    capOpaqueFile += "&mgm.mtime=";
	    capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, fMd->fMd.mtime);
	    capOpaqueFile += "&mgm.mtime_ns=";
	    capOpaqueFile += XrdCommonFileSystem::GetSizeString(mTimeString, fMd->fMd.mtime_ns);
	    
	    capOpaqueFile += "&mgm.add.fsid=";
	    capOpaqueFile += (int)fMd->fMd.fsid;

	    if (verifyfile->commitSize || verifyfile->commitChecksum) {
	      if (localUpdate) eos_static_info("commited verified meta data centrally id=%llu on fs=%u path=%s",verifyfile->fId, verifyfile->fsId, fstPath.c_str());
	      int rc = gOFS.CallManager(&error, verifyfile->path.c_str(),verifyfile->managerId.c_str(), capOpaqueFile);
	      if (rc) {
		eos_static_err("unable to verify file id=%s fs=%u at manager %s",hexfid.c_str(), verifyfile->fsId, verifyfile->managerId.c_str()); 
	      }
	    }
	  }
	}
	if (checksummer) {delete checksummer;}
	if (fMd) {delete fMd;}
      }
    }
    runningVerify=0;
    gOFS.LockManager.UnLock(verifyfile->fId);
    if (verifyfile) delete verifyfile;
  }
}

/*----------------------------------------------------------------------------*/
bool 
XrdFstOfsStorage::OpenTransaction(unsigned int fsid, unsigned long long fid) {
  XrdFstOfsFileSystem* fs = fileSystemsMap[fsid];
  if (fs) {
    return fs->OpenTransaction(fid);
  }
  return false;
}


/*----------------------------------------------------------------------------*/
bool 
XrdFstOfsStorage::CloseTransaction(unsigned int fsid, unsigned long long fid) {
  XrdFstOfsFileSystem* fs = fileSystemsMap[fsid];
  if (fs) {
    return fs->CloseTransaction(fid);
  }
  return false;
}
