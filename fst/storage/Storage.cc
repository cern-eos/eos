/*----------------------------------------------------------------------------*/
#include "fst/Config.hh"
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "common/Fmd.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "mq/XrdMqMessaging.hh"
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
/*----------------------------------------------------------------------------*/
#include "XrdOss/XrdOssApi.hh"

extern XrdOssSys  *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

#ifdef __APPLE__ 
#define O_DIRECT 0
#endif

/*----------------------------------------------------------------------------*/
FileSystem::FileSystem(const char* queuepath, const char* queue, XrdMqSharedObjectManager* som) : eos::common::FileSystem(queuepath,queue,som, true) 
{
  last_blocks_free=0;
  last_status_broadcast=0;
  transactionDirectory="";
  statFs = 0;
  scanDir = 0;
  std::string n1 = queuepath; n1 += "/drain";
  std::string n2 = queuepath; n2 += "/balance";
  std::string n3 = queuepath; n3 += "/extern";
  mTxDrainQueue   = new TransferQueue   (&mDrainQueue, n1.c_str());
  mTxBalanceQueue = new TransferQueue (&mBalanceQueue, n2.c_str());
  mTxExternQueue  = new TransferQueue  (&mExternQueue, n3.c_str());

  mTxMultiplexer.Add(mTxDrainQueue);
  mTxMultiplexer.Add(mTxBalanceQueue);
  mTxMultiplexer.Add(mTxExternQueue);
  mTxMultiplexer.Run();
}

/*----------------------------------------------------------------------------*/
FileSystem::~FileSystem() {
  if (scanDir) {
    delete scanDir;
  }

  // FIXME !!!
  // we accept this tiny memory leak to be able to let running transfers callback their queue
  // -> we don't delete them here!
  //  if (mTxDrainQueue) {
  //    delete mTxDrainQueue;
  //  }
  //  if (mTxBalanceQueue) {
  //    delete mTxBalanceQueue;
  //  }
  //  if (mTxExternQueue) {
  //    delete mTxExternQueue;
  //  }
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastError(const char* msg) 
{
  SetStatus(eos::common::FileSystem::kOpsError);
  SetError(errno?errno:EIO,msg);

  //  eos_debug("broadcasting error message: %s", msgbody.c_str());
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastError(int errc, const char* errmsg) 
{
  SetStatus(eos::common::FileSystem::kOpsError);
  SetError(errno?errno:EIO,errmsg);
  
  //  eos_debug("broadcasting error message: %s", msgbody.c_str());
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastStatus()
{

  //  SetStatFs
  //  gOFS.OpenFidString(Id, rwstatus);
  //  GetLoadString(loadstatus);
  //  msgbody += rwstatus;
  //  msgbody += "&";
  //  msgbody += loadstatus;
  //  eos_debug("broadcasting status message: %s", msgbody.c_str());
}


/*----------------------------------------------------------------------------*/
eos::common::Statfs*
FileSystem::GetStatfs() 
{ 

  statFs = eos::common::Statfs::DoStatfs(GetPath().c_str());
  if ((!statFs) && GetPath().length()) {
    eos_err("cannot statfs");
    BroadcastError("cannot statfs");
    return 0;
  }

  return statFs;
}


/*----------------------------------------------------------------------------*/
void
FileSystem::RunScanner(Load* fstLoad, time_t interval) 
{ 
  if (scanDir) {
    delete scanDir;
  }

  // create the object running the scanner thread
  scanDir = new ScanDir(GetPath().c_str(), fstLoad, true, interval);
  eos_info("Started 'ScanDir' thread with interval time of %u seconds", (unsigned long) interval);
}

/*----------------------------------------------------------------------------*/
bool 
FileSystem::OpenTransaction(unsigned long long fid) {
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  XrdOucString hexstring="";
  eos::common::FileId::Fid2Hex(fid, hexstring);
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
FileSystem::CloseTransaction(unsigned long long fid) {
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  XrdOucString hexstring="";
  eos::common::FileId::Fid2Hex(fid, hexstring);
  tagfile += hexstring;
  if (unlink (tagfile.c_str())) 
    return false;
  return true;
}



/*----------------------------------------------------------------------------*/
Storage::Storage(const char* metadirectory)
{
  SetLogId("FstOfsStorage");

  // make metadir
  XrdOucString mkmetalogdir = "mkdir -p "; mkmetalogdir += metadirectory;  mkmetalogdir += " >& /dev/null";
  system(mkmetalogdir.c_str());

  // own the directory
  mkmetalogdir = "chown -R daemon.daemon "; mkmetalogdir += metadirectory; mkmetalogdir += " >& /dev/null"; 

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

  eos_info("starting scrubbing thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsScrub, static_cast<void *>(this),
                              0, "Scrubber"))) {
    eos_crit("cannot start scrubber thread");
    zombie = true;
  }

  eos_info("starting trim thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsTrim, static_cast<void *>(this),
                              0, "Meta Store Trim"))) {
    eos_crit("cannot start trimming theread");
    zombie = true;
  }

  eos_info("starting deletion thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsRemover, static_cast<void *>(this),
                              0, "Data Store Remover"))) {
    eos_crit("cannot start deletion theread");
    zombie = true;
  }

  eos_info("starting report thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsReport, static_cast<void *>(this),
                              0, "Report Thread"))) {
    eos_crit("cannot start report thread");
    zombie = true;
  }

  eos_info("starting error report thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsErrorReport, static_cast<void *>(this),
                              0, "Error Report Thread"))) {
    eos_crit("cannot start error report thread");
    zombie = true;
  }

  eos_info("starting verification thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsVerify, static_cast<void *>(this),
			      0, "Verify Thread"))) {
    eos_crit("cannot start verify thread");
    zombie = true;
  }

  eos_info("starting filesystem communication thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsCommunicator, static_cast<void *>(this),
			      0, "Communicator Thread"))) {
    eos_crit("cannot start communicator thread");
    zombie = true;
  }

  eos_info("starting filesystem publishing thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsPublisher, static_cast<void *>(this),
			      0, "Publisher Thread"))) {
    eos_crit("cannot start publisher thread");
    zombie = true;
  }

  eos_info("enabling net/io load monitor");
  fstLoad.Monitor();
}


/*----------------------------------------------------------------------------*/
Storage*
Storage::Create(const char* metadirectory)
{
  Storage* storage = new Storage(metadirectory);
  if (storage->IsZombie()) {
    delete storage;
    return 0;
  } 
  return storage;
}


/*----------------------------------------------------------------------------*/
void 
Storage::Boot(FileSystem *fs)
{
  fs->SetStatus(eos::common::FileSystem::kBooting);

  if ( !fs) {
    return;
  }

  eos::common::FileSystem::fsid_t fsid = fs->GetId();
  std::string uuid = fs->GetString("uuid");

  eos_info("booting filesystem %s id=%u uuid=%s",fs->GetQueuePath().c_str(), (unsigned int)fsid, uuid.c_str());

  if (!fsid) 
    return;
  
  // try to statfs the filesystem
  eos::common::Statfs* statfs = eos::common::Statfs::DoStatfs(fs->GetPath().c_str());
  if (!statfs) {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno?errno:EIO,"cannot statfs filesystem");
    return;
  }

  // try to own that directory
  XrdOucString chownline="chown daemon.daemon ";chownline += fs->GetPath().c_str();
  system(chownline.c_str());
  
  // test if we have rw access
  struct stat buf;
  if ( ::stat( fs->GetPath().c_str(), &buf) || (buf.st_uid != geteuid()) || ( (buf.st_mode & S_IRWXU ) != S_IRWXU) ) {

    if ( (buf.st_mode & S_IRWXU ) != S_IRWXU) {
      errno = EPERM;
    }

    if (buf.st_uid != geteuid()) {
      errno = ENOTCONN;
    }
    
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno?errno:EIO, "cannot have <rw> access");
    return;
  }
  
  gOFS.OpenFidMutex.Lock();
  gOFS.ROpenFid.clear_deleted_key();
  gOFS.ROpenFid[fsid].clear_deleted_key();
  gOFS.WOpenFid[fsid].clear_deleted_key();
  gOFS.ROpenFid.set_deleted_key(0);
  gOFS.ROpenFid[fsid].set_deleted_key(0);
  gOFS.WOpenFid[fsid].set_deleted_key(0);
  gOFS.OpenFidMutex.UnLock();

  if (!eos::common::gFmdHandler.AttachLatestChangeLogFile(metaDirectory.c_str(), fsid)) {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(EFAULT,"cannot attach to latest change log file - see the fst logfile for details");
    return;
  }

  // check if there is a lable on the disk and if the configuration shows the same fsid + uuid
  if (!CheckLabel(fs->GetPath(),fsid,uuid)) {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(EFAULT,"the filesystem has a different label (fsid+uuid) than the configuration");
    return;
  } 

  if (!FsLabel(fs->GetPath(),fsid,uuid)) {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(EFAULT,"cannot write the filesystem label (fsid+uuid) - please check filesystem state/permissions");
    return;
  }

  // create FS transaction directory
  std::string transactionDirectory = fs->GetPath();
  transactionDirectory += "/.eostransaction";
  
  if (mkdir(transactionDirectory.c_str(), S_IRWXU | S_IRGRP| S_IXGRP | S_IROTH | S_IXOTH)) {
    if (errno != EEXIST) {
      fs->SetStatus(eos::common::FileSystem::kBootFailure);
      fs->SetError(errno?errno:EIO,"cannot create transactiondirectory");
      return;
    }
  }

  if (chown(transactionDirectory.c_str(), geteuid(),getegid())) {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno?errno:EIO,"cannot change ownership of transactiondirectory");
    return;
  }

  fs->SetTransactionDirectory(transactionDirectory.c_str());

  fs->SetLongLong("stat.bootdonetime", (unsigned long long) time(NULL));
  fs->SetStatus(eos::common::FileSystem::kBooted);
  fs->SetError(0,"");
  return;
}

bool 
Storage::FsLabel(std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid)
{
  //----------------------------------------------------------------
  //! writes file system label files .eosfsid .eosuuid according to config (if they didn't exist!)
  //----------------------------------------------------------------

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";
 
  struct stat buf;

  if (stat(fsidfile.c_str(), &buf)) {
    int fd = open(fsidfile.c_str(),O_TRUNC|O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) {
      return false;
    } else {
      char ssfid[32];
      snprintf(ssfid,32,"%u", fsid);
      if ( (write(fd,ssfid,strlen(ssfid))) != (int)strlen(ssfid) ) {
	close(fd);
	return false;
      }
    }
    close(fd);
  }

  std::string uuidfile = path;
  uuidfile += "/.eosfsuuid";

  if (stat(uuidfile.c_str(), &buf)) {
    int fd = open(uuidfile.c_str(),O_TRUNC|O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) {
      return false;
    } else {
      if ( (write(fd,uuid.c_str(),strlen(uuid.c_str())+1)) != (int)(strlen(uuid.c_str())+1)) {
	close(fd);
	return false;
      }
    }
    close(fd);
  }
  return true;
}

/*----------------------------------------------------------------------------*/
bool 
Storage::CheckLabel(std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid, bool failenoent) 
{
  //----------------------------------------------------------------
  //! checks that the label on the filesystem is the same as the configuration
  //----------------------------------------------------------------

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";
  
  struct stat buf;

  std::string ckuuid=uuid;
  eos::common::FileSystem::fsid_t ckfsid=fsid;

  if (!stat(fsidfile.c_str(), &buf)) {
    int fd = open(fsidfile.c_str(),O_RDONLY);
    if (fd < 0) {
      return false;
    } else {
      char ssfid[32];
      memset(ssfid,0,sizeof(ssfid));
      int nread = read(fd,ssfid,sizeof(ssfid)-1);
      if (nread<0) {
	close(fd);
	return false;
      }
      close(fd);
      // for safety
      if (nread < (int)(sizeof(ssfid)-1)) 
	ssfid[nread]=0;
      else
	ssfid[31]=0;
      if (ssfid[strnlen(ssfid,sizeof(ssfid))-1] == '\n') {
        ssfid[strnlen(ssfid, sizeof(ssfid))-1] = 0;
      }
      ckfsid = atoi(ssfid);
    }
  } else {
    if (failenoent) 
      return false;
  }

  // write FS uuid file
  std::string uuidfile = path;
  uuidfile += "/.eosfsuuid";

  if (!stat(uuidfile.c_str(), &buf)) {
    int fd = open(uuidfile.c_str(),O_RDONLY);
    if (fd < 0) {
      return false;
    } else {
      char suuid[4096];
      memset(suuid,0,sizeof(suuid));
      int nread = read(fd,suuid,sizeof(suuid));
      if (nread <0) {
	close(fd);
	return false;
      }
      close(fd);
      // for protection
      suuid[4095]=0;
      // remove \n 
      if (suuid[strnlen(suuid,sizeof(suuid))-1] == '\n')
	suuid[strnlen(suuid,sizeof(suuid))-1] = 0;

      ckuuid = suuid;
    }
  } else {
    if (failenoent) 
      return false;
  }
  
  //  fprintf(stderr,"%d <=> %d %s <=> %s\n", fsid, ckfsid, ckuuid.c_str(), uuid.c_str());
  if ( (fsid != ckfsid) || (ckuuid != uuid) ) {
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetFsidFromLabel(std::string path, eos::common::FileSystem::fsid_t &fsid)
{
  //----------------------------------------------------------------
  //! return the file system id from the filesystem fsid label file
  //----------------------------------------------------------------

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";
  
  struct stat buf;
  fsid = 0;

  if (!stat(fsidfile.c_str(), &buf)) {
    int fd = open(fsidfile.c_str(),O_RDONLY);
    if (fd < 0) {
      return false;
    } else {
      char ssfid[32];
      memset(ssfid,0,sizeof(ssfid));
      int nread = read(fd,ssfid,sizeof(ssfid)-1);
      if (nread<0) {
	close(fd);
	return false;
      }
      close(fd);
      // for safety
      if (nread < (int)(sizeof(ssfid)-1)) 
	ssfid[nread]=0;
      else
	ssfid[31]=0;
      if (ssfid[strnlen(ssfid,sizeof(ssfid))-1] == '\n') {
        ssfid[strnlen(ssfid, sizeof(ssfid))-1] = 0;
      }
      fsid = atoi(ssfid);
    }
  } 
  if (fsid) 
    return true;
  else 
    return false;
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetFsidFromPath(std::string path, eos::common::FileSystem::fsid_t &fsid)
{
  //----------------------------------------------------------------
  //! return the file system id from the configured filesystem vector
  //----------------------------------------------------------------
  
  eos::common::RWMutexReadLock lock (fsMutex);
  fsid = 0;
  
  for (unsigned int i=0; i< fileSystemsVector.size(); i++) {
    if (fileSystemsVector[i]->GetPath() == path) {
      fsid = fileSystemsVector[i]->GetId();
      break;
    }
  }
  if (fsid) 
    return true;
  else
    return false;
}
  

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsScrub(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Scrub();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsTrim(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Trim();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsRemover(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Remover();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsReport(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Report();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsErrorReport(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->ErrorReport();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsVerify(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Verify();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsCommunicator(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Communicator();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsPublisher(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Publish();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void
Storage::Scrub()
{
  // create a 1M pattern
  eos_static_info("Creating Scrubbing pattern ...");
  for (int i=0;i< 1024*1024/16; i+=2) {
    scrubPattern[0][i]=0xaaaa5555aaaa5555ULL;
    scrubPattern[0][i+1]=0x5555aaaa5555aaaaULL;
    scrubPattern[1][i]=0x5555aaaa5555aaaaULL;
    scrubPattern[1][i+1]=0xaaaa5555aaaa5555ULL;
  }


  eos_static_info("Start Scrubbing ...");

  // this thread reads the oldest files and checks their integrity
  while(1) {
    time_t start = time(0);
    unsigned int nfs=0;
    {
      eos::common::RWMutexReadLock lock (fsMutex);
      nfs = fileSystemsVector.size();
      eos_static_info("FileSystem Vector %u",nfs);
    }    
    for (unsigned int i=0; i< nfs; i++) {
      fsMutex.LockRead();

      if (i< fileSystemsVector.size()) {
	std::string path = fileSystemsVector[i]->GetPath();
        
	if (!fileSystemsVector[i]->GetStatfs()) {
          eos_static_info("GetStatfs failed");
	  fsMutex.UnLockRead();
	  continue;
	}

        
	unsigned long long free   = fileSystemsVector[i]->GetStatfs()->GetStatfs()->f_bfree;
	unsigned long long blocks = fileSystemsVector[i]->GetStatfs()->GetStatfs()->f_blocks;
	unsigned long id = fileSystemsVector[i]->GetId();
        eos::common::FileSystem::fsstatus_t bootstatus = fileSystemsVector[i]->GetStatus();
        eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[i]->GetConfigStatus();

	fsMutex.UnLockRead();
        
        if (!id) 
          continue;

        // check if there is a lable on the disk and if the configuration shows the same fsid
        if ( (bootstatus == eos::common::FileSystem::kBooted) &&
             (configstatus >= eos::common::FileSystem::kRO) &&
             ( !CheckLabel(fileSystemsVector[i]->GetPath(),fileSystemsVector[i]->GetId(),fileSystemsVector[i]->GetString("uuid"),true)) ) {
          fileSystemsVector[i]->BroadcastError(EIO,"filesystem seems to be not mounted anymore");
          continue;
        } 
        

        // don't scrub on filesystems which are not in writable mode!
        if (configstatus < eos::common::FileSystem::kWO) 
          continue;

        if (bootstatus != eos::common::FileSystem::kBooted) 
          continue;

        // don't scrub on filesystems which are not booted
        
	if (ScrubFs(path.c_str(),free,blocks,id)) {
	  // filesystem has errors!
	  fsMutex.LockRead();
	  if ((i < fileSystemsVector.size()) && fileSystemsVector[i]) {
	    fileSystemsVector[i]->BroadcastError(EIO,"filesystem probe error detected");
	  }
	  fsMutex.UnLockRead();
	}
      } else {
	fsMutex.UnLockRead();
      }
    }
    time_t stop = time(0);

    int nsleep = ( (300)-(stop-start));
    eos_static_info("Scrubber will pause for %u seconds",nsleep);
    sleep(nsleep);
  }
}

/*----------------------------------------------------------------------------*/
int
Storage::ScrubFs(const char* path, unsigned long long free, unsigned long long blocks, unsigned long id) 
{
  int MB = 1; // the test files have 1 MB

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
Storage::Trim()
{
  // this thread supervises the changelogfile and trims them from time to time to shrink their size
  while(1) {
    sleep(10);
    google::sparse_hash_map<unsigned long long, google::dense_hash_map<unsigned long long, unsigned long long> >::const_iterator it;
    eos_static_info("Trimming Size  %u", eos::common::gFmdHandler.FmdMap.size());
    eos::common::RWMutexWriteLock (eos::common::gFmdHandler.Mutex);
    for ( it = eos::common::gFmdHandler.FmdMap.begin(); it != eos::common::gFmdHandler.FmdMap.end(); ++it) {
      eos_static_info("Trimming fsid=%llu ",it->first);
      int fsid = it->first;

      // stat the size of this logfile
      struct stat buf;
      if (fstat(eos::common::gFmdHandler.fdChangeLogRead[fsid],&buf)) {
	eos_static_err("Cannot stat the changelog file for fsid=%llu for", it->first);
      } else {
	// we trim only if the file reached 6 GB
	if (buf.st_size > (6000LL * 1024 * 1024)) {
	  if (!eos::common::gFmdHandler.TrimLogFile(fsid)) {
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
Storage::Remover()
{
  // this thread unlinks stored files
  while(1) {
    // since we use vector and erase from the beginning, this is not really a perfect choice, but we don't have any performance issues here
    deletionsMutex.Lock();
    if (deletions.size()) {
      eos_static_debug("%u files to delete",deletions.size());
      Deletion todelete =  deletions[0];
      deletionsMutex.UnLock();

      for (unsigned int j=0; j< todelete.fIdVector.size(); j++) {
        eos_static_debug("Deleting File Id=%llu on Fs=%u", todelete.fIdVector[j], todelete.fsId);
        // delete the file
        XrdOucString hexstring="";
        eos::common::FileId::Fid2Hex(todelete.fIdVector[j],hexstring);
        XrdOucErrInfo error;
        
        XrdOucString capOpaqueString="/?mgm.pcmd=drop";
        XrdOucString OpaqueString = "";
        OpaqueString+="&mgm.fsid="; OpaqueString += (int)todelete.fsId;
        OpaqueString+="&mgm.fid=";  OpaqueString += hexstring;
        OpaqueString+="&mgm.localprefix="; OpaqueString += todelete.localPrefix;
        XrdOucEnv Opaque(OpaqueString.c_str());
        capOpaqueString += OpaqueString;
        
        if ( (gOFS._rem("/DELETION",error, (const XrdSecEntity*)0, &Opaque)!= SFS_OK)) {
          eos_static_err("unable to remove fid %s fsid %lu localprefix=%s",hexstring.c_str(), todelete.fsId, todelete.localPrefix.c_str());
        } 
        
        // update the manager
        int rc = gOFS.CallManager(0, 0, todelete.managerId.c_str(), capOpaqueString);
        if (rc) {
          eos_static_err("unable to drop file id %s fsid %u at manager %s",hexstring.c_str(), todelete.fsId, todelete.managerId.c_str()); 
        }
      }
      deletionsMutex.Lock();
      deletions.erase(deletions.begin());
      deletionsMutex.UnLock();
    } else {
      deletionsMutex.UnLock();
      usleep(100000);
    }
  }
}

/*----------------------------------------------------------------------------*/
void
Storage::Report()
{
  // this thread send's report messages from the report queue
  bool failure;

  XrdOucString monitorReceiver = Config::gConfig.FstDefaultReceiverQueue;
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
Storage::ErrorReport()
{
  // this thread send's error report messages from the error queue
  bool failure;

  XrdOucString errorReceiver = Config::gConfig.FstDefaultReceiverQueue;
  errorReceiver.replace("*/mgm", "*/errorreport");
  
  eos::common::Logging::LogCircularIndex localCircularIndex;
  localCircularIndex.resize(LOG_DEBUG+1);

  // initialize with the current positions of the circular index
  for (size_t i=LOG_EMERG; i<= LOG_DEBUG; i++) {
    localCircularIndex[i] = eos::common::Logging::gLogCircularIndex[i];
  }
  
  while(1) {
    failure = false;

    // push messages from the circular buffers to the error queue
    for (size_t i=LOG_EMERG; i <= LOG_ERR; i++) {
      eos::common::Logging::gMutex.Lock();
      size_t endpos = eos::common::Logging::gLogCircularIndex[i];
      eos::common::Logging::gMutex.UnLock();
	
      if (endpos > localCircularIndex[i] ) {
	// we have to follow the messages and add them to the queue
	gOFS.ErrorReportQueueMutex.Lock();
	for (unsigned long j = localCircularIndex[i]; j < endpos; j++) {
	  // copy the messages to the queue
	  eos::common::Logging::gMutex.Lock();
	  gOFS.ErrorReportQueue.push(eos::common::Logging::gLogMemory[i][j%eos::common::Logging::gCircularIndexSize]);
	  eos::common::Logging::gMutex.UnLock();
	}
	localCircularIndex[i] = endpos;
	gOFS.ErrorReportQueueMutex.UnLock();
	
      }
    }

    gOFS.ErrorReportQueueMutex.Lock();
    while ( gOFS.ErrorReportQueue.size()>0) {
      gOFS.ErrorReportQueueMutex.UnLock();

      gOFS.ErrorReportQueueMutex.Lock();
      // send all reports away and dump them into the log
      XrdOucString report = gOFS.ErrorReportQueue.front().c_str();
      gOFS.ErrorReportQueueMutex.UnLock();

      // this type of messages can have no receiver
      XrdMqMessage message("errorreport");
      message.MarkAsMonitor();

      message.SetBody(report.c_str());
      
      eos_debug("broadcasting errorreport message: %s", report.c_str());
      

      if (!XrdMqMessaging::gMessageClient.SendMessage(message, errorReceiver.c_str())) {
	// display communication error
	eos_err("cannot send errorreport broadcast");
	failure = true;
	gOFS.ErrorReportQueueMutex.Lock();
	break;
      }
      gOFS.ErrorReportQueueMutex.Lock();
      gOFS.ErrorReportQueue.pop();
    }
    gOFS.ErrorReportQueueMutex.UnLock();

    if (failure) 
      sleep(10);
    else 
      sleep(1);
  }
}

/*----------------------------------------------------------------------------*/
void
Storage::Verify()
{
  // this thread unlinks stored files
  while(1) {
     verificationsMutex.Lock();
    if (!verifications.size()) {
      verificationsMutex.UnLock();
      sleep(1);
      continue;
    }
    
    eos::fst::Verify* verifyfile = verifications.front();
    if (verifyfile) {
      eos_static_debug("got %llu\n", (unsigned long long) verifyfile);
      verifications.pop();
      runningVerify=verifyfile;

      // try to lock this file
      if (!gOFS.LockManager.TryLock(verifyfile->fId)) {
	eos_static_info("verifying File Id=%x on Fs=%u postponed - file is currently open for writing", verifyfile->fId, verifyfile->fsId);
	verifications.push(verifyfile);
	verificationsMutex.UnLock();
	continue;
      }
    } else {
      eos_static_debug("got nothing");
      verificationsMutex.UnLock();
      runningVerify=0;
      continue;
    }
    verificationsMutex.UnLock();

    eos_static_debug("verifying File Id=%x on Fs=%u", verifyfile->fId, verifyfile->fsId);
    // verify the file
    XrdOucString hexfid="";
    eos::common::FileId::Fid2Hex(verifyfile->fId,hexfid);
    XrdOucErrInfo error;
    
    XrdOucString fstPath = "";
    
    eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(), verifyfile->localPrefix.c_str(),fstPath);
    
    // get current size on disk
    struct stat statinfo;
    if ((XrdOfsOss->Stat(fstPath.c_str(), &statinfo))) {
      eos_static_err("unable to verify file id=%x on fs=%u path=%s - stat on local disk failed", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
    } else {
      // attach meta data
      eos::common::Fmd* fMd = 0;
      fMd = eos::common::gFmdHandler.GetFmd(verifyfile->fId, verifyfile->fsId, 0, 0, 0, verifyfile->commitFmd);
      bool localUpdate = false;
      if (!fMd) {
	eos_static_err("unable to verify id=%x on fs=%u path=%s - no local MD stored", verifyfile->fId, verifyfile->fsId, fstPath.c_str());
      } else {
	if ( fMd->fMd.size != (unsigned long long)statinfo.st_size) {
	  eos_static_err("updating file size: path=%s fid=%s changelog value %x - fs value %llu",verifyfile->path.c_str(),hexfid.c_str(), statinfo.st_size, fMd->fMd.size);
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
	CheckSum* checksummer = ChecksumPlugins::GetChecksumObject(fMd->fMd.lid);
	
	unsigned long long scansize=0;
	float scantime = 0; // is ms
	
	if ((checksummer) && verifyfile->computeChecksum && (!checksummer->ScanFile(fstPath.c_str(), scansize, scantime, verifyfile->verifyRate))) {
	  eos_static_crit("cannot scan file to recalculate the checksum id=%llu on fs=%u path=%s",verifyfile->fId, verifyfile->fsId, fstPath.c_str());
	} else {
	  XrdOucString sizestring;
	  if (checksummer && verifyfile->computeChecksum) 
	    eos_static_info("rescanned checksum - size=%s time=%.02fms rate=%.02f MB/s limit=%d MB/s", eos::common::StringConversion::GetReadableSizeString(sizestring, scansize, "B"), scantime, 1.0*scansize/1000/(scantime?scantime:99999999999999LL), verifyfile->verifyRate);

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
	    eos::common::Attr *attr = eos::common::Attr::OpenAttr(fstPath.c_str());
	    if (attr) {
	      // update the extended attributes
	      attr->Set("user.eos.checksum",checksummer->GetBinChecksum(checksumlen), checksumlen);
	      delete attr;
	    }
	  }

	  eos::common::Path cPath(verifyfile->path.c_str());
	  if (cPath.GetName())strncpy(fMd->fMd.name,cPath.GetName(),255);
	  if (verifyfile->container.length()) 
	    strncpy(fMd->fMd.container,verifyfile->container.c_str(),255);
	  
	  // commit local
	  if (localUpdate && (!eos::common::gFmdHandler.Commit(fMd))) {
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
	    capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fMd->fMd.mtime);
	    capOpaqueFile += "&mgm.mtime_ns=";
	    capOpaqueFile += eos::common::StringConversion::GetSizeString(mTimeString, (unsigned long long)fMd->fMd.mtime_ns);
	    
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
void
Storage::Communicator()
{
  // this thread retrieves changes on the ObjectManager queue and maintains associated filesystem objects
  eos_static_info("Communicator activated ...");  

  while (1) {
    // wait for new filesystem definitions
    gOFS.ObjectManager.SubjectsSem.Wait();
    bool unlocked;
    
    eos_static_debug("received shared object notification ...");

    /////////////////////////////////////////////////////////////////////////////////////
    // => implements the creation of filesystem objects
    /////////////////////////////////////////////////////////////////////////////////////

    unlocked = false;
    gOFS.ObjectManager.SubjectsMutex.Lock(); // here we have to take care that we lock this only to retrieve the subject ... to create a new queue we have to free the lock
    
    while (gOFS.ObjectManager.CreationSubjects.size()) {
      std::string newsubject = gOFS.ObjectManager.CreationSubjects.front();
      gOFS.ObjectManager.CreationSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;

      XrdOucString queue = newsubject.c_str();

      if (queue == Config::gConfig.FstQueueWildcard)
	continue;
      
      if ( (queue.find("/txqueue/")!= STR_NPOS) ) {
        // this is a transfer queue we, don't need to take action
        continue;
      }

      if (! queue.beginswith(Config::gConfig.FstQueue)) {
	eos_static_info("no action on creation of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
	continue;
      } else {
	eos_static_info("received creation notification of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
      }
      
      eos::common::RWMutexWriteLock lock(fsMutex);
      FileSystem* fs = 0;

      if (!(fileSystems.count(queue.c_str()))) {
	fs = new FileSystem(queue.c_str(),Config::gConfig.FstQueue.c_str(), &gOFS.ObjectManager);
	fileSystems[queue.c_str()] = fs;
	fileSystemsVector.push_back(fs);
	fileSystemsMap[fs->GetId()] = fs;
	eos_static_info("setting up filesystem %s", queue.c_str());
	fs->SetStatus(eos::common::FileSystem::kDown);
      }
    }

    if (!unlocked) {
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;
    }

    /////////////////////////////////////////////////////////////////////////////////////
    // => implements the deletion of filesystem objects
    /////////////////////////////////////////////////////////////////////////////////////    

    unlocked = false;
    gOFS.ObjectManager.SubjectsMutex.Lock(); // here we have to take care that we lock this only to retrieve the subject ... to create a new queue we have to free the lock

    // implements the deletion of filesystem objects
    while (gOFS.ObjectManager.DeletionSubjects.size()) {
      std::string newsubject = gOFS.ObjectManager.DeletionSubjects.front();
      gOFS.ObjectManager.DeletionSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked=true;

      XrdOucString queue = newsubject.c_str();

      if ( (queue.find("/txqueue/")!= STR_NPOS) ) {
        // this is a transfer queue we, don't need to take action
        continue;
      }

      if (! queue.beginswith(Config::gConfig.FstQueue)) {
	eos_static_err("illegal subject found in deletion list <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
	continue;
      } else {
	eos_static_info("received deletion notification of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
      }

      eos::common::RWMutexWriteLock lock(fsMutex);
      if ((fileSystems.count(queue.c_str()))) {
	if (fileSystems.count(queue.c_str())) {
	  std::map<eos::common::FileSystem::fsid_t , FileSystem*>::iterator mit;

	  for (mit = fileSystemsMap.begin(); mit != fileSystemsMap.end(); mit++) {
	    if (mit->second == fileSystems[queue.c_str()]) {
	      fileSystemsMap.erase(mit);
	      break;
	    }
	  }

	  std::vector <FileSystem*>::iterator it;
	  for (it=fileSystemsVector.begin(); it!=fileSystemsVector.end(); it++) {
	    if (*it == fileSystems[queue.c_str()]) {
	      fileSystemsVector.erase(it);
	      break;
	    }
	  }
	  delete fileSystems[queue.c_str()];
	  fileSystems.erase(queue.c_str());
	}
	eos_static_info("deleting filesystem %s", queue.c_str());
      }
    }

    if (!unlocked) {
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;
    }


    /////////////////////////////////////////////////////////////////////////////////////
    // => implements the modification notification of filesystem objects
    /////////////////////////////////////////////////////////////////////////////////////    

    unlocked = false;
    gOFS.ObjectManager.SubjectsMutex.Lock(); // here we have to take care that we lock this only to retrieve the subject ... to create a new queue we have to free the lock
    
    // listens on modifications on filesystem objects
    if (gOFS.ObjectManager.ModificationSubjects.size()) {
      std::string newsubject = gOFS.ObjectManager.ModificationSubjects.front();
      gOFS.ObjectManager.ModificationSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked=true;

      XrdOucString queue = newsubject.c_str();
      if (! queue.beginswith(Config::gConfig.FstQueue)) {
	eos_static_err("illegal subject found in modification list <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
	break;
      } else {
	eos_static_info("received modification notification of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
      }

      // seperate <path> from <key>
      XrdOucString key=queue;
      int dpos = 0;
      if ((dpos = queue.find(";"))!= STR_NPOS){
	key.erase(0,dpos+1);
	queue.erase(dpos);
      }

      eos::common::RWMutexReadLock lock(fsMutex);
      if ((fileSystems.count(queue.c_str()))) {
	eos_static_info("got modification on <subqueue>=%s <key>=%s", queue.c_str(),key.c_str());
	
	gOFS.ObjectManager.HashMutex.LockRead();

	XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(),"hash");
	if (hash) {
	  if (key == "id") {
	    unsigned int fsid= hash->GetUInt(key.c_str());
	    gOFS.ObjectManager.HashMutex.UnLockRead();

	    // setup the reverse lookup by id
	    fileSystemsMap[fsid] = fileSystems[queue.c_str()];
	    eos_static_info("setting reverse lookup for fsid %u", fsid);
	    // check if we are autobooting
	    if (eos::fst::Config::gConfig.autoBoot && (fileSystems[queue.c_str()]->GetStatus() <= eos::common::FileSystem::kDown) && (fileSystems[queue.c_str()]->GetConfigStatus() > eos::common::FileSystem::kOff) ) {
	      Boot(fileSystems[queue.c_str()]);
	    }
	  }
	  if (key == "bootsenttime") {
	    gOFS.ObjectManager.HashMutex.UnLockRead();
	    // this is a request to (re-)boot a filesystem
	    if (fileSystems.count(queue.c_str())) {
	      Boot(fileSystems[queue.c_str()]);
	    } else {
	      eos_static_err("got boot time update on not existant filesystem %s", queue.c_str());
	    }
	  }
          if (key == "scaninterval") {
            gOFS.ObjectManager.HashMutex.UnLockRead();
            if (fileSystems.count(queue.c_str())) {
              time_t interval = (time_t) fileSystems[queue.c_str()]->GetLongLong("scaninterval");
              if (interval>0) {
                fileSystems[queue.c_str()]->RunScanner(&fstLoad, interval);
              }
            }
          }
	} else {
	  gOFS.ObjectManager.HashMutex.UnLockRead();
	}
      } else {
	eos_static_err("illegal subject found - no filesystem object existing for modification %s;%s", queue.c_str(),key.c_str());
	gOFS.ObjectManager.HashMutex.UnLockRead();
      }
    }

    if (!unlocked) {
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;
    }
  }
}

/*----------------------------------------------------------------------------*/
void
Storage::Publish()
{
  eos_static_info("Publisher activated ...");  
  struct timeval tv1, tv2; 
  struct timezone tz;

  // give some time before publishing
  sleep(3);

  while (1) {
    gettimeofday(&tv1, &tz);

    // TODO: derive this from a global variable
    // smear the publishing cycle around 5+-5 seconds
    unsigned int lReportIntervalMilliSeconds = 5000 + (unsigned int)(10000.0*rand()/RAND_MAX);

    {
      // run through our defined filesystems and publish with a MuxTransaction all changes
      eos::common::RWMutexReadLock lock (fsMutex);
      
      if (!gOFS.ObjectManager.OpenMuxTransaction()) {
        eos_static_err("cannot open mux transaction");
      } else {
        // copy out statfs info
        for (size_t i=0; i<fileSystemsVector.size(); i++) {
          if (!fileSystemsVector[i]) {
            eos_static_err("found 0 vector in filesystems vector %u", i);
            continue;
          }

          if (!fileSystemsVector[i]->GetId()) {
            // during the boot phase we can find a filesystem without ID
            continue;
          }


          eos::common::Statfs* statfs = 0;
          if ( (statfs= fileSystemsVector[i]->GetStatfs()) ) {
            // call the update function which stores into the filesystem shared hash
            if (!fileSystemsVector[i]->SetStatfs(statfs->GetStatfs())) {
              eos_static_err("cannot SetStatfs on filesystem %s", fileSystemsVector[i]->GetPath().c_str());
            }
          }
          
          bool success = true;
          // copy out net info 
          // TODO: take care of eth0 only ..
          // somethimg has to tell us if we are 1GBit, or 10GBit ... we assume 1GBit now as the default
          success &= fileSystemsVector[i]->SetDouble("stat.net.ethratemib", 1000000000/(8*1024*1024));
          success &= fileSystemsVector[i]->SetDouble("stat.net.inratemib",  fstLoad.GetNetRate("eth0","rxbytes")/1024.0/1024.0);
          success &= fileSystemsVector[i]->SetDouble("stat.net.outratemib", fstLoad.GetNetRate("eth0","txbytes")/1024.0/1024.0);
          //          eos_static_debug("Path is %s %f\n", fileSystemsVector[i]->GetPath().c_str(), fstLoad.GetDiskRate(fileSystemsVector[i]->GetPath().c_str(),"writeSectors")*512.0/1000000.0);
          success &= fileSystemsVector[i]->SetDouble("stat.disk.readratemb", fstLoad.GetDiskRate(fileSystemsVector[i]->GetPath().c_str(),"readSectors")*512.0/1000000.0);
          success &= fileSystemsVector[i]->SetDouble("stat.disk.writeratemb", fstLoad.GetDiskRate(fileSystemsVector[i]->GetPath().c_str(),"writeSectors")*512.0/1000000.0);
          success &= fileSystemsVector[i]->SetDouble("stat.disk.load", fstLoad.GetDiskRate(fileSystemsVector[i]->GetPath().c_str(),"millisIO")/1000.0);
          gOFS.OpenFidMutex.Lock();
          success &= fileSystemsVector[i]->SetLongLong("stat.ropen", (long long)gOFS.ROpenFid[fileSystemsVector[i]->GetId()].size());
          success &= fileSystemsVector[i]->SetLongLong("stat.wopen", (long long)gOFS.WOpenFid[fileSystemsVector[i]->GetId()].size());
          success &= fileSystemsVector[i]->SetLongLong("stat.statfs.freebytes",  fileSystemsVector[i]->GetLongLong("stat.statfs.bfree")*fileSystemsVector[i]->GetLongLong("stat.statfs.bsize"));
          success &= fileSystemsVector[i]->SetLongLong("stat.statfs.usedbytes", (fileSystemsVector[i]->GetLongLong("stat.statfs.blocks")-fileSystemsVector[i]->GetLongLong("stat.statfs.bfree"))*fileSystemsVector[i]->GetLongLong("stat.statfs.bsize"));
          success &= fileSystemsVector[i]->SetLongLong("stat.statfs.capacity",   fileSystemsVector[i]->GetLongLong("stat.statfs.blocks")*fileSystemsVector[i]->GetLongLong("stat.statfs.bsize"));
          success &= fileSystemsVector[i]->SetLongLong("stat.statfs.fused",     (fileSystemsVector[i]->GetLongLong("stat.statfs.files")-fileSystemsVector[i]->GetLongLong("stat.statfs.ffree"))*fileSystemsVector[i]->GetLongLong("stat.statfs.bsize"));
          success &= fileSystemsVector[i]->SetLongLong("stat.usedfiles", (long long) (eos::common::gFmdHandler.FmdMap.count(fileSystemsVector[i]->GetId())?eos::common::gFmdHandler.FmdMap[fileSystemsVector[i]->GetId()].size():0));
                                                       
          success &= fileSystemsVector[i]->SetString("stat.boot", fileSystemsVector[i]->GetString("stat.boot").c_str());
          gOFS.OpenFidMutex.UnLock();
          if (!success) {
            eos_static_err("cannot set net parameters on filesystem %s", fileSystemsVector[i]->GetPath().c_str());
          }
        }
      }
      
      gOFS.ObjectManager.CloseMuxTransaction();
    }
    gettimeofday(&tv2, &tz);
    int lCycleDuration = (int)( (tv2.tv_sec*1000.0)-(tv1.tv_sec*1000.0) + (tv2.tv_usec/1000.0) - (tv1.tv_usec/1000.0) );
    int lSleepTime = lReportIntervalMilliSeconds - lCycleDuration;
    if (lSleepTime < 0) {
      eos_static_warning("Publisher cycle exceeded %d millisecons - took %d milliseconds", lReportIntervalMilliSeconds,lCycleDuration);
    } else {
      usleep(1000*lSleepTime);
    }
  }
}

/*----------------------------------------------------------------------------*/
bool 
Storage::OpenTransaction(unsigned int fsid, unsigned long long fid) 
{
  FileSystem* fs = fileSystemsMap[fsid];
  if (fs) {
    return fs->OpenTransaction(fid);
  }
  return false;
}


/*----------------------------------------------------------------------------*/
bool 
Storage::CloseTransaction(unsigned int fsid, unsigned long long fid) 
{
  FileSystem* fs = fileSystemsMap[fsid];
  if (fs) {
    return fs->CloseTransaction(fid);
  }
  return false;
}

EOSFSTNAMESPACE_END
