// ----------------------------------------------------------------------
// File: Storage.cc
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
#include "XrdSys/XrdSysTimer.hh"
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
FileSystem::CleanTransactions()
{
  DIR* tdir = opendir(GetTransactionDirectory());
  if (tdir) {
    struct dirent* name;
    while ( ( name = readdir(tdir) ) ) {
      XrdOucString sname = name->d_name;
      // skipp . & ..
      if ( sname.beginswith("."))
	continue;
      XrdOucString fulltransactionpath = GetTransactionDirectory();
      fulltransactionpath += "/"; fulltransactionpath += name->d_name;
      struct stat buf;
      if (!stat(fulltransactionpath.c_str(), &buf)) {
	XrdOucString hexfid = name->d_name;
	const char* localprefix = GetPath().c_str();
	XrdOucString fstPath;
	eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(), localprefix, fstPath);
	unsigned long long fileid = eos::common::FileId::Hex2Fid(hexfid.c_str());
	
	// we allow to keep files open for 1 week
	if (buf.st_mtime < (time(NULL) - (7*86400))) {
	  eos_static_info("action=delete transaction=%llx fstpath=%s",sname.c_str(), fulltransactionpath.c_str());
      
	  // -------------------------------------------------------------------------------------------------------
	  // clean-up this file locally
	  // -------------------------------------------------------------------------------------------------------
	  
	  XrdOucErrInfo error;
	  int retc =  gOFS._rem("/CLEANTRANSACTIONS", error, 0, 0, fstPath.c_str(), fileid, GetId(), true);
	  if (retc) {
	    eos_static_debug("deletion failed for %s", fstPath.c_str());
	  }
	} else {
	  eos_static_info("action=keep transaction=%llx fstpath=%s",sname.c_str(), fulltransactionpath.c_str());
	}
      }
    }
  } else {
    eos_static_err("Unable to open transactiondirectory %s",GetTransactionDirectory());
  }
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
  int rc = system(mkmetalogdir.c_str());
  if (rc) rc=0;
  // own the directory
  mkmetalogdir = "chown -R daemon.daemon "; mkmetalogdir += metadirectory; mkmetalogdir += " >& /dev/null"; 

  rc = system(mkmetalogdir.c_str());
  if (rc) rc=0;
  metaDirectory = metadirectory;
  
  // check if the meta directory is accessible
  if (access(metadirectory,R_OK|W_OK|X_OK)) {
    eos_crit("cannot access meta data directory %s", metadirectory);
    zombie = true;
  }

  zombie = false;
  // start threads
  pthread_t tid;

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

  ThreadSet.insert(tid);

  eos_info("starting trim thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsTrim, static_cast<void *>(this),
                              0, "Meta Store Trim"))) {
    eos_crit("cannot start trimming theread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting deletion thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsRemover, static_cast<void *>(this),
                              0, "Data Store Remover"))) {
    eos_crit("cannot start deletion theread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting report thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsReport, static_cast<void *>(this),
                              0, "Report Thread"))) {
    eos_crit("cannot start report thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting error report thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsErrorReport, static_cast<void *>(this),
                              0, "Error Report Thread"))) {
    eos_crit("cannot start error report thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting verification thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsVerify, static_cast<void *>(this),
                              0, "Verify Thread"))) {
    eos_crit("cannot start verify thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem communication thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsCommunicator, static_cast<void *>(this),
                              0, "Communicator Thread"))) {
    eos_crit("cannot start communicator thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem publishing thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsPublisher, static_cast<void *>(this),
                              0, "Publisher Thread"))) {
    eos_crit("cannot start publisher thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem balancer thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsBalancer, static_cast<void *>(this),
                              0, "Balancer Thread"))) {
    eos_crit("cannot start balancer thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem drainer thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsDrainer, static_cast<void *>(this),
                              0, "Drainer Thread"))) {
    eos_crit("cannot start drainer thread");
    zombie = true;
  }

  ThreadSet.insert(tid);


  eos_info("starting filesystem transaction cleaner thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsCleaner, static_cast<void *>(this),
                              0, "Cleaner Thread"))) {
    eos_crit("cannot start cleaner thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

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

  // test if we are on the root partition
  struct stat root_buf;
  if ( ::stat( "/", &root_buf)) {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno?errno:EIO, "cannot stat root / filesystems");
    return;
  }

  if (root_buf.st_dev == buf.st_dev) {
    // this filesystem is on the ROOT partition
    if (!CheckLabel(fs->GetPath(),fsid,uuid, false, true)) {
      fs->SetStatus(eos::common::FileSystem::kBootFailure);
      fs->SetError(EIO,"filesystem is on the root partition without or wrong <uuid> label file .eosfsuuid");
      return;
    }
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
  fs->CleanTransactions();
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
Storage::CheckLabel(std::string path, eos::common::FileSystem::fsid_t fsid, std::string uuid, bool failenoid, bool failenouuid) 
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
    if (failenoid) 
      return false;
  }

  // read FS uuid file
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
    if (failenouuid) 
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
void*
Storage::StartFsBalancer(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Balancer();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsDrainer(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Drainer();
  return 0;
}    

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsCleaner(void * pp)
{
  Storage* storage = (Storage*)pp;
  storage->Cleaner();
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

  eos_static_debug("Running Scrubber on filesystem path=%s id=%u free=%llu blocks=%llu index=%d", path, id, free,blocks,index);

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
      eos_static_debug("Scrubbing file %s", scrubfile[k].c_str());
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
        eos_static_debug("rshift is %d", rshift);
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
          eos_static_warning("unable to remove fid %s fsid %lu localprefix=%s",hexstring.c_str(), todelete.fsId, todelete.localPrefix.c_str());
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
	if (queue.beginswith("/config/") && queue.endswith(Config::gConfig.FstHostPort)) {
	  // this is the configuration entry and we should store it to have access to it since it's name depends on the instance name and we don't know (yet)
	  Config::gConfig.FstNodeConfigQueue = queue;
	  eos_static_info("storing config queue name <%s>", Config::gConfig.FstNodeConfigQueue.c_str());
	} else {
	  eos_static_info("no action on creation of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
	}
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
      if ((! queue.beginswith(Config::gConfig.FstQueue)) && (! queue.beginswith(Config::gConfig.FstNodeConfigQueue))) {
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

      if (queue == Config::gConfig.FstNodeConfigQueue) {
	if (key == "symkey") {
	  // we received a new symkey 
	  XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(),"hash");
	  if (hash) {
	    std::string symkey = hash->Get("symkey");
	    eos_static_info("symkey=%s", symkey.c_str());
	    eos::common::gSymKeyStore.SetKey64(symkey.c_str(),0);
	  }
	}
      } else {
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
	    } else {
	      if (key == "bootsenttime") {
		gOFS.ObjectManager.HashMutex.UnLockRead();
		// this is a request to (re-)boot a filesystem
		if (fileSystems.count(queue.c_str())) {
		  Boot(fileSystems[queue.c_str()]);
		} else {
		  eos_static_err("got boot time update on not existant filesystem %s", queue.c_str());
		}
	      } else {
		if (key == "scaninterval") {
		  gOFS.ObjectManager.HashMutex.UnLockRead();
		  if (fileSystems.count(queue.c_str())) {
		    time_t interval = (time_t) fileSystems[queue.c_str()]->GetLongLong("scaninterval");
		    if (interval>0) {
		      fileSystems[queue.c_str()]->RunScanner(&fstLoad, interval);
		    }
		  }
		} else {
		  gOFS.ObjectManager.HashMutex.UnLockRead();
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
  unsigned long long netspeed=1000000000;
  // ---------------------------------------------------------------------
  // get our network speed
  // ---------------------------------------------------------------------
  char* tmpname = tmpnam(NULL);
  XrdOucString getnetspeed = "ip route list | sed -ne '/^default/s/.*dev //p' | xargs ethtool | grep Speed | cut -d ':' -f2 | cut -d 'M' -f1 >> "; getnetspeed += tmpname;

  system(getnetspeed.c_str());
  
  FILE* fnetspeed = fopen(tmpname,"r");
  if (fnetspeed) {
    if ( (fscanf(fnetspeed,"%llu", &netspeed)) == 1) {
      // we get MB as a number => convert into bytes
      netspeed *= 1000000;
      eos_static_info("ethtool:networkspeed=%.02f GB/s", 1.0*netspeed/1000000000.0);
    } 
    fclose(fnetspeed);
  }
  
  eos_static_info("publishing:networkspeed=%.02f GB/s", 1.0*netspeed/1000000000.0);

  // ---------------------------------------------------------------------
  // give some time before publishing
  // ---------------------------------------------------------------------
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
          success &= fileSystemsVector[i]->SetDouble("stat.net.ethratemib", netspeed/(8*1024*1024));
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
	  success &= fileSystemsVector[i]->SetDouble("stat.statfs.filled", 100.0 * ((fileSystemsVector[i]->GetLongLong("stat.statfs.blocks")-fileSystemsVector[i]->GetLongLong("stat.statfs.bfree"))) / (1+fileSystemsVector[i]->GetLongLong("stat.statfs.blocks")));
          success &= fileSystemsVector[i]->SetLongLong("stat.statfs.capacity",   fileSystemsVector[i]->GetLongLong("stat.statfs.blocks")*fileSystemsVector[i]->GetLongLong("stat.statfs.bsize"));
          success &= fileSystemsVector[i]->SetLongLong("stat.statfs.fused",     (fileSystemsVector[i]->GetLongLong("stat.statfs.files")-fileSystemsVector[i]->GetLongLong("stat.statfs.ffree"))*fileSystemsVector[i]->GetLongLong("stat.statfs.bsize"));
          success &= fileSystemsVector[i]->SetLongLong("stat.usedfiles", (long long) (eos::common::gFmdHandler.FmdMap.count(fileSystemsVector[i]->GetId())?eos::common::gFmdHandler.FmdMap[fileSystemsVector[i]->GetId()].size():0));
                                                       
          success &= fileSystemsVector[i]->SetString("stat.boot", fileSystemsVector[i]->GetString("stat.boot").c_str());
	  success &= fileSystemsVector[i]->SetLongLong("stat.drainer.running",fileSystemsVector[i]->GetDrainQueue()->GetRunningAndQueued());
	  success &= fileSystemsVector[i]->SetLongLong("stat.balancer.running",fileSystemsVector[i]->GetBalanceQueue()->GetRunningAndQueued());
          gOFS.OpenFidMutex.UnLock();

	  {
	    XrdSysMutexHelper(fileSystemFullMapMutex);
	    long long fbytes = fileSystemsVector[i]->GetLongLong("stat.statfs.freebytes");
	    if ( (fbytes < 1024ll*1024ll*1024ll) || (fbytes <= fileSystemsVector[i]->GetLongLong("headroom")) ) {
	      fileSystemFullMap[fileSystemsVector[i]->GetId()] = true;
	    } else {
	      fileSystemFullMap[fileSystemsVector[i]->GetId()] = false;
	    }
	  }

          if (!success) {
            eos_static_err("cannot set net parameters on filesystem %s", fileSystemsVector[i]->GetPath().c_str());
          }
        }
	gOFS.ObjectManager.CloseMuxTransaction();
      }
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
void
Storage::Drainer()
{
  eos_static_info("Start Drainer ...");

  std::string nodeconfigqueue="";
  
  const char* val =0;
  // we have to wait that we know our node config queue
  while ( !(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str())) {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }
  
  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();
  unsigned int cycler=0;

  std::vector<bool> should_ask; // vector describing if a file system is within the treshould to be a balancing source
  std::vector<bool> has_work;   // vector describing if a file system got a transfer scheduled in the last balancing round

  unsigned long long nscheduled=0;
  unsigned long long nscheduled_new=0;

  int expsleep=50000;

  while(1) {
    eos_static_debug("Doing draining round ...");
    bool ask = false;

    // ---------------------------------------
    // get some global variables
    // ---------------------------------------
    gOFS.ObjectManager.HashMutex.LockRead();
    
    XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(nodeconfigqueue.c_str());
    std::string manager = confighash?confighash->Get("manager"):"unknown";
    unsigned long long nparalleltx = confighash?confighash->GetLongLong("stat.drain.ntx"):0;
    unsigned long long ratetx    = confighash?confighash->GetLongLong("stat.drain.rate"):0;
    
    if (nparalleltx == 0) nparalleltx = 0;
    if (ratetx      == 0) ratetx     = 25;
    
    eos_static_debug("manager=%s nparalleltransfers=%llu transferrate=%llu", manager.c_str(), nparalleltx, ratetx);
    gOFS.ObjectManager.HashMutex.UnLockRead();
    // ---------------------------------------

    unsigned int nfs=0;
    {
      eos::common::RWMutexReadLock lock (fsMutex);
      nfs = fileSystemsVector.size();
      should_ask.resize(nfs);
      has_work.resize(nfs);

      // if we didn't schedule yet, we just look into the queues what is there
      if (!nscheduled) {
	for (unsigned int s=0; s < nfs; s++) {
	  if (s < fileSystemsVector.size()) {
	    nscheduled += fileSystemsVector[s]->GetDrainQueue()->GetRunningAndQueued();
	  }
	}
      }
    }

    nscheduled_new = 0;

    for (unsigned int i=0; i< nfs; i++) {
      unsigned int index = (i+cycler) % nfs;
      eos::common::RWMutexReadLock lock(fsMutex);
      if (index < fileSystemsVector.size()) {
	should_ask[index] = false;
	has_work[index]   = false;

        std::string path = fileSystemsVector[index]->GetPath();
	unsigned long id = fileSystemsVector[index]->GetId();
	eos_static_debug("FileSystem %lu ",id);
     
	// check if this filesystem has to 'schedule2drain' 

	if (fileSystemsVector[index]->GetString("stat.drainer") != "on") {
	  // nothing to do here
	  continue;
	}

	ask = true;

	unsigned long long freebytes   = fileSystemsVector[index]->GetLongLong("stat.statfs.freebytes");

	if (fileSystemsVector[index]->GetDrainQueue()->GetBandwidth() != ratetx) {
	  // modify the bandwidth setting for this queue
	  fileSystemsVector[index]->GetDrainQueue()->SetBandwidth(ratetx);
	}
	
	if (fileSystemsVector[index]->GetDrainQueue()->GetSlots() != nparalleltx) {
	  // modify slot settings for this queue
	  fileSystemsVector[index]->GetDrainQueue()->SetSlots(nparalleltx);
	}
	
	eos::common::FileSystem::fsstatus_t bootstatus   = fileSystemsVector[index]->GetStatus();
	eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[index]->GetConfigStatus();
	
	eos_static_info("id=%u nscheduled=%llu nparalleltx=%llu", id, nscheduled, nparalleltx);
	
	// we drain into filesystems which are booted and 'in production' e.g. not draining or down
	if ( (bootstatus == eos::common::FileSystem::kBooted) && 
	     (configstatus > eos::common::FileSystem::kDrain) ) {
	  should_ask[index] = true;
	  // we allows max. <nparalleltx> transfers to run at the same time
	  if (nscheduled < nparalleltx) {
	    eos_static_debug("asking for new job %d/%d", nscheduled, nparalleltx);
	    XrdOucErrInfo error;
	    XrdOucString managerQuery="/?";
	    managerQuery += "mgm.pcmd=schedule2drain";
	    managerQuery += "&mgm.target.fsid=";
	    char sid[1024];  snprintf(sid,sizeof(sid)-1,"%lu", id);
	    managerQuery += sid;
	    managerQuery += "&mgm.target.freebytes=";
	    char sfree[1024]; snprintf(sfree,sizeof(sfree)-1,"%llu", freebytes);
	    managerQuery += sfree;
	    // the log ID to the schedule2balance
	    managerQuery += "&mgm.logid="; managerQuery += logId;
	    
	    XrdOucString response="";
	    int rc = gOFS.CallManager(&error, "/",manager.c_str(), managerQuery, &response);
	    if (rc) {
	      eos_static_err("manager returned errno=%d", rc);
	    } else {
	      if (response == "submitted") {
		eos_static_info("got a new job");
		nscheduled++;
		nscheduled_new++;
		has_work[index] = true;
		eos_static_debug("manager scheduled a transfer for us!");
	      } else {
		eos_static_debug("manager returned no file to schedule [ENODATA]");
	      }
	    }
	  } else {
	    eos_static_info("asking for new job stopped");
	    // if all slots are busy anyway, we leave the loop
	    break;
	  }
	}
      }
    }

    if ((!ask)) {
      // ---------------------------------------------------------------------------------------------
      // we have no filesystem which is member of a draining group at the moment
      // ---------------------------------------------------------------------------------------------
      nscheduled = 0;
      XrdSysTimer sleeper;
      // go to sleep for a while if there was nothing to do
      eos_static_debug("doing a long sleep of 30s");
      sleeper.Snooze(30);
    } else {
      if (nscheduled_new) {
	// ---------------------------------------------------------------------------------------------
	// if we scheduled in the last round, we go one more until we cannot schedule anymore
	// ---------------------------------------------------------------------------------------------
	eos_static_debug("asking for new job sleep 100000");
	usleep(100000);
	expsleep = 100000;
      } else {
	// ---------------------------------------------------------------------------------------------
	// we are actually running transfers, we check more frequently, if we have to ask for more 
	// and interrupt the sleepy period as soon as we have a slot free
	// ---------------------------------------------------------------------------------------------
	for (unsigned int sleeper = 0; sleeper < 30; sleeper++) {
	  eos::common::RWMutexReadLock lock(fsMutex);     
	  nfs = fileSystemsVector.size();
	  should_ask.resize(nfs);
	  unsigned long total_running=0;
	  for (unsigned int i=0; i< nfs; i++) {
	    if (should_ask[i]) {
	      total_running += fileSystemsVector[i]->GetDrainQueue()->GetRunningAndQueued();
	    }
	  }
	  if ( (total_running >= nparalleltx) || (nscheduled == nparalleltx) ) {
	    XrdSysTimer sleeper;
	    sleeper.Snooze(1);
	    nscheduled = 0; // this trick allows that we give atleast a second time for a transfer job to appear in the FST queue
	  } else {
	    if (total_running == 0) {
	      // we don't run anything
	      XrdSysTimer sleeper;
	      sleeper.Snooze(1);
	      expsleep = 50000;
	    } else {
	      // we run something, we could ask for more immedeatly but we ramp up the sleep
	      usleep(expsleep);
	      expsleep *=2;
	      if (expsleep > 10000000) {
		expsleep = 10000000;
	      }
	    }
	    break;
	  }
	}
	nscheduled=0;
      }
    }
    cycler++;
  }
}


/*----------------------------------------------------------------------------*/
void
Storage::Balancer()
{
  eos_static_info("Start Balancer ...");

  std::string nodeconfigqueue="";
  const char* val =0;
  // we have to wait that we know our node config queue
  while ( !(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str())) {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }
  
  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();

  unsigned int cycler=0;

  std::vector<bool> should_ask; // vector describing if a file system is within the treshould to be a balancing source
  std::vector<bool> has_work;   // vector describing if a file system got a transfer scheduled in the last balancing round

  unsigned long long nscheduled=0;
  unsigned long long nscheduled_new=0;

  while(1) {
    eos_static_debug("Doing balancing round ...");

    bool   ask=false;
    // ---------------------------------------
    // get some global variables
    // ---------------------------------------
    gOFS.ObjectManager.HashMutex.LockRead();
    
    XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(nodeconfigqueue.c_str());
    std::string manager = confighash?confighash->Get("manager"):"unknown";
    unsigned long long nparalleltx = confighash?confighash->GetLongLong("stat.balance.ntx"):0;
    unsigned long long ratetx    = confighash?confighash->GetLongLong("stat.balance.rate"):0;
    
    if (nparalleltx == 0) nparalleltx = 0;
    if (ratetx      == 0) ratetx     = 25;
    
    eos_static_debug("manager=%s nparalleltransfers=%llu transferrate=%llu", manager.c_str(), nparalleltx, ratetx);
    gOFS.ObjectManager.HashMutex.UnLockRead();
    // ---------------------------------------

    unsigned int nfs=0;
    {
      // determin the number of configured filesystems
      eos::common::RWMutexReadLock lock (fsMutex);
      nfs = fileSystemsVector.size();
      should_ask.resize(nfs);
      has_work.resize(nfs);

      // if we didn't schedule yet, we just look into the queues what is there
      if (!nscheduled) {
	for (unsigned int s=0; s < nfs; s++) {
	  if (s < fileSystemsVector.size()) {
	    nscheduled += fileSystemsVector[s]->GetBalanceQueue()->GetRunningAndQueued();
	  }
	}
      }
    }    

    nscheduled_new = 0;

    for (unsigned int i=0; i< nfs; i++) {
      unsigned int index = (i+cycler) % nfs;
      eos::common::RWMutexReadLock lock(fsMutex);     
      if (index < fileSystemsVector.size()) {
	should_ask[index] = false;
	has_work[index]   = false;

        std::string path = fileSystemsVector[index]->GetPath();
	double nominal = fileSystemsVector[index]->GetDouble("stat.nominal.filled");
	double filled  = fileSystemsVector[index]->GetDouble("stat.statfs.filled");

	unsigned long id = fileSystemsVector[index]->GetId();

	eos_static_debug("FileSystem %lu %.02f %.02f",id, filled, nominal);
      

	if (filled < nominal) {
	  should_ask[index] = true;
	  ask = true;
	  // if the fill status is less than nominal we can ask a balancer transfer to the MGM
	  unsigned long long freebytes   = fileSystemsVector[index]->GetLongLong("stat.statfs.freebytes");
	  
	  if (fileSystemsVector[index]->GetBalanceQueue()->GetBandwidth() != ratetx) {
	    // modify the bandwidth setting for this queue
	    fileSystemsVector[index]->GetBalanceQueue()->SetBandwidth(ratetx);
	  }

	  if (fileSystemsVector[index]->GetBalanceQueue()->GetSlots() != nparalleltx) {
	    // modify slot settings for this queue
	    fileSystemsVector[index]->GetBalanceQueue()->SetSlots(nparalleltx);
	  }

	  eos::common::FileSystem::fsstatus_t bootstatus   = fileSystemsVector[index]->GetStatus();
	  eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[index]->GetConfigStatus();

	  eos_static_debug("id=%u nscheduled=%llu nparalleltx=%llu", id, nscheduled, nparalleltx);

	  // ---------------------------------------------------------------------------------------------
	  // we balance filesystems which are booted and 'in production' e.g. not draining or down
	  // ---------------------------------------------------------------------------------------------
	  if ( (bootstatus == eos::common::FileSystem::kBooted) && 
	       (configstatus > eos::common::FileSystem::kDrain) ) {

	    if (nscheduled < nparalleltx) {	    
	      XrdOucErrInfo error;
	      XrdOucString managerQuery="/?";
	      managerQuery += "mgm.pcmd=schedule2balance";
	      managerQuery += "&mgm.target.fsid=";
	      char sid[1024];  snprintf(sid,sizeof(sid)-1,"%lu", id);
	      managerQuery += sid;
	      managerQuery += "&mgm.target.freebytes=";
	      char sfree[1024]; snprintf(sfree,sizeof(sfree)-1,"%llu", freebytes);
	      managerQuery += sfree;
	      // the log ID to the schedule2balance
	      managerQuery += "&mgm.logid="; managerQuery += logId;
	      
	      XrdOucString response="";
	      int rc = gOFS.CallManager(&error, "/",manager.c_str(), managerQuery, &response);
	      if (rc) {
		eos_static_err("manager returned errno=%d", rc);
	      } else {
		if (response == "submitted") {
		  eos_static_debug("id=%u result=%s", id, response.c_str());
		  nscheduled++;
		  nscheduled_new++;
		  has_work[index]=true;
		} else {
		  eos_static_debug("manager returned no file to schedule [ENODATA]");
		}
	      } 
	    } else {
	      eos_static_info("asking for new job stopped");
	      // if all slots are busy anyway, we leave the loop
	      break;
	    }
	  }
	}
      }      
    }

    if ((!ask)) {
      // ---------------------------------------------------------------------------------------------
      // we have no filesystem which is member of a balancing group at the moment
      // ---------------------------------------------------------------------------------------------
      XrdSysTimer sleeper;
      // go to sleep for a while if there was nothing to do
      sleeper.Snooze(30);
    } else {
      if (nscheduled_new) {
	// ---------------------------------------------------------------------------------------------
	// if we scheduled in the last round, we go one more until we cannot schedule anymore
	// ---------------------------------------------------------------------------------------------
	eos_static_debug("asking for new job sleep 100000");
	usleep(100000);
      } else {
	// ---------------------------------------------------------------------------------------------
	// we are actually running transfers, we check more frequently, if we have to ask for more 
	// and interrupt the sleepy period as soon as we have a slot free
	// ---------------------------------------------------------------------------------------------
	for (unsigned int sleeper = 0; sleeper < 30; sleeper++) {
	  eos::common::RWMutexReadLock lock(fsMutex);     
	  nfs = fileSystemsVector.size();
	  should_ask.resize(nfs);
	  unsigned long total_running=0;
	  for (unsigned int i=0; i< nfs; i++) {
	    if (should_ask[i]) {
	      total_running += fileSystemsVector[i]->GetBalanceQueue()->GetRunningAndQueued();
	    }
	  }
	  if ( (total_running >= nparalleltx) || (nscheduled == nparalleltx) ) {
            XrdSysTimer sleeper;
            sleeper.Snooze(1);
            nscheduled = 0; // this trick allows that we give atleast a second time for a transfer job to appear in the FST queue
          } else {
            if (total_running == 0) {
              XrdSysTimer sleeper;
              sleeper.Snooze(1);
            }
            break;
          }
	}
	nscheduled=0;
      }
    }    
    cycler++;
  }
}

/*----------------------------------------------------------------------------*/
void
Storage::Cleaner()
{
  eos_static_info("Start Cleaner ...");

  std::string nodeconfigqueue="";
  
  const char* val =0;
  // we have to wait that we know our node config queue
  while ( !(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str())) {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }
  
  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();

  while(1) {
    eos_static_debug("Doing cleaning round ...");

    unsigned int nfs=0;
    {
      eos::common::RWMutexReadLock lock (fsMutex);
      nfs = fileSystemsVector.size();
    }    
    for (unsigned int i=0; i< nfs; i++) {
      eos::common::RWMutexReadLock lock(fsMutex);     
      if (i< fileSystemsVector.size()) {
	if (fileSystemsVector[i]->GetStatus() == eos::common::FileSystem::kBooted) 
	  fileSystemsVector[i]->CleanTransactions();
      }
    }

    // go to sleep for a day since we allow a transaction to stay for 1 week
    XrdSysTimer sleeper;
    sleeper.Snooze(24*3600);
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
