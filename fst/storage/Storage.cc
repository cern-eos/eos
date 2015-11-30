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
#include "fst/FmdDbMap.hh"
#include "common/Fmd.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/LinuxStat.hh"
#include "mq/XrdMqMessaging.hh"
/*----------------------------------------------------------------------------*/
#include <google/dense_hash_map>
#include <math.h>
/*----------------------------------------------------------------------------*/
#include "fst/XrdFstOss.hh"
#include "XrdSys/XrdSysTimer.hh"
/*----------------------------------------------------------------------------*/

extern eos::fst::XrdFstOss *XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
Storage::Storage (const char* metadirectory)
{
  SetLogId("FstOfsStorage");

  // make metadir
  XrdOucString mkmetalogdir = "mkdir -p ";
  mkmetalogdir += metadirectory;
  mkmetalogdir += " >& /dev/null";
  int rc = system(mkmetalogdir.c_str());
  if (rc) rc = 0;
  // own the directory
  mkmetalogdir = "chown -R daemon.daemon ";
  mkmetalogdir += metadirectory;
  mkmetalogdir += " >& /dev/null";

  rc = system(mkmetalogdir.c_str());
  if (rc) rc = 0;
  metaDirectory = metadirectory;

  // check if the meta directory is accessible
  if (access(metadirectory, R_OK | W_OK | X_OK))
  {
    eos_crit("cannot access meta data directory %s", metadirectory);
    zombie = true;
  }

  zombie = false;
  // start threads
  pthread_t tid;

  // we need page aligned addresses for direct IO
  long pageval = sysconf(_SC_PAGESIZE);
  if (pageval < 0)
  {
    eos_crit("cannot get page size");
    exit(-1);
  }

  if (posix_memalign((void**) &scrubPattern[0], pageval, 1024 * 1024) ||
      posix_memalign((void**) &scrubPattern[1], pageval, 1024 * 1024) ||
      posix_memalign((void**) &scrubPatternVerify, pageval, 1024 * 1024))
  {
    eos_crit("cannot allocate memory aligned scrub buffer");
    exit(-1);
  }

  eos_info("starting scrubbing thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsScrub,
                              static_cast<void *> (this),
                              0, "Scrubber")))
  {
    eos_crit("cannot start scrubber thread");
    zombie = true;
  }

  XrdSysMutexHelper tsLock(ThreadSetMutex);
  ThreadSet.insert(tid);

  eos_info("starting trim thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsTrim,
                              static_cast<void *> (this),
                              0, "Meta Store Trim")))
  {
    eos_crit("cannot start trimming theread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting deletion thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsRemover,
                              static_cast<void *> (this),
                              0, "Data Store Remover")))
  {
    eos_crit("cannot start deletion theread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting report thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsReport,
                              static_cast<void *> (this),
                              0, "Report Thread")))
  {
    eos_crit("cannot start report thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting error report thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsErrorReport,
                              static_cast<void *> (this),
                              0, "Error Report Thread")))
  {
    eos_crit("cannot start error report thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting verification thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsVerify,
                              static_cast<void *> (this),
                              0, "Verify Thread")))
  {
    eos_crit("cannot start verify thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem communication thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsCommunicator,
                              static_cast<void *> (this),
                              0, "Communicator Thread")))
  {
    eos_crit("cannot start communicator thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting daemon supervisor thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartDaemonSupervisor,
                              static_cast<void *> (this),
                              0, "Supervisor Thread")))
  {
    eos_crit("cannot start supervisor thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem publishing thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsPublisher,
                              static_cast<void *> (this),
                              0, "Publisher Thread")))
  {
    eos_crit("cannot start publisher thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem balancer thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsBalancer,
                              static_cast<void *> (this),
                              0, "Balancer Thread")))
  {
    eos_crit("cannot start balancer thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("starting filesystem drainer thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsDrainer,
                              static_cast<void *> (this),
                              0, "Drainer Thread")))
  {
    eos_crit("cannot start drainer thread");
    zombie = true;
  }

  ThreadSet.insert(tid);


  eos_info("starting filesystem transaction cleaner thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsCleaner,
                              static_cast<void *> (this),
                              0, "Cleaner Thread")))
  {
    eos_crit("cannot start cleaner thread");
    zombie = true;
  }

  ThreadSet.insert(tid);



  eos_info("starting mgm synchronization thread");
  if ((rc = XrdSysThread::Run(&tid, Storage::StartMgmSyncer,
                              static_cast<void *> (this),
                              0, "MgmSyncer Thread")))
  {
    eos_crit("cannot start mgm syncer thread");
    zombie = true;
  }

  ThreadSet.insert(tid);

  eos_info("enabling net/io load monitor");
  fstLoad.Monitor();

  // create gw queue
  XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
  std::string n = eos::fst::Config::gConfig.FstQueue.c_str();
  n += "/gw";
  mGwQueue = new eos::common::TransferQueue(eos::fst::Config::gConfig.FstQueue.c_str(),
                                            n.c_str(), "txq",
                                            (eos::common::FileSystem*)0,
                                            &gOFS.ObjectManager, true);
  n += "/txqueue";
  mTxGwQueue = new TransferQueue(&mGwQueue, n.c_str());
  if (mTxGwQueue)
  {
    mGwMultiplexer.Add(mTxGwQueue);
  }
  else
  {
    eos_err("unable to create transfer queue");
  }
}

/*----------------------------------------------------------------------------*/
Storage*
Storage::Create (const char* metadirectory)
{
  Storage* storage = new Storage(metadirectory);
  if (storage->IsZombie())
  {
    delete storage;
    return 0;
  }
  return storage;
}

/*----------------------------------------------------------------------------*/
void
Storage::Boot (FileSystem *fs)
{
  fs->SetStatus(eos::common::FileSystem::kBooting);

  if (!fs)
  {
    return;
  }

  // we have to wait that we know who is our manager
  std::string manager = "";
  size_t cnt = 0;
  do
  {
    cnt++;
    {
      XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
      manager = eos::fst::Config::gConfig.Manager.c_str();
    }
    if (manager != "")
      break;

    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_info("msg=\"waiting to know manager\"");
    if (cnt > 20)
    {
      eos_static_alert("didn't receive manager name, aborting");
      XrdSysTimer sleeper;
      sleeper.Snooze(10);
      XrdFstOfs::xrdfstofs_shutdown(1);
    }
  }
  while (1);

  eos_info("msg=\"manager known\" manager=\"%s\"", manager.c_str());

  eos::common::FileSystem::fsid_t fsid = fs->GetId();
  std::string uuid = fs->GetString("uuid");

  eos_info("booting filesystem %s id=%u uuid=%s", fs->GetQueuePath().c_str(),
           (unsigned int) fsid, uuid.c_str());

  if (!fsid)
    return;

  // try to statfs the filesystem
  eos::common::Statfs* statfs = eos::common::Statfs::DoStatfs(fs->GetPath().c_str());
  if (!statfs)
  {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno ? errno : EIO, "cannot statfs filesystem");
    return;
  }

  // test if we have rw access
  struct stat buf;
  if (::stat(fs->GetPath().c_str(), &buf) ||
      (buf.st_uid != 2) ||
      ((buf.st_mode & S_IRWXU) != S_IRWXU))
  {

    if ((buf.st_mode & S_IRWXU) != S_IRWXU)
    {
      errno = EPERM;
    }

    if (buf.st_uid != 2)
    {
      errno = ENOTCONN;
    }

    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno ? errno : EIO, "cannot have <rw> access");
    return;
  }

  // test if we are on the root partition
  struct stat root_buf;
  if (::stat("/", &root_buf))
  {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno ? errno : EIO, "cannot stat root / filesystems");
    return;
  }

  if (root_buf.st_dev == buf.st_dev)
  {
    // this filesystem is on the ROOT partition
    if (!CheckLabel(fs->GetPath(), fsid, uuid, false, true))
    {
      fs->SetStatus(eos::common::FileSystem::kBootFailure);
      fs->SetError(EIO, "filesystem is on the root partition without or wrong <uuid> label file .eosfsuuid");
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

  XrdOucString dbfilename;
  gFmdDbMapHandler.CreateDBFileName(metaDirectory.c_str(), dbfilename);

  // attach to the SQLITE DB
  if (!gFmdDbMapHandler.SetDBFile(dbfilename.c_str(), fsid))
  {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(EFAULT, "cannot set DB filename - see the fst logfile for details");
    return;
  }

  bool resyncmgm = (gFmdDbMapHandler.IsDirty(fsid) ||
                      (fs->GetLongLong("bootcheck") == eos::common::FileSystem::kBootResync));
  bool resyncdisk = (gFmdDbMapHandler.IsDirty(fsid) ||
                     (fs->GetLongLong("bootcheck") >= eos::common::FileSystem::kBootForced));

  eos_info("msg=\"start disk synchronisation\"");
  // resync the DB 
  gFmdDbMapHandler.StayDirty(fsid, true); // indicate the flag to keep the DP dirty

  if (resyncdisk)
  {
    if (resyncmgm)
    {
      // clean-up the DB
      if (!gFmdDbMapHandler.ResetDB(fsid))
      {
        fs->SetStatus(eos::common::FileSystem::kBootFailure);
        fs->SetError(EFAULT, "cannot clean SQLITE DB on local disk");
        return;
      }
    }
    if (!gFmdDbMapHandler.ResyncAllDisk(fs->GetPath().c_str(), fsid, resyncmgm))
    {
      fs->SetStatus(eos::common::FileSystem::kBootFailure);
      fs->SetError(EFAULT, "cannot resync the SQLITE DB from local disk");
      return;
    }
    eos_info("msg=\"finished disk synchronisation\" fsid=%lu",
             (unsigned long) fsid);
  }
  else
  {
    eos_info("msg=\"skipped disk synchronisization\" fsid=%lu",
             (unsigned long) fsid);
  }

  // if we detect an unclean shutdown, we resync with the MGM
  // if we see the stat.bootcheck resyncflag for the filesystem, we also resync

  // remove the bootcheck flag
  fs->SetLongLong("bootcheck", 0);

  if (resyncmgm)
  {
    eos_info("msg=\"start mgm synchronisation\" fsid=%lu", (unsigned long) fsid);
    // resync the MGM meta data
    if (!gFmdDbMapHandler.ResyncAllMgm(fsid, manager.c_str()))
    {
      fs->SetStatus(eos::common::FileSystem::kBootFailure);
      fs->SetError(EFAULT, "cannot resync the mgm meta data");
      return;
    }
    eos_info("msg=\"finished mgm synchronization\" fsid=%lu", (unsigned long) fsid);
  }
  else
  {
    eos_info("msg=\"skip mgm resynchronization - had clean shutdown\" fsid=%lu",
             (unsigned long) fsid);
  }

  gFmdDbMapHandler.StayDirty(fsid, false); // indicate the flag to unset the DB dirty flag at shutdown

  // check if there is a lable on the disk and if the configuration shows the same fsid + uuid
  if (!CheckLabel(fs->GetPath(), fsid, uuid))
  {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(EFAULT, "the filesystem has a different label (fsid+uuid) than the configuration");
    return;
  }

  if (!FsLabel(fs->GetPath(), fsid, uuid))
  {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(EFAULT, "cannot write the filesystem label (fsid+uuid) - please check filesystem state/permissions");
    return;
  }

  // create FS transaction directory
  std::string transactionDirectory = fs->GetPath();
  transactionDirectory += "/.eostransaction";

  if (mkdir(transactionDirectory.c_str(),
            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH))
  {
    if (errno != EEXIST)
    {
      fs->SetStatus(eos::common::FileSystem::kBootFailure);
      fs->SetError(errno ? errno : EIO, "cannot create transactiondirectory");
      return;
    }
  }

  if (chown(transactionDirectory.c_str(), 2, 2))
  {
    fs->SetStatus(eos::common::FileSystem::kBootFailure);
    fs->SetError(errno ? errno : EIO, "cannot change ownership of transactiondirectory");
    return;
  }

  fs->SetTransactionDirectory(transactionDirectory.c_str());
  if (fs->SyncTransactions(manager.c_str()))
    fs->CleanTransactions();
  fs->SetLongLong("stat.bootdonetime", (unsigned long long) time(NULL));
  fs->IoPing();
  fs->SetStatus(eos::common::FileSystem::kBooted);
  fs->SetError(0, "");
  eos_info("msg=\"finished boot procedure\" fsid=%lu", (unsigned long) fsid);

  return;
}

bool
Storage::FsLabel (std::string path,
                  eos::common::FileSystem::fsid_t fsid, std::string uuid)
{
  //----------------------------------------------------------------
  //! writes file system label files .eosfsid .eosuuid according to config (if they didn't exist!)
  //----------------------------------------------------------------

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";

  struct stat buf;

  if (stat(fsidfile.c_str(), &buf))
  {
    int fd = open(fsidfile.c_str(),
                  O_TRUNC | O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0)
    {
      return false;
    }
    else
    {
      char ssfid[32];
      snprintf(ssfid, 32, "%u", fsid);
      if ((write(fd, ssfid, strlen(ssfid))) != (int) strlen(ssfid))
      {
        close(fd);
        return false;
      }
    }
    close(fd);
  }

  std::string uuidfile = path;
  uuidfile += "/.eosfsuuid";

  if (stat(uuidfile.c_str(), &buf))
  {
    int fd = open(uuidfile.c_str(),
                  O_TRUNC | O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0)
    {
      return false;
    }
    else
    {
      if ((write(fd, uuid.c_str(), strlen(uuid.c_str()) + 1))
          != (int) (strlen(uuid.c_str()) + 1))
      {
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
Storage::CheckLabel (std::string path,
                     eos::common::FileSystem::fsid_t fsid,
                     std::string uuid, bool failenoid, bool failenouuid)
{
  //----------------------------------------------------------------
  //! checks that the label on the filesystem is the same as the configuration
  //----------------------------------------------------------------

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";

  struct stat buf;

  std::string ckuuid = uuid;
  eos::common::FileSystem::fsid_t ckfsid = fsid;

  if (!stat(fsidfile.c_str(), &buf))
  {
    int fd = open(fsidfile.c_str(), O_RDONLY);
    if (fd < 0)
    {
      return false;
    }
    else
    {
      char ssfid[32];
      memset(ssfid, 0, sizeof (ssfid));
      int nread = read(fd, ssfid, sizeof (ssfid) - 1);
      if (nread < 0)
      {
        close(fd);
        return false;
      }
      close(fd);
      // for safety
      if (nread < (int) (sizeof (ssfid) - 1))
        ssfid[nread] = 0;
      else
        ssfid[31] = 0;
      if (ssfid[strnlen(ssfid, sizeof (ssfid)) - 1] == '\n')
      {
        ssfid[strnlen(ssfid, sizeof (ssfid)) - 1] = 0;
      }
      ckfsid = atoi(ssfid);
    }
  }
  else
  {
    if (failenoid)
      return false;
  }

  // read FS uuid file
  std::string uuidfile = path;
  uuidfile += "/.eosfsuuid";

  if (!stat(uuidfile.c_str(), &buf))
  {
    int fd = open(uuidfile.c_str(), O_RDONLY);
    if (fd < 0)
    {
      return false;
    }
    else
    {
      char suuid[4096];
      memset(suuid, 0, sizeof (suuid));
      int nread = read(fd, suuid, sizeof (suuid));
      if (nread < 0)
      {
        close(fd);
        return false;
      }
      close(fd);
      // for protection
      suuid[4095] = 0;
      // remove \n 
      if (suuid[strnlen(suuid, sizeof (suuid)) - 1] == '\n')
        suuid[strnlen(suuid, sizeof (suuid)) - 1] = 0;

      ckuuid = suuid;
    }
  }
  else
  {
    if (failenouuid)
      return false;
  }

  //  fprintf(stderr,"%d <=> %d %s <=> %s\n", fsid, ckfsid, ckuuid.c_str(), uuid.c_str());
  if ((fsid != ckfsid) || (ckuuid != uuid))
  {
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetFsidFromLabel (std::string path,
                           eos::common::FileSystem::fsid_t &fsid)
{
  //----------------------------------------------------------------
  //! return the file system id from the filesystem fsid label file
  //----------------------------------------------------------------

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";

  struct stat buf;
  fsid = 0;

  if (!stat(fsidfile.c_str(), &buf))
  {
    int fd = open(fsidfile.c_str(), O_RDONLY);
    if (fd < 0)
    {
      return false;
    }
    else
    {
      char ssfid[32];
      memset(ssfid, 0, sizeof (ssfid));
      int nread = read(fd, ssfid, sizeof (ssfid) - 1);
      if (nread < 0)
      {
        close(fd);
        return false;
      }
      close(fd);
      // for safety
      if (nread < (int) (sizeof (ssfid) - 1))
        ssfid[nread] = 0;
      else
        ssfid[31] = 0;
      if (ssfid[strnlen(ssfid, sizeof (ssfid)) - 1] == '\n')
      {
        ssfid[strnlen(ssfid, sizeof (ssfid)) - 1] = 0;
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
Storage::GetFsidFromPath (std::string path, eos::common::FileSystem::fsid_t &fsid)
{
  //----------------------------------------------------------------
  //! return the file system id from the configured filesystem vector
  //----------------------------------------------------------------

  eos::common::RWMutexReadLock lock(fsMutex);
  fsid = 0;

  for (unsigned int i = 0; i < fileSystemsVector.size(); i++)
  {
    if (fileSystemsVector[i]->GetPath() == path)
    {
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
Storage::StartFsScrub (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Scrub();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsTrim (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Trim();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsRemover (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Remover();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsReport (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Report();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsErrorReport (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->ErrorReport();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsVerify (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Verify();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsCommunicator (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Communicator();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartDaemonSupervisor (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Supervisor();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsPublisher (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Publish();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsBalancer (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Balancer();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsDrainer (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Drainer();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartFsCleaner (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->Cleaner();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartMgmSyncer (void * pp)
{
  Storage* storage = (Storage*) pp;
  storage->MgmSyncer();
  return 0;
}

/*----------------------------------------------------------------------------*/
void*
Storage::StartBoot (void * pp)
{
  if (pp)
  {
    BootThreadInfo* info = (BootThreadInfo*) pp;
    info->storage->Boot(info->filesystem);
    // remove from the set containing the ids of booting filesystems
    XrdSysMutexHelper bootLock(info->storage->BootSetMutex);
    info->storage->BootSet.erase(info->filesystem->GetId());
    XrdSysMutexHelper tsLock(info->storage->ThreadSetMutex);
    info->storage->ThreadSet.erase(XrdSysThread::ID());
    delete info;
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
bool
Storage::RunBootThread (FileSystem* fs)
{
  bool retc = false;
  if (fs)
  {
    XrdSysMutexHelper bootLock(BootSetMutex);
    // check if this filesystem is currently already booting
    if (BootSet.count(fs->GetId()))
    {
      eos_warning("discard boot request: filesytem fsid=%lu is currently booting", (unsigned long) fs->GetId());
      return false;
    }
    else
    {
      // insert into the set of booting filesystems
      BootSet.insert(fs->GetId());
    }

    BootThreadInfo* info = new BootThreadInfo;
    if (info)
    {
      info->storage = this;
      info->filesystem = fs;
      pthread_t tid;
      if ((XrdSysThread::Run(&tid, Storage::StartBoot, static_cast<void *> (info),
                             0, "Booter")))
      {
        eos_crit("cannot start boot thread");
        retc = false;
        BootSet.erase(fs->GetId());
      }
      else
      {
        retc = true;
        XrdSysMutexHelper tsLock(ThreadSetMutex);
        ThreadSet.insert(tid);
        eos_notice("msg=\"started boot thread\" fsid=%ld", (unsigned long) info->filesystem->GetId());
      }
    }
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
bool
Storage::OpenTransaction (unsigned int fsid, unsigned long long fid)
{
  FileSystem* fs = fileSystemsMap[fsid];
  if (fs)
  {
    return fs->OpenTransaction(fid);
  }
  return false;
}

/*----------------------------------------------------------------------------*/
bool
Storage::CloseTransaction (unsigned int fsid, unsigned long long fid)
{
  FileSystem* fs = fileSystemsMap[fsid];
  if (fs)
  {
    return fs->CloseTransaction(fid);
  }
  return false;
}

EOSFSTNAMESPACE_END
