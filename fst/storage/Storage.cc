//------------------------------------------------------------------------------
// File: Storage.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "fst/Config.hh"
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/FmdDbMap.hh"
#include "fst/Verify.hh"
#include "fst/Deletion.hh"
#include "fst/txqueue/TransferQueue.hh"
#include "common/FileSystem.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "MonitorVarPartition.hh"
#include <google/dense_hash_map>
#include <math.h>
#include "fst/XrdFstOss.hh"

extern eos::fst::XrdFstOss* XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Create new Storage object
//------------------------------------------------------------------------------
Storage*
Storage::Create(const char* meta_dir)
{
  Storage* storage = new Storage(meta_dir);

  if (storage->IsZombie()) {
    delete storage;
    return 0;
  }

  return storage;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Storage::Storage(const char* meta_dir)
{
  SetLogId("FstOfsStorage", "<service>");
  XrdOucString mkmetalogdir = "mkdir -p ";
  mkmetalogdir += meta_dir;
  mkmetalogdir += " >& /dev/null";
  int rc = system(mkmetalogdir.c_str());

  if (rc) {
    rc = 0;
  }

  mkmetalogdir = "chown -R daemon.daemon ";
  mkmetalogdir += meta_dir;
  mkmetalogdir += " >& /dev/null";
  rc = system(mkmetalogdir.c_str());

  if (rc) {
    rc = 0;
  }

  mMetaDir = meta_dir;

  // Check if the meta directory is accessible
  if (access(meta_dir, R_OK | W_OK | X_OK)) {
    eos_crit("cannot access meta data directory %s", meta_dir);
    mZombie = true;
  }

  mZombie = false;
  pthread_t tid;
  // We need page aligned addresses for direct IO
  long pageval = sysconf(_SC_PAGESIZE);

  if (pageval < 0) {
    eos_crit("cannot get page size");
    exit(-1);
  }

  if (posix_memalign((void**) &mScrubPattern[0], pageval, 1024 * 1024) ||
      posix_memalign((void**) &mScrubPattern[1], pageval, 1024 * 1024) ||
      posix_memalign((void**) &mScrubPatternVerify, pageval, 1024 * 1024)) {
    eos_crit("cannot allocate memory aligned scrub buffer");
    exit(-1);
  }

  eos_info("starting scrubbing thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsScrub,
                              static_cast<void*>(this),
                              0, "Scrubber"))) {
    eos_crit("cannot start scrubber thread");
    mZombie = true;
  }

  XrdSysMutexHelper tsLock(mThreadsMutex);
  mThreadSet.insert(tid);
  eos_info("starting trim thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsTrim,
                              static_cast<void*>(this),
                              0, "Meta Store Trim"))) {
    eos_crit("cannot start trimming theread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting deletion thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsRemover,
                              static_cast<void*>(this),
                              0, "Data Store Remover"))) {
    eos_crit("cannot start deletion theread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting report thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsReport,
                              static_cast<void*>(this),
                              0, "Report Thread"))) {
    eos_crit("cannot start report thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting error report thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsErrorReport,
                              static_cast<void*>(this),
                              0, "Error Report Thread"))) {
    eos_crit("cannot start error report thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting verification thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsVerify,
                              static_cast<void*>(this),
                              0, "Verify Thread"))) {
    eos_crit("cannot start verify thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting filesystem communication thread");
  mCommunicatorThread.reset(&Storage::Communicator, this);
  mCommunicatorThread.setName("Communicator Thread");

  if (gOFS.mMqOnQdb) {
    mQdbCommunicatorThread.reset(&Storage::QdbCommunicator,
                                 this, gOFS.mQdbContactDetails);
    mQdbCommunicatorThread.setName("QDB Communicator Thread");
  }

  eos_info("starting daemon supervisor thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartDaemonSupervisor,
                              static_cast<void*>(this),
                              0, "Supervisor Thread"))) {
    eos_crit("cannot start supervisor thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting filesystem publishing thread");
  mPublisherThread.reset(&Storage::Publish, this);
  mPublisherThread.setName("Publisher Thread");

  if (gOFS.mMqOnQdb) {
    eos_info("starting filesystem QDB publishing thread");
    mQdbNodePublisherThread.reset(&Storage::QdbPublish, this,
                                  gOFS.mQdbContactDetails);
    mQdbNodePublisherThread.setName("QDB Publisher Thread");
  }

  eos_info("starting filesystem balancer thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsBalancer,
                              static_cast<void*>(this),
                              0, "Balancer Thread"))) {
    eos_crit("cannot start balancer thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting filesystem transaction cleaner thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsCleaner,
                              static_cast<void*>(this),
                              0, "Cleaner Thread"))) {
    eos_crit("cannot start cleaner thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("starting mgm synchronization thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartMgmSyncer,
                              static_cast<void*>(this),
                              0, "MgmSyncer Thread"))) {
    eos_crit("cannot start mgm syncer thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  // Starting FstPartitionMonitor
  eos_info("starting /var/ partition monitor thread ...");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartVarPartitionMonitor,
                              static_cast<void*>(this),
                              0, "Var Partition Monitor"))) {
    eos_crit("Cannot start Var Partition Monitor thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("enabling net/io load monitor");
  mFstLoad.Monitor();
  eos_info("enabling local disk S.M.A.R.T attribute monitor");
  mFstHealth.Monitor();
  // Create gw queue
  XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
  mGwQueue = new eos::common::TransferQueue(
    eos::common::TransferQueueLocator(eos::fst::Config::gConfig.FstQueue.c_str(),
                                      "txq"),
    gOFS.mMessagingRealm.get(), true);
  mTxGwQueue = new TransferQueue(&mGwQueue);

  if (mTxGwQueue) {
    mGwMultiplexer.Add(mTxGwQueue);
  } else {
    eos_err("unable to create transfer queue");
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Storage::~Storage()
{
  delete mTxGwQueue;
  delete mGwQueue;
}

//------------------------------------------------------------------------------
// Shutdown all helper threads
//------------------------------------------------------------------------------
void
Storage::ShutdownThreads()
{
  XrdSysMutexHelper scope_lock(mThreadsMutex);

  for (auto it = mThreadSet.begin(); it != mThreadSet.end(); it++) {
    eos_warning("op=shutdown threadid=%llx", (unsigned long long) *it);
    XrdSysThread::Cancel(*it);
  }
}

//------------------------------------------------------------------------------
// Push new verification job to the queue if the maximum number of pending
// verifications is not exceeded.
//------------------------------------------------------------------------------
void
Storage::PushVerification(eos::fst::Verify* entry)
{
  XrdSysMutexHelper scope_lock(mVerifyMutex);

  if (mVerifications.size() < 1000000) {
    mVerifications.push(entry);
    entry->Show();
  } else {
    eos_err("verify list has already 1 Mio. entries - discarding verify message");
  }
}

//------------------------------------------------------------------------------
// Boot file system
//------------------------------------------------------------------------------
void
Storage::Boot(FileSystem* fs)
{
  if (!fs) {
    return;
  }

  fs->SetStatus(eos::common::BootStatus::kBooting);
  // Wait to know who is our manager
  std::string manager = "";
  size_t cnt = 0;

  do {
    cnt++;
    {
      XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
      manager = eos::fst::Config::gConfig.Manager.c_str();
    }

    if (manager != "") {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    eos_info("msg=\"waiting to know manager\"");

    if (cnt > 20) {
      eos_static_alert("%s", "msg=\"didn't receive manager name, aborting\"");
      std::this_thread::sleep_for(std::chrono::seconds(10));
      XrdFstOfs::xrdfstofs_shutdown(1);
    }
  } while (true);

  eos_info("msg=\"manager known\" manager=\"%s\"", manager.c_str());
  eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
  std::string uuid = fs->GetLocalUuid();
  eos_info("booting filesystem %s id=%u uuid=%s", fs->GetQueuePath().c_str(),
           (unsigned int) fsid, uuid.c_str());

  if (!fsid) {
    return;
  }

  // Try to statfs the filesystem
  std::unique_ptr<eos::common::Statfs> statfs = fs->GetStatfs();

  if (!statfs) {
    fs->SetStatus(eos::common::BootStatus::kBootFailure);
    fs->SetError(errno ? errno : EIO, "cannot statfs filesystem");
    return;
  }

  // Exclude remote disks
  if (fs->GetPath()[0] == '/') {
    // Test if we have rw access
    struct stat buf;

    if (::stat(fs->GetPath().c_str(), &buf) ||
        (buf.st_uid != DAEMONUID) ||
        ((buf.st_mode & S_IRWXU) != S_IRWXU)) {
      if (buf.st_uid != DAEMONUID) {
        errno = ENOTCONN;
      }

      if ((buf.st_mode & S_IRWXU) != S_IRWXU) {
        errno = EPERM;
      }

      fs->SetStatus(eos::common::BootStatus::kBootFailure);
      fs->SetError(errno ? errno : EIO, "cannot have <rw> access");
      return;
    }

    // Test if we are on the root partition
    struct stat root_buf;

    if (::stat("/", &root_buf)) {
      fs->SetStatus(eos::common::BootStatus::kBootFailure);
      fs->SetError(errno ? errno : EIO, "cannot stat root / filesystems");
      return;
    }

    if (root_buf.st_dev == buf.st_dev) {
      // This filesystem is on the ROOT partition
      if (!CheckLabel(fs->GetPath(), fsid, uuid, false, true)) {
        fs->SetStatus(eos::common::BootStatus::kBootFailure);
        fs->SetError(EIO, "filesystem is on the root partition without or "
                     "wrong <uuid> label file .eosfsuuid");
        return;
      }
    }
  }

  {
    XrdSysMutexHelper scope_lock(gOFS.OpenFidMutex);
    gOFS.WNoDeleteOnCloseFid[fsid].clear_deleted_key();
    gOFS.WNoDeleteOnCloseFid[fsid].set_deleted_key(0);
  }


  std::string fmd_on_disk = getenv("EOS_FST_FMD_ON_DATA_DISK")?getenv("EOS_FST_FMD_ON_DATA_DISK"):"";

  std::string metadir = (fmd_on_disk=="1")? fs->GetPath() : mMetaDir.c_str();

  if ( fmd_on_disk == "1") {
    // e.g. we store on /data01/.eosmd/<leveldb>
    if (metadir.back() != '/') {
      metadir+="/";
    }
    metadir += ".eosmd/";
    eos::common::Path cPath( std::string(metadir + "dummy").c_str());
    cPath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP);
  }

  // Attach to the local DB
  if (!gFmdDbMapHandler.SetDBFile(metadir.c_str(), fsid)) {
    fs->SetStatus(eos::common::BootStatus::kBootFailure);
    fs->SetError(EFAULT, "cannot set DB filename - see the fst logfile "
                 "for details");
    return;
  }

  bool resyncmgm = (fs->GetLongLong("bootcheck") ==
                    eos::common::FileSystem::kBootResync);
  bool resyncdisk = (fs->GetLongLong("bootcheck") >=
                     eos::common::FileSystem::kBootForced);
  // If we see the bootcheck resyncflag for the filesystem, we resync with
  // the mgm. Remove the bootcheck flag.
  fs->SetLongLong("bootcheck", 0);
  eos_info("msg=\"start disk synchronisation\" fsid=%u", fsid);

  // Sync only local disks
  if (resyncdisk && (fs->GetPath()[0] == '/')) {
    if (resyncmgm) {
      if (!gFmdDbMapHandler.ResetDB(fsid)) {
        fs->SetStatus(eos::common::BootStatus::kBootFailure);
        fs->SetError(EFAULT, "cannot clean DB on local disk");
        return;
      }
    }

    if (!gFmdDbMapHandler.ResyncAllDisk(fs->GetPath().c_str(), fsid, resyncmgm)) {
      fs->SetStatus(eos::common::BootStatus::kBootFailure);
      fs->SetError(EFAULT, "cannot resync the DB from local disk");
      return;
    }

    eos_info("msg=\"finished disk synchronisation\" fsid=%u", fsid);
  } else {
    eos_info("msg=\"skipped disk synchronisization\" fsid=%u", fsid);
  }

  if (resyncmgm) {
    eos_info("msg=\"start mgm synchronisation\" fsid=%u", fsid);

    if (!gOFS.mQdbContactDetails.empty()) {
      // Resync meta data connecting directly to QuarkDB
      eos_info("msg=\"synchronizing from QuarkDB backend\"");

      if (!gFmdDbMapHandler.ResyncAllFromQdb(gOFS.mQdbContactDetails, fsid)) {
        fs->SetStatus(eos::common::BootStatus::kBootFailure);
        fs->SetError(EFAULT, "cannot resync meta data from QuarkDB");
        return;
      }
    } else {
      // Resync the MGM meta data using dumpmd
      if (!gFmdDbMapHandler.ResyncAllMgm(fsid, manager.c_str())) {
        fs->SetStatus(eos::common::BootStatus::kBootFailure);
        fs->SetError(EFAULT, "cannot resync the mgm meta data");
        return;
      }
    }

    eos_info("msg=\"finished mgm synchronization\" fsid=%u", fsid);
  } else {
    eos_info("msg=\"skip mgm resynchronization\" fsid=%u", fsid);
  }

  // @note the disk and mgm synchronization can end up in a state where files
  // present on disk but not tracked by the MGM are still accounted in EOS. They
  // are tracked in the local database and also show up in the "used_files" info
  // displayed per file system.

  // Check if there is a label on the disk and if the configuration shows the
  // same fsid + uuid
  if (!CheckLabel(fs->GetPath(), fsid, uuid)) {
    fs->SetStatus(eos::common::BootStatus::kBootFailure);
    fs->SetError(EFAULT, SSTR("filesystem has a different label (fsid="
                              << fsid << ", uuid=" << uuid << ") than "
                              << "the configuration").c_str());
    return;
  }

  if (!FsLabel(fs->GetPath(), fsid, uuid)) {
    fs->SetStatus(eos::common::BootStatus::kBootFailure);
    fs->SetError(EFAULT, "cannot write the filesystem label (fsid+uuid) - "
                 "please check filesystem state/permissions");
    return;
  }

  // Create FS transaction directory
  std::string transactionDirectory = fs->GetPath();

  if (fs->GetPath()[0] != '/') {
    transactionDirectory = mMetaDir.c_str();
    transactionDirectory += "/.eostransaction";
    transactionDirectory += "-";
    transactionDirectory += (int) fs->GetLocalId();
  } else {
    transactionDirectory += "/.eostransaction";
  }

  if (mkdir(transactionDirectory.c_str(),
            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
    if (errno != EEXIST) {
      fs->SetStatus(eos::common::BootStatus::kBootFailure);
      fs->SetError(errno ? errno : EIO, "cannot create transaction directory");
      return;
    }
  }

  if (chown(transactionDirectory.c_str(), 2, 2)) {
    fs->SetStatus(eos::common::BootStatus::kBootFailure);
    fs->SetError(errno ? errno : EIO,
                 "cannot change ownership of transaction directory");
    return;
  }

  fs->SetTransactionDirectory(transactionDirectory.c_str());

  if (fs->SyncTransactions(manager.c_str())) {
    fs->CleanTransactions();
  }

  fs->SetLongLong("stat.bootdonetime", (unsigned long long) time(NULL));
  fs->IoPing();
  fs->SetStatus(eos::common::BootStatus::kBooted);
  fs->SetError(0, "");
  // Create FS orphan directory
  std::string orphanDirectory = fs->GetPath();

  if (fs->GetPath()[0] != '/') {
    orphanDirectory = mMetaDir.c_str();
    orphanDirectory += "/.eosorphans";
    orphanDirectory += "-";
    orphanDirectory += (int) fs->GetLocalId();
  } else {
    orphanDirectory += "/.eosorphans";
  }

  if (mkdir(orphanDirectory.c_str(),
            S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
    if (errno != EEXIST) {
      fs->SetStatus(eos::common::BootStatus::kBootFailure);
      fs->SetError(errno ? errno : EIO, "cannot create orphan directory");
      return;
    }
  }

  if (chown(orphanDirectory.c_str(), 2, 2)) {
    fs->SetStatus(eos::common::BootStatus::kBootFailure);
    fs->SetError(errno ? errno : EIO, "cannot change ownership of orphan "
                 "directory");
    return;
  }

  eos_info("msg=\"finished boot procedure\" fsid=%lu", (unsigned long) fsid);
  return;
}

//------------------------------------------------------------------------------
// Start scurbber thread
//------------------------------------------------------------------------------
void*
Storage::StartFsScrub(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Scrub();
  return 0;
}

//------------------------------------------------------------------------------
// Start fmd DB trimmer thread
//------------------------------------------------------------------------------
void*
Storage::StartFsTrim(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Trim();
  return 0;
}

//------------------------------------------------------------------------------
// Start remover thread
//------------------------------------------------------------------------------
void*
Storage::StartFsRemover(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Remover();
  return 0;
}

//------------------------------------------------------------------------------
// Start reporter thread
//------------------------------------------------------------------------------
void*
Storage::StartFsReport(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Report();
  return 0;
}

//------------------------------------------------------------------------------
// Start error reporter thread
//------------------------------------------------------------------------------
void*
Storage::StartFsErrorReport(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->ErrorReport();
  return 0;
}

//------------------------------------------------------------------------------
// Start verification thread
//------------------------------------------------------------------------------
void*
Storage::StartFsVerify(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Verify();
  return 0;
}

//------------------------------------------------------------------------------
// Start supervisor thread doing automatic restart if needed
//------------------------------------------------------------------------------
void*
Storage::StartDaemonSupervisor(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Supervisor();
  return 0;
}

//------------------------------------------------------------------------------
// Start balancer thread
//------------------------------------------------------------------------------
void*
Storage::StartFsBalancer(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Balancer();
  return 0;
}

//------------------------------------------------------------------------------
// Start cleaner thread
//------------------------------------------------------------------------------
void*
Storage::StartFsCleaner(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->Cleaner();
  return 0;
}

//------------------------------------------------------------------------------
// Start mgm syncer thread
//------------------------------------------------------------------------------
void*
Storage::StartMgmSyncer(void* pp)
{
  Storage* storage = (Storage*) pp;
  storage->MgmSyncer();
  return 0;
}

//------------------------------------------------------------------------------
// Start /var/ monitoring thread
//------------------------------------------------------------------------------
void* Storage::StartVarPartitionMonitor(void* pp)
{
  Storage* storage = (Storage*) pp;
  MonitorVarPartition<std::vector<FileSystem*>> mon(5., 30, "/var/");
  mon.Monitor(storage->mFsVect, storage->mFsMutex);
  return 0;
}

//------------------------------------------------------------------------------
// Start boot thread
//------------------------------------------------------------------------------
void*
Storage::StartBoot(void* pp)
{
  if (pp) {
    BootThreadInfo* info = (BootThreadInfo*) pp;
    info->storage->Boot(info->filesystem);
    // Remove from the set containing the ids of booting filesystems
    XrdSysMutexHelper bootLock(info->storage->mBootingMutex);
    info->storage->mBootingSet.erase(info->filesystem->GetLocalId());
    XrdSysMutexHelper tsLock(info->storage->mThreadsMutex);
    info->storage->mThreadSet.erase(XrdSysThread::ID());
    delete info;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Run boot thread for specified filesystem
//------------------------------------------------------------------------------
bool
Storage::RunBootThread(FileSystem* fs)
{
  bool retc = false;

  if (fs) {
    XrdSysMutexHelper bootLock(mBootingMutex);

    // Check if this filesystem is currently already booting
    if (mBootingSet.count(fs->GetLocalId())) {
      eos_warning("discard boot request: filesytem fsid=%lu is currently booting",
                  (unsigned long) fs->GetLocalId());
      return retc;
    } else {
      // Insert into the set of booting filesystems
      mBootingSet.insert(fs->GetLocalId());
    }

    BootThreadInfo* info = new BootThreadInfo();

    if (info) {
      info->storage = this;
      info->filesystem = fs;
      pthread_t tid;

      if ((XrdSysThread::Run(&tid, Storage::StartBoot, static_cast<void*>(info),
                             0, "Booter"))) {
        eos_crit("cannot start boot thread");
        mBootingSet.erase(fs->GetLocalId());
      } else {
        retc = true;
        XrdSysMutexHelper tsLock(mThreadsMutex);
        mThreadSet.insert(tid);
        eos_notice("msg=\"started boot thread\" fsid=%lu",
                   info->filesystem->GetLocalId());
      }
    }
  }

  return retc;
}

//------------------------------------------------------------------------------
// Open transaction operation
//------------------------------------------------------------------------------
bool
Storage::OpenTransaction(unsigned int fsid, unsigned long long fid)
{
  auto it = mFsMap.find(fsid);

  if (it != mFsMap.end()) {
    return it->second->OpenTransaction(fid);
  }

  return false;
}

//------------------------------------------------------------------------------
// Close transaction operation
//------------------------------------------------------------------------------
bool
Storage::CloseTransaction(unsigned int fsid, unsigned long long fid)
{
  auto it = mFsMap.find(fsid);

  if (it != mFsMap.end()) {
    return it->second->CloseTransaction(fid);
  }

  return false;
}

//------------------------------------------------------------------------------
// Add deletion to the list of pending ones
//------------------------------------------------------------------------------
void
Storage::AddDeletion(std::unique_ptr<Deletion> del)
{
  XrdSysMutexHelper scope_lock(mDeletionsMutex);
  mListDeletions.push_front(std::move(del));
}

//------------------------------------------------------------------------------
// Get deletion object removing it from the list
//------------------------------------------------------------------------------
std::unique_ptr<Deletion>
Storage::GetDeletion()
{
  std::unique_ptr<Deletion> del;
  XrdSysMutexHelper scope_lock(mDeletionsMutex);

  if (mListDeletions.size()) {
    del.swap(mListDeletions.back());
    mListDeletions.pop_back();
  }

  return del;
}

//------------------------------------------------------------------------------
// Get number of pending deletions
//------------------------------------------------------------------------------
size_t
Storage::GetNumDeletions()
{
  size_t total = 0;
  XrdSysMutexHelper scope_lock(mDeletionsMutex);

  for (auto it = mListDeletions.cbegin(); it != mListDeletions.cend(); ++it) {
    total += (*it)->mFidVect.size();
  }

  return total;
}

//------------------------------------------------------------------------------
// Get the filesystem associated with the given filesystem id
//------------------------------------------------------------------------------
FileSystem*
Storage::GetFileSystemById(eos::common::FileSystem::fsid_t fsid)
{
  auto it = mFsMap.find(fsid);

  if (it != mFsMap.end()) {
    return it->second;
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Writes file system label files .eosfsid .eosuuid according to config (if
// they didn't exist!)
//------------------------------------------------------------------------------
bool
Storage::FsLabel(std::string path, eos::common::FileSystem::fsid_t fsid,
                 std::string uuid)
{
  // exclude remote disks
  if (path[0] != '/') {
    return true;
  }

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";
  struct stat buf;

  if (stat(fsidfile.c_str(), &buf)) {
    int fd = open(fsidfile.c_str(),
                  O_TRUNC | O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);

    if (fd < 0) {
      return false;
    } else {
      char ssfid[32];
      snprintf(ssfid, 32, "%u", fsid);

      if ((write(fd, ssfid, strlen(ssfid))) != (int) strlen(ssfid)) {
        close(fd);
        return false;
      }
    }

    close(fd);
  }

  std::string uuidfile = path;
  uuidfile += "/.eosfsuuid";

  if (stat(uuidfile.c_str(), &buf)) {
    int fd = open(uuidfile.c_str(),
                  O_TRUNC | O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);

    if (fd < 0) {
      return false;
    } else {
      if ((write(fd, uuid.c_str(), strlen(uuid.c_str()) + 1))
          != (int)(strlen(uuid.c_str()) + 1)) {
        close(fd);
        return false;
      }
    }

    close(fd);
  }

  return true;
}

//------------------------------------------------------------------------------
// Checks that the label on the filesystem matches the one in the config
//------------------------------------------------------------------------------
bool
Storage::CheckLabel(std::string path,
                    eos::common::FileSystem::fsid_t fsid,
                    std::string uuid, bool fail_noid, bool fail_nouuid)
{
  // exclude remote disks
  if (path[0] != '/') {
    return true;
  }

  XrdOucString fsidfile = path.c_str();
  fsidfile += "/.eosfsid";
  struct stat buf;
  std::string ckuuid = uuid;
  eos::common::FileSystem::fsid_t ckfsid = fsid;

  if (!stat(fsidfile.c_str(), &buf)) {
    int fd = open(fsidfile.c_str(), O_RDONLY);

    if (fd == -1) {
      return false;
    } else {
      ssize_t len = 32;
      char ssfid[len];
      memset(ssfid, 0, sizeof(ssfid));
      ssize_t nread = read(fd, ssfid, sizeof(ssfid) - 1);

      if (nread == -1) {
        close(fd);
        return false;
      }

      close(fd);
      ssfid[std::min(nread, len - 1)] = '\0';

      if (ssfid[strlen(ssfid) - 1] == '\n') {
        ssfid[strlen(ssfid) - 1] = '\0';
      }

      ckfsid = atoi(ssfid);
    }
  } else {
    if (fail_noid) {
      return false;
    }
  }

  // read FS uuid file
  std::string uuidfile = path;
  uuidfile += "/.eosfsuuid";

  if (!stat(uuidfile.c_str(), &buf)) {
    int fd = open(uuidfile.c_str(), O_RDONLY);

    if (fd < 0) {
      return false;
    } else {
      ssize_t sz = 4096;
      char suuid[sz];
      (void)memset(suuid, 0, sz);
      ssize_t nread = read(fd, suuid, sz);

      if (nread == -1) {
        close(fd);
        return false;
      }

      close(fd);
      suuid[std::min(nread, sz - 1)] = '\0';

      if (suuid[strlen(suuid) - 1] == '\n') {
        suuid[strlen(suuid) - 1] = '\0';
      }

      ckuuid = suuid;
    }
  } else {
    if (fail_nouuid) {
      return false;
    }
  }

  if ((fsid != ckfsid) || (ckuuid != uuid)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if node is active i.e. the stat.active
//------------------------------------------------------------------------------
bool
Storage::IsNodeActive() const
{
  std::string status;
  GetFstConfigValue("stat.active", status);

  if (status == "online") {
    return true;
  } else {
    return false;
  }
}

//----------------------------------------------------------------------------
// Check if the selected FST needs to be registered as "full" or "warning"
// CAUTION: mFsMutex must be at-least-read locked before calling
// this function.
//
// Parameter i is the index into mFsVect.
//----------------------------------------------------------------------------
void
Storage::CheckFilesystemFullness(FileSystem* fs,
                                 eos::common::FileSystem::fsid_t fsid)
{
  long long freebytes = fs->GetLongLong("stat.statfs.freebytes");

  // Watch out for stat.statfs.freebytes not yet set
  if (freebytes == 0 && fs->GetString("stat.statfs.freebytes").length() == 0) {
    eos_static_info("%s", "msg=\"stat.statfs.freebytes has not yet been "
                    "defined, not setting file system fill status\"");
    return;
  }

  XrdSysMutexHelper lock(mFsFullMapMutex);
  // stop the writers if it get's critical under 5 GB space
  int full_gb = 5;

  if (getenv("EOS_FS_FULL_SIZE_IN_GB")) {
    full_gb = atoi(getenv("EOS_FS_FULL_SIZE_IN_GB"));
  }

  if ((freebytes < full_gb * 1024ll * 1024ll * 1024ll)) {
    mFsFullMap[fsid] = true;
  } else {
    mFsFullMap[fsid] = false;
  }

  if ((freebytes < 1024ll * 1024ll * 1024ll) ||
      (freebytes <= fs->GetLongLong("headroom"))) {
    mFsFullWarnMap[fsid] = true;
  } else {
    mFsFullWarnMap[fsid] = false;
  }
}

//------------------------------------------------------------------------------
// Get storage path for a particular file system id
//------------------------------------------------------------------------------
std::string
Storage::GetStoragePath(eos::common::FileSystem::fsid_t fsid) const
{
  std::string path;
  eos::common::RWMutexReadLock rd_lock(mFsMutex);
  auto it = mFsMap.find(fsid);

  if (it != mFsMap.end()) {
    path = it->second->GetPath();
  }

  return path;
}

EOSFSTNAMESPACE_END
