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
#include "fst/Verify.hh"
#include "fst/Deletion.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/XrdFstOss.hh"
#include "common/Fmd.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/Constants.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/LinuxStat.hh"
#include "common/ShellCmd.hh"
#include "common/StringUtils.hh"
#include "fst/utils/FTSWalkTree.hh"
#include "MonitorVarPartition.hh"
#include "qclient/structures/QSet.hh"
#include <google/dense_hash_map>
#include <math.h>
// @note (esindril)use this when Clang (>= 6.0.0) supports it
//#include <filesystem>

extern eos::fst::XrdFstOss* XrdOfsOss;

namespace
{
//------------------------------------------------------------------------------
//! Get minimum free space threshold after which a file system is considered
//! full
//------------------------------------------------------------------------------
static long long
GetFullFsThresholdBytes()
{
  static std::string s_full_env("EOS_FS_FULL_SIZE_IN_GB");
  static long long s_full_threshold =
    (std::stoll(getenv(s_full_env.c_str()) ?
                getenv(s_full_env.c_str()) : "5")
     * 1024ll * 1024ll * 1024ll);
  return s_full_threshold;
}
}

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Check a few files randomly to make sure they have the FMD xattr converted
//!
//! @param fst_path file system mount point
//!
//! @return true if conversion was done, otherwise false
//------------------------------------------------------------------------------
bool RandomCheckFsXattrConverted(const std::string& fs_path)
{
  std::set<uint64_t> dir_hash {0, 1, 100, 1000, 2000, 10000, 20000, 50000, 100000};
  std::set<std::string> existing_dirs;
  struct stat info;
  char full_path[16384];
  // Lambda function to check for existence of FMD xattr
  auto check_fmd_xattr = [](std::string_view abs_path) -> bool {
    static const std::string xattr_key = "user.eos.fmd";
    FsIo local_io {abs_path.data()};
    std::string xattr_val;
    return (local_io.attrGet(xattr_key, xattr_val) == 0);
  };

  // Check which hash directories actually exist on this mountpoint
  for (unsigned long long hash : dir_hash) {
    sprintf(full_path, "%s/%08llx/", fs_path.c_str(), hash);

    if (stat(full_path, &info) == 0) {
      existing_dirs.insert(full_path);
    }
  }

  int checked_files = 0;
  int correct_files = 0;
  constexpr int max_index = 100;
  std::set<int> file_index {1, 10, 50, max_index};
  char* fts_argv[2];
  fts_argv[1] = nullptr;

  // Check the above index files in the existing hash directories for the xattr
  for (const auto& dir : existing_dirs) {
    fts_argv[0] = (char*)dir.c_str();
    FTS* tree = fts_open(fts_argv, FTS_NOCHDIR | FTS_NOSTAT, 0);

    if (tree == nullptr) {
      eos_static_notice("msg=\"fts_open failed\" path=\"%s\"", dir.c_str());
      continue;
    }

    int count = 0;
    FTSENT* node;

    while ((node = fts_read(tree))) {
      if (node->fts_level > 0 && node->fts_name[0] == '.') {
        fts_set(tree, node, FTS_SKIP);
      } else {
        if (node->fts_info == FTS_F) {
          ++count;

          if (file_index.find(count) != file_index.end()) {
            // Skip check for block checksum files
            if (strstr(node->fts_path, XSMAP_SUFFIX.data()) != nullptr) {
              continue;
            }

            ++checked_files;

            if (!check_fmd_xattr(node->fts_path)) {
              eos_static_err("msg=\"no xattr for file\" path=\"%s\"",
                             node->fts_path);
              return false;
            } else {
              ++correct_files;
            }
          }

          // Don't list more then max index entries
          if (count > max_index) {
            break;
          }
        }
      }
    }

    if (fts_close(tree)) {
      eos_static_err("msg=\"fts_close failed\" path=\"%s\" errno=%d",
                     dir.c_str(), errno);
    }
  }

  eos_static_notice("msg=\"%i out of %i checked files with converted xattrs\"",
                    correct_files, checked_files);
  return true;
}

//------------------------------------------------------------------------------
//! Check that the given file system has been converted to xattr Fmd. This
//! method only checks for the existence of the ".eosattrconverted" special file
//!
//! @param path file system mount-point
//!
//! @return true if conversion was done, otherwise false
//------------------------------------------------------------------------------
bool
CheckFsXattrConverted(std::string fs_path)
{
  // Skip xattr conversion check for non-local filesystems (with protocol prefixes)
  if (fs_path.find("://") != std::string::npos || fs_path[0] != '/') {
    eos_static_info("msg=\"skipping xattr conversion check for non-local filesystem\" "
                    "path=\"%s\"", fs_path.c_str());
    return true;
  }

  const std::string xattr_conv_marker = ".eosattrconverted";
  const std::string xattr_path = fs_path + "/" + xattr_conv_marker;
  struct stat info;

  if (stat(xattr_path.c_str(), &info) != 0) {
    // Check if this is a new file system and add by default
    // the converted file marker
    DIR* dir = opendir(fs_path.c_str());

    if (dir == nullptr) {
      eos_static_err("msg=\"failed to open file system root directory\" "
                     "path=\"%s\"",  fs_path.c_str());
      return false;
    }

    struct dirent* dent {
      nullptr
    };

    while ((dent = readdir(dir))) {
      if ((dent->d_type == DT_DIR) &&
          ((strncmp(dent->d_name, ".", strlen(dent->d_name)) != 0) &&
           (strncmp(dent->d_name, "..", strlen(dent->d_name)) != 0))) {
        // No xattr marker, check some random files for user.eos.fmd xattr
        if (!RandomCheckFsXattrConverted(fs_path)) {
          return false;
        } else {
          // Exit loop and mark file system as converted
          break;
        }
      }
    }

    (void) closedir(dir);
    // This is a new fs, add the converted marker
    std::ofstream file;
    file.open(xattr_path, std::ios::out);
  }

  return true;
}

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

  mFsConfigThread.reset(&Storage::FsConfigUpdate, this);
  mFsConfigThread.setName("FsConfigUpdate Thread");

  if (gOFS.mMessagingRealm->haveQDB()) {
    mRegisterFsThread.reset(&Storage::UpdateRegisteredFs, this);
    mRegisterFsThread.setName("RegisterFS Thread");
    mQdbCommunicatorThread.reset(&Storage::QdbCommunicator, this);
    mQdbCommunicatorThread.setName("QDB Communicator Thread");
  } else {
    mCommunicatorThread.reset(&Storage::Communicator, this);
    mCommunicatorThread.setName("Communicator Thread");
  }

  XrdSysMutexHelper tsLock(mThreadsMutex);
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
  eos_info("starting verification thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartFsVerify,
                              static_cast<void*>(this),
                              0, "Verify Thread"))) {
    eos_crit("cannot start verify thread");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_static_info("%s", "msg=\"starting daemon supervisor thread\"");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartDaemonSupervisor,
                              static_cast<void*>(this),
                              0, "Supervisor Thread"))) {
    eos_static_crit("%s", "msg=\"cannot start supervisor thread\"");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  mErrorReportThread.reset(&Storage::ErrorReport, this);
  mErrorReportThread.setName("Error Report Thread");
  mPublisherThread.reset(&Storage::Publish, this);
  mPublisherThread.setName("Publisher Thread");
  eos_info("starting mgm synchronization thread");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartMgmSyncer,
                              static_cast<void*>(this),
                              0, "MgmSyncer Thread"))) {
    eos_static_crit("%s", "msg=\"cannot start mgm syncer thread\"");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  // Starting FstPartitionMonitor
  eos_info("%s", "msg=\"starting /var/ partition monitor thread ...\"");

  if ((rc = XrdSysThread::Run(&tid, Storage::StartVarPartitionMonitor,
                              static_cast<void*>(this),
                              0, "Var Partition Monitor"))) {
    eos_crit("%s", "msg=\"annot start /var partition monitor thread\"");
    mZombie = true;
  }

  mThreadSet.insert(tid);
  eos_info("%s", "msg=\"enabling net/io load monitor\"");
  mFstLoad.Monitor();
  eos_info("%s", "msg=\"enabling local disk S.M.A.R.T attribute monitor\"");
  mFstHealth.Monitor();
}

//------------------------------------------------------------------------------
// General shutdown including stopping the helper threads and also
// cleaning up the registered file systems
//------------------------------------------------------------------------------
void
Storage::Shutdown()
{
  ShutdownThreads();
  // Collect all the file systems to be deleted and then trigger the actual
  // deletion outside the mFsMutex to avoid any deadlocks
  std::set<eos::fst::FileSystem*> set_fs;
  {
    while (mFsMutex.TryLockWrite() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    for (auto* ptr_fs : mFsVect) {
      set_fs.insert(ptr_fs);
    }

    for (auto& elem : mFsMap) {
      set_fs.insert(elem.second);
    }

    mFsVect.clear();
    mFsMap.clear();
    mFsMutex.UnLockWrite();
  }

  for (auto& ptr_fs : set_fs) {
    eos_static_warning("msg=\"deleting file system\" fsid=%lu",
                       ptr_fs->GetLocalId());
    delete ptr_fs;
  }
}

//------------------------------------------------------------------------------
// Shutdown all helper threads
//------------------------------------------------------------------------------
void
Storage::ShutdownThreads()
{
  mCommunicatorThread.join();
  mQdbCommunicatorThread.join();
  mPublisherThread.join();
  mErrorReportThread.join();
  mFsUpdQueue.emplace(0, "ACTION", "EXIT");
  mFsConfigThread.join();
  XrdSysMutexHelper scope_lock(mThreadsMutex);

  for (auto it = mThreadSet.begin(); it != mThreadSet.end(); it++) {
    eos_warning("op=shutdown thread_id=%llx", (unsigned long long) *it);
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
    eos_err("%s", "msg=\"verify list has already 1 Mio. entries - discarding "
            "verify message\"");
  }
}

//------------------------------------------------------------------------------
// Start boot thread
//------------------------------------------------------------------------------
void*
Storage::StartBoot(void* pp)
{
  if (pp) {
    BootThreadInfo* info = (BootThreadInfo*) pp;

    if (info->filesystem->ShouldBoot(info->mTriggerKey)) {
      info->storage->Boot(info->filesystem);
    } else {
      eos_static_info("msg=\"skip booting\" fsid=%lu trigger=\"%s\"",
                      info->filesystem->GetId(), info->mTriggerKey.c_str());
    }

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
// Boot file system
//------------------------------------------------------------------------------
void
Storage::Boot(FileSystem* fs)
{
  if (!fs) {
    eos_static_warning("%s", "msg=\"skip booting of NULL file system\"");
    return;
  }

  fs->SetStatus(eos::common::BootStatus::kBooting);
  fs->SetError(0, "");
  // Wait to know who is our manager
  std::string manager = "";
  size_t cnt = 0;

  do {
    cnt++;
    {
      XrdSysMutexHelper lock(gConfig.Mutex);
      manager = gConfig.Manager.c_str();
    }

    if (manager != "") {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    eos_static_info("msg=\"waiting to know manager\" fsid=%lu",
                    fs->GetLocalId());

    if (cnt > 20) {
      eos_static_alert("%s", "msg=\"didn't receive manager name, aborting\"");
      std::this_thread::sleep_for(std::chrono::seconds(5));
      XrdFstOfs::xrdfstofs_shutdown(1);
    }
  } while (true);

  eos_static_info("msg=\"manager known\" manager=\"%s\"", manager.c_str());
  eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
  std::string uuid = fs->GetLocalUuid();
  eos_static_info("msg=\"booting filesystem\" qpath=%s fsid=%lu uuid=%s",
                  fs->GetQueuePath().c_str(), fsid, uuid.c_str());

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
    const std::string path = fs->GetPath();
    int stat_rc = ::stat(path.c_str(), &buf);

    if (stat_rc || (buf.st_uid != DAEMONUID) ||
        ((buf.st_mode & S_IRWXU) != S_IRWXU)) {
      eos_static_err("msg=\"potential failed stat\" errno=%d path=\"%s\"",
                     errno, path.c_str());

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

  // Make sure the Fmd info was moved to xattrs
  if (!CheckFsXattrConverted(fs->GetPath())) {
    eos_static_crit("msg=\"files don't have Fmd info in xattr\" "
                    "fs_path=\"%s\"", fs->GetPath().c_str());
    eos_static_crit("%s", "msg=\"process will abort now, please convert "
                    "your file systems to drop LeveDB and use xattrs\"");
    std::abort();
  } else {
    eos_static_info("msg=\"check for Fmd xattr conversion successful\" "
                    "fs_path=%s", fs->GetPath().c_str());
  }

  {
    XrdSysMutexHelper scope_lock(gOFS.OpenFidMutex);
    gOFS.WNoDeleteOnCloseFid[fsid].clear_deleted_key();
    gOFS.WNoDeleteOnCloseFid[fsid].set_deleted_key(0);
  }

  bool resyncmgm = (fs->GetLongLong("bootcheck") ==
                    eos::common::FileSystem::kBootMgm);
  bool resyncdisk = (fs->GetLongLong("bootcheck") >=
                     eos::common::FileSystem::kBootDisk);
  // If we see the bootcheck kBootMgm for the filesystem, we resync with
  // the mgm. Remove the bootcheck flag.
  fs->SetLongLong("bootcheck", 0);
  eos_info("msg=\"booting\" fsid=%u resync_mgm=%d resync_disk=%d", fsid,
           resyncmgm, resyncdisk);

  // Sync only local disks
  if (resyncdisk && (fs->GetPath()[0] == '/')) {
    eos_info("msg=\"start disk synchronisation\" fsid=%u", fsid);

    if (!gOFS.mFmdHandler->ResyncAllDisk(fs->GetPath().c_str(), fsid, resyncmgm)) {
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
      eos_info("msg=\"synchronizing from QuarkDB backend\" fsid=%u", fsid);

      if (!gOFS.mFmdHandler->ResyncAllFromQdb(gOFS.mQdbContactDetails, fsid)) {
        fs->SetStatus(eos::common::BootStatus::kBootFailure);
        fs->SetError(EFAULT, "cannot resync meta data from QuarkDB");
        return;
      }
    } else {
      eos_info("msg=\"only mgm synchronization via QDB supported but missing "
               "QDB connection details\" fsid=%u", fsid);
    }

    eos_info("msg=\"finished mgm synchronization\" fsid=%u", fsid);
  } else {
    eos_info("msg=\"skip mgm resynchronization\" fsid=%u", fsid);
  }

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

  fs->SetLongLong("stat.bootdonetime", (unsigned long long) time(NULL));
  fs->IoPing();
  fs->SetStatus(eos::common::BootStatus::kBooted);
  fs->SetError(0, "");
  // Create FS orphans and deletions directories
  std::string orphans_dir = fs->GetPath();
  std::string deletions_dir = fs->GetPath();

  if (fs->GetPath()[0] != '/') {
    orphans_dir = mMetaDir.c_str();
    orphans_dir += "/.eosorphans";
    orphans_dir += "-";
    orphans_dir += (int) fs->GetLocalId();
    deletions_dir = mMetaDir.c_str();
    deletions_dir += "/.eosdeletions";
    deletions_dir += "-";
    deletions_dir += (int) fs->GetLocalId();
  } else {
    orphans_dir += "/.eosorphans";
    deletions_dir += "/.eosdeletions";
  }

  const std::list<std::string> lst_dirs = {orphans_dir, deletions_dir};

  for (const auto& dir : lst_dirs) {
    if (mkdir(dir.c_str(),
              S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
      if (errno != EEXIST) {
        fs->SetStatus(eos::common::BootStatus::kBootFailure);
        fs->SetError(errno ? errno : EIO, "cannot create orphans/deletions "
                     " directories");
        return;
      }
    }

    if (chown(dir.c_str(), 2, 2)) {
      fs->SetStatus(eos::common::BootStatus::kBootFailure);
      fs->SetError(errno ? errno : EIO, "cannot change ownership of "
                   " orphans/deletions directories");
      return;
    }
  }

  // Apply scanner configuration after booting is done
  const std::list<std::string> scan_keys {
    eos::common::SCAN_IO_RATE_NAME, eos::common::SCAN_ENTRY_INTERVAL_NAME,
    eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME, eos::common::SCAN_DISK_INTERVAL_NAME,
    eos::common::SCAN_NS_INTERVAL_NAME, eos::common::SCAN_NS_RATE_NAME};

  for (const auto& key : scan_keys) {
    const std::string sval = fs->GetString(key.c_str());

    try {
      long long val = std::stoll(sval);

      if (val >= 0) {
        fs->ConfigScanner(&mFstLoad, key.c_str(), val);
      }
    } catch (...) {
      eos_static_err("msg=\"failed to convert value\" key=\"%s\" val=\"%s\"",
                     key.c_str(), sval.c_str());
    }
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
// Run boot thread for specified filesystem
//------------------------------------------------------------------------------
bool
Storage::RunBootThread(FileSystem* fs, const std::string& trigger_key)
{
  bool retc = false;

  if (fs) {
    if (fs->GetLocalId() == 0) {
      eos_warning("msg=\"defer booting for fsid 0\" fs_ptr=%x", fs);
      return retc;
    }

    XrdSysMutexHelper boot_lock(mBootingMutex);

    // Check if this filesystem is currently already booting
    if (mBootingSet.count(fs->GetLocalId())) {
      eos_warning("msg=\"discard boot request: filesytem fsid=%lu is currently booting",
                  (unsigned long) fs->GetLocalId());
      return retc;
    } else {
      // Insert into the set of booting filesystems
      mBootingSet.insert(fs->GetLocalId());
    }

    BootThreadInfo* info = new BootThreadInfo();
    info->storage = this;
    info->filesystem = fs;
    info->mTriggerKey = trigger_key;
    pthread_t tid;

    if ((XrdSysThread::Run(&tid, Storage::StartBoot, static_cast<void*>(info),
                           0, "Booter"))) {
      eos_crit("msg=\"failed to start boot thread\" fsid=%lu", fs->GetLocalId());
      mBootingSet.erase(fs->GetLocalId());
    } else {
      retc = true;
      eos_notice("msg=\"started boot thread\" fsid=%lu", fs->GetLocalId());
      XrdSysMutexHelper ls_lock(mThreadsMutex);
      mThreadSet.insert(tid);
    }
  }

  return retc;
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

//----------------------------------------------------------------------------
// Delete file by moving it to a special directory on the file system root
// mount location in the .eosdeletions directory
//----------------------------------------------------------------------------
void
Storage::DeleteByMove(std::unique_ptr<Deletion> del)
{
  using eos::common::FileId;
  static const std::string del_dir = ".eosdeletions";
  const std::string sfxid = FileId::Fid2Hex(del->mFidVect[0]);
  const std::string fpath = FileId::FidPrefix2FullPath(sfxid.c_str(),
                            del->mLocalPrefix.c_str());
  eos::common::Path cpath(fpath.c_str());
  size_t cpath_sz = cpath.GetSubPathSize();

  if (cpath_sz <= 2) {
    eos_static_err("msg=\"failed to extract FST mount/fid hex\" path=%s",
                   fpath.c_str());
    return;
  }

  std::ostringstream oss;
  oss << cpath.GetSubPath(cpath_sz - 2) << ".eosdeletions/" << sfxid;
  std::string fdeletion = oss.str();
  // Store the original path name as an extended attribute in case ...
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(fpath));
  io->attrSet("user.eos.deletion", fpath.c_str());

  // Move it into the deletions directory
  if (!rename(fpath.c_str(), fdeletion.c_str())) {
    eos_static_warning("msg=\"deletion quarantined\" path=%s del-path=%s",
                       fpath.c_str(), fdeletion.c_str());
  } else {
    eos_static_err("msg=\"failed to quarantine deletion\" path=%s del-path=%s",
                   fpath.c_str(), fdeletion.c_str());
  }
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
Storage::GetFileSystemById(eos::common::FileSystem::fsid_t fsid) const
{
  auto it = mFsMap.find(fsid);

  if (it != mFsMap.end()) {
    return it->second;
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Get configuration associated with the given file system id
//------------------------------------------------------------------------------
std::string
Storage::GetFileSystemConfig(eos::common::FileSystem::fsid_t fsid,
                             const std::string& key) const
{
  std::string value;
  eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
  FileSystem* fs = GetFileSystemById(fsid);

  if (fs) {
    value = fs->GetString(key.c_str());
  }

  return value;
}

//------------------------------------------------------------------------------
// Check if file system is in operational state i.e. config status < kDrain
//------------------------------------------------------------------------------
bool
Storage::IsFsOperational(eos::common::FileSystem::fsid_t fsid) const
{
  eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
  FileSystem* fs = GetFileSystemById(fsid);

  if (!fs) {
    return false;
  }

  if (fs->GetConfigStatus() < eos::common::ConfigStatus::kDrain) {
    return false;
  }

  return true;
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

//----------------------------------------------------------------------------
// Check if the selected FST needs to be registered as "full" or "warning"
//----------------------------------------------------------------------------
void
Storage::CheckFilesystemFullness(eos::common::FileSystem::fsid_t fsid)
{
  long long headroom = 0ll;
  long long freebytes = 0ll;
  {
    // Collect headroom and free bytes values for the given file system
    eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
    auto it = mFsMap.find(fsid);

    if (it == mFsMap.end()) {
      return;
    }

    FileSystem* fs = it->second;
    headroom = fs->GetLongLong("headroom");
    freebytes = fs->GetLongLong("stat.statfs.freebytes");

    // Watch out for stat.statfs.freebytes not yet set
    if (freebytes == 0 && fs->GetString("stat.statfs.freebytes").length() == 0) {
      eos_static_info("msg=\"stat.statfs.freebytes has not yet been "
                      "defined, not setting file system fill status\" "
                      "fsid=%lu", fsid);
      return;
    }
  }
  XrdSysMutexHelper lock(mFsFullMapMutex);

  if (freebytes < GetFullFsThresholdBytes())  {
    mFsFullMap[fsid] = true;
  } else {
    mFsFullMap[fsid] = false;
  }

  if ((freebytes < 1024ll * 1024ll * 1024ll) || (freebytes <= headroom)) {
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

//------------------------------------------------------------------------------
// Cleanup orphans
//------------------------------------------------------------------------------
bool
Storage::CleanupOrphans(eos::common::FileSystem::fsid_t fsid,
                        std::ostringstream& err_msg)
{
  bool success = true;
  std::map<eos::common::FileSystem::fsid_t, std::string> map;
  {
    eos::common::RWMutexReadLock rd_lock(mFsMutex);

    for (const auto& elem : mFsMap) {
      if (fsid == 0ul) {
        if (elem.second->GetStatus() != eos::common::BootStatus::kBooted) {
          eos_static_warning("msg=\"skip orphans clean up for not-booted file "
                             "system, best-effort\" fsid=%lu", elem.first);
          continue;
        }

        map.emplace(elem.first, elem.second->GetPath());
      } else {
        if (fsid == elem.first) {
          if (elem.second->GetStatus() != eos::common::BootStatus::kBooted) {
            err_msg << "skip orphans clean up for not-booted file system fsid="
                    << elem.first << std::endl;
            eos_static_warning("msg=\"skip orphans clean up for not-booted file "
                               "system\" fsid=%lu", elem.first);
            success = false;
            break;
          }

          map.emplace(elem.first, elem.second->GetPath());
          break;
        }
      }
    }
  }

  // Perform the actual cleanup for the selected file systems
  for (const auto& elem : map) {
    std::set<uint64_t> fids;

    if (!CleanupOrphansDisk(elem.second, fids)) {
      err_msg << "error: failed orphans cleanup on disk fsid="
              << elem.first << std::endl;
      eos_static_err("msg=\"failed orphans cleanup on disk\" fsid=%lu",
                     elem.first);
      success = false;
    }

    if (!CleanupOrphansQdb(elem.first, fids)) {
      err_msg << "error: failed orphans cleanup in QDB fsid="
              << elem.first << std::endl;
      eos_static_err("msg=\"failed orphans cleanup in QDB\" fsid=%lu",
                     elem.first);
      success = false;
    }
  }

  return success;
}

//------------------------------------------------------------------------------
// Cleanup orphans on disk
//------------------------------------------------------------------------------
bool
Storage::CleanupOrphansDisk(const std::string& mount,
                            std::set<uint64_t>& fids)
{
  bool success = true;
  eos_static_info("msg=\"doing orphans cleanup on disk\" path=\"%s\"",
                  mount.c_str());
  std::string path_orphans = mount + "/.eosorphans/";
  DIR* dir {nullptr};
  struct dirent* entry {
    nullptr
  };
  std::string fn_path;

  if (!(dir = opendir(path_orphans.c_str()))) {
    eos_static_err("msg=\"failed to open dir\" errno=%d path=%s", errno,
                   path_orphans.c_str());
    return success;
  }

  while ((entry = readdir(dir)) != nullptr) {
    eos_debug("msg=\"dir contents\" name=%s type=%i", entry->d_name, entry->d_type);

    // Fallback to stat if readdir does not provide the d_type for the entries
    if (entry && entry->d_type == DT_UNKNOWN) {
      struct stat buf;
      fn_path = path_orphans + entry->d_name;

      if (stat(fn_path.c_str(), &buf)) {
        entry = nullptr;
      } else {
        entry->d_type = S_ISDIR(buf.st_mode) ? DT_DIR : DT_REG;
      }
    }

    if (entry && (entry->d_type == DT_REG)) {
      fn_path = path_orphans + entry->d_name;
      eos_static_info("msg=\"delete orphan entry\" path=\"%s\"",
                      fn_path.c_str());

      try {
        fids.insert(std::stoull(entry->d_name, nullptr, 16));
      } catch (...) {
        eos_static_info("msg=\"failed to convert orphan entry\" "
                        "path=\"%s\"", fn_path.c_str());
      }

      if (unlink(fn_path.c_str())) {
        eos_static_err("msg=\"delete failed\" path=\"%s\"", fn_path.c_str());
        success = false;
      }
    }
  }

  closedir(dir);
  /* @note (esindril) Use this once clang (>= 6.0.0) supports std::filesystem
  for (auto& entry : std::filesystem::directory_iterator(path_orphans)) {
    if (std::filesystem::is_regular_file(entry.status())) {
      eos_static_info("msg=\"delete orphan entry\" path=\"%s\"",
                      entry.path().c_str());

      if (!std::filesystem::remove(entry.path())) {
        eos_static_info("msg=\"delete failed\" path=\"%s\"",
                        entry.path().c_str());
        success = false;
      }
    }
  }
  */
  return success;
}

//------------------------------------------------------------------------------
// Cleanup orphans from QDB
//------------------------------------------------------------------------------
bool
Storage::CleanupOrphansQdb(eos::common::FileSystem::fsid_t fsid,
                           const std::set<uint64_t>& fids)
{
  static const uint32_t s_max_batch_size = 10000;
  eos_static_info("msg=\"doing orphans cleanup in QDB\" fsid=%lu", fsid);

  if (fids.empty()) {
    return true;
  }

  std::list<std::string> to_delete;
  qclient::QSet qset(*gOFS.mFsckQcl.get(),
                     SSTR("fsck:" << eos::common::FSCK_ORPHANS_N));

  for (const auto& fid : fids) {
    to_delete.push_back(SSTR(fid << ":" << fsid));

    if (to_delete.size() >= s_max_batch_size) {
      try {
        (void) qset.srem(to_delete);
      } catch (const std::runtime_error& e) {
        eos_static_err("msg=\"failed clean orphans in QDB\" msg=\"%s\"",
                       e.what());
        return false;
      }

      to_delete.clear();
    }
  }

  if (!to_delete.empty()) {
    try {
      (void) qset.srem(to_delete);
    } catch (const std::runtime_error& e) {
      eos_static_err("msg=\"failed clean orphans in QDB\" msg=\"%s\"",
                     e.what());
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Get number of file systems
//------------------------------------------------------------------------------
size_t
Storage::GetFSCount() const
{
  eos::common::RWMutexReadLock rd_lock(mFsMutex);
  return mFsMap.size();
}

//------------------------------------------------------------------------------
// Push collected errors to quarkdb
//------------------------------------------------------------------------------
bool
Storage::PushToQdb(eos::common::FileSystem::fsid_t fsid,
                   const eos::common::FsckErrsPerFsMap& errs_map)
{
#ifndef _NOOFS
  static const uint32_t s_max_batch_size = 10000;

  if (gOFS.mFsckQcl == nullptr) {
    eos_notice("%s", "msg=\"no qclient present, push to QDB failed\"");
    return false;
  }

  qclient::AsyncHandler ah;
  qclient::QSet fsck_set(*gOFS.mFsckQcl, "");

  for (const auto& elem : errs_map) {
    std::list<std::string> values; // contains fid:fsid entries

    for (auto& errfsid : elem.second) {
      for (auto& fid : errfsid.second) {
        if (values.size() <= s_max_batch_size) {
          values.push_back(SSTR(fid << ":" << errfsid.first));
        } else {
          fsck_set.setKey(SSTR("fsck:" << elem.first).c_str());
          fsck_set.sadd_async(values, &ah);
          values.clear();
        }
      }
    }

    if (!values.empty()) {
      fsck_set.setKey(SSTR("fsck:" << elem.first).c_str());
      fsck_set.sadd_async(values, &ah);
    }
  }

  if (!ah.Wait()) {
    eos_err("msg=\"some qset async requests failed\" fsid=%lu", fsid);
    return false;
  }

#endif
  return true;
}

//------------------------------------------------------------------------------
// Publish a paricular fsck error to QDB
//------------------------------------------------------------------------------
void
Storage::PublishFsckError(eos::common::FileId::fileid_t fid,
                          eos::common::FileSystem::fsid_t fsid,
                          eos::common::FsckErr err_type)
{
  eos::common::FsckErrsPerFsMap errs_map;
  errs_map[eos::common::FsckErrToString(err_type)][fsid].insert(fid);

  if (!PushToQdb(fsid, errs_map)) {
    eos_static_err("msg=\"failed to push fsck error to QDB\" fxid=%08llx "
                   "fsid=%lu err=%s", fid, fsid,
                   eos::common::FsckErrToString(err_type).c_str());
  }
}

EOSFSTNAMESPACE_END
