//------------------------------------------------------------------------------
// File: ScanDir.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "fst/ScanDir.hh"
#include "common/Path.hh"
#include "common/Constants.hh"
#include "common/IoPriority.hh"
#include "console/commands/helpers/FsHelper.hh"
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/FmdDbMap.hh"
#include "fst/Deletion.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "namespace/ns_quarkdb/Constants.hh"
#include "qclient/structures/QSet.hh"
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <syslog.h>
#include <unistd.h>
#include <fcntl.h>
#ifndef __APPLE__
#include <sys/syscall.h>
#include <asm/unistd.h>
#endif


EOSFSTNAMESPACE_BEGIN


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ScanDir::ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
                 eos::fst::Load* fstload, bool bgthread,
                 long int file_rescan_interval, int ratebandwidth,
                 bool fake_clock) :
  mFstLoad(fstload), mFsId(fsid), mDirPath(dirpath),
  mRateBandwidth(ratebandwidth), mEntryIntervalSec(file_rescan_interval),
  mDiskIntervalSec(DEFAULT_DISK_INTERVAL), mNsIntervalSec(DEFAULT_NS_INTERVAL),
  mFsckRefreshIntervalSec(DEFAULT_FSCK_INTERVAL),
  mConfDiskIntervalSec(DEFAULT_DISK_INTERVAL),
  mConfFsckIntervalSec(DEFAULT_FSCK_INTERVAL),
  mNumScannedFiles(0), mNumCorruptedFiles(0),
  mNumHWCorruptedFiles(0),  mTotalScanSize(0), mNumTotalFiles(0),
  mNumSkippedFiles(0), mBuffer(nullptr),
  mBufferSize(0), mBgThread(bgthread), mClock(fake_clock), mRateLimit(nullptr)
{
  long alignment = pathconf((mDirPath[0] != '/') ? "/" : mDirPath.c_str(),
                            _PC_REC_XFER_ALIGN);

  if (alignment > 0) {
    mBufferSize = 256 * alignment;

    if (posix_memalign((void**) &mBuffer, alignment, mBufferSize)) {
      fprintf(stderr, "error: error calling posix_memaling on dirpath=%s. \n",
              mDirPath.c_str());
      std::abort();
    }
  } else {
    mBufferSize = 256 * 1024;
    mBuffer = (char*) malloc(mBufferSize);
    fprintf(stderr,
            "error: OS does not provide alignment or path does not exist\n");
  }

  if (mBgThread) {
    openlog("scandir", LOG_PID | LOG_NDELAY, LOG_USER);
    mDiskThread.reset(&ScanDir::RunDiskScan, this);
#ifndef _NOOFS
    mRateLimit.reset(new eos::common::RequestRateLimit());
    mRateLimit->SetRatePerSecond(sDefaultNsScanRate);
    mNsThread.reset(&ScanDir::RunNsScan, this);
#endif
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ScanDir::~ScanDir()
{
  if (mBgThread) {
    mDiskThread.join();
    mNsThread.join();
    closelog();
  }

  if (mBuffer) {
    free(mBuffer);
  }
}

//------------------------------------------------------------------------------
// Update scanner configuration
//------------------------------------------------------------------------------
void
ScanDir::SetConfig(const std::string& key, long long value)
{
  eos_info("msg=\"update scanner configuration\" key=\"%s\" value=\"%s\"",
           key.c_str(), std::to_string(value).c_str());

  if (key == eos::common::SCAN_IO_RATE_NAME) {
    mRateBandwidth.store(static_cast<int>(value), std::memory_order_relaxed);
  } else if (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) {
    mEntryIntervalSec.store(value, std::memory_order_release);
  } else if (key == eos::common::SCAN_DISK_INTERVAL_NAME) {
    if (mDiskIntervalSec.compare_exchange_strong(mConfDiskIntervalSec,
                                                 static_cast<uint64_t>(value),
                                                 std::memory_order_acq_rel))
    {
      // Move the following line after join if you want to prevent a toggle until join
      mConfDiskIntervalSec = static_cast<uint64_t>(value);
      mDiskThread.join();
      mDiskThread.reset(&ScanDir::RunDiskScan, this);
    }
  } else if (key == eos::common::FSCK_REFRESH_INTERVAL_NAME) {
    if (mFsckRefreshIntervalSec.compare_exchange_strong(mConfFsckIntervalSec,
                                                        static_cast<uint64_t>(value),
                                                        std::memory_order_acq_rel))
    {
      mConfFsckIntervalSec = static_cast<uint64_t>(value);
      mDiskThread.join();
      mDiskThread.reset(&ScanDir::RunDiskScan, this);
    }
  } else if (key == eos::common::SCAN_NS_INTERVAL_NAME) {
#ifndef _NOOFS

    if (mNsIntervalSec != static_cast<uint64_t>(value)) {
      mNsIntervalSec.store(value, std::memory_order_relaxed);
      mNsThread.join();
      mNsThread.reset(&ScanDir::RunNsScan, this);
    }

#endif
  } else if (key == eos::common::SCAN_NS_RATE_NAME) {
    mRateLimit->SetRatePerSecond(value);
  }
}

#ifndef _NOOFS
//------------------------------------------------------------------------------
// Infinite loop doing the scanning of namespace entries
//------------------------------------------------------------------------------
void
ScanDir::RunNsScan(ThreadAssistant& assistant) noexcept
{
  using namespace std::chrono;
  using eos::common::FileId;
  eos_info("msg=\"started the ns scan thread\" fsid=%lu dirpath=\"%s\" "
           "ns_scan_interval_sec=%llu", mFsId, mDirPath.c_str(),
           mNsIntervalSec.load(std::memory_order_relaxed));

  if (gOFS.mFsckQcl == nullptr) {
    eos_notice("%s", "msg=\"no qclient present, skipping ns scan\"");
    return;
  }

  // Wait for the corresponding file system to boot before starting
  while (gOFS.Storage->IsFsBooting(mFsId)) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      eos_info("%s", "msg=\"stopping ns scan thread\"");
      return;
    }
  }

  // Get a random smearing and avoid that all start at the same time
  size_t sleep_sec = (1.0 * mNsIntervalSec.load(std::memory_order_relaxed) * random() / RAND_MAX);
  eos_info("msg=\"delay ns scan thread by %llu seconds\" fsid=%lu dirpath=\"%s\"",
           sleep_sec, mFsId, mDirPath.c_str());
  assistant.wait_for(seconds(sleep_sec));

  while (!assistant.terminationRequested()) {
    AccountMissing();
    CleanupUnlinked();
    assistant.wait_for(seconds(mNsIntervalSec.load(std::memory_order_relaxed)));
  }
}

//----------------------------------------------------------------------------
// Account for missing replicas
//----------------------------------------------------------------------------
void
ScanDir::AccountMissing()
{
  using eos::common::FileId;
  struct stat info;
  auto fids = CollectNsFids(eos::fsview::sFilesSuffix);
  eos_info("msg=\"scanning %llu attached namespace entries\"", fids.size());

  while (!fids.empty()) {
    // Tag any missing replicas
    eos::IFileMD::id_t fid = fids.front();
    fids.pop_front();
    std::string fpath =
      FileId::FidPrefix2FullPath(FileId::Fid2Hex(fid).c_str(), mDirPath.c_str());

    if (stat(fpath.c_str(), &info)) {
      // Double check that this not a file which was deleted in the meantime
      try {
        if (IsBeingDeleted(fid)) {
          // Give it one more kick by dropping the file from disk and db
          XrdOucErrInfo tmp_err;

          if (gOFS._rem("/DELETION_FSCK", tmp_err, nullptr, nullptr, fpath.c_str(),
                        fid, mFsId, true)) {
            eos_err("msg=\"failed to remove local file\" path=%s fxid=%08llx "
                    "fsid=%lu", fpath.c_str(), fid, mFsId);
          }
        } else {
          // File missing on disk - create fmd entry and mark it as missing but
          // then also check the MGM info since the file might be 0-size so we
          // need to remove the kMissing flag
          auto fmd = gFmdDbMapHandler.LocalGetFmd(fid, mFsId, true, true);

          if (fmd) {
            fmd->mProtoFmd.set_layouterror(fmd->mProtoFmd.layouterror() |
                                           LayoutId::kMissing);
            gFmdDbMapHandler.Commit(fmd.get());
            (void) gFmdDbMapHandler.ResyncFileFromQdb(fid, mFsId, fpath,
                gOFS.mFsckQcl);
          } else {
            eos_err("msg=\"faile to create local fmd entry for missing file\" "
                    "fxid=%08llx fsid=%lu", fid, mFsId);
          }
        }
      } catch (eos::MDException& e) {
        // No file on disk, no ns file metadata object but we have a ghost entry
        // in the file system view - delete it
        if (!DropGhostFid(mFsId, fid)) {
          eos_err("msg=\"failed to drop ghost entry\" fxid=%08llx fsid=%lu",
                  fid, mFsId);
        }
      }
    }

    // Rate limit enforced for the current disk
    mRateLimit->Allow();
  }
}

//----------------------------------------------------------------------------
// Cleanup unlinked replicas older than 10 min still laying around
//----------------------------------------------------------------------------
void
ScanDir::CleanupUnlinked()
{
  using eos::common::FileId;
  // Loop over the unlinked files and force unlink them if too old
  auto unlinked_fids = CollectNsFids(eos::fsview::sUnlinkedSuffix);
  eos_info("msg=\"scanning %llu unlinked namespace entries\"",
           unlinked_fids.size());

  while (!unlinked_fids.empty()) {
    eos::IFileMD::id_t fid = unlinked_fids.front();
    unlinked_fids.pop_front();

    try {
      if (IsBeingDeleted(fid) == false) {
        // Put the fid in the queue of files to be deleted and this should
        // clean both the disk file and update the namespace entry
        eos_info("msg=\"resubmit for deletion\" fxid=%08llx fsid=%lu",
                 fid, mFsId);
        std::vector<unsigned long long> id_vect {fid};
        auto deletion = std::make_unique<Deletion>
                        (id_vect, mFsId, mDirPath.c_str());
        gOFS.Storage->AddDeletion(std::move(deletion));
      }
    } catch (eos::MDException& e) {
      // There is no file metadata object so we delete any potential file from
      // the local disk and also drop the ghost entry from the file system view
      eos_info("msg=\"cleanup ghost unlinked file\" fxid=%08llx fsid=%lu",
               fid, mFsId);
      std::string fpath =
        FileId::FidPrefix2FullPath(FileId::Fid2Hex(fid).c_str(), mDirPath.c_str());
      // Drop the file from disk and local DB
      XrdOucErrInfo tmp_err;

      if (gOFS._rem("/DELETION_FSCK", tmp_err, nullptr, nullptr, fpath.c_str(),
                    fid, mFsId, true)) {
        eos_err("msg=\"failed remove local file\" path=%s fxid=%08llx fsid=%lu",
                fpath.c_str(), fid, mFsId);
      }

      if (!DropGhostFid(mFsId, fid)) {
        eos_err("msg=\"failed to drop ghost entry\" fxid=%08llx fsid=%lu",
                fid, mFsId);
      }
    }

    mRateLimit->Allow();
  }
}

//------------------------------------------------------------------------------
// Drop ghost fid from the given file system id
//------------------------------------------------------------------------------
bool
ScanDir::DropGhostFid(const eos::common::FileSystem::fsid_t fsid,
                      const eos::IFileMD::id_t fid) const
{
  GlobalOptions opts;
  opts.mForceSss = true;
  FsHelper fs_cmd(opts);

  if (fs_cmd.ParseCommand(SSTR("fs dropghosts " << fsid
                               << " --fid " << fid).c_str())) {
    eos_err("%s", "msg=\"failed to parse fs dropghosts command\"");
    return false;
  }

  if (fs_cmd.ExecuteWithoutPrint()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if file is unlinked from the namespace and in the process of being
// deleted from the disk. Files that are unlinked for more than 10 min
// definetely have a problem and we don't account them as in the process of
// being deleted.
//------------------------------------------------------------------------------
bool
ScanDir::IsBeingDeleted(const eos::IFileMD::id_t fid) const
{
  using namespace std::chrono;
  auto file_fut = eos::MetadataFetcher::getFileFromId(*gOFS.mFsckQcl.get(),
                  eos::FileIdentifier(fid));
  // Throws an exception if file metadata object doesn't exist
  eos::ns::FileMdProto fmd = std::move(file_fut).get();
  return (fmd.cont_id() == 0ull);
}

//------------------------------------------------------------------------------
// Collect all file ids present on the current file system from the NS view
//------------------------------------------------------------------------------
std::deque<eos::IFileMD::id_t>
ScanDir::CollectNsFids(const std::string& type) const
{
  std::deque<eos::IFileMD::id_t> queue;

  if ((type != eos::fsview::sFilesSuffix) &&
      (type != eos::fsview::sUnlinkedSuffix)) {
    eos_err("msg=\"unsupported type %s\"", type.c_str());
    return queue;
  }

  std::ostringstream oss;
  oss << eos::fsview::sPrefix << mFsId << ":" << type;
  const std::string key = oss.str();
  qclient::QSet qset(*gOFS.mFsckQcl.get(), key);

  try {
    for (qclient::QSet::Iterator it = qset.getIterator(); it.valid(); it.next()) {
      try {
        queue.push_back(std::stoull(it.getElement()));
      } catch (...) {
        eos_err("msg=\"failed to convert fid entry\" data=\"%s\"",
                it.getElement().c_str());
      }
    }
  } catch (const std::runtime_error& e) {
    // There is no such set in QDB
  }

  return queue;
}

#endif

//------------------------------------------------------------------------------
// Infinite loop doing the scanning
//------------------------------------------------------------------------------
void
ScanDir::RunDiskScan(ThreadAssistant& assistant) noexcept
{
  using namespace std::chrono;

  if (mBgThread) {
    pid_t tid = (pid_t) syscall(SYS_gettid);
    int retc = 0;

    if ((retc = ioprio_set(IOPRIO_WHO_PROCESS,
                           IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7)))) {
      eos_err("msg=\"cannot set io priority to lowest best effort\" "
              "retc=%d errno=%d\n", retc, errno);
    } else {
      eos_notice("msg=\"set io priority to 7(lowest best-effort)\" pid=%u", tid);
    }
  }

#ifndef _NOOFS

  // Wait for the corresponding file system to boot before starting
  while (gOFS.Storage->IsFsBooting(mFsId)) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      eos_info("%s", "msg=\"stopping disk scan thread\"");
      return;
    }
  }

  if (gOFS.mFsckQcl == nullptr) {
    eos_notice("%s", "msg=\"no qclient present, skipping disk scan\"");
    return;
  }

#endif
  // If there is a reconfiguration of Disk/Fsck Interval, reload these only
  // after current run
  uint64_t DiskIntervalSec = mDiskIntervalSec.load(std::memory_order_acquire);
  uint64_t FsckIntervalSec = mFsckRefreshIntervalSec.load(std::memory_order_acquire);

  if (mBgThread) {
    // Make sure we update the inconsistencies once before the initial sleep
#ifndef _NOOFS
    if (!gOFS.Storage->UpdateInconsistencyInfo(mFsId)) {
      eos_notice("msg=\"file system (being) deleted, abort any further scanning\""
                 " fsid=%lu", mFsId);
      return;
    } else {
      eos_info("msg=\"done initial collection of inconsistency stats\" fsid=%lu "
               "disk_scan_interval_sec=%llu fsck_refresh_interval_sec=%llu",
               mFsId, DiskIntervalSec, FsckIntervalSec);
    }

#endif
    // Get a random smearing and avoid that all start at the same time! 0-4 hours
    size_t sleeper = (1.0 * DiskIntervalSec * random() / RAND_MAX);
    assistant.wait_for(seconds(sleeper));
  }

  while (!assistant.terminationRequested()) {
    mNumScannedFiles =  mTotalScanSize =  mNumCorruptedFiles = 0;
    mNumHWCorruptedFiles =  mNumTotalFiles = mNumSkippedFiles = 0;
    auto start_ts = std::chrono::system_clock::now();
    // Do the heavy work
    ScanSubtree(assistant);
    auto finish_ts = std::chrono::system_clock::now();
    seconds duration = duration_cast<seconds>(finish_ts - start_ts);
    // Check if there was a config update before we sleep
    DiskIntervalSec = mDiskIntervalSec.load(std::memory_order_acquire);
    FsckIntervalSec = mFsckRefreshIntervalSec.load(std::memory_order_acquire);

    std::string log_msg =
      SSTR("[ScanDir] Directory: " << mDirPath << " files=" << mNumTotalFiles
           << " scanduration=" << duration.count() << " [s] scansize="
           << mTotalScanSize << " [Bytes] [ " << (mTotalScanSize / 1e6)
           << " MB ] scannedfiles=" << mNumScannedFiles << " corruptedfiles="
           << mNumCorruptedFiles << " hwcorrupted=" << mNumHWCorruptedFiles
           << " skippedfiles=" << mNumSkippedFiles
           << " disk_scan_interval_sec=" << DiskIntervalSec
           << " fsck_refresh_interval_sec=" << FsckIntervalSec);

    if (mBgThread) {
      syslog(LOG_ERR, "%s\n", log_msg.c_str());
      eos_notice("%s", log_msg.c_str());
    } else {
      fprintf(stderr, "%s\n", log_msg.c_str());
    }

    if (mBgThread) {
      // Run again after (default) 4 hours. In the meantime update the
      // inconsistencies every mFsckRefreshIntervalSec or every mDiskIntervalSec
      // if this is more frequent.
      long effective_delay = (FsckIntervalSec > DiskIntervalSec ?
                              DiskIntervalSec : FsckIntervalSec) - 1;
      auto deadline = std::chrono::system_clock::now() +
                      std::chrono::seconds(effective_delay);

      do {
#ifndef _NOOFS

        if (!gOFS.Storage->UpdateInconsistencyInfo(mFsId)) {
          eos_notice("msg=\"file system (being) deleted, abort any further scanning\""
                     " fsid=%lu", mFsId);
          return;
        }

#endif
        assistant.wait_for(std::chrono::seconds(effective_delay));

        if (assistant.terminationRequested()) {
          break;
        }
      } while (deadline > std::chrono::system_clock::now());

      // @todo(esindril): this will not be needed anymore once we drop the
      // local db. If needed we could add it as an individual command
      // Call the ghost entry clean-up function
      // eos_notice("msg=\"cleaning ghost entries\" dir=%s fsid=%d",
      //            mDirPath.c_str(), mFsId);
      // gFmdDbMapHandler.RemoveGhostEntries(mDirPath.c_str(), mFsId);
    } else {
      break;
    }
  }
}

//------------------------------------------------------------------------------
// Method traversing all the files in the subtree and potentially rescanning
// some of them.
//------------------------------------------------------------------------------
void
ScanDir::ScanSubtree(ThreadAssistant& assistant) noexcept
{
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(mDirPath.c_str()));

  if (!io) {
    LogMsg(LOG_ERR, "msg=\"no IO plug-in available\" url=\"%s\"",
           mDirPath.c_str());
    return;
  }

  std::unique_ptr<FileIo::FtsHandle> handle {io->ftsOpen()};

  if (!handle) {
    LogMsg(LOG_ERR, "msg=\"fts_open failed\" dir=%s", mDirPath.c_str());
    return;
  }

  std::string fpath;

  while ((fpath = io->ftsRead(handle.get())) != "") {
    if (!mBgThread) {
      fprintf(stderr, "[ScanDir] processing file %s\n", fpath.c_str());
    }

    CheckFile(fpath);

    if (assistant.terminationRequested()) {
      return;
    }
  }

  if (io->ftsClose(handle.get())) {
    LogMsg(LOG_ERR, "msg=\"fts_close failed\" dir=%s", mDirPath.c_str());
  }
}

//------------------------------------------------------------------------------
// Check the given file for errors and properly account them both at the
// scanner level and also by setting the proper xattrs on the file.
//------------------------------------------------------------------------------
void
ScanDir::CheckFile(const std::string& fpath)
{
  using eos::common::LayoutId;
  eos_debug("msg=\"running check file\" path=\"%s\"", fpath.c_str());
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(fpath.c_str()));
  ++mNumTotalFiles;
  // Get last modification time
  struct stat buf1;
  struct stat buf2;

  if ((io->fileOpen(0, 0)) || io->fileStat(&buf1)) {
    LogMsg(LOG_ERR, "msg=\"open/stat failed\" path=%s\"", fpath.c_str());
    return;
  }

#ifndef _NOOFS
  auto fid = eos::common::FileId::PathToFid(fpath.c_str());

  if (!fid) {
    eos_static_info("msg=\"skip file which is not a eos data file\", "
                    "path=\"%s\"", fpath.c_str());
    return;
  }

  if (mBgThread) {
    if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
      syslog(LOG_ERR, "skipping scan w-open file: localpath=%s fsid=%d fxid=%08llx\n",
             fpath.c_str(), mFsId, fid);
      eos_warning("msg=\"skipping scan of w-open file\" localpath=%s fsid=%d "
                  "fxid=%08llx", fpath.c_str(), mFsId, fid);
      return;
    }
  }

#endif
  std::string xs_stamp_sec;
  io->attrGet("user.eos.timestamp", xs_stamp_sec);

  // Handle the old format in microseconds, truncate to seconds
  if (xs_stamp_sec.length() > 10) {
    xs_stamp_sec.erase(10);
  }

  // If rescan not necessary just return
  if (!DoRescan(xs_stamp_sec)) {
    ++mNumSkippedFiles;
    return;
  }

  std::string lfn, previous_xs_err;
  io->attrGet("user.eos.lfn", lfn);
  io->attrGet("user.eos.filecxerror", previous_xs_err);
  bool was_healthy = (previous_xs_err == "0");
  // Flag if file has been modified since the last time we scanned it
  bool didnt_change = (buf1.st_mtime < atoll(xs_stamp_sec.c_str()));
  bool blockxs_err = false;
  bool filexs_err = false;
  unsigned long long scan_size {0ull};
  std::string scan_xs_hex;

  if (!ScanFileLoadAware(io, scan_size, scan_xs_hex, filexs_err, blockxs_err)) {
    return;
  }

  bool reopened = false;
#ifndef _NOOFS

  if (mBgThread) {
    if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
      eos_err("msg=\"file reopened during the scan, ignore checksum error\" "
              "path=%s", fpath.c_str());
      reopened = true;
    }
  }

#endif

  // If the file was changed in the meanwhile or is reopened for update
  // we leave it for a later scan
  if (reopened || io->fileStat(&buf2) || (buf1.st_mtime != buf2.st_mtime)) {
    LogMsg(LOG_ERR, "msg=\"[ScanDir] skip file modified during scan path=%s",
           fpath.c_str());
    return;
  }

  if (filexs_err) {
    if (mBgThread) {
      syslog(LOG_ERR, "corrupted file checksum path=%s lfn=%s\n",
             fpath.c_str(), lfn.c_str());
      eos_err("corrupted file checksum path=%s lfn=%s", fpath.c_str(),
              lfn.c_str());
    } else {
      fprintf(stderr, "[ScanDir] corrupted file checksum path=%s lfn=%s\n",
              fpath.c_str(), lfn.c_str());
    }

    if (was_healthy && didnt_change) {
      ++mNumHWCorruptedFiles;

      if (mBgThread) {
        syslog(LOG_ERR, "HW corrupted file found path=%s lfn=%s\n",
               fpath.c_str(), lfn.c_str());
      } else {
        fprintf(stderr, "HW corrupted file found path=%s lfn=%s\n",
                fpath.c_str(), lfn.c_str());
      }
    }
  }

  // Collect statistics
  mTotalScanSize += scan_size;

  if ((io->attrSet("user.eos.timestamp", GetTimestampSmearedSec())) ||
      (io->attrSet("user.eos.filecxerror", filexs_err ? "1" : "0")) ||
      (io->attrSet("user.eos.blockcxerror", blockxs_err ? "1" : "0"))) {
    LogMsg(LOG_ERR, "msg=\"failed to set xattrs\" path=%s", fpath.c_str());
  }

#ifndef _NOOFS

  if (mBgThread) {
    gFmdDbMapHandler.UpdateWithScanInfo(fid, mFsId, fpath, scan_size,
                                        scan_xs_hex, gOFS.mFsckQcl);
  }

#endif
}

//------------------------------------------------------------------------------
// Get block checksum object for the given file. First we need to check if
// there is a block checksum file (.xsmap) corresponding to the given raw file.
//------------------------------------------------------------------------------
std::unique_ptr<eos::fst::CheckSum>
ScanDir::GetBlockXS(const std::string& file_path)
{
  using eos::common::LayoutId;
  std::string str_bxs_type, str_bxs_size;
  std::string filexs_path = file_path + ".xsmap";
  std::unique_ptr<eos::fst::FileIo> io(FileIoPluginHelper::GetIoObject(
                                         filexs_path));
  struct stat info;

  if (!io->fileStat(&info, 0)) {
    io->attrGet("user.eos.blockchecksum", str_bxs_type);
    io->attrGet("user.eos.blocksize", str_bxs_size);

    if (str_bxs_type.compare("")) {
      unsigned long bxs_type = LayoutId::GetBlockChecksumFromString(str_bxs_type);
      int bxs_size = atoi(str_bxs_size.c_str());
      int bxs_size_type = LayoutId::BlockSizeEnum(bxs_size);
      auto layoutid = LayoutId::GetId(LayoutId::kPlain, LayoutId::kNone, 0,
                                      bxs_size_type, bxs_type);
      std::unique_ptr<eos::fst::CheckSum> checksum =
        eos::fst::ChecksumPlugins::GetChecksumObject(layoutid, true);

      if (checksum) {
        if (checksum->OpenMap(filexs_path.c_str(), info.st_size, bxs_size, false)) {
          return checksum;
        } else {
          return nullptr;
        }
      } else {
        LogMsg(LOG_ERR, "%s", SSTR("msg=\"failed to get checksum object\" "
                                   << "layoutid=" << std::hex << layoutid
                                   << std::dec << "path=" << filexs_path).c_str());
      }
    } else {
      LogMsg(LOG_ERR, "%s", SSTR("msg=\"file has no blockchecksum xattr\""
                                 << " path=" << filexs_path).c_str());
    }
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Decide if a rescan is needed based on the timestamp provided and the
// configured rescan interval
//------------------------------------------------------------------------------
bool
ScanDir::DoRescan(const std::string& timestamp_sec) const
{
  using namespace std::chrono;

  if (!timestamp_sec.compare("")) {
    if (mEntryIntervalSec.load(std::memory_order_acquire) == 0ull) {
      return false;
    } else {
      // Check the first time if scanner is not completely disabled
      return true;
    }
  }

  uint64_t elapsed_sec {0ull};

  // Used only during testing
  if (mClock.IsFake()) {
    steady_clock::time_point old_ts(seconds(std::stoull(timestamp_sec)));
    steady_clock::time_point now_ts(mClock.getTime());
    elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();
  } else {
    system_clock::time_point old_ts(seconds(std::stoull(timestamp_sec)));
    system_clock::time_point now_ts(system_clock::now());
    elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();
  }

  if (elapsed_sec < mEntryIntervalSec.load(std::memory_order_relaxed)) {
    return false;
  } else {
    if (mEntryIntervalSec.load(std::memory_order_relaxed)) {
      return true;
    } else {
      return false;
    }
  }
}

//------------------------------------------------------------------------------
// Scan file taking the load into consideration
//------------------------------------------------------------------------------
bool
ScanDir::ScanFileLoadAware(const std::unique_ptr<eos::fst::FileIo>& io,
                           unsigned long long& scan_size,
                           std::string& scan_xs_hex,
                           bool& filexs_err, bool& blockxs_err)
{
  scan_size = 0ull;
  filexs_err = blockxs_err = false;
  int scan_rate = mRateBandwidth.load(std::memory_order_relaxed);
  std::string file_path = io->GetPath();
  struct stat info;

  if (io->fileStat(&info)) {
    eos_err("msg=\"failed stat\" path=%s\"", file_path.c_str());
    return false;
  }

  // Get checksum type and value
  std::string xs_type;
  char xs_val[SHA256_DIGEST_LENGTH];
  memset(xs_val, 0, sizeof(xs_val));
  size_t xs_len = SHA256_DIGEST_LENGTH;
  io->attrGet("user.eos.checksumtype", xs_type);
  io->attrGet("user.eos.checksum", xs_val, xs_len);
  auto comp_file_xs = eos::fst::ChecksumPlugins::GetXsObj(xs_type);
  std::unique_ptr<eos::fst::CheckSum> blockXS {GetBlockXS(file_path)};

  if (comp_file_xs) {
    comp_file_xs->Reset();
  }

  int64_t nread = 0;
  off_t offset = 0;
  const auto open_ts = std::chrono::system_clock::now();

  do {
    nread = io->fileRead(offset, mBuffer, mBufferSize);

    if (nread < 0) {
      if (blockXS) {
        blockXS->CloseMap();
      }

      eos_err("msg=\"failed read\" offset=%llu path=%s", offset,
              file_path.c_str());
      return false;
    }

    if (nread) {
      if (nread > mBufferSize) {
        eos_err("msg=\"read returned more than the buffer size\" buff_sz=%llu "
                "nread=%lli\"", mBufferSize, nread);

        if (blockXS) {
          blockXS->CloseMap();
        }

        return false;
      }

      if (blockXS && (blockxs_err == false)) {
        if (!blockXS->CheckBlockSum(offset, mBuffer, nread)) {
          blockxs_err = true;
        }
      }

      if (comp_file_xs) {
        comp_file_xs->Add(mBuffer, nread, offset);
      }

      offset += nread;
      EnforceAndAdjustScanRate(offset, open_ts, scan_rate);
    }
  } while (nread == mBufferSize);

  scan_size = (unsigned long long) offset;
  const auto close_ts = std::chrono::system_clock().now();
  auto tx_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>
                        (close_ts - open_ts).count();
  eos_static_debug("path=%s size(bytes)=%llu scan_duration_ms=%llu rate(MB/s)=%.02f",
                   file_path.c_str(), scan_size, tx_duration_ms,
                   (((1.0 * offset) / (1024 * 1024)) * 1000) / tx_duration_ms)

  if (comp_file_xs) {
    comp_file_xs->Finalize();
  }

  // Check file checksum only for replica layouts
  if (comp_file_xs) {
    scan_xs_hex = comp_file_xs->GetHexChecksum();

    if (!comp_file_xs->Compare(xs_val)) {
      auto exp_file_xs = eos::fst::ChecksumPlugins::GetXsObj(xs_type);
      exp_file_xs->SetBinChecksum(xs_val, xs_len);
      LogMsg(LOG_ERR, "msg=\"file checksum error\" expected_file_xs=%s "
             "computed_file_xs=%s scan_size=%llu path=%s",
             exp_file_xs->GetHexChecksum(), comp_file_xs->GetHexChecksum(),
             scan_size, file_path.c_str());
      ++mNumCorruptedFiles;
      filexs_err = true;
    }
  }

  // Check block checksum
  if (blockxs_err) {
    LogMsg(LOG_ERR, "msg=\"corrupted block checksum\" path=%s "
           "blockxs_path=%s.xsmap", file_path.c_str(), file_path.c_str());

    if (mBgThread) {
      syslog(LOG_ERR, "corrupted block checksum path=%s blockxs_path=%s.xsmap\n",
             file_path.c_str(), file_path.c_str());
    }
  }

  if (blockXS) {
    blockXS->CloseMap();
  }

  ++mNumScannedFiles;
  return true;
}

//------------------------------------------------------------------------------
// Enforce the scan rate by throttling the current thread and also adjust it
// depending on the IO load on the mountpoint
//------------------------------------------------------------------------------
void
ScanDir::EnforceAndAdjustScanRate(const off_t offset,
                                  const std::chrono::time_point
                                  <std::chrono::system_clock> open_ts,
                                  int& scan_rate)
{
  using namespace std::chrono;

  if (scan_rate && mFstLoad) {
    const auto now_ts = std::chrono::system_clock::now();
    uint64_t scan_duration_msec =
      duration_cast<milliseconds>(now_ts - open_ts).count();
    uint64_t expect_duration_msec =
      (uint64_t)((1.0 * offset) / (scan_rate * 1024 * 1024)) * 1000;

    if (expect_duration_msec > scan_duration_msec) {
      std::this_thread::sleep_for(milliseconds(expect_duration_msec -
                                  scan_duration_msec));
    }

    // Adjust the rate according to the load information
    double load = mFstLoad->GetDiskRate(mDirPath.c_str(), "millisIO") / 1000.0;

    if (load > 0.7) {
      // Adjust the scan_rate which is in MB/s but no lower then 5 MB/s
      if (scan_rate > 5) {
        scan_rate = 0.9 * scan_rate;
      }
    } else {
      scan_rate = mRateBandwidth.load(std::memory_order_relaxed);
    }
  }
}

//------------------------------------------------------------------------------
// Get timestamp smeared +/-20% of mEntryIntervalSec around the current
// timestamp value
//------------------------------------------------------------------------------
std::string
ScanDir::GetTimestampSmearedSec() const
{
  using namespace std::chrono;
  uint64_t entry_interval_sec = mEntryIntervalSec.load(std::memory_order_relaxed);

  int64_t smearing =
    (int64_t)(0.2 * 2 * entry_interval_sec * random() / RAND_MAX) -
    (int64_t)(0.2 * entry_interval_sec);
  uint64_t ts_sec;

  if (mClock.IsFake()) {
    ts_sec = duration_cast<seconds>(mClock.getTime().time_since_epoch()).count();
  } else {
    ts_sec = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
  }

  // Avoid underflow when using the steady_clock for testing
  if ((uint64_t)std::abs(smearing) < ts_sec) {
    ts_sec += smearing;
  }

  return std::to_string(ts_sec);
}

EOSFSTNAMESPACE_END
