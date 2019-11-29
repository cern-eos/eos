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
#include "fst/Config.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/FmdDbMap.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/io/FileIoPluginCommon.hh"
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

//------------------------------------------------------------------------------
// We're missing ioprio.h and gettid
//------------------------------------------------------------------------------
static int
ioprio_set(int which, int who, int ioprio)
{
#ifdef __APPLE__
  return 0;
#else
  return syscall(SYS_ioprio_set, which, who, ioprio);
#endif
}

/*
 * Gives us 8 prio classes with 13-bits of data for each class
 */
#define IOPRIO_BITS             (16)
#define IOPRIO_CLASS_SHIFT      (13)
#define IOPRIO_PRIO_MASK        ((1UL << IOPRIO_CLASS_SHIFT) - 1)

#define IOPRIO_PRIO_CLASS(mask) ((mask) >> IOPRIO_CLASS_SHIFT)
#define IOPRIO_PRIO_DATA(mask)  ((mask) & IOPRIO_PRIO_MASK)
#define IOPRIO_PRIO_VALUE(class, data)  (((class) << IOPRIO_CLASS_SHIFT) | data)

#define ioprio_valid(mask)      (IOPRIO_PRIO_CLASS((mask)) != IOPRIO_CLASS_NONE)

/*
 * These are the io priority groups as implemented by CFQ. RT is the realtime
 * class, it always gets premium service. BE is the best-effort scheduling
 * class, the default for any process. IDLE is the idle scheduling class, it
 * is only served when no one else is using the disk.
 */

enum {
  IOPRIO_CLASS_NONE,
  IOPRIO_CLASS_RT,
  IOPRIO_CLASS_BE,
  IOPRIO_CLASS_IDLE,
};

/*
 * 8 best effort priority levels are supported
 */
#define IOPRIO_BE_NR (8)

enum {
  IOPRIO_WHO_PROCESS = 1,
  IOPRIO_WHO_PGRP,
  IOPRIO_WHO_USER,
};

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ScanDir::ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
                 eos::fst::Load* fstload, bool bgthread, long int testinterval,
                 int ratebandwidth, bool setchecksum, bool fake_clock) :
  mFstLoad(fstload), mFsId(fsid), mDirPath(dirpath),
  mRescanIntervalSec(testinterval), mRateBandwidth(ratebandwidth),
  mNumScannedFiles(0), mNumCorruptedFiles(0), mNumHWCorruptedFiles(0),
  mTotalScanSize(0), mNumTotalFiles(0),  mNumSkippedFiles(0),
  mSetChecksum(setchecksum), mBuffer(nullptr), mBufferSize(0),
  mBgThread(bgthread), mForcedScan(false), mClock(fake_clock)
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
    mThread.reset(&ScanDir::Run, this);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ScanDir::~ScanDir()
{
  if (mBgThread) {
    mThread.join();
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

  if (key == "scaninterval") {
    mRescanIntervalSec = value;
  } else if (key == "scanrate") {
    mRateBandwidth = (int) value;
  }
}

//------------------------------------------------------------------------------
// Infinite loop doing the scanning
//------------------------------------------------------------------------------
void
ScanDir::Run(ThreadAssistant& assistant) noexcept
{
  using namespace std::chrono;

  if (mBgThread) {
    int retc = 0;
    pid_t tid = (pid_t) syscall(SYS_gettid);

    if ((retc = ioprio_set(IOPRIO_WHO_PROCESS, tid,
                           IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7)))) {
      eos_err("msg=\"cannot set io priority to lowest best effort\" "
              "retc=%d errno=%d\n", retc, errno);
    } else {
      eos_notice("msg=\"set io priority to 7(lowest best-effort)\" pid=%u", tid);
    }
  }

  UpdateForcedScan();

  if (mBgThread && !mForcedScan) {
    // Get a random smearing and avoid that all start at the same time! 0-4 hours
    size_t sleeper = (4 * 3600.0 * random() / RAND_MAX);
    assistant.wait_for(seconds(sleeper));
  }

  while (!assistant.terminationRequested()) {
    mNumScannedFiles =  mTotalScanSize =  mNumCorruptedFiles =
                                            mNumHWCorruptedFiles =  mNumTotalFiles = mNumSkippedFiles = 0;
    UpdateForcedScan();
    auto start_ts = mClock.getTime();
    // Do the heavy work
    ScanSubtree(assistant);
    auto finish_ts = mClock.getTime();
    seconds duration = duration_cast<seconds>(finish_ts - start_ts);
    std::string log_msg =
      SSTR("[ScanDir] Directory: " << mDirPath << " files=" << mNumTotalFiles
           << " scanduration=" << duration.count() << " [s] scansize="
           << mTotalScanSize << " [Bytes] [ " << (mTotalScanSize / 1e6)
           << " MB ] scannedfiles=" << mNumScannedFiles << " corruptedfiles="
           << mNumCorruptedFiles << " hwcorrupted=" << mNumHWCorruptedFiles
           << " skippedfiles=" << mNumSkippedFiles);

    if (mBgThread) {
      syslog(LOG_ERR, "%s\n", log_msg.c_str());
      eos_notice("%s", log_msg.c_str());
    } else {
      fprintf(stderr, "%s\n", log_msg.c_str());
    }

    if (mBgThread) {
      if (!mForcedScan) {
        // Run again after 4 hours
        assistant.wait_for(std::chrono::hours(4));
      } else {
        // Call the ghost entry clean-up function
        eos_notice("msg=\"cleaning ghost entries\" dir=%s fsid=%d",
                   mDirPath.c_str(), mFsId);
        gFmdDbMapHandler.RemoveGhostEntries(mDirPath.c_str(), mFsId);
        assistant.wait_for(std::chrono::seconds(60));
      }
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
  unsigned long long scan_size {0ull};
  std::string xs_type, xs_stamp, lfn, previous_xs_err;
  char xs_val[SHA_DIGEST_LENGTH];
  memset(xs_val, 0, sizeof(xs_val));
  size_t xs_len = SHA_DIGEST_LENGTH;
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
  eos::common::Path cPath(fpath.c_str());
  eos::common::FileId::fileid_t fid {0ull};

  try {
    fid = std::stoull(cPath.GetName(), 0, 16);
  } catch (...) {
    LogMsg(LOG_ERR, "msg=\"failed to extract fid\" path=%s", fpath.c_str());
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
  io->attrGet("user.eos.checksumtype", xs_type);
  io->attrGet("user.eos.checksum", xs_val, xs_len);
  io->attrGet("user.eos.timestamp", xs_stamp);
  io->attrGet("user.eos.lfn", lfn);
  io->attrGet("user.eos.filecxerror", previous_xs_err);
  bool rescan = DoRescan(xs_stamp);
  // A file which was checked as ok, but got a checksum error
  bool was_healthy = (previous_xs_err == "0");
  // Check if this file has been modified since the last time we scanned it
  time_t scanTime = atoll(xs_stamp.c_str()) / 1000000;
  bool didnt_change = (buf1.st_mtime < scanTime);

  if (rescan || mForcedScan) {
    bool blockcxerror = false;
    bool filecxerror = false;
    bool skip_settime = false;

    if (!ScanFileLoadAware(io, scan_size, xs_type, xs_val, lfn,
                           filecxerror, blockcxerror)) {
      bool reopened = false;
#ifndef _NOOFS

      if (mBgThread) {
        if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
          eos_err("msg=\"file reopened during the scan, ignore checksum error\""
                  " path=%s", fpath.c_str());
          reopened = true;
        }
      }

#endif

      if (!reopened && (!io->fileStat(&buf2)) &&
          (buf1.st_mtime == buf2.st_mtime)) {
        if (filecxerror) {
          if (mBgThread) {
            syslog(LOG_ERR, "corrupted file checksum: localpath=%s lfn=\"%s\"\n",
                   fpath.c_str(), lfn.c_str());
            eos_err("corrupted file checksum: localpath=%s lfn=\"%s\"", fpath.c_str(),
                    lfn.c_str());
          } else {
            fprintf(stderr, "[ScanDir] corrupted  file checksum: localpath=%slfn=\"%s\"\n",
                    fpath.c_str(), lfn.c_str());
          }

          if (was_healthy && didnt_change) {
            ++mNumHWCorruptedFiles;

            if (mBgThread) {
              syslog(LOG_ERR, "HW corrupted file found: localpath=%s lfn=\"%s\" \n",
                     fpath.c_str(), lfn.c_str());
            } else {
              fprintf(stderr, "HW corrupted file found: localpath=%s lfn=\"%s\" \n",
                      fpath.c_str(), lfn.c_str());
            }
          }
        }
      } else {
        // If the file was changed in the meanwhile or is reopened for update
        // we leave it for a later scan
        blockcxerror = false;
        filecxerror = false;
        skip_settime = true;
        LogMsg(LOG_ERR, "msg=\"[ScanDir] file modified during scan, ignore "
               "checksum error\" path=%s", fpath.c_str());
      }
    }

    // Collect statistics
    if (rescan) {
      mTotalScanSize += scan_size;
      bool failedtoset = false;

      if (!skip_settime) {
        if (io->attrSet("user.eos.timestamp", GetTimestampSmeared())) {
          failedtoset |= true;
        }
      }

      if ((io->attrSet("user.eos.filecxerror", filecxerror ? "1" : "0")) ||
          (io->attrSet("user.eos.blockcxerror", blockcxerror ? "1" : "0"))) {
        failedtoset |= true;
      }

      if (failedtoset) {
        LogMsg(LOG_ERR, "msg=\"failed to set xattrs\" path=%s", fpath.c_str());
      }
    }

#ifndef _NOOFS

    if (mBgThread) {
      UpdateLocalDB(fpath, fid, filecxerror, blockcxerror);
    }

#endif
  } else {
    ++mNumSkippedFiles;
  }

  io->fileClose();
}

//------------------------------------------------------------------------------
// Update the local database based on the checksum information
//------------------------------------------------------------------------------
bool
ScanDir::UpdateLocalDB(const std::string& file_path,
                       eos::common::FileId::fileid_t fid,
                       bool filexs_error, bool blockxs_error)
{
  if (!filexs_error && !blockxs_error && !mForcedScan) {
    return true;
  }

  std::string manager = eos::fst::Config::gConfig.GetManager();

  if (manager.empty()) {
    eos_err("msg=\"no manager hostname info available\"");
    return false;
  }

  // Check if we have this file in the local DB, if not, we resync first
  // the disk and then the MGM meta data
  bool orphaned = false;
  std::unique_ptr<FmdHelper> fmd {gFmdDbMapHandler.LocalGetFmd(fid, mFsId, 0, 0,
                                  false,  true)};

  if (fmd) {
    // Real orphans get rechecked
    if (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kOrphan) {
      orphaned = true;
    }

    // Unregistered replicas get rechecked
    if (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered) {
      orphaned = true;
    }
  }

  if (filexs_error || blockxs_error || !fmd || orphaned) {
    eos_notice("msg=\"resyncing from disk\" fsid=%d fxid=%08llx", mFsId, fid);
    // ask the meta data handling class to update the error flags for this file
    gFmdDbMapHandler.ResyncDisk(file_path.c_str(), mFsId, false);
    eos_notice("msg=\"resyncing from mgm\" fsid=%d fxid=%08llx", mFsId, fid);
    bool resynced = false;
    resynced = gFmdDbMapHandler.ResyncMgm(mFsId, fid, manager.c_str());
    fmd.reset(gFmdDbMapHandler.LocalGetFmd(fid, mFsId, 0, 0, 0, false, true));

    if (resynced && fmd) {
      if ((fmd->mProtoFmd.layouterror() ==  eos::common::LayoutId::kOrphan) ||
          ((!(fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kReplicaWrong))
           && (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered))) {
        char oname[4096];
        snprintf(oname, sizeof(oname), "%s/.eosorphans/%08llx",
                 mDirPath.c_str(), (unsigned long long) fid);
        // Store the original path name as an extended attribute in case ...
        std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(file_path));
        io->attrSet("user.eos.orphaned", file_path.c_str());

        // If orphan move it into the orphaned directory
        if (!rename(file_path.c_str(), oname)) {
          eos_warning("msg=\"orphaned/unregistered quarantined\" "
                      "fst-path=%s orphan-path=%s", file_path.c_str(), oname);
        } else {
          eos_err("msg=\"failed to quarantine orphaned/unregistered\" "
                  "fst-path=%s orphan-path=%s", file_path.c_str(), oname);
        }

        gFmdDbMapHandler.LocalDeleteFmd(fid, mFsId);
      }
    }

    // Call the autorepair method on the MGM - but not for orphaned or
    // unregistered files. If MGM autorepair is disabled then it doesn't do
    // anything
    bool do_autorepair = false;

    if (orphaned == false) {
      if (fmd) {
        if ((fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered)
            == false) {
          do_autorepair = true;
        }
      } else {
        // The fmd could be null since LocalGetFmd returns null in case of a
        // checksum error
        do_autorepair = true;
      }
    }

    if (do_autorepair) {
      gFmdDbMapHandler.CallAutoRepair(manager.c_str(), fid);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Get block checksum object for the given file. First we need to check if
// there is a block checksum file (.xsmap) correspnding to the given raw
// file.
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
        eos::fst::ChecksumPlugins::GetChecksumObjectPtr(layoutid, true);

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
ScanDir::DoRescan(const std::string& timestamp_us) const
{
  using namespace std::chrono;

  if (!timestamp_us.compare("")) {
    if (mRescanIntervalSec == 0ull) {
      return false;
    } else {
      // Check the first time if scanner is not completely disabled
      return true;
    }
  }

  steady_clock::time_point old_ts(microseconds(std::stoull(timestamp_us)));
  steady_clock::time_point now_ts(mClock.getTime());
  uint64_t elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();

  if (elapsed_sec < mRescanIntervalSec) {
    return false;
  } else {
    if (mRescanIntervalSec) {
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
                           const std::string& xs_type, const char* xs_val,
                           const std::string& lfn, bool& filexs_err,
                           bool& blockxs_err)
{
  scan_size = 0ull;
  filexs_err = blockxs_err = false;
  bool ret = false;
  int scan_rate = mRateBandwidth;
  struct stat info;

  if (io->fileStat(&info)) {
    return false;
  }

  struct timezone tz;

  struct timeval opentime;

  gettimeofday(&opentime, &tz);

  std::string file_path = io->GetPath();

  auto lid = LayoutId::GetId(LayoutId::kPlain,
                             LayoutId::GetChecksumFromString(xs_type));

  std::unique_ptr<eos::fst::CheckSum> normalXS
  {eos::fst::ChecksumPlugins::GetChecksumObjectPtr(lid)};

  std::unique_ptr<eos::fst::CheckSum> blockXS {GetBlockXS(file_path)};

  // If no checksum then there is nothing to check ...
  if (!normalXS && !blockXS) {
    return false;
  }

  if (normalXS) {
    normalXS->Reset();
  }

  size_t nread = 0;
  off_t offset = 0;

  // Read whole file
  do {
    nread = io->fileRead(offset, mBuffer, mBufferSize);

    if (nread < 0) {
      if (blockXS) {
        blockXS->CloseMap();
      }

      return false;
    }

    if (nread) {
      if (blockXS && (blockxs_err == false)) {
        if (!blockXS->CheckBlockSum(offset, mBuffer, nread)) {
          blockxs_err = true;
        }
      }

      if (normalXS) {
        normalXS->Add(mBuffer, nread, offset);
      }

      offset += nread;
      EnforceAndAdjustScanRate(offset, opentime, scan_rate);
    }
  } while (nread == mBufferSize);

  scan_size = (unsigned long long) offset;

  if (normalXS) {
    normalXS->Finalize();
  }

  // Check file checksum only for replica layouts
  if (normalXS && (!normalXS->Compare(xs_val))) {
    LogMsg(LOG_ERR, "msg=\file checksum error\" computed_xs=%s scan_size=%llu",
           normalXS->GetHexChecksum(), scan_size);

    if (!mBgThread && mSetChecksum) {
      int checksumlen = 0;
      normalXS->GetBinChecksum(checksumlen);

      if (io->attrSet("user.eos.checksum",
                      normalXS->GetBinChecksum(checksumlen), checksumlen) ||
          io->attrSet("user.eos.filecxerror", "0")) {
        fprintf(stderr, "error: failed to reset existing checksum \n");
      } else {
        fprintf(stdout, "success: reset checksum of %s to %s\n",
                file_path.c_str(), normalXS->GetHexChecksum());
      }
    }

    ++mNumCorruptedFiles;
    ret = false;
    filexs_err = true;
  } else {
    ret = true;
  }

  // Check block checksum
  if (blockxs_err) {
    LogMsg(LOG_ERR, "msg=\"corrupted block checksum\" local_path=%s, "
           "blockxs_path=%s.xsmap lfn=%s", file_path.c_str(), file_path.c_str(),
           lfn.c_str());

    if (mBgThread) {
      syslog(LOG_ERR, "corrupted block checksum: localpath=%s "
             "blockxspath=%s.xsmap lfn=%s\n", file_path.c_str(),
             file_path.c_str(), lfn.c_str());
    }

    ret &= false;
  } else {
    ret &= true;
  }

  ++mNumScannedFiles;

  if (blockXS) {
    blockXS->CloseMap();
  }

  return ret;
}

//------------------------------------------------------------------------------
// Enforce the scan rate by throttling the current thread and also adjust it
// depending on the IO load on the mountpoint
//------------------------------------------------------------------------------
void
ScanDir::EnforceAndAdjustScanRate(const off_t offset,
                                  const struct timeval& open_ts, int& scan_rate)
{
  if (scan_rate && mFstLoad) {
    struct timezone tz;
    struct timeval now_ts;
    gettimeofday(&now_ts, &tz);
    float scan_duration = (((now_ts.tv_sec - open_ts.tv_sec) * 1000.0) +
                           ((now_ts.tv_usec - open_ts.tv_usec) / 1000.0));
    float expect_duration = (1.0 * offset / scan_rate) / 1000.0;

    if (expect_duration > scan_duration) {
      std::this_thread::sleep_for
      (std::chrono::milliseconds((int)(expect_duration - scan_duration)));
    }

    // Adjust the rate according to the load information
    double load = mFstLoad->GetDiskRate(mDirPath.c_str(), "millisIO") / 1000.0;

    if (load > 0.7) {
      // Adjust the scan_rate
      if (scan_rate > 5) {
        scan_rate = 0.9 * scan_rate;
      }
    } else {
      scan_rate = mRateBandwidth;
    }
  }
}

//------------------------------------------------------------------------------
// Update the forced scan flag based on the existence of the .eosscan file
// on the FST mountpoint
//------------------------------------------------------------------------------
void
ScanDir::UpdateForcedScan()
{
  struct stat buf;
  std::string forcedrun = mDirPath.c_str();
  forcedrun += "/.eosscan";

  if (!stat(forcedrun.c_str(), &buf)) {
    if (!mForcedScan) {
      mForcedScan = true;
      LogMsg(LOG_NOTICE, "%s", "msg=\"scanner is in forced mode\"");
    }
  } else {
    if (mForcedScan) {
      mForcedScan = false;
      LogMsg(LOG_NOTICE, "%s", "msg=\"scanner is back to non-forced mode\"");
    }
  }
}

/*----------------------------------------------------------------------------*/
std::string
ScanDir::GetTimestampSmeared()
{
  char buffer[65536];
  size_t size = sizeof(buffer) - 1;
  long long timestamp;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  timestamp = tv.tv_sec * 1000000 + tv.tv_usec;
  // smear +- 20% of mRescanIntervalSec around the value
  long int smearing = (long int)((0.2 * 2 * mRescanIntervalSec * random() /
                                  RAND_MAX))
                      - ((long int)(0.2 * mRescanIntervalSec));
  snprintf(buffer, size, "%lli", timestamp + smearing * 1000000);
  return std::string(buffer);
}

EOSFSTNAMESPACE_END
