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
                 eos::fst::Load* fstload, bool bgthread,
                 long int file_rescan_interval, int ratebandwidth,
                 bool setchecksum, bool fake_clock) :
  mFstLoad(fstload), mFsId(fsid), mDirPath(dirpath),
  mRescanIntervalSec(file_rescan_interval), mRerunIntervalSec(4 * 3600),
  mRateBandwidth(ratebandwidth), mNumScannedFiles(0), mNumCorruptedFiles(0),
  mNumHWCorruptedFiles(0),  mTotalScanSize(0), mNumTotalFiles(0),
  mNumSkippedFiles(0), mSetChecksum(setchecksum), mBuffer(nullptr),
  mBufferSize(0), mBgThread(bgthread), mFakeClock(fake_clock),
  mClock(mFakeClock)
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
    mBufferSize = 256*1024;
    mBuffer = (char*) malloc(mBufferSize);
    fprintf(stderr, "error: OS does not provide alignment or path does not exist\n");
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

  if (key == eos::common::SCAN_RATE_NAME) {
    mRateBandwidth = (int) value;
  } else if (key == eos::common::SCAN_INTERVAL_NAME) {
    mRescanIntervalSec = value;
  } else if (key == eos::common::SCAN_RERUNINTERVAL_NAME) {
    mRerunIntervalSec = value;
    mThread.join();
    mThread.reset(&ScanDir::Run, this);
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

  if (mBgThread) {
    // Get a random smearing and avoid that all start at the same time! 0-4 hours
    size_t sleeper = (1.0 * mRerunIntervalSec * random() / RAND_MAX);
    assistant.wait_for(seconds(sleeper));
  }

  while (!assistant.terminationRequested()) {
    mNumScannedFiles =  mTotalScanSize =  mNumCorruptedFiles = 0;
    mNumHWCorruptedFiles =  mNumTotalFiles = mNumSkippedFiles = 0;
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
      // Run again after (default) 4 hours
      assistant.wait_for(std::chrono::seconds(mRerunIntervalSec));
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
  unsigned long long scan_size {0ull};
  std::string xs_stamp_sec, lfn, previous_xs_err;
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
  io->attrGet("user.eos.lfn", lfn);
  io->attrGet("user.eos.filecxerror", previous_xs_err);
  io->attrGet("user.eos.timestamp", xs_stamp_sec);

  // Handle the old format in microseconds, truncate to seconds
  if (xs_stamp_sec.length() > 10) {
    xs_stamp_sec.erase(10);
  }

  bool was_healthy = (previous_xs_err == "0");
  // Check if this file has been modified since the last time we scanned it
  bool didnt_change = (buf1.st_mtime < atoll(xs_stamp_sec.c_str()));

  if (DoRescan(xs_stamp_sec)) {
    bool blockxs_err = false;
    bool filexs_err = false;
    bool skip_settime = false;

    if (!ScanFileLoadAware(io, scan_size, lfn, filexs_err, blockxs_err)) {
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

      // If not reopened and not modified then account any errors
      if (!reopened &&
          (!io->fileStat(&buf2)) && (buf1.st_mtime == buf2.st_mtime)) {
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
      } else {
        // If the file was changed in the meanwhile or is reopened for update
        // we leave it for a later scan
        blockxs_err = false;
        filexs_err = false;
        skip_settime = true;
        LogMsg(LOG_ERR, "msg=\"[ScanDir] skip file modified during scan path=%s",
               fpath.c_str());
      }
    }

    // Collect statistics
    mTotalScanSize += scan_size;
    bool failed_set = false;

    if (!skip_settime) {
      if (io->attrSet("user.eos.timestamp", GetTimestampSmearedSec())) {
        failed_set |= true;
      }
    }

    if ((io->attrSet("user.eos.filecxerror", filexs_err ? "1" : "0")) ||
        (io->attrSet("user.eos.blockcxerror", blockxs_err ? "1" : "0"))) {
      failed_set |= true;
    }

    if (failed_set) {
      LogMsg(LOG_ERR, "msg=\"failed to set xattrs\" path=%s", fpath.c_str());
    }

#ifndef _NOOFS

    if (mBgThread) {
      gFmdDbMapHandler.UpdateWithScanInfo(mFsId, mDirPath, fpath, filexs_err,
                                          blockxs_err);
    }

#endif
  } else {
    ++mNumSkippedFiles;
  }
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
ScanDir::DoRescan(const std::string& timestamp_sec) const
{
  using namespace std::chrono;

  if (!timestamp_sec.compare("")) {
    if (mRescanIntervalSec == 0ull) {
      return false;
    } else {
      // Check the first time if scanner is not completely disabled
      return true;
    }
  }

  uint64_t elapsed_sec {0ull};

  // Used only during testing
  if (mFakeClock) {
    steady_clock::time_point old_ts(seconds(std::stoull(timestamp_sec)));
    steady_clock::time_point now_ts(mClock.getTime());
    elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();
  } else {
    system_clock::time_point old_ts(seconds(std::stoull(timestamp_sec)));
    system_clock::time_point now_ts(system_clock::now());
    elapsed_sec = duration_cast<seconds>(now_ts - old_ts).count();
  }

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
                           const std::string& lfn, bool& filexs_err,
                           bool& blockxs_err)
{
  scan_size = 0ull;
  filexs_err = blockxs_err = false;
  int scan_rate = mRateBandwidth;
  struct stat info;

  if (io->fileStat(&info)) {
    return false;
  }

  std::string file_path = io->GetPath();
  // Get checksum type and value
  std::string xs_type;
  char xs_val[SHA_DIGEST_LENGTH];
  memset(xs_val, 0, sizeof(xs_val));
  size_t xs_len = SHA_DIGEST_LENGTH;
  io->attrGet("user.eos.checksumtype", xs_type);
  io->attrGet("user.eos.checksum", xs_val, xs_len);
  auto lid = LayoutId::GetId(LayoutId::kPlain,
                             LayoutId::GetChecksumFromString(xs_type));
  std::unique_ptr<eos::fst::CheckSum> normalXS
  {eos::fst::ChecksumPlugins::GetChecksumObjectPtr(lid)};
  std::unique_ptr<eos::fst::CheckSum> blockXS {GetBlockXS(file_path)};

  // If no checksum then there is nothing to check
  if (!normalXS && !blockXS) {
    return false;
  }

  if (normalXS) {
    normalXS->Reset();
  }

  size_t nread = 0;
  off_t offset = 0;
  uint64_t open_ts_sec = std::chrono::duration_cast<std::chrono::seconds>
                         (mClock.getTime().time_since_epoch()).count();

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
      EnforceAndAdjustScanRate(offset, open_ts_sec, scan_rate);
    }
  } while (nread == mBufferSize);

  scan_size = (unsigned long long) offset;

  if (normalXS) {
    normalXS->Finalize();
  }

  bool ret = true;

  // Check file checksum only for replica layouts
  if (normalXS && (!normalXS->Compare(xs_val))) {
    LogMsg(LOG_ERR, "msg=\file checksum error\" expected_xs=%s computed_xs=%s "
           "scan_size=%llu", xs_val, normalXS->GetHexChecksum(), scan_size);

    if (!mBgThread && mSetChecksum) {
      int checksumlen = 0;
      normalXS->GetBinChecksum(checksumlen);

      if (io->attrSet("user.eos.checksum",
                      normalXS->GetBinChecksum(checksumlen), checksumlen) ||
          io->attrSet("user.eos.checksumhex", normalXS->GetHexChecksum()) ||
          io->attrSet("user.eos.filecxerror", "0")) {
        fprintf(stderr, "error: failed to reset existing checksum \n");
      } else {
        fprintf(stdout, "success: reset checksum of %s to %s\n",
                file_path.c_str(), normalXS->GetHexChecksum());
      }
    }

    ++mNumCorruptedFiles;
    filexs_err = true;
    ret = false;
  }

  // Check block checksum
  if (blockxs_err) {
    LogMsg(LOG_ERR, "msg=\"corrupted block checksum\" path=%s "
           "blockxs_path=%s.xsmap lfn=%s", file_path.c_str(), file_path.c_str(),
           lfn.c_str());

    if (mBgThread) {
      syslog(LOG_ERR, "corrupted block checksum path=%s blockxs_path=%s.xsmap "
             "lfn=%s\n", file_path.c_str(), file_path.c_str(), lfn.c_str());
    }

    ret &= false;
  }

  if (blockXS) {
    blockXS->CloseMap();
  }

  ++mNumScannedFiles;
  return ret;
}

//------------------------------------------------------------------------------
// Enforce the scan rate by throttling the current thread and also adjust it
// depending on the IO load on the mountpoint
//------------------------------------------------------------------------------
void
ScanDir::EnforceAndAdjustScanRate(const off_t offset,
                                  const uint64_t open_ts_sec,
                                  int& scan_rate)
{
  using namespace std::chrono;

  if (scan_rate && mFstLoad) {
    uint64_t now_ts_sec = duration_cast<seconds>
                          (mClock.getTime().time_since_epoch()).count();
    uint64_t scan_duration = now_ts_sec - open_ts_sec;
    uint64_t expect_duration = (uint64_t)((1.0 * offset / scan_rate) / 1000.0);

    if (expect_duration > scan_duration) {
      std::this_thread::sleep_for(milliseconds(expect_duration - scan_duration));
    }

    // Adjust the rate according to the load information
    double load = mFstLoad->GetDiskRate(mDirPath.c_str(), "millisIO") / 1000.0;

    if (load > 0.7) {
      // Adjust the scan_rate which is in MB/s but no lower then 5 MB/s
      if (scan_rate > 5) {
        scan_rate = 0.9 * scan_rate;
      }
    } else {
      scan_rate = mRateBandwidth;
    }
  }
}

//------------------------------------------------------------------------------
// Get timestamp smeared +/-20% of mRescanIntervalSec around the current
// timestamp value
//------------------------------------------------------------------------------
std::string
ScanDir::GetTimestampSmearedSec() const
{
  using namespace std::chrono;
  int64_t smearing =
    (int64_t)(0.2 * 2 * mRescanIntervalSec.load() * random() / RAND_MAX) -
    (int64_t)(0.2 * mRescanIntervalSec.load());
  uint64_t ts_sec;

  if (mFakeClock) {
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
