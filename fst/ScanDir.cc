// ----------------------------------------------------------------------
// File: ScanDir.cc
// Author: Elvin Sindrilaru - CERN
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

#include "common/FileId.hh"
#include "common/Path.hh"
#include "fst/ScanDir.hh"
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

//static int ioprio_get(int which, int who)
//{
//  return syscall(SYS_ioprio_get, which, who);
//}

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

/*----------------------------------------------------------------------------*/
void*
ScanDir::StaticThreadProc(void* arg)
{
  return reinterpret_cast<ScanDir*>(arg)->ThreadProc();
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ScanDir::ScanDir(const char* dirpath, eos::common::FileSystem::fsid_t fsid,
                 eos::fst::Load* fstload, bool bgthread, long int testinterval,
                 int ratebandwidth, bool setchecksum) :
  mFstLoad(fstload), mFsId(fsid), mDirPath(dirpath),
  mRescanIntervalSec(testinterval), mRateBandwidth(ratebandwidth),
  mScanDuration(0), mNumScannedFiles(0), mNumCorruptedFiles(0),
  mNumHWCorruptedFiles(0), mTotalScanSize(0), mNumTotalFiles(0),
  mNumSkippedFiles(0), mSetChecksum(setchecksum), mBuffer(nullptr),
  mBufferSize(0), mForcedScan(false)
{
  thread = 0;
  bgThread = bgthread;
  size_t alignment = pathconf((mDirPath[0] != '/') ? "/" : mDirPath.c_str(),
                              _PC_REC_XFER_ALIGN);

  if (alignment > 0) {
    mBufferSize = 256 * alignment;

    if (posix_memalign((void**) &mBuffer, alignment, mBufferSize)) {
      fprintf(stderr, "error: error calling posix_memaling on dirpath=%s. \n",
              mDirPath.c_str());
      std::abort();
    }
  } else {
    fprintf(stderr, "error: OS does not provide alignment\n");
    std::abort();
  }

  if (bgthread) {
    openlog("scandir", LOG_PID | LOG_NDELAY, LOG_USER);
    XrdSysThread::Run(&thread, ScanDir::StaticThreadProc, static_cast<void*>(this),
                      XRDSYSTHREAD_HOLD, "ScanDir Thread");
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
ScanDir::~ScanDir()
{
  if ((bgThread && thread)) {
    XrdSysThread::Cancel(thread);
    XrdSysThread::Join(thread, NULL);
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

/*----------------------------------------------------------------------------*/
void
scandir_cleanup_handle(void* arg)
{
  FileIo::FtsHandle* handle = static_cast<FileIo::FtsHandle*>(arg);

  if (handle) {
    delete handle;
  }
}

//------------------------------------------------------------------------------
// Method traversing all the files in the subtree and potentially rescanning
// some of them.
//------------------------------------------------------------------------------
void
ScanDir::ScanFiles()
{
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(mDirPath.c_str()));

  if (!io) {
    if (bgThread) {
      eos_err("msg=\"no IO plug-in available\" url=\"%s\"", mDirPath.c_str());
    } else {
      fprintf(stderr, "error: no IO plug-in available for url=%s\n",
              mDirPath.c_str());
    }

    return;
  }

  FileIo::FtsHandle* handle = io->ftsOpen();

  if (!handle) {
    if (bgThread) {
      eos_err("msg=\"fts_open failed\" dir=%s", mDirPath.c_str());
    } else {
      fprintf(stderr, "error: fts_open failed! \n");
    }

    return;
  }

  pthread_cleanup_push(scandir_cleanup_handle, handle);
  std::string fpath;

  while ((fpath = io->ftsRead(handle)) != "") {
    if (!bgThread) {
      fprintf(stderr, "[ScanDir] processing file %s\n", fpath.c_str());
    }

    CheckFile(fpath.c_str());

    if (bgThread) {
      XrdSysThread::CancelPoint();
    }
  }

  if (io->ftsClose(handle)) {
    if (bgThread) {
      eos_err("fts_close failed");
    } else {
      fprintf(stderr, "error: fts_close failed \n");
    }
  }

  delete handle;
  pthread_cleanup_pop(0);
}

/*----------------------------------------------------------------------------*/
void
ScanDir::CheckFile(const char* filepath)
{
  float scan_duration;
  unsigned long layoutid = 0;
  unsigned long long scansize;
  std::string filePath, checksumType, checksumStamp, logicalFileName,
      previousFileCxError;
  char checksumVal[SHA_DIGEST_LENGTH];
  size_t checksumLen;
  filePath = filepath;
  std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(filepath));
  ++mNumTotalFiles;
  // get last modification time
  struct stat buf1;
  struct stat buf2;

  if ((io->fileOpen(0, 0)) || io->fileStat(&buf1)) {
    if (bgThread) {
      eos_err("cannot open/stat %s", filePath.c_str());
    } else {
      fprintf(stderr, "error: cannot open/stat %s\n", filePath.c_str());
    }

    return;
  }

#ifndef _NOOFS

  if (bgThread) {
    eos::common::Path cPath(filePath.c_str());
    eos::common::FileId::fileid_t fid = strtoul(cPath.GetName(), 0, 16);
    // Check if somebody is still writing on that file and skip in that case
    XrdSysMutexHelper wLock(gOFS.OpenFidMutex);

    if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
      syslog(LOG_ERR, "skipping scan w-open file: localpath=%s fsid=%d fid=%08llx\n",
             filePath.c_str(), mFsId, fid);
      eos_warning("skipping scan of w-open file: localpath=%s fsid=%d fid=%08llx",
                  filePath.c_str(), mFsId, fid);
      return;
    }
  }

#endif
  io->attrGet("user.eos.checksumtype", checksumType);
  memset(checksumVal, 0, sizeof(checksumVal));
  checksumLen = SHA_DIGEST_LENGTH;

  if (io->attrGet("user.eos.checksum", checksumVal, checksumLen)) {
    checksumLen = 0;
  }

  io->attrGet("user.eos.timestamp", checksumStamp);
  io->attrGet("user.eos.lfn", logicalFileName);
  io->attrGet("user.eos.filecxerror", previousFileCxError);
  bool rescan = DoRescan(checksumStamp);
  // a file which was checked as ok, but got a checksum error
  bool was_healthy = (previousFileCxError == "0");
  // check if this file has been modified since the last time we scanned it
  bool didnt_change = false;
  time_t scanTime = atoll(checksumStamp.c_str()) / 1000000;

  if (buf1.st_mtime < scanTime) {
    didnt_change = true;
  }

  if (rescan || mForcedScan) {
    bool blockcxerror = false;
    bool filecxerror = false;
    bool skip_settime = false;
    XrdOucString envstring = "eos.layout.checksum=";
    envstring += checksumType.c_str();
    XrdOucEnv env(envstring.c_str());
    unsigned long checksumtype = eos::common::LayoutId::GetChecksumFromEnv(env);
    layoutid = eos::common::LayoutId::GetId(eos::common::LayoutId::kPlain,
                                            checksumtype);

    if (rescan &&
        (!ScanFileLoadAware(io, scansize, scan_duration, checksumVal, layoutid,
                            logicalFileName.c_str(), filecxerror, blockcxerror))) {
      bool reopened = false;
#ifndef _NOOFS

      if (bgThread) {
        eos::common::Path cPath(filePath.c_str());
        eos::common::FileId::fileid_t fid = strtoul(cPath.GetName(), 0, 16);
        // Check if somebody is again writing on that file and skip in that case

        if (gOFS.openedForWriting.isOpen(mFsId, fid)) {
          eos_err("file %s has been reopened for update during the scan ... "
                  "ignoring checksum error", filePath.c_str());
          reopened = true;
        }
      }

#endif

      if ((!io->fileStat(&buf2)) && (buf1.st_mtime == buf2.st_mtime) &&
          !reopened) {
        if (filecxerror) {
          if (bgThread) {
            syslog(LOG_ERR, "corrupted file checksum: localpath=%s lfn=\"%s\" \n",
                   filePath.c_str(), logicalFileName.c_str());
            eos_err("corrupted file checksum: localpath=%s lfn=\"%s\"", filePath.c_str(),
                    logicalFileName.c_str());

            if (was_healthy && didnt_change) {
              syslog(LOG_ERR, "HW corrupted file found: localpath=%s lfn=\"%s\" \n",
                     filePath.c_str(), logicalFileName.c_str());
              ++mNumHWCorruptedFiles;
            }
          } else {
            fprintf(stderr, "[ScanDir] corrupted  file checksum: localpath=%slfn=\"%s\" \n",
                    filePath.c_str(), logicalFileName.c_str());

            if (was_healthy && didnt_change) {
              fprintf(stderr, "HW corrupted file found: localpath=%s lfn=\"%s\" \n",
                      filePath.c_str(), logicalFileName.c_str());
              ++mNumHWCorruptedFiles;
            }
          }
        }
      } else {
        // If the file was changed in the meanwhile or is reopened for update,
        // the checksum might have changed in the meanwhile, we cannot know
        // now and leave it up to a later moment.
        blockcxerror = false;
        filecxerror = false;
        skip_settime = true;

        if (bgThread) {
          eos_err("file %s has been modified during the scan ... ignoring checksum error",
                  filePath.c_str());
        } else {
          fprintf(stderr,
                  "[ScanDir] file %s has been modified during the scan ... "
                  "ignoring checksum error\n", filePath.c_str());
        }
      }
    }

    // Collect statistics
    if (rescan) {
      mScanDuration += scan_duration;
      mTotalScanSize += scansize;
    }

    if (rescan) {
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
        if (bgThread) {
          eos_err("msg=\"failed to set xattrs\" file=%s", filePath.c_str());
        } else {
          fprintf(stderr, "error: [CheckFile] Can not set extended "
                  "attributes to file. \n");
        }
      }
    }

#ifndef _NOOFS

    if (bgThread) {
      if (filecxerror || blockcxerror || mForcedScan) {
        XrdOucString manager = "";
        {
          XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
          manager = eos::fst::Config::gConfig.Manager.c_str();
        }

        if (manager.length()) {
          errno = 0;
          eos::common::Path cPath(filePath.c_str());
          eos::common::FileId::fileid_t fid = strtoul(cPath.GetName(), 0, 16);

          if (fid && !errno) {
            // check if we have this file in the local DB, if not, we
            // resync first the disk and then the mgm meta data
            FmdHelper* fmd = gFmdDbMapHandler.LocalGetFmd(fid, mFsId, 0, 0, false,
                             true);
            bool orphaned = false;

            if (fmd) {
              // real orphanes get rechecked
              if (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kOrphan) {
                orphaned = true;
              }

              // unregistered replicas get rechecked
              if (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered) {
                orphaned = true;
              }

              delete fmd;
              fmd = nullptr;
            }

            if (filecxerror || blockcxerror || !fmd || orphaned) {
              eos_notice("msg=\"resyncing from disk\" fsid=%d fid=%08llx", mFsId, fid);
              // ask the meta data handling class to update the error flags for this file
              gFmdDbMapHandler.ResyncDisk(filePath.c_str(), mFsId, false);
              eos_notice("msg=\"resyncing from mgm\" fsid=%d fid=%08llx", mFsId, fid);
              bool resynced = false;
              resynced = gFmdDbMapHandler.ResyncMgm(mFsId, fid, manager.c_str());
              fmd = gFmdDbMapHandler.LocalGetFmd(fid, mFsId, 0, 0, 0, false, true);

              if (resynced && fmd) {
                if ((fmd->mProtoFmd.layouterror() ==  eos::common::LayoutId::kOrphan) ||
                    ((!(fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kReplicaWrong))
                     && (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered))) {
                  char oname[4096];
                  snprintf(oname, sizeof(oname), "%s/.eosorphans/%08llx",
                           mDirPath.c_str(), (unsigned long long) fid);
                  // store the original path name as an extended attribute in case ...
                  io->attrSet("user.eos.orphaned", filePath.c_str());

                  // if this is an orphaned file - we move it into the orphaned directory
                  if (!rename(filePath.c_str(), oname)) {
                    eos_warning("msg=\"orphaned/unregistered quarantined\" "
                                "fst-path=%s orphan-path=%s", filePath.c_str(),
                                oname);
                  } else {
                    eos_err("msg=\"failed to quarantine orphaned/unregistered"
                            "\" fst-path=%s orphan-path=%s", filePath.c_str(),
                            oname);
                  }

                  // remove the entry from the FMD database
                  gFmdDbMapHandler.LocalDeleteFmd(fid, mFsId);
                }

                delete fmd;
                fmd = nullptr;
              }

              // Call the autorepair method on the MGM - but not for orphaned
              // or unregistered filed. If MGM autorepair is disabled then it
              // doesn't do anything.
              bool do_autorepair = false;

              if (orphaned == false) {
                if (fmd) {
                  if ((fmd->mProtoFmd.layouterror() &
                       eos::common::LayoutId::kUnregistered) == false) {
                    do_autorepair = true;
                  }
                } else {
                  // The fmd could be null since LocalGetFmd returns null in
                  // case of a checksum error
                  do_autorepair = true;
                }
              }

              if (do_autorepair) {
                gFmdDbMapHandler.CallAutoRepair(manager.c_str(), fid);
              }
            }
          }
        }
      }
    }

#endif
  } else {
    ++mNumSkippedFiles;
  }

  io->fileClose();
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
  std::string str_bxs_type, str_bxs_size, logicalFileName;
  std::string filexs_path = file_path + ".xsmap";
  std::unique_ptr<eos::fst::FileIo> io(FileIoPluginHelper::GetIoObject(
                                         filexs_path));
  struct stat info;

  if (!io->fileStat(&info, 0)) {
    io->attrGet("user.eos.blockchecksum", str_bxs_type);
    io->attrGet("user.eos.blocksize", str_bxs_size);
    io->attrGet("user.eos.lfn", logicalFileName);

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
        std::string err_msg = SSTR("msg=\"failed to get checksum object\" "
                                   << "layoutid=" << std::hex << layoutid
                                   << std::dec << "path=" << filexs_path);

        if (bgThread) {
          eos_err("%s", err_msg.c_str());
        } else {
          fprintf(stderr, "%s\n", err_msg.c_str());
        }
      }
    } else {
      std::string err_msg = SSTR("msg=\"file has no blockchecksum xattr\""
                                 << " path=" << filexs_path);

      if (bgThread) {
        eos_err("%s", err_msg.c_str());
      } else {
        fprintf(stderr, "%s\n", err_msg.c_str());
      }
    }
  }

  return nullptr;
}

/*----------------------------------------------------------------------------*/
std::string
ScanDir::GetTimestamp() const
{
  char buffer[65536];
  size_t size = sizeof(buffer) - 1;
  long long timestamp;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  timestamp = tv.tv_sec * 1000000 + tv.tv_usec;
  snprintf(buffer, size, "%lli", timestamp);
  return std::string(buffer);
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
  snprintf(buffer, size, "%lli", timestamp + smearing);
  return std::string(buffer);
}

//------------------------------------------------------------------------------
// Decide if a rescan is needed based on the timestamp provided and the
// configured rescan interval
//------------------------------------------------------------------------------
bool
ScanDir::DoRescan(const std::string& timestamp_us) const
{
  if (!timestamp_us.compare("")) {
    if (mRescanIntervalSec == 0ull) {
      return false;
    } else {
      // Check the first time if scanner is not completely disabled
      return true;
    }
  }

  uint64_t old_ts = std::stoull(timestamp_us);
  uint64_t new_ts = std::stoull(GetTimestamp());

  if (((new_ts - old_ts) / 1000000) < mRescanIntervalSec) {
    return false;
  } else {
    return true;
  }
}

//------------------------------------------------------------------------------
// Infinite loop doing the scanning
//------------------------------------------------------------------------------
void*
ScanDir::ThreadProc(void)
{
  if (bgThread) {
    // set low IO priority
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

  if (bgThread) {
    XrdSysThread::SetCancelOn();
  }

  mForcedScan = false;
  struct stat buf;
  std::string forcedrun = mDirPath.c_str();
  forcedrun += "/.eosscan";

  if (!stat(forcedrun.c_str(), &buf)) {
    mForcedScan = true;
    eos_notice("msg=\"scanner is in forced mode\"");
  } else {
    if (mForcedScan) {
      mForcedScan = false;
      eos_notice("msg=\"scanner is back to non-forced mode\"");
    }
  }

  if (bgThread && !mForcedScan) {
    // Get a random smearing and avoid that all start at the same time!
    // start in the range of 0 to 4 hours
    size_t sleeper = (4 * 3600.0 * random() / RAND_MAX);

    for (size_t s = 0; s < (sleeper); s++) {
      if (bgThread) {
        XrdSysThread::CancelPoint();
      }

      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  do {
    struct timezone tz;
    struct timeval tv_start, tv_end;
    struct stat buf;

    if (!stat(forcedrun.c_str(), &buf)) {
      if (!mForcedScan) {
        mForcedScan = true;
        eos_notice("msg=\"scanner is in forced mode\"");
      }
    } else {
      if (mForcedScan) {
        mForcedScan = false;
        eos_notice("msg=\"scanner is back to non-forced mode\"");
      }
    }

    mNumScannedFiles = 0;
    mTotalScanSize = 0;
    mNumCorruptedFiles = 0;
    mNumHWCorruptedFiles = 0;
    mNumTotalFiles = 0;
    mNumSkippedFiles = 0;
    gettimeofday(&tv_start, &tz);
    // Do the heavy work
    ScanFiles();
    gettimeofday(&tv_end, &tz);
    mScanDuration = ((tv_end.tv_sec - tv_start.tv_sec) * 1000.0) + ((
                      tv_end.tv_usec - tv_start.tv_usec) / 1000.0);

    if (bgThread) {
      syslog(LOG_ERR,
             "Directory: %s, files=%li scanduration=%.02f [s] scansize=%lli "
             "[Bytes] [ %lli MB ] scannedfiles=%li  corruptedfiles=%li "
             "hwcorrupted=%li skippedfiles=%li\n",
             mDirPath.c_str(), mNumTotalFiles, (mScanDuration / 1000.0),
             mTotalScanSize, ((mTotalScanSize / 1000) / 1000), mNumScannedFiles,
             mNumCorruptedFiles, mNumHWCorruptedFiles, mNumSkippedFiles);
      eos_notice("Directory: %s, files=%li scanduration=%.02f [s] scansize=%lli "
                 "[Bytes] [ %lli MB ] scannedfiles=%li  corruptedfiles=%li "
                 "hwcorrupted=%li skippedfiles=%li",
                 mDirPath.c_str(), mNumTotalFiles, (mScanDuration / 1000.0),
                 mTotalScanSize, ((mTotalScanSize / 1000) / 1000), mNumScannedFiles,
                 mNumCorruptedFiles, mNumHWCorruptedFiles, mNumSkippedFiles);
    } else {
      fprintf(stderr,
              "[ScanDir] Directory: %s, files=%li scanduration=%.02f [s] "
              "scansize=%lli [Bytes] [ %lli MB ] scannedfiles=%li "
              "corruptedfiles=%li hwcorrupted=%li skippedfiles=%li\n",
              mDirPath.c_str(), mNumTotalFiles,
              (mScanDuration / 1000.0), mTotalScanSize,
              ((mTotalScanSize / 1000) / 1000), mNumScannedFiles,
              mNumCorruptedFiles, mNumHWCorruptedFiles, mNumSkippedFiles);
    }

    if (!bgThread) {
      break;
    } else {
      if (!mForcedScan) {
        // Run again after 4 hours
        for (size_t s = 0; s < (4 * 3600); s++) {
          if (bgThread) {
            XrdSysThread::CancelPoint();
          }

          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
      } else {
        // Call the ghost entry clean-up function
        if (bgThread) {
          eos_notice("Directory: %s fsid=%d - cleaning ghost entries",
                     mDirPath.c_str(), mFsId);
          gFmdDbMapHandler.RemoveGhostEntries(mDirPath.c_str(), mFsId);
          std::this_thread::sleep_for(std::chrono::seconds(60));
        }
      }
    }

    if (bgThread) {
      XrdSysThread::CancelPoint();
    }
  } while (true);

  return NULL;
}

//------------------------------------------------------------------------------
// Scan file taking the load into consideration
//------------------------------------------------------------------------------
bool
ScanDir::ScanFileLoadAware(const std::unique_ptr<eos::fst::FileIo>& io,
                           unsigned long long& scansize, float& scan_duration,
                           const char* checksumVal, unsigned long layoutid,
                           const char* lfn, bool& filecxerror, bool& blockcxerror)
{
  bool retVal, corrupt_bxs = false;
  int scan_rate = mRateBandwidth;
  struct timezone tz;
  struct timeval opentime;
  struct timeval currenttime;
  scansize = 0;
  scan_duration = 0;
  struct stat info;

  if (io->fileStat(&info)) {
    return false;
  }

  gettimeofday(&opentime, &tz);
  std::string file_path = io->GetPath();
  std::unique_ptr<eos::fst::CheckSum> normalXS =
    eos::fst::ChecksumPlugins::GetChecksumObjectPtr(layoutid);
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

  do {
    nread = io->fileRead(offset, mBuffer, mBufferSize);

    if (nread < 0) {
      if (blockXS) {
        blockXS->CloseMap();
      }

      return false;
    }

    if (nread) {
      if (blockXS && (corrupt_bxs == false)) {
        if (!blockXS->CheckBlockSum(offset, mBuffer, nread)) {
          corrupt_bxs = true;
        }
      }

      if (normalXS) {
        normalXS->Add(mBuffer, nread, offset);
      }

      offset += nread;
      EnforceAndAdjustScanRate(offset, opentime, scan_rate);
    }
  } while (nread == mBufferSize);

  gettimeofday(&currenttime, &tz);
  scan_duration = (((currenttime.tv_sec - opentime.tv_sec) * 1000.0) + ((
                     currenttime.tv_usec - opentime.tv_usec) / 1000.0));
  scansize = (unsigned long long) offset;

  if (normalXS) {
    normalXS->Finalize();
  }

  // Check file checksum only for replica layouts
  if ((normalXS) && (!normalXS->Compare(checksumVal))) {
    if (bgThread) {
      eos_err("Computed checksum is %s scansize %llu\n", normalXS->GetHexChecksum(),
              scansize);
    } else {
      fprintf(stderr, "error: computed checksum is %s scansize %llu\n",
              normalXS->GetHexChecksum(), scansize);

      if (mSetChecksum) {
        int checksumlen = 0;
        normalXS->GetBinChecksum(checksumlen);

        if (io->attrSet("user.eos.checksum", normalXS->GetBinChecksum(checksumlen),
                        checksumlen) ||
            io->attrSet("user.eos.filecxerror", "0")) {
          fprintf(stderr, "error: failed to reset existing checksum \n");
        } else {
          fprintf(stdout, "success: reset checksum of %s to %s\n",
                  file_path.c_str(), normalXS->GetHexChecksum());
        }
      }
    }

    ++mNumCorruptedFiles;
    retVal = false;
    filecxerror = true;
  } else {
    retVal = true;
  }

  // Check block checksum
  if (corrupt_bxs) {
    blockcxerror = true;

    if (bgThread) {
      syslog(LOG_ERR,
             "corrupted block checksum: localpath=%s blockxspath=%s.xsmap lfn=%s\n",
             file_path.c_str(), file_path.c_str(), lfn);
      eos_crit("corrupted block checksum: localpath=%s blockxspath=%s.xsmap lfn=%s",
               file_path.c_str(), file_path.c_str(), lfn);
    } else {
      fprintf(stderr, "[ScanDir] corrupted block checksum: localpath=%s.xsmap "
              "blockxspath=%s lfn=%s\n", file_path.c_str(), file_path.c_str(), lfn);
    }

    retVal &= false;
  } else {
    retVal &= true;
  }

  ++mNumScannedFiles;

  if (blockXS) {
    blockXS->CloseMap();
  }

  normalXS.reset();

  if (bgThread) {
    XrdSysThread::CancelPoint();
  }

  return retVal;
}

//------------------------------------------------------------------------------
// Enforce the scan rate by throttling the current thread and also adjust it
// depending on the IO load on the mountpoint
//------------------------------------------------------------------------------
void
ScanDir::EnforceAndAdjustScanRate(const off_t offset,
                                  const struct timeval& open_ts, int& scan_rate)
{
  if (scan_rate) {
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

EOSFSTNAMESPACE_END
