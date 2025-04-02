//------------------------------------------------------------------------------
// File: XrdFstOfsFile.cc
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
 * but WITHOUT ANY WARRANTY; without even the implied waDon'trranty of  *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/
#include "fst/XrdFstOfsFile.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Config.hh"
#include "common/Constants.hh"
#include "common/Path.hh"
#include "common/http/OwnCloud.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "common/SecEntity.hh"
#include "common/CtaCommon.hh"
#include "common/IoPriority.hh"
#include "common/Timing.hh"
#include "common/xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"
#include "fst/layout/Layout.hh"
#include "fst/layout/LayoutPlugin.hh"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/storage/FileSystem.hh"
#include <XrdOss/XrdOssApi.hh>
#include "fst/io/FileIoPluginCommon.hh"
#include "namespace/utils/Etag.hh"
#include <XrdOuc/XrdOucPgrwUtils.hh>

// includes for gRPC
#include <grpc++/grpc++.h>
#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/WFEClient.hh"

extern XrdOss* XrdOfsOss;

EOSFSTNAMESPACE_BEGIN

constexpr uint16_t XrdFstOfsFile::msDefaultTimeout;
thread_local int t_iopriority = 0;

//------------------------------------------------------------------------------
// Get TPC key expiration timestamp
//------------------------------------------------------------------------------
time_t
XrdFstOfsFile::GetTpcKeyExpireTS(std::string_view tpc_ttl, time_t now_ts)
{
  using namespace std::chrono;
  time_t now = time(nullptr);

  if (now_ts) {
    now = now_ts;
  }

  time_t expire_ts = now + gOFS.mTpcKeyMinValidity.count();

  if (!tpc_ttl.empty()) {
    unsigned int ttl_val = 0ul;

    if (eos::common::StringToNumeric(tpc_ttl, ttl_val)) {
      if ((ttl_val >= gOFS.mTpcKeyMinValidity.count()) &&
          (ttl_val <= gOFS.mTpcKeyMaxValidity.count())) {
        expire_ts = now + ttl_val;
      } else {
        if (ttl_val < gOFS.mTpcKeyMinValidity.count()) {
          expire_ts = now + gOFS.mTpcKeyMinValidity.count();
        } else {
          expire_ts = now + gOFS.mTpcKeyMaxValidity.count();
        }
      }
    }
  }

  eos_static_debug("msg=\"tpc key validity\" seconds=%u", expire_ts - now);
  return expire_ts;
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdFstOfsFile::XrdFstOfsFile(const char* user, int MonID) :
  XrdOfsFileBase(user, MonID), eos::common::LogId(),
  mOpenOpaque(nullptr), mCapOpaque(nullptr), mFstPath(""), mBookingSize(0),
  mTargetSize(0), mMinSize(0), mMaxSize(0), viaDelete(false),
  mWrDelete(false), mRainSize(0), mNsPath(""), mLocalPrefix(""),
  mRdrManager(""), mTapeEnabled(false), mSecString(""), mEtag(""),
  mFileId(0), mFsId(0), mLid(0), mCid(0), mForcedMtime(1), mForcedMtime_ms(0),
  mFusex(false), mFusexIsUnlinked(false), mClosed(false), mCloseRc(0),
  mOpened(false), mHasWrite(false), mHasWriteErr(false), mHasReadErr(false),
  mIsRW(false), mIsDevNull(false), mIsCreation(false), mIsReplication(false),
  noAtomicVersioning(false),
  mIsInjection(false), mRainReconstruct(false), mDelOnClose(false),
  mRepairOnClose(false), mIsOCchunk(false), writeErrorFlag(false),
  mEventOnClose(false), mSyncOnClose(false), mEventWorkflow("default"),
  mSyncEventOnClose(false), mFmd(nullptr), mCheckSum(nullptr),
  mLayout(nullptr), mMaxOffsetWritten(0ull),
  mWritePosition(0ull), mOpenSize(0),
  mCloseSize(0), mTpcThreadStatus(EINVAL), mTpcState(kTpcIdle),
  mTpcFlag(kTpcNone), mTpcKey(""), mIsTpcDst(false), mTpcRetc(0),
  mTpcCancel(false), mIsHttp(false)
{
  rBytes = wBytes = sFwdBytes = sBwdBytes = sXlFwdBytes
                                = sXlBwdBytes = rOffset = wOffset = 0;
  rStart.tv_sec = wStart.tv_sec = rvStart.tv_sec = rTime.tv_sec = lrTime.tv_sec =
                                    rvTime.tv_sec = lrvTime.tv_sec = 0;
  rStart.tv_usec = wStart.tv_usec = rvStart.tv_usec = rTime.tv_usec =
                                      lrTime.tv_usec = rvTime.tv_usec = lrvTime.tv_usec = 0;
  wTime.tv_sec = lwTime.tv_sec = cTime.tv_sec = 0;
  wTime.tv_usec = lwTime.tv_usec = cTime.tv_usec = 0;
  rCalls = wCalls = nFwdSeeks = nBwdSeeks = nXlFwdSeeks = nXlBwdSeeks = 0;
  closeTime.tv_sec = closeTime.tv_usec = 0;
  cacheITime.tv_sec = cacheITime.tv_usec = 0;
  currentTime.tv_sec = openTime.tv_usec = 0;
  openTime.tv_sec = openTime.tv_usec = 0;
  totalBytes = 0;
  msSleep = 0;
  mBandwidth = 0;
  timeToOpen = 0;
  timeToClose = 0;
  timeToRead = 0;
  timeToReadV = 0;
  timeToWrite = 0;
  tz.tz_dsttime = tz.tz_minuteswest = 0;
  mIoPriorityValue = 0;
  mIoPriorityClass = IOPRIO_CLASS_NONE;
  mIoPriorityErrorReported = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdFstOfsFile::~XrdFstOfsFile()
{
  viaDelete = true;

  if (!mClosed) {
    close();
  }
}

//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
XrdFstOfsFile::open(const char* path, XrdSfsFileOpenMode open_mode,
                    mode_t create_mode, const XrdSecEntity* client,
                    const char* opaque)
{
  EPNAME("open");
  eos::common::Timing tm("open");
  COMMONTIMING("begin", &tm);
  const char* tident = error.getErrUser();
  SetLogId(ExtractLogId(opaque).c_str(), client, tident);
  mTident = error.getErrUser();
  char* val = 0;
  mIsRW = false;
  int retc = SFS_OK;
  int envlen = 0;
  mNsPath = path;
  gettimeofday(&openTime, &tz);
  bool hasCreationMode = (open_mode & (SFS_O_CREAT | SFS_O_TRUNC));
  bool isRepairRead = false;
  // Mask some opaque parameters to shorten the logging
  XrdOucString maskOpaque = opaque ? opaque : "";
  eos::common::StringConversion::MaskTag(maskOpaque, "cap.sym");
  eos::common::StringConversion::MaskTag(maskOpaque, "cap.msg");
  eos::common::StringConversion::MaskTag(maskOpaque, "authz");
  eos_info("path=%s info=%s open_mode=%x", mNsPath.c_str(),
           maskOpaque.c_str(), open_mode);
  // Process and filter open opaque information
  std::string in_opaque = (opaque ? opaque : "");
  in_opaque += "&mgm.path=";
  in_opaque += mNsPath.c_str();
  // Process TPC information - after this mOpenOpaque and mCapOpaque will be
  // properly populated and decrypted.
  int tpc_retc = ProcessTpcOpaque(in_opaque, client);

  if (tpc_retc == SFS_ERROR) {
    eos_err("msg=\"failed while processing TPC/open opaque\" "
            "path=\"%s\"", path);
    return SFS_ERROR;
  } else if (tpc_retc >= SFS_STALL) {
    return tpc_retc; // this is stall time in seconds
  }

  if (ProcessOpenOpaque()) {
    eos_err("msg=\"failed while processing open opaque info\" "
            "path=\"%s\"", path);
    return SFS_ERROR;
  }

  eos::common::VirtualIdentity vid;

  if (ProcessCapOpaque(isRepairRead, vid)) {
    eos_err("msg=\"failed while processing cap opaque info\" "
            "path=\"%s\"", path);
    return SFS_ERROR;
  }

  if (ProcessMixedOpaque()) {
    eos_err("msg=\"failed while processing mixed opaque info\" "
            "path=\"%s\"", path);
    return SFS_ERROR;
  }

  // For RAIN layouts if the opaque information contains the tag mgm.rain.store=1
  // the corrupted files are recovered back on disk. There is no other way to make
  // the distinction between an open for write and open for recovery
  if (mCapOpaque && (val = mCapOpaque->Get("mgm.rain.store"))) {
    if (strncmp(val, "1", 1) == 0) {
      eos_info("msg=\"enabling RAIN store recovery\" fxid=%08llx", mFileId);
      open_mode = SFS_O_RDWR;
      mRainReconstruct = true;
      mHasWrite = true;

      // Get logical file size
      if ((val = mCapOpaque->Get("mgm.rain.size"))) {
        try {
          mRainSize = std::stoull(val);
        } catch (...) {
          // ignore
        }
      } else {
        eos_warning("msg=\"unknown RAIN file size during reconstruction\" "
                    "fxid=%08llx", mFileId);
      }
    }
  }

  if ((open_mode & (SFS_O_WRONLY | SFS_O_RDWR | SFS_O_CREAT | SFS_O_TRUNC))) {
    mIsRW = true;
  }

  if (mNsPath.beginswith("/replicate:")) {
    if (gOFS.openedForWriting.isOpen(mFsId, mFileId)) {
      eos_err("msg=\"forbid replica open, file %s opened in RW mode\"",
              mNsPath.c_str());
      return gOFS.Emsg(epname, error, ETXTBSY, "open - cannot replicate: file "
                       "is opened in RW mode", mNsPath.c_str());
    }

    // File is supposed to act as a sink, used for draining
    if (mNsPath == "/replicate:0") {
      if (!mIsRW) {
        eos_err("msg=\"replicate file can only be opened for RW\" fxid=%08llx "
                "path=\"%s\"", mFileId, mNsPath.c_str());
        return gOFS.Emsg(epname, error, EIO, "open - replicate file can only "
                         "be opened in RW mode", mNsPath.c_str());
      } else {
        eos_info("%s", "msg=\"file fxid=0 acting as a sink i.e. /dev/null\"");
        mIsDevNull = true;
        return SFS_OK;
      }
    }

    mIsReplication = true;
  }

  COMMONTIMING("path::print", &tm);

  // Check if this is an open for HTTP
  if (!mIsRW && ((std::string(client->tident) == "http"))) {
    if (gOFS.openedForWriting.isOpen(mFsId, mFileId)) {
      eos_err("msg=\"forbid replica open for synchronization, file %s "
              "opened in RW mode\"", mNsPath.c_str());
      return gOFS.Emsg(epname, error, ETXTBSY, "open - cannot synchronize "
                       "file opened in RW mode", mNsPath.c_str());
    }
  }

  // Get the layout object
  mLayout.reset(eos::fst::LayoutPlugin::GetLayoutObject
                (this, mLid, client, &error, mFstPath.c_str(), msDefaultTimeout,
                 mRainReconstruct));

  if (mLayout == nullptr) {
    int envlen;
    eos_err("msg=\"unable to handle layout for %s\"", mCapOpaque->Env(envlen));
    return gOFS.Emsg(epname, error, EINVAL, "open - illegal layout specified ",
                     mCapOpaque->Env(envlen));
  }

  mLayout->SetLogId(logId, client, tident);
  errno = 0;

  if ((mRainReconstruct && (mTpcFlag == kTpcSrcCanDo)) ||
      (mTpcFlag == kTpcSrcSetup)) {
    eos_info("msg=\"kTpcSrcSetup return SFS_OK\" fxid=%08llx", mFileId);
    return SFS_OK;
  }

  COMMONTIMING("creation::barrier", &tm);
  OpenFileTracker::CreationBarrier creationSerialization(gOFS.runningCreation,
      mFsId, mFileId);
  COMMONTIMING("layout::exists", &tm);

  if ((retc = mLayout->GetFileIo()->fileExists())) {
    // We have to distinguish if an Exists call fails or returns ENOENT,
    // otherwise we might trigger an automatic clean-up of a file!!!
    if (errno != ENOENT) {
      return gOFS.Emsg(epname, error, EIO, "open - unable to check for "
                       "existence of file ", mCapOpaque->Env(envlen));
    }

    if (mIsRW || (mCapOpaque->Get("mgm.zerosize"))) {
      if (!mIsRW && mCapOpaque->Get("mgm.zerosize")) {
        // this commit should not call the versioning/atomic functionality
        noAtomicVersioning = true;
      }

      // File does not exist, keep the create flag for writers and readers
      // with 0-size at MGM
      mIsRW = true;
      mIsCreation = true;
      mOpenSize = 0;
      mWritePosition = 0;
      // Used to indicate if a file was written in the meanwhile by someone else
      updateStat.st_mtime = 0;
      open_mode |= SFS_O_CREAT;
      create_mode |= SFS_O_MKPTH;
      eos_debug("msg=\"adding creation flag\" retc=%d errno=%d fxid=%08llx",
                retc, errno, mFileId);
    } else {
      // The open will fail but the client will get a recoverable error,
      // therefore it will try to read again from the other replicas.
      eos_warning("msg=\"open for read, local file does not exists\" "
                  "fxid=%08llx path=\"%s\"", mFileId, mFstPath.c_str());
      return gOFS.Emsg(epname, error, ENOENT, "open, file does not exist ",
                       mCapOpaque->Env(envlen));
    }
  } else {
    eos_debug("msg=\"removing creation flag\" retc=%d errno=%d fxid=%08llx",
              retc, errno, mFileId);

    // Remove the creat flag
    if (open_mode & SFS_O_CREAT) {
      open_mode -= SFS_O_CREAT;
    }
  }

  if (!mIsCreation) {
    creationSerialization.Release();
  }

  // Capability access distinction
  if (mIsRW) {
    if (mIsCreation) {
      if (mCapOpaque->Get("mgm.zerosize") == nullptr) {
        if (!mCapOpaque->Get("mgm.access")
            || ((strcmp(mCapOpaque->Get("mgm.access"), "create")) &&
                (strcmp(mCapOpaque->Get("mgm.access"), "write")) &&
                (strcmp(mCapOpaque->Get("mgm.access"), "update")))) {
          return gOFS.Emsg(epname, error, EPERM, "open - capability does not "
                           "allow to create/write/update this file", path);
        }
      }
    } else {
      if (!mCapOpaque->Get("mgm.access")
          || ((strcmp(mCapOpaque->Get("mgm.access"), "create")) &&
              (strcmp(mCapOpaque->Get("mgm.access"), "write")) &&
              (strcmp(mCapOpaque->Get("mgm.access"), "update")))) {
        return gOFS.Emsg(epname, error, EPERM, "open - capability does not "
                         "allow to update/write/create this file", path);
      }
    }
  } else {
    if (!mCapOpaque->Get("mgm.access")
        || ((strcmp(mCapOpaque->Get("mgm.access"), "read")) &&
            (strcmp(mCapOpaque->Get("mgm.access"), "create")) &&
            (strcmp(mCapOpaque->Get("mgm.access"), "write")) &&
            (strcmp(mCapOpaque->Get("mgm.access"), "update")))) {
      return gOFS.Emsg(epname, error, EPERM, "open - capability does not allow "
                       "to read this file", path);
    }
  }

  // Get IO priority
  if (mCapOpaque->Get("mgm.iopriority")) {
    std::string key;
    std::string value;
    std::string kv = mCapOpaque->Get("mgm.iopriority");

    if (eos::common::StringConversion::SplitKeyValue(kv, key, value, ":")) {
      mIoPriorityClass = ioprio_class(key);
      mIoPriorityValue = ioprio_value(value);
    }
  }

  if (mCapOpaque->Get("mgm.iobw")) {
    mBandwidth = strtoull(mCapOpaque->Get("mgm.iobw"), 0, 10);
    eos_info("msg=\"bandwidth limited\" bw=%d, fxid=%08llx",
             mBandwidth, mFileId);
  }

  // Bookingsize is only needed for file creation
  if (mIsRW && mIsCreation && !mFusex) {
    const char* sbookingsize = 0;
    const char* stargetsize = 0;
    // If fsid=0 then all replicas/stripes are dropped and the logical file
    // is also removed, otherwise only the current mFsId is dropped.
    eos::common::FileSystem::fsid_t drop_fsid =
      ((!mIsReplication && !mIsInjection && !mRainReconstruct) ? 0u : mFsId);

    if (!(sbookingsize = mCapOpaque->Get("mgm.bookingsize"))) {
      DropFromMgm(mFileId, drop_fsid, mNsPath.c_str(), mRdrManager.c_str());
      eos_err("msg=\"no bookingsize in capability\" fxid=%08llx", mFileId);
      return gOFS.Emsg(epname, error, EINVAL, "open - no booking size in capability",
                       mNsPath.c_str());
    } else {
      mBookingSize = strtoull(mCapOpaque->Get("mgm.bookingsize"), 0, 10);

      if (errno == ERANGE) {
        DropFromMgm(mFileId, drop_fsid, mNsPath.c_str(), mRdrManager.c_str());
        eos_err("msg=\"invalid bookingsize in capability\" fxid=%08llx "
                "bookingsize=\"%s\"", mFileId, sbookingsize);
        return gOFS.Emsg(epname, error, EINVAL, "open - invalid bookingsize "
                         "in capability", mNsPath.c_str());
      }
    }

    if ((stargetsize = mCapOpaque->Get("mgm.targetsize"))) {
      mTargetSize = strtoull(mCapOpaque->Get("mgm.targetsize"), 0, 10);

      if (errno == ERANGE) {
        DropFromMgm(mFileId, drop_fsid, mNsPath.c_str(), mRdrManager.c_str());
        eos_err("msg=\"invalid targetsize in capability\" fxid=%08llx "
                "targetsize=%s", mFileId, stargetsize);
        return gOFS.Emsg(epname, error, EINVAL, "open - invalid targetsize "
                         "in capability", mNsPath.c_str());
      }
    }

    // Check if the booking size violates the min/max-size criteria
    if (mBookingSize && mMaxSize) {
      if (mBookingSize > mMaxSize) {
        DropFromMgm(mFileId, drop_fsid, mNsPath.c_str(), mRdrManager.c_str());
        eos_err("msg=\"invalid bookingsize specified - violates maximum "
                "file size criteria\" booking_sz=%llu", mBookingSize);
        return gOFS.Emsg(epname, error, ENOSPC, "open - bookingsize violates "
                         "maximum allowed filesize", mNsPath.c_str());
      }
    }

    if (mBookingSize && mMinSize) {
      if (mBookingSize < mMinSize) {
        DropFromMgm(mFileId, drop_fsid, mNsPath.c_str(), mRdrManager.c_str());
        eos_err("msg=\"invalid bookingsize specified - violates minimum "
                "file size criteria\" fxid=%08llx booking_sz=%llu",
                mFileId, mBookingSize);
        return gOFS.Emsg(epname, error, ENOSPC, "open - bookingsize violates "
                         "minimum allowed filesize", mNsPath.c_str());
      }
    }
  }

  if (gOFS.mSimFmdOpenErr) {
    eos_warning("msg=\"simulate FMD open error\" fxid=%08llx", mFileId);
    return gOFS.Emsg(epname, error, ENOENT, "open - no FMD record found, "
                     "simulated error");
  }

  COMMONTIMING("clone::fst", &tm);
  char* sCloneFST = mCapOpaque->Get("mgm.cloneFST");
  int clone_create_rc = 1;

  if (sCloneFST) {
    std::string mc_fst_path = eos::common::FileId::FidPrefix2FullPath(sCloneFST,
                              mLocalPrefix.c_str());
    struct stat clone_stat;
    int clonerc = ::stat(mc_fst_path.c_str(), &clone_stat) ? errno : 0;
    eos_info("fstpath=%s clonepath=%s clonerc=%d len=%d", mFstPath.c_str(),
             mc_fst_path.c_str(), clonerc, clonerc ? -1 : clone_stat.st_size);

    /* clone handling:
     * if read-write and clone does not exist, create it
     * if read-only switch to clone if it exists
     * (note: if several clones were allowed, we'd might have to search!)
     */
    if (mIsRW && clonerc != 0) { /* for RW, only if clone not yet created */
      if (open_mode & SFS_O_TRUNC) {
        /* rename data file to clone, it will be re-created */
        clone_create_rc = ::rename(mFstPath.c_str(), mc_fst_path.c_str()) ? errno : 0;
        eos_info("copy-on-write: rename %s %s rc=%d", mFstPath.c_str(),
                 mc_fst_path.c_str(), clone_create_rc);
      } else {
        /* copy data file to clone before modyfying */
        char sbuff[1024];
        snprintf(sbuff, sizeof(sbuff),
                 "cp --preserve=xattr,ownership,mode --reflink=auto %s %s",
                 mFstPath.c_str(), mc_fst_path.c_str());
        clone_create_rc = system(sbuff);
        eos_info("copy-on-write: %s rc=%d", sbuff, clone_create_rc);
      }
    }
  }

  XrdOucString oss_opaque = "";
  oss_opaque += "&mgm.lid=";
  oss_opaque += std::to_string(mLid).c_str();
  oss_opaque += "&mgm.bookingsize=";
  oss_opaque += std::to_string(mBookingSize).c_str();

  if (!(val = mCapOpaque->Get("mgm.iotype"))) {
    // provided by a client
    if ((val = mOpenOpaque->Get("eos.iotype"))) {
      oss_opaque += "&mgm.ioflag=";
      oss_opaque += val;

      if (std::string(val) == "csync") {
        // cannot be done in the OSS
        mSyncOnClose = true;
      }
    }
  } else {
    // forced by the MGM configuration
    oss_opaque += "&mgm.ioflag=";
    oss_opaque += val;

    if (std::string(val) == "csync") {
      // cannot be done in the OSS
      mSyncOnClose = true;
    }
  }

  // Open layout implementation
  eos_info("path=%s open-mode=%x create-mode=%x layout-name=%s oss-opaque=%s",
           mFstPath.c_str(), open_mode, create_mode, mLayout->GetName(),
           oss_opaque.c_str());
  COMMONTIMING("layout::open", &tm);
  int rc = mLayout->Open(open_mode, create_mode, oss_opaque.c_str());
  COMMONTIMING("layout::opened", &tm);

  if (rc) {
    // If we have local errors in open we don't disable the filesystem -
    // this is done by the Scrub thread if necessary!
    if (mLayout->IsEntryServer() && !mIsReplication) {
      eos_warning("msg=\"open error return recoverable error "
                  "EIO(kXR_IOError)\" fid=%08llx", mFileId);

      // Clean-up before re-bouncing
      if (hasCreationMode && !mRainReconstruct && !mIsInjection) {
        DropFromMgm(mFileId, 0ul, path, mRdrManager.c_str());
      }
    }

    return gOFS.Emsg(epname, error, EIO, "open - failed open");
  }

  if (gOFS.mSimOpenDelay) {
    eos_warning("msg=\"simulate open timeout that becomes a client error\" "
                "fxid=%08llx", mFileId);
    std::this_thread::sleep_for(std::chrono::seconds(gOFS.mSimOpenDelaySec.load()));
  }

  COMMONTIMING("get::localfmd", &tm);
  mFmd = gOFS.mFmdHandler->LocalGetFmd(mFileId, mFsId, isRepairRead, mIsRW,
                                       vid.uid, vid.gid, mLid);
  COMMONTIMING("resync::localfmd", &tm);

  if (mFmd == nullptr) {
    if (gOFS.mFmdHandler->ResyncMgm(mFsId, mFileId, mRdrManager.c_str())) {
      eos_info("msg=\"resync ok\" fsid=%u fxid=%08llx", mFsId, mFileId);
      mFmd = gOFS.mFmdHandler->LocalGetFmd(mFileId, mFsId, isRepairRead,
                                           mIsRW, vid.uid, vid.gid, mLid);
      std::string dummy_xs;
      int rc = 0;

      if ((rc = gOFS.mFmdHandler->ResyncDisk(mFstPath.c_str(), mFsId, false, 0,
                                             dummy_xs))) {
        eos_err("msg=\"failed to resync from disk\" fsid=%lu fxid=%llx "
                "path=%s rc=%d", mFsId, mFileId, mFstPath.c_str(), rc);
      } else {
        eos_info("msg=\"resync from disk\" path=%s", mFstPath.c_str());
      }
    } else {
      eos_err("msg=\"resync failed\" fsid=%u fxid=%08llx", mFsId, mFileId);
    }
  }

  if (mFmd == nullptr) {
    eos_err("msg=\"no FMD record found\" fsid=%u fxid=%08llx", mFsId, mFileId);

    if (!mIsRW || (mLayout->IsEntryServer() && !mIsReplication)) {
      eos_warning("msg=\"failed to get FMD record\" fsid=%u fxid=%08llx "
                  "path=\"%s\" rc=ENOENT(kXR_NotFound)", mFsId, mFileId,
                  mFstPath.c_str());

      if (hasCreationMode && !mRainReconstruct && !mIsInjection) {
        DropFromMgm(mFileId, 0ul, path, mRdrManager.c_str());
      }
    }

    // Return an error that can be recovered at the MGM
    return gOFS.Emsg(epname, error, ENOENT, "open - no FMD record found");
  }

  // Update the fmd information for any clone objects
  if (sCloneFST) {
    if (mIsRW && (clone_create_rc == 0)) {
      // Populate local DB (future reads need it)
      unsigned long long clFid = eos::common::FileId::Hex2Fid(sCloneFST);
      auto lfmd = gOFS.mFmdHandler->LocalGetFmd(clFid, mFsId, false, mIsRW,
                  vid.uid, vid.gid, mLid);

      if (lfmd == nullptr) {
        // We have an invalid FMD, drop and try again!
        gOFS.mFmdHandler->LocalDeleteFmd(clFid, mFsId);
        lfmd = gOFS.mFmdHandler->LocalGetFmd(clFid, mFsId, false, mIsRW,
                                             vid.uid, vid.gid, mLid);

        // FIXME: maybe we don't need to exit here?
        if (!lfmd) {
          return gOFS.Emsg(epname, error, ENOENT, "open unable to create FMD");
        }
      }

      lfmd->mProtoFmd.set_checksum(mFmd->mProtoFmd.checksum());
      lfmd->mProtoFmd.set_diskchecksum(mFmd->mProtoFmd.diskchecksum());
      lfmd->mProtoFmd.set_mgmchecksum(mFmd->mProtoFmd.mgmchecksum());

      if (!gOFS.mFmdHandler->Commit(lfmd.get())) {
        eos_err("copy-on-write unable to commit meta data to local database");
        (void) gOFS.Emsg(epname, this->error, EIO,
                         "copy-on-write - unable to commit meta data", mNsPath.c_str());
      }

      eos_debug("fid %lld cs %s diskcs %s mgmcs %s", lfmd->mProtoFmd.fid(),
                lfmd->mProtoFmd.checksum().c_str(), lfmd->mProtoFmd.diskchecksum().c_str(),
                lfmd->mProtoFmd.mgmchecksum().c_str());
    } else {
      eos_debug("fid %lld cs %s diskcs %s mgmcs %s", mFmd->mProtoFmd.fid(),
                mFmd->mProtoFmd.checksum().c_str(), mFmd->mProtoFmd.diskchecksum().c_str(),
                mFmd->mProtoFmd.mgmchecksum().c_str());
    }
  }

  if (mIsCreation) {
    creationSerialization.Release();
  }

  COMMONTIMING("layout::stat", &tm);

  if (!mIsCreation && mIsReplication) {
    mLayout->Stat(&updateStat);
  }

  if (mIsCreation && mBookingSize) {
    COMMONTIMING("full::mutex", &tm);
    // Check if the file system is full
    XrdSysMutexHelper lock(gOFS.Storage->mFsFullMapMutex);

    if (gOFS.Storage->mFsFullMap[mFsId]) {
      if (mLayout->IsEntryServer() && !mIsReplication) {
        writeErrorFlag = kOfsDiskFullError;
        mLayout->Remove();
        eos_warning("msg=\"not enough space\" fsid=%u fxid=%08llx "
                    "rc=ENODEV(kXR_FSError)", mFsId, mFileId);

        if (hasCreationMode && !mRainReconstruct && !mIsInjection) {
          // Clean-up all stripes
          DropFromMgm(mFileId, 0ul, path, mRdrManager.c_str());
        }

        // Return an error that can be recovered at the MGM
        return gOFS.Emsg(epname, error, ENODEV, "open - not enough space");
      }

      return gOFS.Emsg(epname, error, ENOSPC, "create file - disk space "
                       "(headroom) exceeded fn=", mFstPath.c_str());
    }

    COMMONTIMING("layout::fallocate", &tm);
    rc = mLayout->Fallocate(mBookingSize);
    COMMONTIMING("layout::fallocated", &tm);

    if (rc) {
      eos_crit("msg=\"file allocation failed\" fsid=%u fxid=%08llx retc=%d "
               "errno=%d size=%llu", mFsId, mFileId, rc, errno, mBookingSize);

      if (mLayout->IsEntryServer() && !mIsReplication) {
        mLayout->Remove();
        eos_warning("msg=\"not enough space, file allocation failed\" fsid=%lu "
                    "fxid=%08llx rc=ENODEV(kXR_FSError", mFsId, mFileId);

        if (hasCreationMode && !mRainReconstruct && !mIsInjection) {
          // Clean-up all stripes
          DropFromMgm(mFileId, 0ul, path, mRdrManager.c_str());
        }

        // Return an error that can be recovered at the MGM
        return gOFS.Emsg(epname, error, ENODEV, "open - file allocation failed");
      }

      mLayout->Remove();
      return gOFS.Emsg(epname, error, ENOSPC, "open - cannot allocate "
                       "required space", mNsPath.c_str());
    }
  }

  if (mIsCreation) {
    gOFS.mFmdHandler->Commit(mFmd.get());
  } else {
    COMMONTIMING("layout::stat", &tm);
    // Get the real size of the file, not the local stripe size!
    struct stat statinfo {};

    if ((retc = mLayout->Stat(&statinfo))) {
      return gOFS.Emsg(epname, error, EIO, "open - cannot stat layout to "
                       "determine file size", mNsPath.c_str());
    }

    // We feed the layout size, not the physical on disk!
    eos_info("msg=\"layout size\" fxid=%08llx disk_size=%zu db_size= %llu",
             mFileId, statinfo.st_size, mFmd->mProtoFmd.size());
    mOpenSize = mFmd->mProtoFmd.size();
    mWritePosition = mOpenSize;

    if (!eos::common::LayoutId::IsRain(mLayout->GetLayoutId())) {
      // If replica layout and physical size of replica difference from the
      // fmd_size it means the file is being written to, so we save the actual
      // size from disk.
      if ((off_t) statinfo.st_size != (off_t) mFmd->mProtoFmd.size()) {
        mOpenSize = statinfo.st_size;
        mWritePosition = mOpenSize;
      }
    }

    // Preset with the last known checksum
    if (mIsRW && mCheckSum && !mIsOCchunk) {
      eos_info("msg=\"checksum reset init\" fxid=%08llx file-xs=%s",
               mFileId, mFmd->mProtoFmd.checksum().c_str());
      mCheckSum->ResetInit(0, mOpenSize, mFmd->mProtoFmd.checksum().c_str());
    }
  }

  // For RAIN layouts we enable full file checksum only at the entry server for
  // write operations. For the rest of the cases we rely on the block and parity
  // checking.
  if (eos::common::LayoutId::IsRain(mLid) &&
      !(mIsRW && mLayout->IsEntryServer())) {
    mCheckSum.reset(nullptr);
  }

  // Set the eos lfn as extended attribute
  std::unique_ptr<FileIo> io
  (FileIoPlugin::GetIoObject(mLayout->GetLocalReplicaPath(), this));
  COMMONTIMING("fileio::object", &tm);

  if (mIsRW) {
    if (mNsPath.beginswith("/replicate:") || mNsPath.beginswith("/fusex-open")) {
      if (mCapOpaque->Get("mgm.path")) {
        XrdOucString unsealedpath = mCapOpaque->Get("mgm.path");
        XrdOucString sealedpath = path;

        if (io->attrSet(std::string("user.eos.lfn"),
                        std::string(unsealedpath.c_str()))) {
          eos_err("msg=\"unable to set extended attr <eos.lfn> \" "
                  "fxid=%08llx errno=%d", mFileId, errno);
        }
      } else {
        eos_err("msg=\"no lfn in replication capability\" fxid=%08lls",
                mFileId);
      }
    } else {
      if (io->attrSet(std::string("user.eos.lfn"), std::string(mNsPath.c_str()))) {
        eos_err("msg=\"unable to set extended attr <eos.lfn>\" "
                "fxid=%08llx errno=%d", mFileId, errno);
      }
    }
  } else {
    // For reading of replica file check for xs errors unless this
    // is a fuse client!
    if (!mFusex &&
        eos::common::LayoutId::IsReplica(mLid) &&
        gOFS.mFmdHandler->FileHasXsError(mLayout->GetLocalReplicaPath(), mFsId)) {
      eos_err("msg=\"open failed due to checksum mismatch\" fxid=%08llx "
              "path=\"%s\"", mFileId, mNsPath.c_str());
      return gOFS.Emsg(epname, error, EIO, "open - replica checksum mismatch",
                       mNsPath.c_str());
    }
  }

  COMMONTIMING("open::accounting", &tm);

  if (mIsRW) {
    gOFS.openedForWriting.up(mFsId, mFileId);
  } else {
    gOFS.openedForReading.up(mFsId, mFileId);
  }

  mOpened = true;
  COMMONTIMING("end", &tm);
  timeToOpen = tm.RealTime();

  // Report slow open as errors if longer than 1000ms
  if (timeToOpen > 1000) {
    eos_err("msg=\"slow open operation\" open-duration=%.03fms fxid=%08llx "
            "path=\"%s\" %s", timeToOpen, mFileId, mNsPath.c_str(),
            tm.Dump().c_str());
  }

  eos_info("open-duration=%.03fms path=\"%s\" fxid=%08llx %s", timeToOpen,
           mNsPath.c_str(), mFileId, tm.Dump().c_str());
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Pre-read into file system cache
//------------------------------------------------------------------------------
int
XrdFstOfsFile::read(XrdSfsFileOffset fileOffset, XrdSfsXferSize amount)
{
  int rc = XrdOfsFile::read(fileOffset, amount);
  eos_debug("rc=%d offset=%lu size=%llu", rc, fileOffset, amount);
  return rc;
}

//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::read(XrdSfsFileOffset fileOffset, char* buffer,
                    XrdSfsXferSize buffer_size)
{
  gettimeofday(&rStart, &tz);
  // use RR scheduling if there is a round-robin app name
  std::mutex* mutex = 0;

  if (!mAppRR.empty()) {
    if (mIsRW) {
      mutex = gOFS.openedForWriting.scheduleRR(mFsId, mAppRR);
    } else {
      mutex = gOFS.openedForReading.scheduleRR(mFsId, mAppRR);
    }
  }

  auto lockScope = (mutex == nullptr) ?
                   std::unique_lock<std::mutex>() :
                   std::unique_lock<std::mutex>(*mutex);
  eos_debug("fileOffset=%lli, buffer_size=%i", fileOffset, buffer_size);

  if (mTpcFlag == kTpcSrcRead) {
    if (!(rCalls % 10)) {
      if (!TpcValid()) {
        eos_err("msg=\"tcp interrupted by control-c - cancel tcp read\" key=%s",
                mTpcKey.c_str());
        return gOFS.Emsg("read", error, EINTR, "read - tpc transfer interrupted"
                         " by client disconnect", FName());
      }
    }
  }

  if (mBandwidth) {
    gettimeofday(&currentTime, &tz);
    float abs_time = static_cast<float>((currentTime.tv_sec -
                                         openTime.tv_sec) * 1000 +
                                        (currentTime.tv_usec - openTime.tv_usec) / 1000);
    // Regulate the io - sleep as desired
    float exp_time = totalBytes / mBandwidth / 1000.0;

    if (abs_time < exp_time) {
      msSleep += (exp_time - abs_time);
      std::int64_t thisSleep = msSleep;
      std::this_thread::sleep_for(std::chrono::milliseconds(thisSleep));
    }
  }

  int rc = mLayout->Read(fileOffset, buffer, buffer_size);
  eos_debug("layout read %d checkSum %d", rc, mCheckSum.get());

  if (gOFS.mSimReadDelay) {
    eos_warning("msg=\"apply read delay\" delay=%is fxid=%08llx",
                gOFS.mSimReadDelaySec.load(), mFileId);
    std::this_thread::sleep_for(std::chrono::seconds(gOFS.mSimReadDelaySec.load()));
  }

  /* maintaining a checksum is tricky if there have been writes,
   * but the read + append case can be supported in "Add" */
  if ((rc > 0) && (mCheckSum) && (!mHasWrite)) {
    XrdSysMutexHelper cLock(mChecksumMutex);
    mCheckSum->Add(buffer, static_cast<size_t>(rc),
                   static_cast<off_t>(fileOffset));
  }

  if (rc > 0) {
    // if required, unobfuscate a buffer server side
    if (mLayout->IsEntryServer() && mHmac.key.length()) {
      eos::common::SymKey::UnobfuscateBuffer(const_cast<char*>(buffer), rc,
                                             fileOffset, mHmac);
    }

    if (mLayout->IsEntryServer() || eos::common::LayoutId::IsRain(mLid)) {
      XrdSysMutexHelper vecLock(vecMutex);
      rvec.push_back(rc);
    }

    rOffset = fileOffset + rc;
    totalBytes += rc;
  }

  if (rc < 0) {
    // Here we might take some other action
    int envlen = 0;
    eos_crit("block-read error=%d offset=%llu len=%llu file=%s",
             error.getErrInfo(),
             static_cast<unsigned long long>(fileOffset),
             static_cast<unsigned long long>(buffer_size),
             FName(), mCapOpaque ? mCapOpaque->Env(envlen) : FName());
    // Used to understand if a reconstruction of a RAIN file worked
    mHasReadErr = true;
  }

  eos_debug("rc=%d offset=%lu size=%llu", rc, fileOffset,
            static_cast<unsigned long long>(buffer_size));

  if ((fileOffset + buffer_size) >= mOpenSize) {
    if (mCheckSum && (!mHasWrite)) {
      /* even if there were only reads up to here the file may still be modified if opened R/W. As
       * VerifyChecksum "finalises" the context, this has to be handled in write anyway;
       * but not finalising now speeds up the slightly less marginal case of "write a lot" +
       * "read a little" + "write a little" (seen in "git") */
      if (!mCheckSum->NeedsRecalculation()) {
        // If this is the last read of sequential reading, we can verify
        // the checksum now (unless we're writing as well)
        if (VerifyChecksum()) {
          return gOFS.Emsg("read", error, EIO, "read file - wrong file "
                           "checksum fn=", FName());
        }
      }
    }
  }

  AddLayoutReadTime();
  return rc;
}

//----------------------------------------------------------------------------
// Read file pages into a buffer and return corresponding checksums
//----------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::pgRead(XrdSfsFileOffset offset, char* buffer,
                      XrdSfsXferSize rdlen, uint32_t* csvec, uint64_t opts)
{
  eos_debug("offset=%lli len=%i", offset, rdlen);
  XrdSfsXferSize bytes;

  // Read the data into the buffer
  if ((bytes = read(offset, buffer, rdlen)) <= 0) {
    return bytes;
  }

  // Generate the crc's
  XrdOucPgrwUtils::csCalc(buffer, offset, bytes, csvec);
  return bytes;
}


//------------------------------------------------------------------------------
// Vector read
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::readv(XrdOucIOVec* readV, int readCount)
{
  eos_debug("msg=\"readv request\" count=%i", readCount);
  gettimeofday(&rvStart, &tz);
  std::string output_init, output_final;
  auto print_readv_request = [](XrdOucIOVec * readv, int num_chunks) {
    std::ostringstream oss;

    for (int i = 0; i < num_chunks; ++i) {
      oss << "index=" << i
          << " offset=" << readv[i].offset
          << " length=" << readv[i].size
          << std::endl;
    }

    return oss.str();
  };

  if (EOS_LOGS_DEBUG) {
    output_init = print_readv_request(readV, readCount);
    eos_debug("msg=\"initial readv request\" obj=%p content=\"%s\"",
              readV, output_init.c_str());
  }

  // Copy the XrdOucIOVec structure to XrdCl::ChunkList
  uint32_t total_read = 0;
  XrdCl::ChunkList chunkList;
  chunkList.reserve(readCount);

  for (int i = 0; i < readCount; ++i) {
    total_read += (uint32_t)readV[i].size;
    chunkList.push_back(XrdCl::ChunkInfo((uint64_t)readV[i].offset,
                                         (uint32_t)readV[i].size,
                                         (void*)readV[i].data));
  }

  int64_t rv = mLayout->ReadV(chunkList, total_read);
  totalBytes += rv;

  if (EOS_LOGS_DEBUG) {
    output_final = print_readv_request(readV, readCount);
    eos_debug("msg=\"final readv request\" obj=%p content=\"%s\"",
              readV, output_final.c_str());

    if (output_init != output_final) {
      eos_crit("%s", "msg=\"readv object corrupted\"");
    }
  }

  if (rv < 0) {
    eos_crit("readv error=%d cnt=%d file=%s", rv, readCount, FName());
    mHasReadErr = true;
  }

  AddLayoutReadVTime();
  return rv;
}

//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::write(XrdSfsFileOffset fileOffset, const char* buffer,
                     XrdSfsXferSize buffer_size)
{
  gettimeofday(&wStart, &tz);

  if (gOFS.mSimUnresponsive) {
    eos_warning("msg=\"simulate unresponsive write, delay by 120s\" "
                "fxid=%08llx", mFileId);
    std::this_thread::sleep_for(std::chrono::seconds(120));
  }

  if (mIsDevNull) {
    eos_debug("offset=%llu, length=%li discarded for sink file", fileOffset,
              buffer_size);
    mMaxOffsetWritten = fileOffset + buffer_size;
    AddLayoutWriteTime();
    return buffer_size;
  }

  {
    // use global RR serialization (we just use fsid 0 for that)
    std::mutex* mutex = 0;

    if (!mAppRR.empty()) {
      if (mIsRW) {
        mutex = gOFS.openedForWriting.scheduleRR(0, mAppRR);
      } else {
        mutex = gOFS.openedForReading.scheduleRR(0, mAppRR);
      }
    }

    auto lockScope = (mutex == nullptr) ?
                     std::unique_lock<std::mutex>() :
                     std::unique_lock<std::mutex>(*mutex);
  }

  // use RR scheduling if there is a round-robin app name per filesystem
  std::mutex* mutex = 0;

  if (!mAppRR.empty()) {
    if (mIsRW) {
      mutex = gOFS.openedForWriting.scheduleRR(mFsId, mAppRR);
    } else {
      mutex = gOFS.openedForReading.scheduleRR(mFsId, mAppRR);
    }
  }

  auto lockScope = (mutex == nullptr) ?
                   std::unique_lock<std::mutex>() :
                   std::unique_lock<std::mutex>(*mutex);

  if (mBandwidth) {
    gettimeofday(&currentTime, &tz);
    float abs_time = static_cast<float>((currentTime.tv_sec -
                                         openTime.tv_sec) * 1000 +
                                        (currentTime.tv_usec - openTime.tv_usec) / 1000);
    // Regulate the io - sleep as desired
    float exp_time = totalBytes / mBandwidth / 1000.0;

    if (abs_time < exp_time) {
      msSleep += (exp_time - abs_time);
      std::int64_t thisSleep = msSleep;
      std::this_thread::sleep_for(std::chrono::milliseconds(thisSleep));
    }
  }

  // if the write position moves the checksum is dirty
  if (mCheckSum) {
    if (mWritePosition != fileOffset) {
      mCheckSum->SetDirty();
    }

    // store next write position
    mWritePosition = fileOffset + buffer_size;
  }

  // if required, obfuscate a buffer server side
  if (mLayout->IsEntryServer() && mHmac.key.length()) {
    eos::common::SymKey::ObfuscateBuffer(const_cast<char*>(buffer),
                                         const_cast<char*>(buffer), buffer_size, fileOffset, mHmac);
  }

  int rc = mLayout->Write(fileOffset, const_cast<char*>(buffer), buffer_size);
  eos_debug("rc=%d offset=%lu size=%lu", rc, fileOffset,
            static_cast<unsigned long>(buffer_size));

  // If we see a remote IO error, we don't fail, we just call repair afterwards,
  // only for replica layouts and not for FuseX clients
  if ((rc < 0) && mIsCreation && !mFusex &&
      eos::common::LayoutId::IsReplica(mLid) &&
      (mLayout->GetErrObj()->getErrInfo() == EREMOTEIO)) {
    mRepairOnClose = true;
    rc = buffer_size;
  }

  // Evt. add checksum
  if (rc > 0) {
    if (mCheckSum) {
      XrdSysMutexHelper cLock(mChecksumMutex);
      mCheckSum->Add(buffer, static_cast<size_t>(rc),
                     static_cast<off_t>(fileOffset));
    }

    totalBytes += rc;

    if (static_cast<unsigned long long>(fileOffset + buffer_size) >
        static_cast<unsigned long long>(mMaxOffsetWritten)) {
      mMaxOffsetWritten = (fileOffset + buffer_size);
    }
  }

  if (rc < 0) {
    int envlen = 0;

    if (!mHasWriteErr || EOS_LOGS_DEBUG) {
      eos_crit("block-write error=%d offset=%llu len=%llu file=%s",
               mLayout->GetErrObj()->getErrInfo(),
               static_cast<unsigned long long>(fileOffset),
               static_cast<unsigned long long>(buffer_size),
               FName(), mCapOpaque ? mCapOpaque->Env(envlen) : FName());
    }

    mHasWriteErr = true;
  } else {
    mHasWrite = true;

    if (mLayout->IsEntryServer() || mIsReplication) {
      XrdSysMutexHelper lock(vecMutex);
      wvec.push_back(rc);
    }
  }

  if (rc < 0) {
    int envlen = 0;
    // Indicate the deletion flag for write errors
    mWrDelete = true;
    XrdOucString errdetail;

    if (mIsCreation) {
      XrdOucString newerr;
      // Add to the error message that this file has been removed after the error,
      // which happens for creations
      newerr = error.getErrText();

      if (writeErrorFlag == kOfsSimulatedIoError) {
        // Simulated IO error
        errdetail += " => file has been removed because of a simulated IO error";
      } else {
        if (writeErrorFlag == kOfsDiskFullError) {
          // Disk full error
          errdetail +=
            " => file has been removed because the target filesystem  was full";
        } else {
          if (writeErrorFlag == kOfsMaxSizeError) {
            // Maximum file size error
            errdetail += " => file has been removed because the maximum target "
                         "filesize defined for that subtree was exceeded (maxsize=";
            char smaxsize[16];
            snprintf(smaxsize, sizeof(smaxsize) - 1, "%llu", (unsigned long long) mMaxSize);
            errdetail += smaxsize;
            errdetail += " bytes)";
          } else {
            if (writeErrorFlag == kOfsIoError) {
              // Generic IO error
              errdetail +=
                " => file has been removed due to an IO error on the target filesystem";
            } else {
              errdetail += " => file has been removed due to an IO error (unspecified)";
            }
          }
        }
      }

      newerr += errdetail.c_str();
      error.setErrData(newerr.c_str());
    }

    eos_err("block-write error=%d offset=%llu len=%llu file=%s error=\"%s\"",
            error.getErrInfo(),
            (unsigned long long) fileOffset,
            (unsigned long long) buffer_size, FName(),
            mCapOpaque ? mCapOpaque->Env(envlen) : FName(),
            errdetail.c_str());
  }

  AddLayoutWriteTime();
  return rc;
}

//----------------------------------------------------------------------------
// Write file pages into a file with corresponding checksums.
//----------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::pgWrite(XrdSfsFileOffset offset, char* buffer,
                       XrdSfsXferSize wrlen, uint32_t* csvec, uint64_t opts)
{
  eos_debug("offset=%lli len=%i", offset, wrlen);

  // If we have a checksum vector and verify is on, do verification.
  if (opts & Verify) {
    XrdOucPgrwUtils::dataInfo dInfo(buffer, csvec, offset, wrlen);
    off_t badoff;
    int   badlen;

    if (!XrdOucPgrwUtils::csVer(dInfo, badoff, badlen)) {
      char eMsg[512];
      snprintf(eMsg, sizeof(eMsg), "Checksum error at offset %lld",
               (long long) badoff);
      error.setErrInfo(EDOM, eMsg);
      return SFS_ERROR;
    }
  }

  return write(offset, buffer, wrlen);
}

//------------------------------------------------------------------------------
// Get file stat information
//------------------------------------------------------------------------------
int
XrdFstOfsFile::stat(struct stat* buf)
{
  EPNAME("stat");
  int rc = SFS_OK;

  if (mIsDevNull) {
    buf->st_size = mMaxOffsetWritten;
    return rc;
  }

  if (mRainReconstruct) {
    buf->st_size = mRainSize;
    return rc;
  }

  if (mLayout) {
    if ((rc = mLayout->Stat(buf)))
      rc = gOFS.Emsg(epname, error, EIO, "stat - cannot stat layout to determine"
                     " file size ", mNsPath.c_str());
  } else {
    rc = gOFS.Emsg(epname, error, ENXIO, "stat - no layout to determine file size ",
                   mNsPath.c_str());
  }

  // store the file id as inode number
  if (!rc) {
    buf->st_ino = eos::common::FileId::FidToInode(mFileId);
  }

  // we store the mtime.ns time in st_dev ... sigh@Xrootd ...
#ifdef __APPLE__
  unsigned long nsec = buf->st_mtimespec.tv_nsec;
#else
  unsigned long nsec = buf->st_mtim.tv_nsec;
#endif
  // mask for 10^9
  nsec &= 0x7fffffff;
  // enable bit 32 as indicator
  nsec |= 0x80000000;
  // overwrite st_dev
  buf->st_dev = nsec;
#ifdef __APPLE__
  eos_info("path=%s fxid=%08llx size=%lu mtime=%lu.%lu", mNsPath.c_str(), mFileId,
           (unsigned long) buf->st_size, buf->st_mtimespec.tv_sec,
           buf->st_dev & 0x7ffffff);
#else
  eos_info("path=%s fxid=%08llx size=%lu mtime=%lu.%lu", mNsPath.c_str(), mFileId,
           (unsigned long) buf->st_size, buf->st_mtim.tv_sec, buf->st_dev & 0x7ffffff);
#endif
  return rc;
}

//------------------------------------------------------------------------------
// Sync file
//------------------------------------------------------------------------------
int
XrdFstOfsFile::sync()
{
  eos_debug("msg=\"sync request\", fxid=%08llx", mFileId);
  static const int cbWaitTime = 3600;

  // TPC transfer
  if (mTpcFlag == kTpcDstSetup) {
    XrdSysMutexHelper scope_lock(&mTpcJobMutex);

    if (mTpcState == kTpcIdle) {
      eos_info("msg=\"tpc enabled -> 1st sync\"");
      mTpcThreadStatus = XrdSysThread::Run(&mTpcThread,
                                           XrdFstOfsFile::StartDoTpcTransfer,
                                           static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                                           "TPC Transfer Thread");

      if (mTpcThreadStatus == 0) {
        mTpcState = kTpcRun;
        scope_lock.UnLock();
        return SFS_OK;
      } else {
        eos_err("msg=\"failed to start TPC job thread\"");
        mTpcState = kTpcDone;

        if (mTpcInfo.Key) {
          free(mTpcInfo.Key);
        }

        mTpcInfo.Key = strdup("Copy failed, could not start job");
        return mTpcInfo.Fail(&error, "could not start job", ECANCELED);
      }
    } else if (mTpcState == kTpcRun) {
      eos_info("msg=\"tpc running -> 2nd sync\"");

      if (mTpcInfo.SetCB(&error)) {
        return SFS_ERROR;
      }

      error.setErrCode(cbWaitTime);
      mTpcInfo.Engage();
      return SFS_STARTED;
    } else if (mTpcState == kTpcDone) {
      eos_info("msg=\"tpc already finished, retc=%i\"", mTpcRetc);

      if (mTpcRetc) {
        error.setErrInfo(mTpcRetc, (mTpcInfo.Key ? mTpcInfo.Key : "failed tpc"));
        return SFS_ERROR;
      } else {
        return SFS_OK;
      }
    } else {
      eos_err("%s", "msg=\"unknown tpc state\"");
      error.setErrInfo(EINVAL, "unknown TPC state");
      return SFS_ERROR;
    }
  } else {
    // Standard file sync
    static bool async_sync_cfg = IsAsyncSyncConfigured();

    if (!async_sync_cfg || DoSyncSync()) {
      eos::common::Timing tm("sync");
      COMMONTIMING("begin", &tm);
      int rc = mLayout->Sync();
      COMMONTIMING("end", &tm);

      if (tm.RealTime() > 2000) {
        eos_warning("msg=\"slow sync operation\" fxid=%08llx", mFileId);
      }

      return rc;
    }

    // Delegate sync call to a differet thread while the client is waiting for
    // the callback (SFS_STARTED)
    eos_info("msg=\"sync delegated to async thread\" fxid=%08llx path=\"%s\" "
             "fst_path=\"%s\"", mFileId, mNsPath.c_str(), mFstPath.c_str());
    auto sync_cb = std::make_shared<XrdOucCallBack>();
    sync_cb->Init(&error);
    error.setErrInfo(600, "delay client up to 10 min for sync call");
    gOFS.mAsyncOpThreadPool.PushTask<void>([&, sync_cb]() -> void {
      // Make a local copy since the XrdFstOfsFile object is destroyed after
      // the callback reply is called!
      const auto fid = mFileId;
      eos_info("msg=\"doing sync in async thread\", fxid=%08llx", fid);
      eos::common::Timing tm("sync");
      COMMONTIMING("begin", &tm);
      int rc = mLayout->Sync();
      COMMONTIMING("end", &tm);

      if (tm.RealTime() > 2000)
      {
        eos_warning("msg=\"slow sync operation\" fxid=%08llx", fid);
      }

      int reply_rc = sync_cb->Reply(rc, (rc ? error.getErrInfo() : 0),
                                    (rc ? error.getErrText() : ""));

      if (reply_rc)
      {
        eos_err("msg=\"sync callback reply failed\" fxid=%08llx", fid);
      }
    });
    return SFS_STARTED;
  }
}

//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
XrdFstOfsFile::truncate(XrdSfsFileOffset fsize)
{
  eos_info("mOpenSize=%llu fsize=%llu ", mOpenSize, fsize);

  if (mIsDevNull) {
    return SFS_OK;
  }

  if (fsize != mOpenSize) {
    if (mCheckSum) {
      if (mWritePosition != fsize) {
        mCheckSum->SetDirty();
      }
    }
  }

  int rc = mLayout->Truncate(fsize);

  if (!rc) {
    if (fsize != mOpenSize) {
      mHasWrite = true;
    }

    mWritePosition = fsize;
  }

  return rc;
}

//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
XrdFstOfsFile::close()
{
  gettimeofday(&closeStart, &tz);

  if (gOFS.mSimUnresponsive) {
    eos_warning("msg=\"simulate unresponsive close, delay by 120s\" "
                "fxid=%08llx", mFileId);
    std::this_thread::sleep_for(std::chrono::seconds(120));
  }

  // Reset the error.getErrInfo() value to 0 since this was hijacked by the
  // XrdXrootdFile object to store the actual file descriptor corresponding to
  // the current object. This was confusing when logging the error.getErrInfo()
  // value at the end of the close.
  error.setErrCode(0);
  static bool async_close_cfg = IsAsyncCloseConfigured();

  if (!async_close_cfg || DoSyncClose()) {
    return _close();
  }

  // Delegate close to a different thread while the client is waiting for the
  // callback (SFS_STARTED). This only happens for written files with size
  // bigger than min size bytes.
  eos_info("msg=\"close delegated to async thread\" fxid=%08llx "
           "path=\"%s\" fst_path=\"%s\"", mFileId, mNsPath.c_str(),
           mFstPath.c_str());
  // Create a close callback and put the client in waiting mode
  auto close_cb = std::make_shared<XrdOucCallBack>();
  close_cb->Init(&error);
  error.setErrInfo(1800, "delay client up to 30 minutes for close");
  gOFS.mAsyncOpThreadPool.PushTask<void>([&, close_cb]() -> void {
    // Make a local copy since the XrdFstOfsFile object is destroyed after
    // the callback reply is called!
    const auto fid = mFileId;
    eos_info("msg=\"doing close in the async thread\" fxid=%08llx", fid);
    int rc = _close();
    // During Reply() we expect the enclosing XrdFstOfsFile to be destroyed,
    // so we don't refer to anything captured by reference once done
    int reply_rc = close_cb->Reply(rc, (rc ? error.getErrInfo() : 0),
                                   (rc ? error.getErrText() : ""));

    if (reply_rc == 0)
    {
      eos_err("msg=\"close callback reply failed\" fxid=%08llx", fid);
    }
  });
  return SFS_STARTED;
}

//------------------------------------------------------------------------------
// Close file - internal method
//------------------------------------------------------------------------------
int
XrdFstOfsFile::_close()
{
  EPNAME("_close");
  int rc = 0; // return code

  // Any close on a file opened in TPC mode invalidates tpc keys
  if (!mTpcKey.empty()) {
    TpcCleanup();
  }

  // This must be done after the TPC cleanup no to leak tpc keys
  if (mIsDevNull) {
    eos_debug("%s", "msg=\"closing sink file i.e. /dev/null\"");
    mClosed = true;
    return SFS_OK;
  }

  // Enter close logic only once, as we can get an explicit close or a close
  // via the destructor
  if (!mOpened || mClosed) {
    eos_info("msg=\"close already done\" fxid=%08llx close_rc=%i",
             mFileId, mCloseRc);
    mClosed = true;
    return mCloseRc;
  }

  mCloseSize = mOpenSize;

  if (mIsRW) {
    if ((rc = _close_wr()) == 0) {
      gOFS.QueueForMgmSync(*mFmd.get());
    }
  } else {
    rc = _close_rd();
  }

  mClosed = true;

  // Prepare a report and add to the report queue
  if (mTpcFlag != kTpcSrcCanDo) {
    // We don't want a report for the source tpc setup. The kTpcSrcRead
    // stage actually uses the opaque info from kTpcSrcSetup and that's
    // why we also generate a report at this stage.
    XrdOucString report = "";
    gettimeofday(&closeStop, &tz);
    CloseTime();
    gettimeofday(&closeTime, &tz);

    // if we were kept in a cache (e.g. HttpHandlerFstFileCache)
    // use the last time we put placed in the cache as the closetime
    // (as this was the last time the user sent a request) for the
    // purpose of the stats
    if (cacheITime.tv_sec != 0) {
      closeTime = cacheITime;
      const unsigned long mus = timeToClose * 1000.0;
      closeTime.tv_sec += (mus / 1000000);
      closeTime.tv_usec += (mus % 1000000);

      if (closeTime.tv_usec >= 1000000) {
        closeTime.tv_sec++;
        closeTime.tv_usec -= 1000000;
      }
    }

    MakeReportEnv(report);
    eos_static_info("msg=\"%s\"", report.c_str());

    if (eos::common::LayoutId::IsRain(mLid) && !mLayout->IsEntryServer()) {
      // Non-entry RAIN stripes do not report any statistics
    } else {
      gOFS.ReportQueueMutex.Lock();
      gOFS.ReportQueue.push(report);
      gOFS.ReportQueueMutex.UnLock();
    }
  }

  // CTA: Trigger an MGM event from the entry server
  if ((rc == 0) && mLayout->IsEntryServer() &&
      (mEventOnClose || mSyncEventOnClose)) {
    rc = TriggerEventOnClose(mArchiveReqId);
  }

  // Mask close error for fuse, if the file has been removed already
  if (mFusex && mFusexIsUnlinked) {
    error.setErrCode(0);
    rc = 0;
  }

  eos_info("msg=\"done close\" rc=%i errc=%d", rc, error.getErrInfo());
  mCloseRc = rc;
  return rc;
}

//------------------------------------------------------------------------------
// Close file opened for read
//------------------------------------------------------------------------------
int
XrdFstOfsFile::_close_rd()
{
  EPNAME("close_rd");
  bool checksum_err = VerifyChecksum();

  if (gOFS.mSimXsReadErr) {
    eos_warning("msg=\"simulate read xs error\" fxid=%08llx", mFileId);
    checksum_err = true;
  }

  int close_rc = ModifiedWhileInUse();
  close_rc |= mLayout->Close();

  if (gOFS.mSimCloseErr) {
    eos_warning("msg=\"simulate close error\" fxid=%08llx", mFileId);
    close_rc = SFS_ERROR;
  }

  gOFS.openedForReading.down(mFsId, mFileId);

  if (checksum_err) {
    int envlen = 0;
    eos_crit("msg=\"file checksum error detected\" info=\"%s\"",
             mCapOpaque->Env(envlen));
    return gOFS.Emsg(epname, error, EIO, "verify checksum - checksum "
                     "error fn=", mNsPath.c_str());
  }

  return close_rc;
}

//------------------------------------------------------------------------------
// Close file opened for write
//------------------------------------------------------------------------------
int
XrdFstOfsFile::_close_wr()
{
  EPNAME("close_wr");
  bool checksum_err = false; // full file checksum error
  bool unit_checksum_err = false; // unit checksum error
  bool target_sz_err = false; // final target file size error
  bool min_sz_err = false; // minimum file size policy error
  bool queuing_err = false; // queuing error for archive
  bool consistency_err = false; // consistency error at the MGM
  bool atomic_overlap = false;
  std::string queuing_msg;
  int rc = 0;
  // Check if the file close comes from a client disconnect e.g. the destructor
  eos_info("viaDelete=%d writeDelete=%d mIsCreation=%d",
           viaDelete, mWrDelete, mIsCreation);
  bool last_writer = (gOFS.openedForWriting.getUseCount(mFsId, mFileId) <= 1);

  if ((viaDelete || mWrDelete) && !mFusex &&
      (mIsCreation || mIsReplication || mIsInjection || mIsOCchunk)) {
    if (last_writer) {
      // It is closed by the destructor e.g. no proper close
      // or the specified checksum does not match the computed one
      if (viaDelete) {
        eos_info("msg=\"(unpersist) deleting file\" reason=\"client disconnect\""
                 " fxid=%08llx fsid=%u", mFileId, mFsId);
      }

      if (mWrDelete) {
        eos_info("msg=\"(unpersist) deleting file\" reason=\"write/policy error\""
                 " fxid=%08llx fsid=%u", mFileId, mFsId);
      }

      mDelOnClose = true;
    } else {
      eos_info("msg=\"(unpersist) suppress delete on close\" reason=\"several "
               "writers\" fxid=%08llx fsid=%u", mFileId, mFsId);
    }
  }

  if (!mDelOnClose) {
    // Check if this was a newly created file
    if (mIsCreation) {
      // If space allocated truncated to the real size of the file
      if (eos::common::LayoutId::IsRain(mLayout->GetLayoutId())) {
        // For RAIN only the entry server truncates, unless this is a recovery
        if (mLayout->IsEntryServer() && !mRainReconstruct) {
          eos_info("msg=\"truncate RAIN file\" offset=%llu", mMaxOffsetWritten);
          mLayout->Truncate(mMaxOffsetWritten);
        }

        //@note: there is a small probability here to have a race condition when
        // computing the checksum for RAIN file in non-streaming mode. We should
        // first collect all write replies and then re-read the file for the xs.
      } else {
        if (mMaxOffsetWritten > mOpenSize) {
          // Check if we have to deallocate something for this file transaction
          if (mBookingSize && (mBookingSize > mMaxOffsetWritten)) {
            eos_debug("msg=\"deallocationg %llu bytes\" fxid=%08llx",
                      mBookingSize - mMaxOffsetWritten, mFileId);
            mLayout->Truncate(mMaxOffsetWritten);
            mLayout->Fdeallocate(mMaxOffsetWritten, mBookingSize);
          }
        }
      }

      if (!eos::common::LayoutId::IsRain(mLayout->GetLayoutId())) {
        // Check target and minimum size policy only for non RAIN files
        target_sz_err = (mTargetSize) ? (mTargetSize != mMaxOffsetWritten) : false;
        min_sz_err = (mMinSize) ? ((off_t) mMaxOffsetWritten < mMinSize) : false;
      }
    }

    checksum_err = VerifyChecksum();
    eos_debug("checksum_err=%i target_sz_err=%i max_offset_written=%zu "
              "target_size=%lli", checksum_err, target_sz_err,
              mMaxOffsetWritten, mTargetSize);

    // Error simulation for checksum errors
    if (gOFS.mSimXsWriteErr) {
      eos_warning("msg=\"simulate write xs error\" fxid=%08llx", mFileId);
      checksum_err = true;

      if (gOFS.mSimXsWriteErrDelay) {
        std::this_thread::sleep_for(std::chrono::seconds(
                                      gOFS.mSimXsWriteErrDelay.load()));
      }
    }

    if (!mLayout->IsEntryServer()) {
      unit_checksum_err = VerifyUnitChecksum();
      eos_debug("unit_checksum_err=%i", unit_checksum_err);
    }

    if (checksum_err || target_sz_err || min_sz_err || unit_checksum_err) {
      mDelOnClose = true;
    }
  }

  // When doing RAIN reconstruction and we are at the entry server we have
  // any read errors or we read less then the full file size it means the
  // recovery failed. This can also be a side effect of a timeout.
  if (mRainReconstruct && mLayout->IsEntryServer()) {
    if (mHasReadErr || (rOffset != mRainSize)) {
      eos_warning("msg=\"failed RAIN reconstruct trigger delete on close\" "
                  "mHasReadErr=%i rd_off=%llu fsize=%llu fxid=%08llx",
                  mHasReadErr, rOffset, mRainSize, mFileId);
      mDelOnClose = true;
    }
  }

  if (!mDelOnClose && (mIsCreation || mHasWrite)) {
    // Commit meta data
    struct stat statinfo;

    if ((rc = mLayout->Stat(&statinfo))) {
      eos_err("msg=\"failed to stat file\" fxid=%08llx", mFileId);
      rc = gOFS.Emsg(epname, error, EIO, "close - cannot stat closed layout"
                     " to determine file size", mNsPath.c_str());
    } else {
      // CTA: Attempt archive queueing if tape support enabled
      if (mTapeEnabled && mSyncEventOnClose &&
          mIsCreation && mLayout->IsEntryServer() &&
          (mEventWorkflow != common::RETRIEVE_WRITTEN_WORKFLOW_NAME)) {
        // Queueing error: queueing for archive failed
        queuing_err = !QueueForArchiving(statinfo, queuing_msg, mArchiveReqId);
        mDelOnClose = queuing_err;
      }

      // For RAIN file, if it's the entry server, we do the commit later, after the close.
      if (!mDelOnClose && !(eos::common::LayoutId::IsRain(mLayout->GetLayoutId()) &&
                            mLayout->IsEntryServer())) {
        // Update size
        mCloseSize = statinfo.st_size;

        if (CommitToLocalFmd(statinfo)) {
          eos_err("msg=\"failed to commit fmd info\" fxid=%08llx", mFileId);
          mDelOnClose = true;
        } else {
          // In case we are doing a RAIN reconstruct delay the commit to MGM
          // until after we have the result of the CLOSE otherwise we risk
          // dropping a good replica for a failed reconstruction which we
          // can not get back.
          if ((mRainReconstruct == false) && (rc = CommitToMgm())) {
            if ((error.getErrInfo() == EIDRM) ||
                (error.getErrInfo() == EBADE) ||
                (error.getErrInfo() == EBADR) ||
                (error.getErrInfo() == EREMCHG)) {
              if (error.getErrInfo() == EIDRM) {
                // File has been deleted in the meanwhile ... we can unlink
                eos_err("msg=\"unlink file since already removed in the ns\"  "
                        "fxid=%08llx path=\"%s\"", mFileId, mNsPath.c_str());
                mFusexIsUnlinked = true;
              }

              if (error.getErrInfo() == EBADE) {
                eos_err("msg=\"unlink file since size does not match "
                        "reference\" fxid=%08llx path=\"%s\"",
                        mFileId, mNsPath.c_str());
                consistency_err = true;
              }

              if (error.getErrInfo() == EBADR) {
                eos_err("msg=\"unlink file since checksum does not match "
                        "reference\" fxid=%08llx path=\"%s\"",
                        mFileId, mNsPath.c_str());
                consistency_err = true;
              }

              if (error.getErrInfo() == EREMCHG) {
                eos_err("msg=\"unlinking fxid=%08llx path=%s - "
                        "overlapping atomic upload - discarding this one\"",
                        mFmd->mProtoFmd.fid(), mNsPath.c_str());
                atomic_overlap = true;
              }

              // Any of the above errors will trigger a delete on close
              mDelOnClose = true;
            } else {
              eos_crit("msg=\"commit returned unknown error (maybe timeout), "
                       "close transaction to keep file safe\" msg=\"%s\" rc=%d",
                       error.getErrText(), rc);
            }
          }
        }
      }
    }
  }

  // Recompute our ETag
  eos::calculateEtag(mCheckSum != nullptr, mFmd->mProtoFmd, mEtag);
  int commit_rc = rc; // return of the commit/stat before the layout close

  if (mSyncOnClose) {
    eos_info("msg=\"syncing layout for iotype=csync\" fxid=%08llx", mFileId);
    rc |= mLayout->Sync();
  }

  int close_rc = mLayout->Close();

  if (gOFS.mSimCloseErr) {
    eos_warning("msg=\"simulate close error\" fxid=%08llx", mFileId);
    close_rc = SFS_ERROR;
  }

  rc |= close_rc;

  if (close_rc) {
    eos_info("msg=\"layout close failed\" rc=%i", close_rc);

    // For RAIN layouts if there is an error on close when writing then we
    // delete the whole file. For RAIN reconstruction we clean the local stripe.
    if (eos::common::LayoutId::IsRain(mLayout->GetLayoutId())) {
      mDelOnClose = true;
    } else {
      // Some (remote) replica didn't make it through ... trigger an auto-repair
      if (!mDelOnClose) {
        mRepairOnClose = true;
      }
    }
  }

  // If target file system is in some non-operational mode, then abort commit
  if (mIsCreation && !gOFS.Storage->IsFsOperational(mFsId)) {
    eos_notice("msg=\"fail transfer since filesystem is in non-operational "
               "state\" fxid=%08llx fsid=%u", mFileId, mFsId);
    mDelOnClose = true;
  }

  {
    XrdSysMutexHelper scope_lock(gOFS.OpenFidMutex);

    if ((rc == 0) && (mDelOnClose == false) &&
        (mIsRW || mIsInjection || mIsOCchunk) &&
        (gOFS.openedForWriting.getUseCount(mFsId, mFileId) > 1)) {
      // Indicate that this file was closed properly and disable further
      // delete on close for concurrent write operations
      gOFS.WNoDeleteOnCloseFid[mFsId][mFileId] = true;
    }

    gOFS.openedForWriting.down(mFsId, mFileId);

    if (mDelOnClose && gOFS.WNoDeleteOnCloseFid[mFsId].count(mFileId)) {
      eos_notice("msg=\"prohibit delete on close since we had a previous "
                 "successful close\" fxid=%08llx path=\"%s\"",
                 mFileId, mNsPath.c_str());
      mDelOnClose = false;
    }

    if (gOFS.openedForWriting.isOpen(mFsId, mFileId) == false) {
      // When the last writer is gone we can remove the prohibiting entry
      gOFS.WNoDeleteOnCloseFid[mFsId].erase(mFileId);
      gOFS.WNoDeleteOnCloseFid[mFsId].resize(0);
    }
  }

  // Commit to MGM in case of rain reconstruction and not del on close
  if (eos::common::LayoutId::IsRain(mLayout->GetLayoutId()) &&
      mLayout->IsEntryServer() && !mDelOnClose) {
    unit_checksum_err = VerifyUnitChecksum();

    if (unit_checksum_err) {
      eos_err("msg=\"error verifying unit checksum\" fxid=%08llx", mFileId);
      mDelOnClose = true;
    }

    mCloseSize = totalBytes;
    struct stat info;
    info.st_size = totalBytes;

    if (CommitToLocalFmd(info)) {
      eos_err("msg=\"failed to commit fmd info\" fxid=%08llx", mFileId);
      mDelOnClose = true;
    } else {
      if ((rc = CommitToMgm())) {
        if ((error.getErrInfo() == EIDRM) ||
            (error.getErrInfo() == EBADE) ||
            (error.getErrInfo() == EBADR) ||
            (error.getErrInfo() == EREMCHG)) {
          eos_err("msg=\"failed commit to MGM for RAIN\" "
                  "fxid=%08llx", mFileId);
          mDelOnClose = true;
        }
      }
    }
  }

  if (mDelOnClose && !mFusex &&
      ( // Match a newly created simple replica/stripe
        (mIsCreation && !mRainReconstruct) ||
        // Match a reconstructed RAIN stripe so that we never delete stripes
        // that are part of the reconstruction process but are not the target
        // of the recovery!
        (mIsCreation && mRainReconstruct) ||
        mIsReplication || mIsInjection || mIsOCchunk)) {
    rc = SFS_ERROR;
    eos_err("msg=\"delete on close\" fxid=%08llx ns_path=\"%s\" ", mFileId,
            mNsPath.c_str());
    int retc = gOFS._rem(mNsPath.c_str(), error, 0, mCapOpaque.get(),
                         mFstPath.c_str(), mFileId, mFsId, true);

    if (retc) {
      eos_debug("msg=\"local remove operation\" fxid=%08llx retc=%d",
                mFileId, retc);
    }

    // Unlink file or just current replica from the MGM
    bool drop_all = false;

    // If mDelOnClose at the gateway then we drop all replicas
    if (mLayout->IsEntryServer() && mIsCreation &&
        !mIsReplication && !mIsInjection &&
        !mIsOCchunk && !mRainReconstruct) {
      drop_all = true;
      mLayout->Remove();
    }

    DropFromMgm(mFileId, (drop_all ? 0u : mFsId), mNsPath.c_str(),
                mRdrManager.c_str());

    if (min_sz_err) {
      // Minimum size criteria not fullfilled
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned "
                "because it is smaller than the required minimum file size"
                " in that directory", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason="
                  "\"minimum file size criteria\"", mNsPath.c_str(),
                  mFstPath.c_str());
    } else if (checksum_err) {
      // Checksum error
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned "
                "because of a checksum error ", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason="
                  "\"checksum error\"", mNsPath.c_str(), mFstPath.c_str());
    } else if (writeErrorFlag == kOfsSimulatedIoError) {
      // Simulated write error
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned "
                "because of a simulated IO error ", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason="
                  "\"simulated IO error\"", mNsPath.c_str(), mFstPath.c_str());
    } else if (writeErrorFlag == kOfsMaxSizeError) {
      // Maximum size criteria not fullfilled
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned "
                "because you exceeded the maximum file size settings for "
                "this namespace branch", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason="
                  "\"maximum file size criteria\"", mNsPath.c_str(),
                  mFstPath.c_str());
    } else if (writeErrorFlag == kOfsDiskFullError) {
      // Disk full detected during write
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned"
                " because the target disk filesystem got full and you "
                "didn't use reservation", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason="
                  "\"filesystem full\"", mNsPath.c_str(), mFstPath.c_str());
    } else if (writeErrorFlag == kOfsIoError) {
      // Generic IO error on the underlying device
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned because"
                " of an IO error during a write operation", mNsPath.c_str());
      eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason="
               "\"write IO error\"", mNsPath.c_str(), mFstPath.c_str());
    } else if (writeErrorFlag == kOfsFsRemovedError) {
      // Filesystem has been unregistered
      gOFS.Emsg(epname, error, EIO, "store file - file has been cleaned because"
                " the target filesystem has been unregistered", mNsPath.c_str());
      eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason="
               "\"FS removed\"", mNsPath.c_str(), mFstPath.c_str());
    } else if (target_sz_err) {
      // Target size is different from the uploaded file size
      gOFS.Emsg(epname, error, EIO, "store file - file has been "
                "cleaned because the stored file does not match "
                "the provided targetsize", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason="
                  "\"target size mismatch\"", mCapOpaque->Get("mgm.path"),
                  mFstPath.c_str());
    } else if (consistency_err) {
      gOFS.Emsg(epname, error, EIO, "store file - file has been "
                "cleaned because the stored file does not match "
                "the reference meta-data size/checksum", mNsPath.c_str());
      eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason="
               "\"meta-data size/checksum mismatch\"", mNsPath.c_str(),
               mFstPath.c_str());
    } else if (atomic_overlap) {
      gOFS.Emsg(epname, error, EIO, "store file - file has been "
                "cleaned because of an overlapping atomic upload "
                "and we are not the last uploader", mNsPath.c_str());
      eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason="
               "\"suppressed atomic upload\"", mNsPath.c_str(),
               mFstPath.c_str());
    } else if (queuing_err) {
      std::string message =
        SSTR("store file - file has been cleaned because of a queueing "
             << "to archive error; reason=\"" << queuing_msg << "\"");
      gOFS.Emsg(epname, error, EIO, message.c_str(), mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s reason=\"%s\"",
                  mNsPath.c_str(), mFstPath.c_str(), queuing_msg.c_str());
    } else if (close_rc == SFS_ERROR) {
      gOFS.Emsg(epname, error, EIO, "store file - file has been "
                "cleaned or recovery aborted because of an error on close",
                mNsPath.c_str());
      eos_crit("info=\"deleting on close\" fn=%s fstpath=%s reason="
               "\"failed layout close\"", mNsPath.c_str(), mFstPath.c_str());
    } else {
      // Client has disconnected and file is cleaned-up
      gOFS.Emsg(epname, error, EIO, "store file - file has been "
                "cleaned because of a client disconnect", mNsPath.c_str());
      eos_warning("info=\"deleting on close\" fn=%s fstpath=%s "
                  "reason=\"client disconnect\"", mNsPath.c_str(),
                  mFstPath.c_str());
    }
  }

  if (mRepairOnClose && !mIsOCchunk) {
    // Trigger adjust replica for the uploaded file
    std::ostringstream oss;
    oss << "/?mgm.pcmd=adjustreplica&mgm.path=" << mNsPath;
    eos_info("msg=\"repair on close\" fxid=%08llx path=%s", mFileId,
             mNsPath.c_str());

    if (gOFS.CallManager(&error, mNsPath.c_str(), mRdrManager.c_str(),
                         oss.str())) {
      eos_err("msg=\"failed adjustreplica\" fxid=%08llx path=%s",
              mFileId, mNsPath.c_str());
      gOFS.Emsg(epname, error, EIO, "create all replicas - uploaded file is at "
                "risk - only one replica has been successfully stored for fn=",
                mNsPath.c_str());
    } else {
      eos_warning("msg=\"executed adjustreplica, file is at low risk due to "
                  "missing replicas\" fxid=%08llx path=%s", mFileId,
                  mNsPath.c_str());

      if (commit_rc == 0) {
        // Reset the return code and clean error message
        error.setErrInfo(0, "");
        rc = 0;
      }
    }
  }

  return rc;
}

//------------------------------------------------------------------------------
// Implementation dependant commands
//------------------------------------------------------------------------------
int
XrdFstOfsFile::fctl(const int cmd, int alen, const char* args,
                    const XrdSecEntity* client)
{
  eos_debug("cmd=%i, args=%s", cmd, args);

  if (cmd == SFS_FCTL_SPEC1) {
    if (strncmp(args, "delete", alen) == 0) {
      eos_warning("Setting deletion flag for file %s", mFstPath.c_str());
      // This indicates to delete the file during the close operation
      viaDelete = true;
      return SFS_OK;
    } else if (strncmp(args, "nochecksum", alen) == 0) {
      int retc = SFS_OK;
      eos_warning("Setting nochecksum flag for file %s", mFstPath.c_str());
      mCheckSum.reset(nullptr);

      // Propagate command to all the replicas/stripes
      if (mLayout) {
        retc = mLayout->Fctl(std::string(args), client);
      }

      return retc;
    }
  }

  error.setErrInfo(ENOTSUP, "fctl command not supported");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Low-level open calling the default XrdOfs plugin
//------------------------------------------------------------------------------
int
XrdFstOfsFile::openofs(const char* path,
                       XrdSfsFileOpenMode open_mode,
                       mode_t create_mode,
                       const XrdSecEntity* client,
                       const char* opaque)
{
  int retc = 0;

  while ((retc = XrdOfsFile::open(path, open_mode, create_mode, client,
                                  opaque)) > 0) {
    eos_static_notice("msg\"xrootd-lock-table busy - snoozing & retry\" "
                      "delay=%d errno=%d", retc, errno);
    std::this_thread::sleep_for(std::chrono::seconds(retc));
  }

  return retc;
}

//------------------------------------------------------------------------------
// Low-level read calling the default XrdOfs plugin
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::readofs(XrdSfsFileOffset fileOffset, char* buffer,
                       XrdSfsXferSize buffer_size)
{
  //  EPNAME("read");
  gettimeofday(&cTime, &tz);
  rCalls++;

  if (!getenv("EOS_FST_NO_IOPRIORITY")) {
    if (ioprio_begin(IOPRIO_WHO_PROCESS, IOPRIO_PRIO_VALUE(mIoPriorityClass,
                     mIoPriorityValue), t_iopriority)) {
      if (!mIoPriorityErrorReported) {
        eos_warning("failed to set IO priority to %d:%d - errno=%d\n", mIoPriorityClass,
                    mIoPriorityValue, errno);
      }
    }
  }

  int rc = XrdOfsFile::read(fileOffset, buffer, buffer_size);
  eos_debug("read %llu %llu %i rc=%d", this, fileOffset, buffer_size, rc);

  if (!getenv("EOS_FST_NO_IOPRIORITY")) {
    t_iopriority = ioprio_end(IOPRIO_WHO_PROCESS,
                              IOPRIO_PRIO_VALUE(mIoPriorityClass, mIoPriorityValue));
  }

  if (gOFS.mSimIoReadErr) {
    if ((gOFS.mSimErrIoReadOff == 0) ||
        (gOFS.mSimErrIoReadOff <= (uint64_t)fileOffset)) {
      eos_warning("msg=\"simulate read IO error\" fxid=%08llx", mFileId);
      return gOFS.Emsg("readofs", error, EIO, "read file - simulated IO error fn=",
                       mNsPath.c_str());
    }
  }

  if (mFsId) {
    if (!gOFS.Storage->mFsMap.count(mFsId)) {
      return gOFS.Emsg("readeofs", error, EBADF,
                       "read file - filesystem has been unregistered");
    }
  }

  // Account seeks for monitoring
  if (rOffset != static_cast<unsigned long long>(fileOffset)) {
    if (rOffset < static_cast<unsigned long long>(fileOffset)) {
      nFwdSeeks++;
      sFwdBytes += (fileOffset - rOffset);
    } else {
      nBwdSeeks++;
      sBwdBytes += (rOffset - fileOffset);
    }

    if ((rOffset + (EOS_FSTOFS_LARGE_SEEKS)) < (static_cast<unsigned long long>
        (fileOffset))) {
      sXlFwdBytes += (fileOffset - rOffset);
      nXlFwdSeeks++;
    }

    if ((static_cast<unsigned long long>(rOffset) > (EOS_FSTOFS_LARGE_SEEKS)) &&
        (rOffset - (EOS_FSTOFS_LARGE_SEEKS)) > (static_cast<unsigned long long>
            (fileOffset))) {
      sXlBwdBytes += (rOffset - fileOffset);
      nXlBwdSeeks++;
    }
  }

  gettimeofday(&lrTime, &tz);
  AddReadTime();
  return rc;
}

//------------------------------------------------------------------------------
// Low-level vector read calling the default XrdOfs plugin
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::readvofs(XrdOucIOVec* readV, uint32_t readCount)
{
  eos_debug("read count=%i", readCount);
  gettimeofday(&cTime, &tz);
  XrdSfsXferSize sz = XrdOfsFile::readv(readV, readCount);
  gettimeofday(&lrvTime, &tz);
  AddReadVTime();

  // Collect monitoring info only if sz is > 0
  if (sz > 0) {
    XrdSysMutexHelper scope_lock(vecMutex);

    for (uint32_t i = 0; i < readCount; ++i) {
      monReadSingleBytes.push_back(readV[i].size);
    }

    monReadvBytes.push_back(sz);
    monReadvCount.push_back(readCount);
  }

  return sz;
}

//------------------------------------------------------------------------------
// Low-level write calling the default XrdOfs plugin
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdFstOfsFile::writeofs(XrdSfsFileOffset fileOffset, const char* buffer,
                        XrdSfsXferSize buffer_size)
{
  if (gOFS.mSimIoWriteErr) {
    if ((gOFS.mSimErrIoWriteOff == 0) ||
        (gOFS.mSimErrIoWriteOff <= (uint64_t)fileOffset)) {
      writeErrorFlag = kOfsSimulatedIoError;
      eos_warning("msg=\"simulate write IO error\" fxid=%08llx", mFileId);
      return gOFS.Emsg("writeofs", error, EIO, "write file - simulated IO error fn=",
                       mNsPath.c_str());
    }
  }

  if (mFsId) {
    if ((mTargetSize && (mTargetSize == mBookingSize)) ||
        (mBookingSize >= fileOffset + buffer_size)) {
      // Space has been successfully pre-allocated, let client write
    } else {
      // Check if the file system is full
      bool isfull = false;
      {
        XrdSysMutexHelper lock(gOFS.Storage->mFsFullMapMutex);
        isfull = gOFS.Storage->mFsFullMap[mFsId];
      }

      if (isfull) {
        writeErrorFlag = kOfsDiskFullError;
        return gOFS.Emsg("writeofs", error, ENOSPC, "write file - disk space "
                         "(headroom) exceeded fn=", mCapOpaque ?
                         (mCapOpaque->Get("mgm.path") ? mCapOpaque->Get("mgm.path") :
                          FName()) : FName());
      }
    }

    // check if the filesystem was unregistered in the meanwhile
    eos::common::RWMutexReadLock lock(gOFS.Storage->mFsMutex);

    if (!gOFS.Storage->mFsMap.count(mFsId)) {
      writeErrorFlag = kOfsFsRemovedError;
      return gOFS.Emsg("writeofs", error, EBADF,
                       "write file - filesystem has been unregistered");
    }
  }

  if (mMaxSize) {
    // Check that the user didn't exceed the maximum file size policy
    if ((fileOffset + buffer_size) > mMaxSize) {
      writeErrorFlag = kOfsMaxSizeError;
      return gOFS.Emsg("writeofs", error, ENOSPC, "write file - your file "
                       "exceeds the maximum file size setting of bytes<=",
                       mCapOpaque ? (mCapOpaque->Get("mgm.maxsize") ?
                                     mCapOpaque->Get("mgm.maxsize") : "<undef>") : "undef");
    }
  }

  gettimeofday(&cTime, &tz);
  wCalls++;
  int rc;

  if (!getenv("EOS_FST_NO_IOPRIORITY")) {
    if (ioprio_begin(IOPRIO_WHO_PROCESS, IOPRIO_PRIO_VALUE(mIoPriorityClass,
                     mIoPriorityValue), t_iopriority)) {
      if (!mIoPriorityErrorReported) {
        eos_warning("failed to set IO priority to %d:%d - errno=%d\n", mIoPriorityClass,
                    mIoPriorityValue, errno);
        mIoPriorityErrorReported = true;
      }
    }
  }

  if (gOFS.mSimDiskWriting) {
    // Simulate disk writing by only truncating
    eos_warning("msg=\"simulate disk writing - do truncate\" fxid=%08llx",
                mFileId);
    XrdFstOfsFile::truncateofs(fileOffset);
    rc = buffer_size;
  } else {
    rc = XrdOfsFile::write(fileOffset, buffer, buffer_size);
  }

  if (!getenv("EOS_FST_NO_IOPRIORITY")) {
    t_iopriority = ioprio_end(IOPRIO_WHO_PROCESS,
                              IOPRIO_PRIO_VALUE(mIoPriorityClass, mIoPriorityValue));
  }

  if (rc != buffer_size) {
    // Tag an io error
    writeErrorFlag = kOfsIoError;
  }

  // Account seeks for monitoring
  if (wOffset != static_cast<unsigned long long>(fileOffset)) {
    if (wOffset < static_cast<unsigned long long>(fileOffset)) {
      nFwdSeeks++;
      sFwdBytes += (fileOffset - wOffset);
    } else {
      nBwdSeeks++;
      sBwdBytes += (wOffset - fileOffset);
    }

    if ((wOffset + (EOS_FSTOFS_LARGE_SEEKS)) < (static_cast<unsigned long long>
        (fileOffset))) {
      sXlFwdBytes += (fileOffset - wOffset);
      nXlFwdSeeks++;
    }

    if ((static_cast<unsigned long long>(wOffset) > (EOS_FSTOFS_LARGE_SEEKS)) &&
        (wOffset - (EOS_FSTOFS_LARGE_SEEKS)) > (static_cast<unsigned long long>
            (fileOffset))) {
      sXlBwdBytes += (wOffset - fileOffset);
      nXlBwdSeeks++;
    }
  }

  if (rc > 0) {
    wOffset = fileOffset + rc;
  }

  gettimeofday(&lwTime, &tz);
  AddWriteTime();
  return rc;
}

//------------------------------------------------------------------------------
// Low-level sync calling the default XrdOfs plugin
//------------------------------------------------------------------------------
int
XrdFstOfsFile::syncofs()
{
  return XrdOfsFile::sync();
}

//------------------------------------------------------------------------------
// Low-level truncate calling the default XrdOfs plugin
//------------------------------------------------------------------------------
int
XrdFstOfsFile::truncateofs(XrdSfsFileOffset fileOffset)
{
  // Truncation moves the max offset written
  eos_debug("value=%llu", (unsigned long long) fileOffset);
  mMaxOffsetWritten = fileOffset;
  struct stat buf;

  // stat the current file size
  // if the file has the proper size we don't truncate
  if (!::stat(mFstPath.c_str(), &buf)) {
    // if the file has the proper size we don't truncate
    if (buf.st_size == fileOffset) {
      return SFS_OK;
    }
  }

  return XrdOfsFile::truncate(fileOffset);
}

//------------------------------------------------------------------------------
// Low-level close calling the default XrdOfs plugin
//------------------------------------------------------------------------------
int
XrdFstOfsFile::closeofs()
{
  return XrdOfsFile::close();
}

//------------------------------------------------------------------------------
// Return FMD checksum
//------------------------------------------------------------------------------
std::string
XrdFstOfsFile::GetFmdChecksum() const
{
  if (mFmd) {
    return mFmd->mProtoFmd.checksum();
  } else {
    return std::string();
  }
}

//------------------------------------------------------------------------------
// Verify if a TPC key is still valid
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::TpcValid() const
{
  XrdSysMutexHelper scope_lock(gOFS.TpcMapMutex);

  if (mTpcKey.length() &&  gOFS.TpcMap[mIsTpcDst].count(mTpcKey)) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Process open opaque information - this can come directly from the client
// or from the MGM redirection and it's not encrypted but sent in plain
// text in the URL
//------------------------------------------------------------------------------
int
XrdFstOfsFile::ProcessOpenOpaque()
{
  using namespace std::chrono;
  EPNAME("open");

  if (!mOpenOpaque) {
    eos_warning("msg=\"no open opaque info to process\"");
    return SFS_OK;
  }

  char* val = nullptr;

  // Handle various tags which are sent in plain text e.g. mgm.etag
  // Extract ETag from the redirection URL if available
  if ((val = mOpenOpaque->Get("mgm.etag"))) {
    mEtag = val;
  }

  // mgm.mtime=0 we set the mtime externaly. This indicates that during commit,
  // it should not update the mtime as in the case of FUSE clients which will
  // call utimes.
  if ((val = mOpenOpaque->Get("mgm.mtime"))) {
    time_t mtime = (time_t)strtoull(val, 0, 10);

    if (mtime == 0) {
      mForcedMtime = 0;
      mForcedMtime_ms = 0;
    } else {
      mForcedMtime = mtime;
      mForcedMtime_ms = 0;
    }
  }

  // mgm.fusex=1 - Suppress the file close broadcast to the fusex network
  // during the file close
  if ((val = mOpenOpaque->Get("mgm.fusex"))) {
    mFusex = true;
  }

  // Handle workflow events
  if ((val = mOpenOpaque->Get("mgm.event"))) {
    std::string event = val;

    if (event == "closew") {
      mEventOnClose = true;
    } else if (event == "sync::closew") {
      mSyncEventOnClose = true;
    }

    if ((val = mOpenOpaque->Get("mgm.workflow"))) {
      mEventWorkflow = val;
    }

    val = mOpenOpaque->Get("mgm.instance");
    mEventInstance = val ? val : "";
    val = mOpenOpaque->Get("mgm.owner_uid");
    mEventOwnerUid = val ? std::stoul(val) : 99;
    val = mOpenOpaque->Get("mgm.owner_gid");
    mEventOwnerGid = val ? std::stoul(val) : 99;
    val = mOpenOpaque->Get("mgm.requestor");
    mEventRequestor = val ? val : "";
    val = mOpenOpaque->Get("mgm.requestorgroup");
    mEventRequestorGroup = val ? val : "";
    val = mOpenOpaque->Get("mgm.attributes");
    mEventAttributes = val ? val : "";
  }

  if ((val = mOpenOpaque->Get("eos.injection"))) {
    mIsInjection = true;
  }

  // enable round-robin scheduling per application/fsid on request
  if ((val = mOpenOpaque->Get("eos.schedule"))) {
    mAppRR = mSecMap["app"];
  }

  // Tag as an OC chunk upload
  if (eos::common::OwnCloud::isChunkUpload(*mOpenOpaque.get())) {
    mIsOCchunk = true;
  }

  if ((val = mOpenOpaque->Get("x-upload-range"))) {
    // For partial range uploads via HTTP we run the same business logic as
    // for OC chunk uploads
    mIsOCchunk = true;
  }

  // Check if transfer is still valid to avoid any open replays
  if ((val = mOpenOpaque->Get("fst.valid"))) {
    try {
      std::string sval = val;
      int64_t valid_sec = std::stoll(sval);
      auto now = system_clock::now();
      auto now_sec = time_point_cast<seconds>(now).time_since_epoch().count();

      if (valid_sec < now_sec) {
        eos_err("msg=\"fst validity expired, avoid open replay\"");
        return gOFS.Emsg(epname, error, EINVAL, "open - fst validity expired",
                         mNsPath.c_str());
      }
    } catch (...) {
      // ignore
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Process capability opaque information - this is encrypted information sent
// by the MGM to the FST
//------------------------------------------------------------------------------
int
XrdFstOfsFile::ProcessCapOpaque(bool& is_repair_read,
                                eos::common::VirtualIdentity& vid)
{
  EPNAME("open");

  if (!mCapOpaque) {
    eos_warning("msg=\"no cap opaque info to process\"");
    return SFS_OK;
  }

  int envlen {0};
  XrdOucString maskOpaque = mCapOpaque->Env(envlen);
  eos::common::StringConversion::MaskTag(maskOpaque, "mgm.obfuscate.key");
  eos::common::StringConversion::MaskTag(maskOpaque, "mgm.encryption.key");
  eos_info("capability=%s", maskOpaque.c_str());
  char* val = nullptr;
  const char* hexfid = 0;
  const char* slid = 0;
  const char* secinfo = 0;
  const char* scid = 0;
  const char* smanager = 0;

  // Determine whether or not support for tape is enabled in the MGM
  if (mCapOpaque->Get("tapeenabled")) {
    mTapeEnabled = true;
  }

  // Handle file id info
  if (!(hexfid = mCapOpaque->Get("mgm.fid"))) {
    return gOFS.Emsg(epname, error, EINVAL, "open - no file id in capability",
                     mNsPath.c_str());
  }

  mFileId = eos::common::FileId::Hex2Fid(hexfid);

  // Handle security info
  if (!(secinfo = mCapOpaque->Get("mgm.sec"))) {
    return gOFS.Emsg(epname, error, EINVAL,
                     "open - no security information in capability", mNsPath.c_str());
  } else {
    mSecString = secinfo;
    mSecMap = eos::common::SecEntity::KeyToMap(std::string(secinfo));
  }

  // Handle min size value
  if ((val = mCapOpaque->Get("mgm.minsize"))) {
    errno = 0;
    mMinSize = strtoull(val, 0, 10);

    if (errno) {
      eos_err("illegal minimum file size specified <%s>- restricting to 1 byte", val);
      mMinSize = 1;
    }
  } else {
    mMinSize = 0;
  }

  // Handle max size value
  if ((val = mCapOpaque->Get("mgm.maxsize"))) {
    errno = 0;
    mMaxSize = strtoull(val, 0, 10);

    if (errno) {
      eos_err("illegal maximum file size specified <%s>- restricting to 1 byte", val);
      mMaxSize = 1;
    }
  } else {
    mMaxSize = 0;
  }

  // Handle repair read flag
  if ((val = mCapOpaque->Get("mgm.repairread"))) {
    is_repair_read = true;
  }

  // Handle layout id
  if (!(slid = mCapOpaque->Get("mgm.lid"))) {
    return gOFS.Emsg(epname, error, EINVAL, "open - no layout id in capability",
                     mNsPath.c_str());
  }

  mLid = atoi(slid);

  // Handle container id
  if (!(scid = mCapOpaque->Get("mgm.cid"))) {
    return gOFS.Emsg(epname, error, EINVAL, "open - no container id in capability",
                     mNsPath.c_str());
  }

  mCid = strtoull(scid, 0, 10);

  // Handle the redirect manager
  if (!(smanager = mCapOpaque->Get("mgm.manager"))) {
    return gOFS.Emsg(epname, error, EINVAL, "open - no manager name in capability",
                     mNsPath.c_str());
  }

  mRdrManager = smanager;
  {
    // evt. update the shared hash manager entry
    XrdSysMutexHelper lock(gConfig.Mutex);
    XrdOucString ConfigManager = gConfig.Manager;

    if (ConfigManager != mRdrManager) {
      eos_warning("msg=\"MGM master seems to have changed - adjusting global "
                  "config\" old-manager=\"%s\" new-manager=\"%s\"",
                  ConfigManager.c_str(), mRdrManager.c_str());
      gConfig.Manager = mRdrManager;
    }
  }
  // Handle virtual identity
  vid = eos::common::VirtualIdentity::Nobody();

  if ((val = mCapOpaque->Get("mgm.ruid"))) {
    vid.uid = atoi(val);
  } else {
    return gOFS.Emsg(epname, error, EINVAL, "open - sec ruid missing",
                     mNsPath.c_str());
  }

  if ((val = mCapOpaque->Get("mgm.rgid"))) {
    vid.gid = atoi(val);
  } else {
    return gOFS.Emsg(epname, error, EINVAL, "open - sec rgid missing",
                     mNsPath.c_str());
  }

  if ((val = mCapOpaque->Get("mgm.uid"))) {
    vid.allowed_uids.clear();
    vid.allowed_uids.insert(atoi(val));
  } else {
    return gOFS.Emsg(epname, error, EINVAL, "open - sec uid missing",
                     mNsPath.c_str());
  }

  if ((val = mCapOpaque->Get("mgm.gid"))) {
    vid.allowed_gids.clear();
    vid.allowed_gids.insert(atoi(val));
  } else {
    return gOFS.Emsg(epname, error, EINVAL, "open - sec gid missing",
                     mNsPath.c_str());
  }

  // enable round-robin scheduling per application/fsid on request
  if ((val = mCapOpaque->Get("mgm.schedule"))) {
    mAppRR = mSecMap["app"];
  }

  std::string obfuscation_key;
  std::string encryption_key;

  // handle obfuscation keys
  if ((val = mCapOpaque->Get("mgm.obfuscate.key"))) {
    obfuscation_key = val;
  }

  // handl encryption keys
  if ((val = mCapOpaque->Get("mgm.encryption.key"))) {
    encryption_key = val;
  }

  mHmac.set(obfuscation_key, encryption_key);
  SetLogId(logId, vid, mTident.c_str());
  return SFS_OK;
}

//----------------------------------------------------------------------------
// Process mixed opaque information - decisions that need to be taken based
// on both the encrypted and un-encrypted opaque info
//----------------------------------------------------------------------------
int
XrdFstOfsFile::ProcessMixedOpaque()
{
  EPNAME("open");
  using eos::common::FileId;
  // Handle checksum request
  std::string opaqueCheckSum;
  char* val = nullptr;

  if (mOpenOpaque == nullptr || mCapOpaque == nullptr) {
    eos_warning("msg=\"open or cap opaque are empty\"");
    return SFS_OK;
  }

  if ((val = mOpenOpaque->Get("mgm.checksum"))) {
    opaqueCheckSum = val;
  }

  // Call the checksum factory function with the selected layout
  if (opaqueCheckSum != "ignore") {
    mCheckSum = eos::fst::ChecksumPlugins::GetChecksumObject(mLid);
    eos_debug("msg=\"checksum requested\" xs_ptr=%p lid=%u mgm.checksum=\"%s\"",
              mCheckSum.get(), mLid, opaqueCheckSum.c_str());
  }

  // Handle file system id and local prefix - If we open a replica we have to
  // take the right filesystem id and filesystem prefix for that replica
  const char* sfsid = 0;

  if (!(sfsid = mCapOpaque->Get("mgm.fsid"))) {
    return gOFS.Emsg(epname, error, EINVAL,
                     "open - no file system id in capability", mNsPath.c_str());
  }

  if (mOpenOpaque->Get("mgm.replicaindex")) {
    XrdOucString replicafsidtag = "mgm.fsid";
    replicafsidtag += (int) atoi(mOpenOpaque->Get("mgm.replicaindex"));

    if (mCapOpaque->Get(replicafsidtag.c_str())) {
      sfsid = mCapOpaque->Get(replicafsidtag.c_str());
    }
  }

  // Extract the local path prefix from the broadcasted configuration
  if (mOpenOpaque->Get("mgm.fsprefix")) {
    mLocalPrefix = mOpenOpaque->Get("mgm.fsprefix");
    mLocalPrefix.replace("#COL#", ":");
  } else {
    // Extract the local path prefix from the broadcasted configuration!
    mFsId = atoi(sfsid ? sfsid : "0");
    eos::common::RWMutexReadLock lock(gOFS.Storage->mFsMutex);

    if (mFsId && gOFS.Storage->mFsMap.count(mFsId)) {
      mLocalPrefix = gOFS.Storage->mFsMap[mFsId]->GetPath().c_str();
    }
  }

  // @note: the localprefix implementation does not work for gateway machines
  if (!mLocalPrefix.length()) {
    return gOFS.Emsg(epname, error, EINVAL, "open - cannot determine the prefix"
                     " path to use for the given filesystem id", mNsPath.c_str());
  }

  mFsId = atoi(sfsid);
  mFstPath = FileId::FidPrefix2FullPath(FileId::Fid2Hex(mFileId).c_str(),
                                        mLocalPrefix.c_str());
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Process TPC (third-party copy) opaque information i.e handle tags like
// tpc.key, tpc.dst, tpc.stage etc.
//------------------------------------------------------------------------------
int
XrdFstOfsFile::ProcessTpcOpaque(std::string& opaque, const XrdSecEntity* client)
{
  EPNAME(__FUNCTION__);
  mIsHttp = (client->tident ? (strncmp(client->tident, "http", 4) == 0) : false);
  eos::common::StringConversion::ReplaceStringInPlace(opaque, "?", "&");
  eos::common::StringConversion::ReplaceStringInPlace(opaque, "&&", "&");
  XrdOucEnv env(opaque.c_str());
  std::string tpc_stage = env.Get("tpc.stage") ? env.Get("tpc.stage") : "";
  std::string tpc_key = env.Get("tpc.key") ? env.Get("tpc.key") : "";
  std::string tpc_src = env.Get("tpc.src") ? env.Get("tpc.src") : "";
  std::string tpc_dst = env.Get("tpc.dst") ? env.Get("tpc.dst") : "";
  std::string tpc_org = env.Get("tpc.org") ? env.Get("tpc.org") : "";
  std::string tpc_lfn = env.Get("tpc.lfn") ? env.Get("tpc.lfn") : "";
  // Remove any TPC flags from now on
  FilterTagsInPlace(opaque, {"tpc.stage", "tpc.key", "tpc.src", "tpc.dst",
                             "tpc.org", "tpc.lfn"
                            });

  // Determine the TPC step that we are in
  if (tpc_stage == "placement") {
    mTpcFlag = kTpcSrcCanDo;
    mIsTpcDst = false;
  } else if ((tpc_stage == "copy") && tpc_key.length() && tpc_dst.length()) {
    mTpcFlag = kTpcSrcSetup;
    mIsTpcDst = false;
  } else if ((tpc_stage == "copy") && tpc_key.length() && tpc_src.length()) {
    mTpcFlag = kTpcDstSetup;
    mIsTpcDst = true;
  } else if (tpc_key.length() && tpc_org.length()) {
    // @note(esindril) The above condition should be as follows but for backwards
    // compatibility we keep it as it is. Consider changing it after 1st Jan 2024.
    // else if ((tpc_stage == "copy") && tpc_key.length() && tpc_org.length()) {
    mTpcFlag = kTpcSrcRead;
    mIsTpcDst = false;
  }

  if ((mTpcFlag == kTpcSrcSetup) || (mTpcFlag == kTpcDstSetup)) {
    // Create a TPC entry in the TpcMap
    XrdSysMutexHelper tpc_lock(gOFS.TpcMapMutex);

    if (gOFS.TpcMap[mIsTpcDst].count(tpc_key)) {
      return gOFS.Emsg(epname, error, EPERM, "open - tpc key replayed",
                       mNsPath.c_str());
    }

    // Compute the tpc origin e.g. <name>:<pid>@<host.domain>
    // @todo(esindril) Xrootd 4.0
    // std::string origin_host = client->addrInfo->Name();
    std::string origin_host = client->host ? client->host : "<sss-auth>";
    std::string origin_tident = client->tident;
    origin_tident.erase(origin_tident.find(":"));
    tpc_org = origin_tident;
    tpc_org += "@";
    tpc_org += origin_host;
    // Store the TPC initialization
    gOFS.TpcMap[mIsTpcDst][tpc_key].key = tpc_key;
    gOFS.TpcMap[mIsTpcDst][tpc_key].org = tpc_org;
    gOFS.TpcMap[mIsTpcDst][tpc_key].src = tpc_src;
    gOFS.TpcMap[mIsTpcDst][tpc_key].dst = tpc_dst;
    gOFS.TpcMap[mIsTpcDst][tpc_key].path = mNsPath.c_str();
    gOFS.TpcMap[mIsTpcDst][tpc_key].lfn = tpc_lfn;

    // Set tpc key expiration time, only relevant for the TPC source
    if (!mIsTpcDst) {
      std::string_view tpc_ttl = env.Get("tpc.ttl") ? env.Get("tpc.ttl") : "";
      gOFS.TpcMap[mIsTpcDst][tpc_key].expires = GetTpcKeyExpireTS(tpc_ttl);
    } else {
      gOFS.TpcMap[mIsTpcDst][tpc_key].expires = time(nullptr) + 3600 - 60;
    }

    mFstTpcInfo = gOFS.TpcMap[mIsTpcDst][tpc_key];
    mTpcKey = tpc_key;

    if (mTpcFlag == kTpcDstSetup) {
      if (!tpc_lfn.length()) {
        return gOFS.Emsg(epname, error, EINVAL, "open - tpc lfn missing",
                         mNsPath.c_str());
      }

      eos_info("msg=\"tpc dst session\" key=%s, org=%s, src=%s path=%s lfn=%s "
               "expires=%llu", gOFS.TpcMap[mIsTpcDst][tpc_key].key.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].org.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].src.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].path.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].lfn.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].expires);
    } else if (mTpcFlag == kTpcSrcSetup) {
      // Store the opaque info but without any tpc.* info
      gOFS.TpcMap[mIsTpcDst][tpc_key].opaque = opaque.c_str();
      // Store also the decoded capability info
      XrdOucEnv tmp_env(opaque.c_str());
      XrdOucEnv* cap_env {nullptr};
      int caprc = eos::common::SymKey::ExtractCapability(&tmp_env, cap_env);

      if (caprc == ENOKEY) {
        delete cap_env;
        return gOFS.Emsg(epname, error, caprc, "open - missing capability");
      } else if (caprc != 0) {
        delete cap_env;
        return gOFS.Emsg(epname, error, caprc, "open - capability illegal",
                         mNsPath.c_str());
      } else {
        int envlen = 0;
        gOFS.TpcMap[mIsTpcDst][tpc_key].capability = cap_env->Env(envlen);
        delete cap_env;
      }

      eos_info("msg=\"tpc src session\" key=%s, org=%s, dst=%s path=%s expires=%llu",
               gOFS.TpcMap[mIsTpcDst][tpc_key].key.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].org.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].dst.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].path.c_str(),
               gOFS.TpcMap[mIsTpcDst][tpc_key].expires);
    }
  } else if (mTpcFlag == kTpcSrcRead) {
    // Verify a TPC entry in the TpcMap since the destination's open can now
    // come before the transfer has been setup we have to give some time for
    // the TPC client to deposit the key the not so nice side effect is that
    // this thread stays busy during that time
    bool exists = false;

    for (size_t i = 0; i < 150; ++i) {
      {
        // Briefly take lock and release it
        XrdSysMutexHelper tpcLock(gOFS.TpcMapMutex);

        if (gOFS.TpcMap[mIsTpcDst].count(tpc_key)) {
          exists = true;
          break;
        }
      }

      if (!exists) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    XrdSysMutexHelper tpc_lock(gOFS.TpcMapMutex);
    time_t now = time(NULL);

    if (!gOFS.TpcMap[mIsTpcDst].count(tpc_key)) {
      eos_err("msg=\"tpc key not valid\" key=%s", tpc_key.c_str());
      return gOFS.Emsg(epname, error, EPERM, "open - tpc key not valid",
                       mNsPath.c_str());
    }

    if (gOFS.TpcMap[mIsTpcDst][tpc_key].expires < now) {
      eos_err("msg=\"tpc key expired\" key=%s", tpc_key.c_str());
      return gOFS.Emsg(epname, error, EPERM, "open - tpc key expired",
                       mNsPath.c_str());
    }

    // We trust 'sss' anyway and we miss the host name in the 'sss' entity
    std::string sec_prot = client->prot;

    if ((sec_prot != "sss")) {
      // Extract hostname from tident to avoid IPV4/6 fqdn mismatch errors
      std::string exp_org, cur_org;

      if (!GetHostFromTident(gOFS.TpcMap[mIsTpcDst][tpc_key].org, exp_org) ||
          !GetHostFromTident(tpc_org, cur_org)) {
        eos_err("failed to parse host from tpc_org=%s or cached_org=%s",
                tpc_org.c_str(), gOFS.TpcMap[mIsTpcDst][tpc_key].org.c_str());
        return gOFS.Emsg(epname, error, EPERM, "open - tpc origin parse error",
                         mNsPath.c_str());
      }

      if (exp_org != cur_org) {
        eos_err("tpc origin missmatch tpc_org=%s, cached_org=%s",
                tpc_org.c_str(), gOFS.TpcMap[mIsTpcDst][tpc_key].org.c_str());
        return gOFS.Emsg(epname, error, EPERM, "open - tpc origin mismatch",
                         mNsPath.c_str());
      }
    }

    eos_info("msg=\"tpc read\" key=%s, org=%s, dst=%s path=%s expires=%llu",
             gOFS.TpcMap[mIsTpcDst][tpc_key].key.c_str(),
             gOFS.TpcMap[mIsTpcDst][tpc_key].org.c_str(),
             gOFS.TpcMap[mIsTpcDst][tpc_key].dst.c_str(),
             gOFS.TpcMap[mIsTpcDst][tpc_key].path.c_str(),
             gOFS.TpcMap[mIsTpcDst][tpc_key].expires);
    // Grab the open information
    mNsPath = gOFS.TpcMap[mIsTpcDst][tpc_key].path.c_str();
    opaque = gOFS.TpcMap[mIsTpcDst][tpc_key].opaque.c_str();
    SetLogId(ExtractLogId(opaque.c_str()).c_str());
    // Store the provided origin to compare with our local connection
    // gOFS.TpcMap[mIsTpcDst][tpc_key].org = tpc_org;
    mFstTpcInfo = gOFS.TpcMap[mIsTpcDst][tpc_key];
    mTpcKey = tpc_key;
    // Save open opaque env
    mOpenOpaque.reset(new XrdOucEnv(opaque.c_str()));

    if (gOFS.TpcMap[mIsTpcDst][tpc_key].capability.length()) {
      mCapOpaque.reset(new XrdOucEnv(
                         gOFS.TpcMap[mIsTpcDst][tpc_key].capability.c_str()));
    } else {
      return gOFS.Emsg(epname, error, EINVAL, "open - capability not found "
                       "for tpc key %s", tpc_key.c_str());
    }
  }

  // Expire keys which are more than one 1 hours expired
  if (mTpcFlag > kTpcNone) {
    time_t now = time(NULL);
    XrdSysMutexHelper tpcLock(gOFS.TpcMapMutex);
    auto it = (gOFS.TpcMap[mIsTpcDst]).begin();
    auto del = (gOFS.TpcMap[mIsTpcDst]).begin();

    while (it != (gOFS.TpcMap[mIsTpcDst]).end()) {
      del = it;
      it++;

      if (now > (del->second.expires + 3600)) {
        eos_info("msg=\"expire tpc key\" key=%s", del->second.key.c_str());
        gOFS.TpcMap[mIsTpcDst].erase(del);
      }
    }
  }

  // For non-TPC transfer, src placement and destination TPCs we need to save
  // and decrypt the open and capability opaque info
  if ((mTpcFlag == kTpcNone) ||
      (mTpcFlag == kTpcDstSetup) ||
      (mTpcFlag == kTpcSrcSetup) ||
      (mTpcFlag == kTpcSrcCanDo)) {
    mOpenOpaque.reset(new XrdOucEnv(opaque.c_str()));
    XrdOucEnv* ptr_opaque {nullptr};
    int caprc = eos::common::SymKey::ExtractCapability(mOpenOpaque.get(),
                ptr_opaque);
    mCapOpaque.reset(ptr_opaque);

    if (caprc) {
      // If we just miss the key, better stall the client
      if (caprc == ENOKEY) {
        eos_err("msg=\"FST still misses the required capability key\"");
        return gOFS.Stall(error, 10, "FST still misses the required capability key");
      }

      return gOFS.Emsg(epname, error, caprc, "open - capability illegal",
                       mNsPath.c_str());
    }
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Compute close time
//------------------------------------------------------------------------------
void
XrdFstOfsFile::CloseTime()
{
  unsigned long mus = (closeStop.tv_sec - closeStart.tv_sec) * 1000000 +
                      (closeStop.tv_usec - closeStart.tv_usec);
  timeToClose = mus / 1000.0;
}
//------------------------------------------------------------------------------
// Account for total read time
//------------------------------------------------------------------------------
void
XrdFstOfsFile::AddReadTime()
{
  unsigned long mus = (lrTime.tv_sec - cTime.tv_sec) * 1000000 +
                      (lrTime.tv_usec - cTime.tv_usec);
  rTime.tv_sec += (mus / 1000000);
  rTime.tv_usec += (mus % 1000000);
}

void
XrdFstOfsFile::AddLayoutReadTime()
{
  struct timeval nowtime;
  gettimeofday(&nowtime, &tz);
  timeToRead += ((nowtime.tv_sec - rStart.tv_sec) * 1000) + ((
                  nowtime.tv_usec - rStart.tv_usec) / 1000.0);
}

//------------------------------------------------------------------------------
// Account for total readv time
//------------------------------------------------------------------------------
void
XrdFstOfsFile::AddReadVTime()
{
  unsigned long mus = (lrvTime.tv_sec - cTime.tv_sec) * 1000000 +
                      (lrvTime.tv_usec - cTime.tv_usec);
  rvTime.tv_sec += (mus / 1000000);
  rvTime.tv_usec += (mus % 1000000);
}

void
XrdFstOfsFile::AddLayoutReadVTime()
{
  struct timeval nowtime;
  gettimeofday(&nowtime, &tz);
  timeToReadV += ((nowtime.tv_sec - rvStart.tv_sec) * 1000) + ((
                   nowtime.tv_usec - rvStart.tv_usec) / 1000.0);
}

//------------------------------------------------------------------------------
// Account for total write time
//------------------------------------------------------------------------------
void
XrdFstOfsFile::AddWriteTime()
{
  unsigned long mus = ((lwTime.tv_sec - cTime.tv_sec) * 1000000) +
                      lwTime.tv_usec - cTime.tv_usec;
  wTime.tv_sec += (mus / 1000000);
  wTime.tv_usec += (mus % 1000000);
}

void
XrdFstOfsFile::AddLayoutWriteTime()
{
  struct timeval nowtime;
  gettimeofday(&nowtime, &tz);
  timeToWrite += ((nowtime.tv_sec - wStart.tv_sec) * 1000) + ((
                   nowtime.tv_usec - wStart.tv_usec) / 1000.0);
}

//------------------------------------------------------------------------------
// Make report
//------------------------------------------------------------------------------
void
XrdFstOfsFile::MakeReportEnv(XrdOucString& reportString)
{
  // compute avg, min, max, sigma for read and written bytes
  unsigned long long rmin, rmax, rsum;
  unsigned long long rvmin, rvmax, rvsum; // readv bytes
  unsigned long long rsmin, rsmax, rssum; // read single bytes
  unsigned long rcmin, rcmax, rcsum;      // readv count
  unsigned long long wmin, wmax, wsum;
  double rsigma, rvsigma, rssigma, rcsigma, wsigma;
  bool ioprio_default = false;
  {
    XrdSysMutexHelper vecLock(vecMutex);
    ComputeStatistics(rvec, rmin, rmax, rsum, rsigma);
    ComputeStatistics(wvec, wmin, wmax, wsum, wsigma);
    ComputeStatistics(monReadvBytes, rvmin, rvmax, rvsum, rvsigma);
    ComputeStatistics(monReadSingleBytes, rsmin, rsmax, rssum, rssigma);
    ComputeStatistics(monReadvCount, rcmin, rcmax, rcsum, rcsigma);
    bool sec_tpc = ((mTpcFlag == kTpcDstSetup) || (mTpcFlag == kTpcSrcRead));
    char report[16384];
    float iot = (float)(((closeTime.tv_sec - openTime.tv_sec) * 1000.0) + ((
                          closeTime.tv_usec - openTime.tv_usec) / 1000.0));
    float rt = ((rTime.tv_sec * 1000.0) + (rTime.tv_usec / 1000.0));
    float rvt = ((rvTime.tv_sec * 1000.0) + (rvTime.tv_usec / 1000.0));
    float wt = ((wTime.tv_sec * 1000.0) + (wTime.tv_usec / 1000.0));
    float idt = iot - timeToOpen - timeToClose - timeToRead - timeToReadV -
                timeToWrite;
    float usage = 100.0 - (100.0 * idt / iot);

    if (rmin == 0xffffffff) {
      rmin = 0;
    }

    if (wmin == 0xffffffff) {
      wmin = 0;
    }

    if (!mIoPriorityClass || mIoPriorityErrorReported) {
      mIoPriorityClass = IOPRIO_CLASS_BE;
      mIoPriorityValue = 4;
      ioprio_default = true;
    }

    std::string path = (mCapOpaque->Get("mgm.path") ?
                        mCapOpaque->Get("mgm.path") : mNsPath.c_str());
    std::string sanitized_path = eos::common::StringConversion::SealXrdPath(path);
    snprintf(report, sizeof(report) - 1,
             "log=%s&path=%s&fstpath=%s&ruid=%u&rgid=%u&td=%s&"
             "host=%s&lid=%lu&fid=%llu&fsid=%u&"
             "ots=%lu&otms=%lu&"
             "cts=%lu&ctms=%lu&"
             "nrc=%lu&nwc=%lu&"
             "rb=%llu&rb_min=%llu&rb_max=%llu&rb_sigma=%.02f&"
             "rv_op=%llu&rvb_min=%llu&rvb_max=%llu&rvb_sum=%llu&rvb_sigma=%.02f&"
             "rs_op=%llu&rsb_min=%llu&rsb_max=%llu&rsb_sum=%llu&rsb_sigma=%.02f&"
             "rc_min=%lu&rc_max=%lu&rc_sum=%lu&rc_sigma=%.02f&"
             "wb=%llu&wb_min=%llu&wb_max=%llu&wb_sigma=%.02f&"
             "sfwdb=%llu&sbwdb=%llu&sxlfwdb=%llu&sxlbwdb=%llu&"
             "nfwds=%lu&nbwds=%lu&nxlfwds=%lu&nxlbwds=%lu&"
             "usage=%.02f&iot=%.03f&idt=%.03f&lrt=%.03f&lrvt=%.03f&"
             "lwt=%.03f&ot=%.03f&ct=%.03f&rt=%.02f&rvt=%.02f&wt=%.02f&"
             "osize=%llu&csize=%llu&delete_on_close=%d&prio_c=%d&prio_l=%d&"
             "prio_d=%d&forced_bw=%d&ms_sleep=%llu&ior_err=%d&iow_err=%d&%s"
             , this->logId
             , sanitized_path.c_str()
             , mFstPath.c_str()
             , this->vid.uid, this->vid.gid, mTident.c_str()
             , gOFS.mHostName, mLid, mFileId, mFsId
             , openTime.tv_sec, (unsigned long) openTime.tv_usec / 1000
             , closeTime.tv_sec, (unsigned long) closeTime.tv_usec / 1000
             , rCalls, wCalls
             , rsum, rmin, rmax, rsigma
             , (unsigned long long)monReadvBytes.size(), rvmin, rvmax, rvsum, rvsigma
             , (unsigned long long)monReadSingleBytes.size(), rsmin, rsmax, rssum, rssigma
             , rcmin, rcmax, rcsum, rcsigma
             , wsum
             , wmin
             , wmax
             , wsigma
             , sFwdBytes
             , sBwdBytes
             , sXlFwdBytes
             , sXlBwdBytes
             , nFwdSeeks
             , nBwdSeeks
             , nXlFwdSeeks
             , nXlBwdSeeks
             , usage
             , iot
             , idt
             , timeToRead
             , timeToReadV
             , timeToWrite
             , timeToOpen
             , timeToClose
             , rt
             , rvt
             , wt
             , (unsigned long long) mOpenSize
             , (unsigned long long) mCloseSize
             , (mDelOnClose) ? 1 : 0
             , mIoPriorityClass
             , mIoPriorityValue
             , ioprio_default
             , mBandwidth
             , msSleep
             , mHasReadErr
             , mHasWriteErr
             , eos::common::SecEntity::ToEnv(mSecString.c_str(),
                 (sec_tpc ? "tpc" : 0)).c_str());
    reportString = report;
  }

  if ((mTpcFlag > kTpcNone) && (mTpcFlag != kTpcSrcCanDo)) {
    XrdSysMutexHelper tpc_lock(gOFS.TpcMapMutex);
    std::ostringstream sstpc;

    if (mTpcFlag == kTpcDstSetup) {
      sstpc << "&tpc.src=" << mFstTpcInfo.src
            << "&tpc.src_lfn=" << mFstTpcInfo.lfn;
    } else if ((mTpcFlag == kTpcSrcSetup) || (mTpcFlag == kTpcSrcRead)) {
      sstpc << "&tpc.dst=" << mFstTpcInfo.dst
            << "&tpc.src_lfn=" << mFstTpcInfo.path;
    }

    reportString += sstpc.str().c_str();
  }
}

//------------------------------------------------------------------------------
// Drop stripe/replica(s) from the MGM
//------------------------------------------------------------------------------
int
XrdFstOfsFile::DropFromMgm(const eos::common::FileId::fileid_t fid,
                           const eos::common::FileSystem::fsid_t fsid,
                           const std::string& path, const std::string& manager)
{
  const std::string hex_fid = eos::common::FileId::Fid2Hex(fid);
  XrdOucErrInfo lerror;
  std::ostringstream oss;
  oss << "/?mgm.pcmd=drop&mgm.fid=" << hex_fid << "&mgm.fsid=" << fsid;

  // Drop all stripes
  if (fsid == 0ul) {
    oss << "&mgm.dropall=1";
  }

  int rcode = gOFS.CallManager(&lerror, path.c_str(), manager.c_str(), oss.str());

  if (rcode && (error.getErrInfo() != EIDRM)) {
    eos_warning("msg=\"failed to drop at manager\" fxid=%08llx fsid=%u "
                "manager=%s", fid, fsid, manager.c_str());
  }

  eos_info("msg=\"drop on manager\" fxid=%08llx fsid=%u drop_all=%d rc=%d "
           "manager=%s", fid, fsid, (fsid == 0ul), rcode, manager.c_str());
  return rcode;
}

//------------------------------------------------------------------------------
// Check if file has been modified while in use
//------------------------------------------------------------------------------
int
XrdFstOfsFile::ModifiedWhileInUse()
{
  int rc = 0;
  bool fileExists = true;
  struct stat statinfo;

  if (mLayout) {
    if (mLayout->Stat(&statinfo)) {
      fileExists = false;
    }
  } else {
    if (XrdOfsOss->Stat(mFstPath.c_str(), &statinfo)) {
      fileExists = false;
    }
  }

  // Check if the file could have been changed in the meanwhile ...
  if (!mIsRW && fileExists && mIsReplication) {
    if (gOFS.openedForWriting.isOpen(mFsId, mFileId)) {
      eos_err("file is now open for writing - discarding replication "
              "[wopen=%d]", gOFS.openedForWriting.getUseCount(mFsId, mFileId));
      rc = gOFS.Emsg("closeofs", error, EIO, "guarantee correctness - "
                     "file has been opened for writing during replication",
                     mNsPath.c_str());
    }

    if ((statinfo.st_mtime != updateStat.st_mtime)) {
      eos_err("file has been modified during replication");
      rc = gOFS.Emsg("closeofs", error, EIO, "guarantee correctness -"
                     "file has been modified during replication",
                     mNsPath.c_str());
    }
  }

  return rc;
}

//------------------------------------------------------------------------------
// Layout read callback
//------------------------------------------------------------------------------
int
XrdFstOfsFile::LayoutReadCB(eos::fst::CheckSum::ReadCallBack::callback_data_t*
                            cbd)
{
  return ((Layout*) cbd->caller)->Read(cbd->offset, cbd->buffer, cbd->size);
}

//------------------------------------------------------------------------------
// File read callback
//------------------------------------------------------------------------------
int
XrdFstOfsFile::FileIoReadCB(eos::fst::CheckSum::ReadCallBack::callback_data_t*
                            cbd)
{
  return ((FileIo*) cbd->caller)->fileRead(cbd->offset, cbd->buffer, cbd->size);
}

//------------------------------------------------------------------------------
// Verify checksum method
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::VerifyChecksum()
{
  bool checksumerror = false;
  int checksumlen = 0;

  // Deal with checksums
  if (mCheckSum) {
    mCheckSum->Finalize();

    if (mCheckSum->NeedsRecalculation()) {
      if ((!mIsRW) && ((sFwdBytes + sBwdBytes) ||
                       (mCheckSum->GetMaxOffset() != mOpenSize))) {
        // We don't rescan files if they are read non-sequential or only
        // partially
        eos_debug("info=\"skipping checksum (re-scan) for non-sequential "
                  "reading ...\"");
        mCheckSum.reset(nullptr);
        return false;
      }
    } else {
      eos_debug("isrw=%d max-offset=%lld opensize=%lld", mIsRW,
                mCheckSum->GetMaxOffset(), mOpenSize);

      if ((!mIsRW) &&
          ((mCheckSum->GetMaxOffset() != mOpenSize) ||
           (mCheckSum->GetMaxOffset() == 0))) {
        eos_debug("info=\"skipping checksum (re-scan) for access without any IO or "
                  "partial sequential read IO from the beginning...\"");
        mCheckSum.reset(nullptr);
        return false;
      }
    }

    // -------------------------------------------------------------------------------------------------------------------
    // !!! CAUTION !!!
    // be careful with adler checksum - finalize can remove the dirty flag if all pieces of a file until the max checksum
    // offset were written - however if the file size is diffrent from the max checksum offset, the checksum is dirty
    // because the ending part of a file was not written
    // -------------------------------------------------------------------------------------------------------------------
    if (mIsRW && mHasWrite && mCheckSum->GetMaxOffset() &&
        (mCheckSum->GetMaxOffset() != (off_t)mMaxOffsetWritten)) {
      // If there was a write which was not extending the file the checksum
      // is dirty!
      mCheckSum->SetDirty();
    }

    if (gOFS.openedForWriting.hadMultiOpen(mFsId, mFileId)) {
      // If there were several writers on the file, we should set the checksum dirty
      mCheckSum->SetDirty();
    }

    // If checksum is not completely computed
    if (mCheckSum->NeedsRecalculation()) {
      unsigned long long scansize = 0;
      float scantime = 0; // is ms

      if (!XrdOfsFile::fctl(SFS_FCTL_GETFD, 0, error)) {
        // Rescan the file
        eos::fst::CheckSum::ReadCallBack::callback_data_t cbd;
        cbd.caller = (void*) mLayout.get();
        eos::fst::CheckSum::ReadCallBack cb(LayoutReadCB, cbd);

        if (mCheckSum->ScanFile(cb, scansize, scantime)) {
          XrdOucString sizestring;
          eos_info("info=\"rescanned checksum\" size=%s time=%.02f ms rate=%.02f MB/s %s",
                   eos::common::StringConversion::GetReadableSizeString(sizestring,
                       scansize, "B"),
                   scantime,
                   1.0 * scansize / 1000 / (scantime ? scantime : 99999999999999LL),
                   mCheckSum->GetHexChecksum());
        } else {
          eos_err("msg=\"checksum rescanning failed\" fxid=%08llx", mFileId);
          mCheckSum.reset(nullptr);
          return false;
        }
      } else {
        eos_err("msg=\"failed to get file descriptor\" fxid=%08llx", mFileId);
        mCheckSum.reset(nullptr);
        return false;
      }
    } else {
      // This was prefect streaming I/O
      if ((!mIsRW) && (mCheckSum->GetMaxOffset() != mOpenSize)) {
        eos_info("info=\"skipping checksum (re-scan) since file was not read "
                 "completely %llu %llu...\"", mCheckSum->GetMaxOffset(), mOpenSize);
        mCheckSum.reset(nullptr);
        return false;
      }
    }

    if (mIsRW) {
      eos_info("(write) checksum type=\"%s\" checksum hex=\"%s\" "
               "requested-checksum hex=\"%s\"", mCheckSum->GetName(),
               mCheckSum->GetHexChecksum(),
               mOpenOpaque->Get("mgm.checksum") ?
               mOpenOpaque->Get("mgm.checksum") : "-none-");

      // Check if the check sum for the file was given at upload time
      if (mOpenOpaque->Get("mgm.checksum")) {
        XrdOucString opaqueChecksum = mOpenOpaque->Get("mgm.checksum");
        XrdOucString hexChecksum = mCheckSum->GetHexChecksum();

        if ((opaqueChecksum != "disable") && (opaqueChecksum != hexChecksum)) {
          eos_err("requested checksum %s does not match checksum %s of uploaded"
                  " file", opaqueChecksum.c_str(), hexChecksum.c_str());
          mCheckSum.reset(nullptr);
          return true;
        }
      }

      mFmd->mProtoFmd.set_checksum(mCheckSum->GetHexChecksum());

      if (mHasWrite) {
        // If we have no write, we don't set this attributes (xrd3cp!)
        // set the eos checksum extended attributes
        std::unique_ptr<eos::fst::FileIo> io(eos::fst::FileIoPlugin::GetIoObject(
                                               mFstPath.c_str(), this));

        if (!eos::common::LayoutId::IsRain(mLid)) {
          // Don't put file checksum tags for complex layouts like raid6,readdp, archive
          if (io->attrSet(std::string("user.eos.checksumtype"),
                          std::string(mCheckSum->GetName()))) {
            eos_err("msg=\"unable to set extended attr <eos.checksumtype>\" "
                    "fxid=%08llx errno=%d", mFileId, errno);
          }

          if (io->attrSet("user.eos.checksum", mCheckSum->GetBinChecksum(checksumlen),
                          checksumlen)) {
            eos_err("msg=\"unable to set extended attr <eos.checksum> \" "
                    "fxid=%08llx errno=%d", mFileId, errno);
          }
        }

        // Reset any tagged error
        if (io->attrSet("user.eos.filecxerror", "0")) {
          eos_err("msg=\"unable to set extended attr <eos.filecxerror>\" "
                  "fxid=%08llx errno=%d", mFileId, errno);
        }

        if (io->attrSet("user.eos.blockcxerror", "0")) {
          eos_err("msg=\"unable to set extended attr <eos.blockcxerror>\" "
                  "fxid=%08llx errno=%d", mFileId, errno);
        }
      }
    } else {
      // This is a read with checksum check, compare with mFmd
      bool isopenforwrite = gOFS.openedForWriting.isOpen(mFsId, mFileId);

      if (isopenforwrite) {
        eos_info("msg=\"read disable checksum check, file being written\" "
                 "fxid=%08llx", mFileId);
        return false;
      }

      std::string computed_xs = mCheckSum->GetHexChecksum();
      eos_info("msg=\"read checksum info\" xs_type=%s xs_computed=%s "
               "xs_local=%s fxid=%08llx fsid=%u", mCheckSum->GetName(),
               computed_xs.c_str(), mFmd->mProtoFmd.checksum().c_str(),
               mFileId, mFsId);

      // We might fetch an unitialized value, that is not to be considered
      // a checksum error yet.
      if (mFmd->mProtoFmd.checksum() != "none") {
        if (computed_xs != mFmd->mProtoFmd.checksum().c_str()) {
          checksumerror = true;
        }
      }
    }
  }

  return checksumerror;
}

bool XrdFstOfsFile::VerifyUnitChecksum()
{
  bool unit_checksum_err = mLayout->VerifyChecksum();

  if (unit_checksum_err) {
    return true;
  }

  if (mLayout->GetUnitChecksum()) {
    mFmd->mProtoFmd.set_unitchecksum(mLayout->GetUnitChecksum()->GetHexChecksum());
  }

  return false;
}

//------------------------------------------------------------------------------
// Queue file for CTA archiving
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::QueueForArchiving(const struct stat& statinfo,
                                 std::string& queueing_errmsg,
                                 std::string& archive_req_id)
{
  std::string decodedAttributes;
  eos::common::SymKey::Base64Decode(mEventAttributes.c_str(), decodedAttributes);
  std::map<std::string, std::string> attributes;
  eos::common::StringConversion::GetKeyValueMap(decodedAttributes.c_str(),
      attributes,
      eos::common::WF_CUSTOM_ATTRIBUTES_TO_FST_EQUALS,
      eos::common::WF_CUSTOM_ATTRIBUTES_TO_FST_SEPARATOR, nullptr);
  std::string mgm_hostname;

  if (!gOFS.mMgmAlias.empty()) {
    mgm_hostname = gOFS.mMgmAlias;
  } else {
    const char* ptr = mCapOpaque->Get("mgm.manager");

    if (ptr != nullptr) {
      mgm_hostname = ptr;
    } else {
      eos_err("%s", "msg=\"count not determine value of MGM hostname");
      return false;
    }
  }

  const int notifyRc = NotifyProtoWfEndPointClosew(mFmd->mProtoFmd.fid(),
                       mFmd->mProtoFmd.lid(),
                       statinfo.st_size,
                       mFmd->mProtoFmd.checksum(),
                       mEventOwnerUid,
                       mEventOwnerGid,
                       mEventRequestor,
                       mEventRequestorGroup,
                       mEventInstance,
                       mCapOpaque->Get("mgm.path"),
                       mgm_hostname,
                       attributes,
                       queueing_errmsg,
                       archive_req_id);

  // Note: error variable defined in XrdSfsFile interface
  if (notifyRc == 0) {
    error.setErrCode(0);
    eos_info("Return code rc=%i errc=%d", SFS_OK, error.getErrInfo());
    return true;
  } else if (SendArchiveFailedToManager(mFmd->mProtoFmd.fid(), queueing_errmsg)) {
    eos_crit("msg=\"Failed to send archive failed event to manager\" "
             "queueing_errmsg=\"%s\"", queueing_errmsg.c_str());
  }

  error.setErrCode(EIO);
  eos_info("Return code rc=%i errc=%d", SFS_ERROR, error.getErrInfo());
  return false;
}

//----------------------------------------------------------------------------
// Static method used to start an asynchronous thread which is doing the TPC
// transfer
//----------------------------------------------------------------------------
void*
XrdFstOfsFile::StartDoTpcTransfer(void* arg)
{
  return reinterpret_cast<XrdFstOfsFile*>(arg)->DoTpcTransfer();
}

//------------------------------------------------------------------------------
// Run method for the thread doing the TPC transfer
//------------------------------------------------------------------------------
void*
XrdFstOfsFile::DoTpcTransfer()
{
  eos_info("msg=\"tpc now running - 1st sync\"");
  std::string src_url = "";
  std::string src_cgi = "";

  // The sync initiates the third party copy
  if (!TpcValid()) {
    eos_err("msg=\"tpc session invalidated during sync\"");
    XrdSysMutexHelper scope_lock(mTpcJobMutex);
    mTpcState = kTpcDone;
    mTpcRetc = ECONNABORTED;
    mTpcInfo.Reply(SFS_ERROR, ECONNABORTED, "sync TPC session closed by "
                   "disconnect");
    return 0;
  }

  {
    XrdSysMutexHelper tpcLock(gOFS.TpcMapMutex);
    // Construct the source URL
    src_url = "root://";
    src_url += gOFS.TpcMap[mIsTpcDst][mTpcKey].src;
    src_url += "/";
    src_url += gOFS.TpcMap[mIsTpcDst][mTpcKey].lfn;
    src_url += "?fst.readahead=true";
    src_cgi = "tpc.key=";
    src_cgi += mTpcKey;
    src_cgi += "&tpc.org=";
    src_cgi += gOFS.TpcMap[mIsTpcDst][mTpcKey].org;
    src_cgi += "&tpc.stage=copy";
  }

  XrdIo tpcIO(src_url);
  tpcIO.SetLogId(logId);
  eos_info("sync-url=%s sync-cgi=%s", src_url.c_str(), src_cgi.c_str());

  if (tpcIO.fileOpen(0, 0, src_cgi)) {
    eos_err("msg=\"TPC open failed for url=%s cgi=%s\"", src_url.c_str(),
            src_cgi.c_str());
    XrdSysMutexHelper scope_lock(mTpcJobMutex);
    mTpcState = kTpcDone;
    mTpcRetc = EFAULT;
    mTpcInfo.Reply(SFS_ERROR, EFAULT,
                   SSTR("sync - TPC open failed for src_url=" << src_url).c_str());
    return 0;
  }

  if (!TpcValid()) {
    tpcIO.fileClose();
    eos_err("msg=\"tpc session invalidated during sync\"");
    XrdSysMutexHelper scope_lock(mTpcJobMutex);
    mTpcState = kTpcDone;
    mTpcRetc = ECONNABORTED;
    mTpcInfo.Reply(SFS_ERROR, ECONNABORTED,
                   SSTR("sync - TPC session closed by disconnect src_url="
                        << src_url).c_str());
    return 0;
  }

  int64_t rbytes = 0;
  int64_t wbytes = 0;
  off_t offset = 0;
  constexpr uint64_t eight_gb = 8 * (1ULL << 30);
  static_assert(eight_gb == 8589934592,
                "eight gb is not computed correctly!");
  std::unique_ptr< std::vector<char> > buffer
  (new std::vector<char>(tpcIO.GetBlockSize()));
  eos_info("msg=\"tpc pull\" ");
  struct stat st_info;

  if (tpcIO.fileStat(&st_info)) {
    eos_err("%s", "msg=\"failed to stat remote file\" src_url=%s",
            src_url.c_str());
    XrdSysMutexHelper scope_lock(mTpcJobMutex);
    mTpcState = kTpcDone;
    mTpcRetc = EIO;
    mTpcInfo.Reply(SFS_ERROR, mTpcRetc, "sync - TPC remote stat failed");
    return 0;
  }

  int64_t file_size = st_info.st_size;
  int64_t nread {0ull};

  while (offset < file_size) {
    // Read the remote file in chunks and check after each chunk if the TPC
    // has been aborted already
    if (file_size - offset >= tpcIO.GetBlockSize()) {
      nread = tpcIO.GetBlockSize();
    } else {
      nread = file_size - offset;
    }

    if (getenv("EOS_FST_TPC_READASYNC")) {
      rbytes = tpcIO.fileReadPrefetch(offset, &((*buffer)[0]), nread, 30);
    } else {
      rbytes = tpcIO.fileRead(offset, &((*buffer)[0]), nread);
    }

    eos_debug("msg=\"tpc read\" rbytes=%lli request=%llu",
              rbytes, tpcIO.GetBlockSize());

    if ((rbytes == -1) || (rbytes != nread)) {
      (void) tpcIO.fileClose();
      eos_err("msg=\"tpc transfer terminated - remote read failed\"");
      XrdSysMutexHelper scope_lock(mTpcJobMutex);
      mTpcState = kTpcDone;
      mTpcRetc = EIO;
      mTpcInfo.Reply(SFS_ERROR, mTpcRetc,
                     SSTR("sync - TPC remote read failed src_url="
                          << src_url).c_str());
      return 0;
    }

    // Write the buffer out through the local object
    wbytes = write(offset, &((*buffer)[0]), rbytes);

    if (offset / eight_gb != (offset + rbytes) /  eight_gb) {
      eos_info("msg=\"tcp write\" offset=%llu", offset);
    }

    if (rbytes != wbytes) {
      (void) tpcIO.fileClose();
      eos_err("%s", "msg=\"tpc transfer terminated - local write failed\"");
      XrdSysMutexHelper scope_lock(mTpcJobMutex);
      mTpcState = kTpcDone;
      mTpcRetc = EIO;
      mTpcInfo.Reply(SFS_ERROR, mTpcRetc, "sync - TPC local write failed");
      return 0;
    }

    offset += rbytes;

    // Got an "ofs.tpc cancel" request from the client who triggered it
    if (mTpcCancel) {
      eos_err("%s", "msg=\"tpc transfer cancelled by the client\"");
      XrdSysMutexHelper scope_lock(mTpcJobMutex);
      mTpcState = kTpcDone;
      mTpcRetc = ECANCELED;
      mTpcInfo.Reply(SFS_ERROR, mTpcRetc,
                     SSTR("sync - TPC cancelled by client src_url="
                          << src_url).c_str());
      return 0;
    }

    // Check validity of the TPC key
    if (!TpcValid()) {
      (void) tpcIO.fileClose();
      eos_err("msg=\"tpc transfer invalidated during sync\"");
      XrdSysMutexHelper scope_lock(mTpcJobMutex);
      mTpcState = kTpcDone;
      mTpcRetc = ECONNABORTED;
      mTpcInfo.Reply(SFS_ERROR, mTpcRetc, "sync - TPC session closed "
                     "by diconnect");
      return 0;
    }
  }

  // Close the remote file
  int close_rc = tpcIO.fileClose();
  eos_info("msg=\"done tpc transfer, close remote file\" is_ok=%s src_url=%s",
           (close_rc ? "false" : "true"), src_url.c_str());
  XrdSysMutexHelper scope_lock(mTpcJobMutex);
  mTpcState = kTpcDone;

  if (close_rc != SFS_OK) {
    mTpcRetc = tpcIO.GetLastErrNo();
    mTpcInfo.Reply(SFS_ERROR, mTpcRetc,
                   SSTR("sync - TPC failed source close src_url=" << src_url
                        << " src_err=" << tpcIO.GetLastErrMsg()).c_str());
  } else {
    mTpcInfo.Reply(SFS_OK, 0, "");
  }

  return 0;
}

//------------------------------------------------------------------------------
// TPC clean up - invalidates the TPC keys at the end of a TPC transfer and
// also joins the TPC helper thread
//------------------------------------------------------------------------------
void
XrdFstOfsFile::TpcCleanup()
{
  if (!mTpcKey.empty()) {
    {
      XrdSysMutexHelper tpcLock(gOFS.TpcMapMutex);

      if (gOFS.TpcMap[mIsTpcDst].count(mTpcKey)) {
        eos_info("msg=\"remove tpc key\" key=%s", mTpcKey.c_str());
        gOFS.TpcMap[mIsTpcDst].erase(mTpcKey);

        try {
          gOFS.TpcMap[mIsTpcDst].resize(0);
        } catch (const std::length_error& e) {}
      }
    }

    // TPC thread is doing the data transfer pull and only runs on the dst
    if (mTpcFlag == kTpcDstSetup) {
      if (!mTpcThreadStatus) {
        int retc = XrdSysThread::Join(mTpcThread, NULL);
        eos_debug("msg=\"TPC thread joined\" rc=%i fxid=%08llx", retc, mFileId);
        mTpcThreadStatus = EINVAL;
      } else {
        eos_warning("msg=\"TPC thread already joined or never started "
                    "successfully\" fxid=%08llx", mFileId);
      }
    }
  }
}

//------------------------------------------------------------------------------
// Filter out particular tags from the opaque information
//------------------------------------------------------------------------------
void
XrdFstOfsFile::FilterTagsInPlace(std::string& opaque,
                                 const std::set<std::string> tags)
{
  bool found = false;
  std::ostringstream oss;
  std::list<std::string> tokens = eos::common::StringTokenizer::split
                                  <std::list<std::string>>(opaque, '&');

  for (const auto& token : tokens) {
    found = false;

    for (const auto& tag : tags) {
      if (token.find(tag) == 0) {
        found = true;
        break;
      }
    }

    if (!found && !token.empty()) {
      oss << token << "&";
    }
  }

  opaque = oss.str();

  if (!opaque.empty()) {
    opaque.pop_back();
  }
}

//------------------------------------------------------------------------------
// Return current mtime while open
//------------------------------------------------------------------------------
time_t
XrdFstOfsFile::GetMtime() const
{
  if (!mIsRW) {
    // this is to report the MGM mtime to http get requests
    if (mForcedMtime != 1) {
      return mForcedMtime;
    }
  }

  if (mFmd) {
    return mFmd->mProtoFmd.mtime();
  } else {
    return 0;
  }
}

//------------------------------------------------------------------------------
// Extract logid from the opaque info
//------------------------------------------------------------------------------
std::string
XrdFstOfsFile::ExtractLogId(const char* opaque) const
{
  std::string log_id = "unknown";

  if (opaque == nullptr) {
    return log_id;
  }

  std::string sopaque = opaque;
  const std::string tag = "mgm.logid=";
  size_t pos_begin = sopaque.find(tag);

  if (pos_begin != std::string::npos) {
    pos_begin += tag.length();
    size_t pos_end = sopaque.find('&', pos_begin);

    if (pos_end != std::string::npos) {
      pos_end -= pos_begin;
    }

    log_id = sopaque.substr(pos_begin, pos_end);
  }

  return log_id;
}

//------------------------------------------------------------------------------
// Notify the workflow protobuf endpoint of closew event
//------------------------------------------------------------------------------
int
XrdFstOfsFile::NotifyProtoWfEndPointClosew(uint64_t file_id,
    uint32_t file_lid, uint64_t file_size,
    const std::string& file_checksum,
    uint32_t owner_uid, uint32_t owner_gid,
    const std::string& requestor_name,
    const std::string& requestor_groupname,
    const std::string& instance_name,
    const std::string& fullpath,
    const std::string& manager_name,
    const std::map<std::string, std::string>& xattrs,
    std::string& errmsg_wfe,
    std::string& archive_req_id)
{
  using namespace eos::common;
  cta::xrd::Request request;
  auto notification = request.mutable_notification();
  notification->mutable_cli()->mutable_user()->set_username(requestor_name);
  notification->mutable_cli()->mutable_user()->set_groupname(requestor_groupname);
  notification->mutable_file()->mutable_owner()->set_uid(owner_uid);
  notification->mutable_file()->mutable_owner()->set_gid(owner_gid);
  notification->mutable_file()->set_size(file_size);
  // Insert a single checksum into the checksum blob
  CtaCommon::SetChecksum(notification->mutable_file()->mutable_csb()->add_cs(),
                         file_lid, file_checksum);
  notification->mutable_wf()->set_event(cta::eos::Workflow::CLOSEW);
  notification->mutable_wf()->mutable_instance()->set_name(instance_name);
  auto xrdname = getenv("XRDNAME");
  auto requester_instance = std::string(gOFS.mHostName) + ":" +
                            (xrdname ? std::string(xrdname) : "NULL");
  notification->mutable_wf()->set_requester_instance(requester_instance);
  notification->mutable_file()->set_lpath(fullpath);
  notification->mutable_file()->set_disk_file_id(std::to_string(file_id));
  auto fxidString = StringConversion::FastUnsignedToAsciiHex(file_id);
  std::string ctaArchiveFileId = "none";
  std::string storageClass = "";

  for (const auto& attrPair : xattrs) {
    google::protobuf::MapPair<std::string, std::string> attr(attrPair.first,
        attrPair.second);
    notification->mutable_file()->mutable_xattr()->insert(attr);

    if (attrPair.first ==
        ARCHIVE_FILE_ID_ATTR_NAME) { // sys.archive.file_id xattr corresponds to archive_file_id first-class attribute
      ctaArchiveFileId = attrPair.second;
    }

    if (attrPair.first == ARCHIVE_STORAGE_CLASS_ATTR_NAME) {
      storageClass = attrPair.second;
    }
  }

  // also make sure to pass the right attribute, don't just use the extended ones (old format), fill in the new ones
  notification->mutable_file()->set_storage_class(storageClass);
  notification->mutable_file()->set_archive_file_id(std::strtoul(
        ctaArchiveFileId.c_str(), nullptr, 10));
  // Build query strings
  std::ostringstream srcStream;
  std::ostringstream reportStream;
  std::ostringstream errorReportStream;
  srcStream << "root://" << manager_name << "/" << fullpath << "?eos.lfn=fxid:"
            << fxidString;
  notification->mutable_wf()->mutable_instance()->set_url(srcStream.str());
  reportStream << "eosQuery://" << manager_name
               << "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" << fxidString
               << "&mgm.logid=cta&mgm.event=sync::archived&mgm.workflow=default&mgm.path=/dummy_path&mgm.ruid=0&mgm.rgid=0"
               "&cta_archive_file_id=" << ctaArchiveFileId;
  notification->mutable_transport()->set_report_url(reportStream.str());
  errorReportStream << "eosQuery://" << manager_name
                    << "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" << fxidString
                    << "&mgm.logid=cta&mgm.event=" << ARCHIVE_FAILED_WORKFLOW_NAME
                    << "&mgm.workflow=default&mgm.path=/dummy_path&mgm.ruid=0&"
                    << "mgm.rgid=0&cta_archive_file_id=" << ctaArchiveFileId
                    << "&mgm.errmsg=";
  notification->mutable_transport()->set_error_report_url(
    errorReportStream.str());
  // Communication with service
  std::string endPoint;
  std::string resource;
  bool protowfusegrpc;
  {
    XrdSysMutexHelper lock(gConfig.Mutex);
    endPoint = gConfig.ProtoWFEndpoint;
    resource = gConfig.ProtoWFResource;
    protowfusegrpc = gConfig.protowfusegrpc;
  }

  if (endPoint.empty() || resource.empty()) {
    eos_static_err("%s", "msg=\"you are running proto wf jobs without "
                   "specifying fstofs.protowfendpoint or "
                   "fstofs.protowfresource in the FST config file\"");
    return ENOTCONN;
  }

  cta::xrd::Response response;
  cta::xrd::Response::ResponseType response_type =
    cta::xrd::Response::RSP_INVALID;

  try {
    // Instantiate service object only once, static is also thread-safe
    // If static initialization throws an exception, it will be retried next time
    static std::unique_ptr<WFEClient> request_sender = CreateRequestSender(
          protowfusegrpc, endPoint, resource);
    auto sentAt = std::chrono::steady_clock::now();
    response_type = request_sender->send(request, response);
    auto receivedAt = std::chrono::steady_clock::now();
    auto timeSpent = std::chrono::duration_cast<std::chrono::milliseconds>
                     (receivedAt - sentAt);
    eos_static_info("WFEClient send time for sync::closew=%ld", timeSpent.count());
  } catch (std::runtime_error& err) {
    eos_static_err("Could not send request to outside service. Reason: %s",
                   err.what());
    return ENOTCONN;
  }

  // also make sure to check not only the extended attribute but also the actual first-class attribute
  switch (response_type) {
  case cta::xrd::Response::RSP_SUCCESS: {
    auto archiveReqIdItor = response.xattr().find("sys.cta.objectstore.id");

    if (response.xattr().end() != archiveReqIdItor) {
      archive_req_id = archiveReqIdItor->second;
    } else {
      eos_static_err("msg=\"Failed to extract sys.cta.objectstore.id from response to closew notification to"
                     " protowfendpoint\" path=\"%s\"", fullpath.c_str());
    }
  }

  return 0;

  case cta::xrd::Response::RSP_ERR_CTA:
  case cta::xrd::Response::RSP_ERR_USER:
  case cta::xrd::Response::RSP_ERR_PROTOBUF:
  case cta::xrd::Response::RSP_INVALID:
    errmsg_wfe = response.message_txt();
    eos_static_err("%s for file %s. Reason: %s",
                   CtaCommon::ctaResponseCodeToString(response_type).c_str(),
                   fullpath.c_str(), response.message_txt().c_str());
    return EPROTO;

  default:
    eos_static_err("Response:\n%s", response.DebugString().c_str());
    return EPROTO;
  }
}

//------------------------------------------------------------------------------
// Send archive failed event to the manager
//------------------------------------------------------------------------------
int XrdFstOfsFile::SendArchiveFailedToManager(const uint64_t fid,
    const std::string& errmsg)
{
  const auto fxidString = eos::common::StringConversion::FastUnsignedToAsciiHex(
                            fid);
  std::string encodedErrMsg;

  if (!common::SymKey::Base64Encode(errmsg.c_str(), errmsg.length(),
                                    encodedErrMsg)) {
    // "Failed to encode message using base64" in base64 is
    // RmFpbGVkIHRvIGVuY29kZSBtZXNzYWdlIHVzaW5nIGJhc2U2NA==
    encodedErrMsg = "RmFpbGVkIHRvIGVuY29kZSBtZXNzYWdlIHVzaW5nIGJhc2U2NA==";
  }

  XrdOucString errorReportOpaque = "";
  errorReportOpaque += "/?";
  errorReportOpaque += "mgm.pcmd=event";
  errorReportOpaque += "&mgm.fid=";
  errorReportOpaque += fxidString.c_str();
  errorReportOpaque += "&mgm.logid=cta";
  errorReportOpaque += "&mgm.event=";
  errorReportOpaque += common::ARCHIVE_FAILED_WORKFLOW_NAME;
  errorReportOpaque += "&mgm.workflow=default";
  errorReportOpaque += "&mgm.path=/dummy_path";
  errorReportOpaque += "&mgm.ruid=0";
  errorReportOpaque += "&mgm.rgid=0";
  errorReportOpaque += "&mgm.errmsg=";
  errorReportOpaque += encodedErrMsg.c_str();
  eos_info("msg=\"sending error message to manager\" path=\"%s\" manager=\"%s\" "
           "errorReportOpaque=\"%s\"", mCapOpaque->Get("mgm.path"),
           mCapOpaque->Get("mgm.manager"), errorReportOpaque.c_str());
  return gOFS.CallManager(&error, mCapOpaque->Get("mgm.path"),
                          mCapOpaque->Get("mgm.manager"),
                          errorReportOpaque, 30, mSyncEventOnClose, false);
}

//------------------------------------------------------------------------------
// Get hostname from tident
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::GetHostFromTident(const std::string& tident,
                                 std::string& hostname)
{
  hostname.clear();
  size_t pos = tident.find('@');

  if ((pos == std::string::npos) || (pos + 1 == tident.length())) {
    return false;
  }

  size_t dot_pos = tident.find('.', pos + 1);
  hostname = tident.substr(pos + 1, dot_pos - pos - 1);
  return true;
}

//------------------------------------------------------------------------------
// Check if async close is configured
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::IsAsyncCloseConfigured()
{
  const char* ptr = getenv("EOS_FST_ASYNC_CLOSE");

  if (!ptr || (strncmp(ptr, "1", 1) != 0)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if async sync is configured
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::IsAsyncSyncConfigured()
{
  const char* ptr = getenv("EOS_FST_ASYNC_SYNC");

  if (!ptr || (strncmp(ptr, "1", 1) != 0)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Decide if close should be done synchronously. There are cases when close
// should happen in the same thread eg. read, http tx, sink writes etc.
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::DoSyncClose()
{
  static uint64_t min_size_async_close = GetAsyncCloseMinSize();

  // Even if async close is enabled there are some cases when close happens in
  // the same XRootD thread
  if (viaDelete || mWrDelete || mIsDevNull || (mIsRW == false) || mIsHttp ||
      (mIsRW && (mMaxOffsetWritten <= (long long) min_size_async_close))) {
    return true;
  }

  // For RAIN layouts especially only the entry server should do an async close.
  // If all stripes are on the same FST (which is not a good idea) there is a
  // risk that the thread pool for handling close requests will deadlock as
  // some close requests will wait forever for queued depended close ops.
  if (mIsRW && mLayout && !mLayout->IsEntryServer()) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Decide if sync should be done synchronously - keep the same boundary
// conditions as for async close on purpose!
//------------------------------------------------------------------------------
bool
XrdFstOfsFile::DoSyncSync()
{
  static uint64_t min_size_async_sync = GetAsyncCloseMinSize(); // on purpose!

  // Even if async close is enabled there are some cases when close happens in
  // the same XRootD thread
  if (viaDelete || mWrDelete || mIsDevNull || (mIsRW == false) || mIsHttp ||
      (mIsRW && (mMaxOffsetWritten <= (long long) min_size_async_sync))) {
    return true;
  }

  // For RAIN layouts especially only the entry server should do an async sync.
  // If all stripes are on the same FST (which is not a good idea) there is a
  // risk that the thread pool for handling close requests will deadlock as
  // some close requests will wait forever for queued depended close ops.
  if (mIsRW && mLayout && !mLayout->IsEntryServer()) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Get configured minimum file size for which the asynchronous close method
// is called.
//------------------------------------------------------------------------------
uint64_t
XrdFstOfsFile::GetAsyncCloseMinSize()
{
  uint64_t min_size_async_close = 0ull;
  const char* ptr = getenv("EOS_FST_ASYNC_CLOSE_MIN_SIZE_BYTES");

  if (ptr) {
    if (!eos::common::StringToNumeric(std::string(ptr), min_size_async_close,
                                      0ul)) {
      eos_static_err("%s", "msg=\"failed to convert "
                     "EOS_ASYNC_CLOSE_MIN_SIZE_BYTES, using by default 0\"");
    }
  }

  return min_size_async_close;
}

//------------------------------------------------------------------------------
// Populate and commit FMD info locally
//------------------------------------------------------------------------------
int
XrdFstOfsFile::CommitToLocalFmd(const struct stat& info)
{
  EPNAME("close");
  mFmd->mProtoFmd.set_size(info.st_size);
  mFmd->mProtoFmd.set_disksize
  (eos::common::LayoutId::ExpectedStripeSize(mLid, info.st_size));
  // Reset the diskchecksum after an update otherwise we might falsely report
  // a checksum error. The diskchecksum will be updated by the scanner.
  mFmd->mProtoFmd.set_diskchecksum("");
  mFmd->mProtoFmd.set_mgmsize(eos::common::FmdHelper::UNDEF);
  mFmd->mProtoFmd.set_mgmchecksum(""); // now again empty
  mFmd->mProtoFmd.set_layouterror(0); // reset layout errors
  mFmd->mProtoFmd.set_locations(""); // reset locations
  mFmd->mProtoFmd.set_filecxerror(0);
  mFmd->mProtoFmd.set_blockcxerror(0);
  mFmd->mProtoFmd.set_locations(""); // reset locations
  mFmd->mProtoFmd.set_cid(mCid);
  mFmd->mProtoFmd.set_mtime(info.st_mtime);
#ifdef __APPLE__
  mFmd->mProtoFmd.set_mtime_ns(0);
#else
  mFmd->mProtoFmd.set_mtime_ns(info.st_mtim.tv_nsec);
#endif

  if (mCapOpaque->Get("mgm.source.lid")) {
    try {
      std::string data = mCapOpaque->Get("mgm.source.lid");
      eos::common::LayoutId::layoutid_t src_lid = std::stoul(data);
      mFmd->mProtoFmd.set_lid(src_lid);

      // For RAIN files size is the size of the logical file and not
      // the size of the current stripe on disk
      if (eos::common::LayoutId::IsRain(src_lid) &&
          mCapOpaque->Get("mgm.bookingsize") && mIsReplication) {
        data = mCapOpaque->Get("mgm.bookingsize");
        mFmd->mProtoFmd.set_size(std::stoull(data));
      }
    } catch (...) {
      eos_err("msg=\"failure to convert layout id or bookingsize\" "
              "lid=\"%s\" bookingsize=\"%s\"",
              mCapOpaque->Get("mgm.source.lid"),
              mCapOpaque->Get("mgm.bookingsize"));
    }
  }

  if (mCapOpaque->Get("mgm.source.ruid")) {
    mFmd->mProtoFmd.set_uid(atoi(mCapOpaque->Get("mgm.source.ruid")));
  }

  if (mCapOpaque->Get("mgm.source.rgid")) {
    mFmd->mProtoFmd.set_gid(atoi(mCapOpaque->Get("mgm.source.rgid")));
  }

  // Commit local
  if (!gOFS.mFmdHandler->Commit(mFmd.get())) {
    eos_err("msg=\"unable to commit fmd info\" fxid=%08llx", mFileId);
    return gOFS.Emsg(epname, error, EIO, "close - unable to "
                     "commit meta data", mNsPath.c_str());
  }

  return 0;
}

//------------------------------------------------------------------------------
// Commit file information to MGM
//------------------------------------------------------------------------------
int
XrdFstOfsFile::CommitToMgm()
{
  using eos::common::StringConversion;
  std::ostringstream oss;
  const std::string smtime = StringConversion::GetSizeString((unsigned long long)
                             (mForcedMtime != 1) ?
                             mForcedMtime :
                             mFmd->mProtoFmd.mtime());
  const std::string smtime_ns = StringConversion::GetSizeString((
                                  unsigned long long)
                                (mForcedMtime != 1) ?
                                mForcedMtime_ms :
                                mFmd->mProtoFmd.mtime_ns());
  int envlen = 0;
  char* ptr = nullptr;
  oss << "/?" << mCapOpaque->Env(envlen)
      << "&mgm.pcmd=commit"
      << "&mgm.size=" << mCloseSize
      << "&mgm.mtime=" << smtime
      << "&mgm.mtime_ns=" << smtime_ns
      << "&mgm.add.fsid=" << mFmd->mProtoFmd.fsid()
      << "&mgm.logid=" << logId;

  if (mCheckSum) {
    oss << "&mgm.checksum=" << mCheckSum->GetHexChecksum();
  }

  auto unitCheckSum = mLayout->GetUnitChecksum();

  if (mFusex) {
    oss << "&mgm.fusex=1";
  }

  if (mHasWrite) {
    oss << "&mgm.modified=1";
  }

  // Prevent atomic/versioning on commmit
  if (noAtomicVersioning) {
    oss << "&mgm.commit.verify=1";
  }

  // If <drainfsid> is set, we can issue a drop replica
  ptr = mCapOpaque->Get("mgm.drainfsid");

  if (ptr) {
    oss << "&mgm.drop.fsid=" << ptr;
  }

  if (mRainReconstruct) {
    // Indicate that this is a commit of a RAIN reconstruction
    if (mLayout->IsEntryServer()) {
      oss << "&mgm.reconstruction=1";
      ptr = mOpenOpaque->Get("eos.pio.recfs");

      if ((mHasReadErr == false) && ptr) {
        oss << "&mgm.drop.fsid=" << ptr;
      }
    }
  } else {
    if (mLayout->IsEntryServer() && !mIsReplication && !mIsInjection) {
      // The entry server commits size and checksum
      oss << "&mgm.commit.size=1";

      if (mCheckSum) {
        oss << "&mgm.commit.checksum=1";
      }
    } else {
      bool last_writer = (gOFS.openedForWriting.getUseCount(mFmd->mProtoFmd.fsid(),
                          mFmd->mProtoFmd.fid()) <= 1);

      if (last_writer) {
        if (mCheckSum) {
          // If we computed a checksum, we verify ONLY IF there is a single
          // writer. Otherwise, if there are several writers we have a
          // significant inconsistency window during commit between replicas.
          oss << "&mgm.replication=1&mgm.verify.checksum=1";
        } else {
          // If no checksum was computed, we disable checksum verification
          // and we indicate replication only if we are the last active
          // writer.
          oss << "&mgm.replication=1&mgm.verify.checksum=0";
        }
      }
    }
  }

  // Evt. tag as an OC-Chunk commit
  if (mIsOCchunk) {
    oss << eos::common::OwnCloud::FilterOcQuery(mOpenOpaque->Env(envlen));
  }

  return gOFS.CallManager(&error, mNsPath.c_str(), mRdrManager.c_str(),
                          oss.str(), 0, true);
}

//------------------------------------------------------------------------------
// Trigger event on close
//------------------------------------------------------------------------------
int
XrdFstOfsFile::TriggerEventOnClose(const std::string& archive_req_id)
{
  std::string event_t = "closer";

  if (mIsRW) {
    event_t = (mSyncEventOnClose ? "sync::closew" : "closew");
  }

  std::ostringstream oss;
  int envlen = 0;
  oss << "/?"
      << mCapOpaque->Env(envlen)
      << "&mgm.pcmd=event"
      << "&mgm.event=" << event_t
      << "&mgm.logid=" << logId
      << "&mgm.ruid=" << mCapOpaque->Get("mgm.ruid")
      << "&mgm.rgid=" << mCapOpaque->Get("mgm.rgid")
      << "&mgm.sec=" << mSecString;

  if (mEventWorkflow.length()) {
    oss << "&mgm.workflow=" << mEventWorkflow;
  }

  if (!archive_req_id.empty()) {
    oss << "&mgm.archive_req_id=" << archive_req_id;
  }

  eos_info("msg=\"notify\" event=\"%s\" workflow=\"%s\" fxid=%08llx",
           event_t.c_str(), mEventWorkflow.c_str(), mFileId);
  return gOFS.CallManager(&error, mNsPath.c_str(), mRdrManager.c_str(),
                          oss.str(), 30, mSyncEventOnClose, false);
}

//----------------------------------------------------------------------------
// Read AIO - not supported
//----------------------------------------------------------------------------
int
XrdFstOfsFile::read(XrdSfsAio* aioparm)
{
  return gOFS.Emsg("read_aio", error, ENOTSUP, "read aio - operation not "
                   "supported");
}

//----------------------------------------------------------------------------
// Read file pages and checksums using asynchronous I/O - no supported
//-----------------------------------------------------------------------------
int
XrdFstOfsFile::pgRead(XrdSfsAio* aioparm, uint64_t opts)
{
  return gOFS.Emsg("pgRead_aio", error, ENOTSUP, "pgRead aio - operation not "
                   "supported");
}

//----------------------------------------------------------------------------
// Write AIO - no supported
//----------------------------------------------------------------------------
int
XrdFstOfsFile::write(XrdSfsAio* aioparm)
{
  return gOFS.Emsg("write_aio", error, ENOTSUP, "write aio - operation not "
                   "supported");
}

//----------------------------------------------------------------------------
// Write file pages and checksums using asynchronous I/O - not supported
//----------------------------------------------------------------------------
int
XrdFstOfsFile::pgWrite(XrdSfsAio* aioparm, uint64_t opts)
{
  return gOFS.Emsg("pgWrite_aio", error, ENOTSUP, "pgWrite aio - operation not "
                   "supported");
}

//------------------------------------------------------------------------------
// Sync AIO - not supported
//------------------------------------------------------------------------------
int
XrdFstOfsFile::sync(XrdSfsAio* aiop)
{
  return gOFS.Emsg("sync_aio", error, ENOTSUP, "sync aio - operation not "
                   "supported");
}


EOSFSTNAMESPACE_END
