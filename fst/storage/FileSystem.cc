// ----------------------------------------------------------------------
// File: FileSystem.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "fst/storage/FileSystem.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/ScanDir.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/utils/DiskMeasurements.hh"
#include "common/Constants.hh"
#include "qclient/shared/SharedHashSubscription.hh"

#ifdef __APPLE__
#define O_DIRECT 0
#endif

EOSFSTNAMESPACE_BEGIN

// Set of key updates to be tracked at the file system level
std::set<std::string> FileSystem::sFsUpdateKeys {
  "id", "uuid", "bootsenttime",
  eos::common::SCAN_IO_RATE_NAME, eos::common::SCAN_ENTRY_INTERVAL_NAME,
  eos::common::SCAN_DISK_INTERVAL_NAME, eos::common::SCAN_NS_INTERVAL_NAME,
  eos::common::SCAN_NS_RATE_NAME };

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystem::FileSystem(const common::FileSystemLocator& locator,
                       mq::MessagingRealm* realm) :
  eos::common::FileSystem(locator, realm, true),
  mLocalId(0ul), mLocalUuid(""),  mScanDir(nullptr), mFileIO(nullptr)
{
  last_blocks_free = 0;
  last_status_broadcast = 0;
  seqBandwidth = 0;
  IOPS = 0;
  mLocalBootStatus = eos::common::BootStatus::kDown;
  mRecoverable = false;
  mFileIO.reset(FileIoPlugin::GetIoObject(mLocator.getStoragePath()));

  if (mRealm->haveQDB()) {
    // Subscribe to the underlying SharedHash object to get updates
    mSubscription = mq::SharedHashWrapper(mRealm, mHashLocator).subscribe();

    if (mSubscription) {
      using namespace std::placeholders;
      mSubscription->attachCallback(std::bind(&FileSystem::ProcessUpdateCb,
                                              this, _1));
    }
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileSystem::~FileSystem()
{
  if (mSubscription) {
    mSubscription->detachCallback();
  }

  mScanDir.release();
  mFileIO.release();

  if (gOFS.FmdOnDb()) {
    auto fmd_handler = static_cast<FmdDbMapHandler*>(gOFS.mFmdHandler.get());
    fmd_handler->ShutdownDB(mLocalId, true);
  }
}

//------------------------------------------------------------------------------
// Process shared hash update
//------------------------------------------------------------------------------
void
FileSystem::ProcessUpdateCb(qclient::SharedHashUpdate&& upd)
{
  if (sFsUpdateKeys.find(upd.key) != sFsUpdateKeys.end()) {
    eos_static_info("msg=\"process update callback\" key=%s value=%s",
                    upd.key.c_str(), upd.value.c_str());
    gOFS.Storage->ProcessFsConfigChange(this, upd.key, upd.value);
  }
}

//------------------------------------------------------------------------------
// Broadcast given error message
//------------------------------------------------------------------------------
void
FileSystem::BroadcastError(const char* msg)
{
  if (!gOFS.sShutdown) {
    SetStatus(eos::common::BootStatus::kOpsError);
    SetError(errno ? errno : EIO, msg);
  }
}

//------------------------------------------------------------------------------
// Broadcast given error code and message
//------------------------------------------------------------------------------
void
FileSystem::BroadcastError(int errc, const char* errmsg)
{
  if (!gOFS.sShutdown) {
    SetStatus(eos::common::BootStatus::kOpsError);
    SetError(errno ? errno : EIO, errmsg);
  }
}

//------------------------------------------------------------------------------
// Set given error code and message
//------------------------------------------------------------------------------
void
FileSystem::SetError(int errc, const char* errmsg)
{
  if (errc) {
    eos_static_err("setting errc=%d errmsg=%s", errc, errmsg ? errmsg : "");
  }

  if (!SetLongLong("stat.errc", errc)) {
    eos_static_err("cannot set errcode for filesystem %s", GetQueuePath().c_str());
  }

  if (errmsg && strlen(errmsg) && !SetString("stat.errmsg", errmsg)) {
    eos_static_err("cannot set errmsg for filesystem %s", GetQueuePath().c_str());
  }
}

//------------------------------------------------------------------------------
// Get statfs info about mountpoint
//------------------------------------------------------------------------------
std::unique_ptr<eos::common::Statfs>
FileSystem::GetStatfs()
{
  if (!GetPath().length()) {
    return nullptr;
  }

  std::unique_ptr<eos::common::Statfs> statFs;

  if (mFileIO) {
    statFs = mFileIO->GetStatfs();
  }

  if ((!statFs) && GetPath().length()) {
    eos_err("msg=\"cannot statfs\" path=\"%s\"", GetPath().c_str());
    BroadcastError("cannot statfs");
    return nullptr;
  } else {
    eos_static_debug("ec=%d error=%s recover=%d", GetStatus(),
                     GetString("stat.errmsg").c_str(), mRecoverable);

    if ((GetStatus() == eos::common::BootStatus::kOpsError) && mRecoverable) {
      if (GetString("stat.errmsg") == "cannot statfs") {
        // reset the statfs error
        SetStatus(eos::common::BootStatus::kBooted);
        SetError(0, "");
      }
    }
  }

  return statFs;
}

//------------------------------------------------------------------------------
// Configure scanner thread - possibly start the scanner
//------------------------------------------------------------------------------
void
FileSystem::ConfigScanner(Load* fst_load, const std::string& key,
                          long long value)
{
  // Don't scan filesystems which are 'remote'
  if (GetPath()[0] != '/') {
    return;
  }

  // If not running then create scanner thread with default parameters
  if (mScanDir == nullptr) {
    mScanDir.reset(new ScanDir(GetPath().c_str(), mLocalId, fst_load, true));
    eos_info("msg=\"started ScanDir thread with default parameters\" fsid=%d",
             mLocalId);
  }

  mScanDir->SetConfig(key, value);
}

//------------------------------------------------------------------------------
// Get file system disk performance metrics eg. IOPS/seq bandwidth
//------------------------------------------------------------------------------
void
FileSystem::IoPing()
{
  IOPS = 0;
  seqBandwidth = 0;

  // Exclude 'remote' disks
  if (GetPath()[0] != '/') {
    eos_static_notice("msg=\"skip disk measurements for \'remote\' disk\" "
                      "path=%s", GetPath().c_str());
    return;
  }

  // Create temporary file (1GB) name on the mountpoint
  uint64_t fn_size = 1 << 30; // 1 GB
  const std::string fn_path = eos::fst::MakeTemporaryFile(GetPath());

  if (fn_path.empty()) {
    eos_static_err("msg=\"failed to create tmp file\" base_path=%s",
                   GetPath().c_str());
    return;
  }

  // Fill the file up to the given size with random data
  if (!eos::fst::FillFileGivenSize(fn_path, fn_size)) {
    eos_static_err("msg=\"failed to fill file\" path=%s", fn_path.c_str());
    unlink(fn_path.c_str());
    return;
  }

  IOPS = eos::fst::ComputeIops(fn_path);
  uint64_t rd_buf_size = 4 * (1 << 20); // 4MB
  seqBandwidth = eos::fst::ComputeBandwidth(fn_path, rd_buf_size);
  unlink(fn_path.c_str());
  eos_info("bw=%lld iops=%d", seqBandwidth, IOPS);
  return;
}

//------------------------------------------------------------------------------
// Collect orphans registered in the db for the current file system
//------------------------------------------------------------------------------
std::set<eos::common::FileId::fileid_t>
FileSystem::CollectOrphans() const
{
  eos::common::RWMutexReadLock rd_lock(mInconsistencyMutex);
  auto it = mInconsistencySets.find("orphans_n");

  if (it != mInconsistencySets.end()) {
    return it->second;
  }

  return std::set<eos::common::FileId::fileid_t>();
}

//------------------------------------------------------------------------------
// Collect inconsistency statistics about the current file system
//------------------------------------------------------------------------------
std::map<std::string, std::string>
FileSystem::CollectInconsistencyStats(const std::string prefix) const
{
  std::map<std::string, std::string> out;
  eos::common::RWMutexReadLock rd_lock(mInconsistencyMutex);

  for (const auto& elem : mInconsistencyStats) {
    out[prefix + elem.first] = std::to_string(elem.second);
  }

  return out;
}

//------------------------------------------------------------------------------
// Update inconsistency info about the current file system
//------------------------------------------------------------------------------
void
FileSystem::UpdateInconsistencyInfo()
{
  decltype(mInconsistencyStats) tmp_stats;
  decltype(mInconsistencySets) tmp_sets;

  if (!gOFS.mFmdHandler->GetInconsistencyStatistics(mLocalId, tmp_stats,
      tmp_sets)) {
    eos_static_err("msg=\"failed to get inconsistency statistics\" fsid=%lu",
                   mLocalId);
    return;
  }

  eos::common::RWMutexWriteLock wr_lock(mInconsistencyMutex);
  mInconsistencyStats.swap(tmp_stats);
  mInconsistencySets.swap(tmp_sets);
}

EOSFSTNAMESPACE_END
