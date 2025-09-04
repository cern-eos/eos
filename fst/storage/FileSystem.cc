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
#include "fst/Config.hh"
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
  eos::common::SCAN_IO_RATE_NAME,
  eos::common::SCAN_ENTRY_INTERVAL_NAME,
  eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME,
  eos::common::SCAN_DISK_INTERVAL_NAME,
  eos::common::SCAN_NS_INTERVAL_NAME,
  eos::common::SCAN_NS_RATE_NAME,
  eos::common::SCAN_ALTXS_INTERVAL_NAME,
  eos::common::ALTXS_SYNC,
  eos::common::ALTXS_SYNC_INTERVAL };

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
  // Notify the MGM this file system is down
  SetStatus(eos::common::BootStatus::kDown);

  // Delete the local SharedHash object attached to it without touching the
  // shared object in QDB, this only for QDB pub-sub mode
  if (mRealm->haveQDB()) {
    DeleteSharedHash(false);
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

    if (upd.key == "id") {
      try {
        mLocalId = std::stoul(upd.value);
      } catch (...) {}
    } else if (upd.key == "uuid") {
      mLocalUuid = upd.value;
    }

    // @note handle here the updates but make sure not to access or set any
    // shared hash values as this will trigger a deadlock. We are now called
    // from the shared hash itself that digest the updates and also pushes them
    // through a subscriber to us. Digesting these update is done in an
    // exclusive lock region that protects the contents of the shared hash -
    // therefore we risk ending up in a deadlock situation
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
  if (mLocalId && !mLocalUuid.empty()) {
    if (mScanDir == nullptr) {
      mScanDir.reset(new ScanDir(GetPath().c_str(), mLocalId, fst_load, true));
      eos_info("msg=\"started ScanDir thread with default parameters\" fsid=%d",
               mLocalId);
    }

    mScanDir->SetConfig(key, value);
  } else {
    eos_static_notice("msg=\"skip scanner config for partial file system\" "
                      "queue=\"%s\"", GetQueuePath().c_str());
  }
}


//------------------------------------------------------------------------------
// Set file system boot status
//------------------------------------------------------------------------------
void
FileSystem::SetStatus(eos::common::BootStatus status)
{
  eos::common::FileSystem::SetStatus(status);

  if (mLocalBootStatus == status) {
    return;
  }

  eos_debug("before=%d after=%d", mLocalBootStatus.load(), status);

  if ((mLocalBootStatus == eos::common::BootStatus::kBooted) &&
      (status == eos::common::BootStatus::kOpsError)) {
    mRecoverable = true;
  } else {
    mRecoverable = false;
  }

  mLocalBootStatus = status;
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

  // Open the file for direct access
  int fd = open(fn_path.c_str(), O_RDWR | O_TRUNC | O_DIRECT | O_SYNC);

  if (fd == -1) {
    eos_static_err("msg=\"failed to open file\" path=%s", fn_path.c_str());
    return;
  }

  // Unlink the file so that we don't leave any behind even in the case of
  // a crash of the FST. The file descritor will still be valid for use.
  (void) unlink(fn_path.c_str());

  // Fill the file up to the given size with random data
  if (!eos::fst::FillFileGivenSize(fd, fn_size)) {
    eos_static_err("msg=\"failed to fill file\" path=%s", fn_path.c_str());
    (void) close(fd);
    return;
  }

  using namespace std::chrono;
  auto start_iops = high_resolution_clock::now();
  IOPS = eos::fst::ComputeIops(fd);
  auto end_iops = high_resolution_clock::now();
  uint64_t rd_buf_size = 4 * (1 << 20); // 4MB
  auto start_bw = high_resolution_clock::now();
  seqBandwidth = eos::fst::ComputeBandwidth(fd, rd_buf_size);
  auto end_bw = high_resolution_clock::now();
  (void) close(fd);
  eos_info("bw=%lld iops=%d iops_time=%llums bw_time=%llums", seqBandwidth, IOPS,
           duration_cast<milliseconds>(end_iops - start_iops).count(),
           duration_cast<milliseconds>(end_bw - start_bw).count());
  return;
}

//------------------------------------------------------------------------------
// Get IO statistics from the `sys.iostats` xattr
//------------------------------------------------------------------------------
bool
FileSystem::GetFileIOStats(std::map<std::string, std::string>& map)
{
  if (!mFileIO) {
    return false;
  }

  // Avoid querying IO stats attributes for certain storage types
  if (mFileIO->GetIoType() == "DavixIo" ||
      mFileIO->GetIoType() == "NfsIo" ||
      mFileIO->GetIoType() == "XrdIo") {
    return false;
  }

  std::string iostats;
  mFileIO->attrGet("sys.iostats", iostats);
  return eos::common::StringConversion::GetKeyValueMap(iostats.c_str(),
         map, "=", ",");
}

//------------------------------------------------------------------------------
// Get health information from the `sys.health` xattr
//------------------------------------------------------------------------------
bool
FileSystem::GetHealthInfo(std::map<std::string, std::string>& map)
{
  if (!mFileIO) {
    return false;
  }

  // Avoid querying Health attributes for certain storage types
  if (mFileIO->GetIoType() == "DavixIo" ||
      mFileIO->GetIoType() == "NfsIo" ||
      mFileIO->GetIoType() == "XrdIo") {
    return false;
  }

  // Avoid querying Health attributes for certain storage types
  if (mFileIO->GetIoType() == "DavixIo" ||
      mFileIO->GetIoType() == "NfsIo" ||
      mFileIO->GetIoType() == "XrdIo") {
    return false;
  }

  std::string health;
  mFileIO->attrGet("sys.health", health);
  return eos::common::StringConversion::GetKeyValueMap(health.c_str(),
         map, "=", ",");
}

//----------------------------------------------------------------------------
// Decide if we should run the boot procedure for current file system
//----------------------------------------------------------------------------
bool
FileSystem::ShouldBoot(const std::string& trigger)
{
  if ((trigger == "id") || (trigger == "uuid")) {
    // Check if we are auto-booting
    if (gConfig.autoBoot &&
        (GetStatus() <= eos::common::BootStatus::kDown) &&
        (GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
      return true;
    }
  }

  if (trigger == "bootsenttime") {
    uint64_t bootcheck_val = GetLongLong("bootcheck");

    if (GetInternalBootStatus() == eos::common::BootStatus::kBooted) {
      if (bootcheck_val) {
        eos_static_info("msg=\"boot enforced\" queue=%s status=%d check=%lld",
                        GetQueuePath().c_str(), GetStatus(), bootcheck_val);
        return true;
      } else {
        eos_static_info("msg=\"skip boot, already booted\" queue=%s "
                        "status=%d check=%lld", GetQueuePath().c_str(),
                        GetStatus(), bootcheck_val);
        SetStatus(eos::common::BootStatus::kBooted);
        return false;
      }
    } else {
      eos_static_info("msg=\"do boot as we're not yet booted\" queue=%s "
                      "status=%d check=%lld", GetQueuePath().c_str(),
                      GetStatus(), bootcheck_val);
      return true;
    }
  }

  if (trigger.empty()) {
    return true;
  }

  return false;
}

EOSFSTNAMESPACE_END
