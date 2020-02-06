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
#include "fst/txqueue/TransferQueue.hh"
#include "fst/FmdDbMap.hh"

#ifdef __APPLE__
#define O_DIRECT 0
#endif

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystem::FileSystem(const common::FileSystemLocator& locator,
  mq::MessagingRealm *realm) :
  eos::common::FileSystem(locator, realm, true),
  mLocalId(0ul), mLocalUuid(""),  mScanDir(nullptr), mFileIO(nullptr),
  mTxDirectory("")
{
  last_blocks_free = 0;
  last_status_broadcast = 0;
  seqBandwidth = 0;
  IOPS = 0;
  mLocalBootStatus = eos::common::BootStatus::kDown;
  mTxBalanceQueue = new TransferQueue(&mBalanceQueue);
  mTxExternQueue = new TransferQueue(&mExternQueue);
  mTxMultiplexer = std::make_unique<TransferMultiplexer>();
  mTxMultiplexer->Add(mTxBalanceQueue);
  mTxMultiplexer->Add(mTxExternQueue);
  mTxMultiplexer->Run();
  mRecoverable = false;
  mFileIO.reset(FileIoPlugin::GetIoObject(mLocator.getStoragePath()));
}

/*----------------------------------------------------------------------------*/
FileSystem::~FileSystem()
{
  mScanDir.release();
  mFileIO.release();
  gFmdDbMapHandler.ShutdownDB(mLocalId);
  mTxMultiplexer.reset();

  if (mTxBalanceQueue) {
    delete mTxBalanceQueue;
  }

  if (mTxExternQueue) {
    delete mTxExternQueue;
  }
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastError(const char* msg)
{
  bool shutdown = false;

  if (gOFS.sShutdown) {
    shutdown = true;
  }

  if (!shutdown) {
    SetStatus(eos::common::BootStatus::kOpsError);
    SetError(errno ? errno : EIO, msg);
  }
}

/*----------------------------------------------------------------------------*/
void
FileSystem::BroadcastError(int errc, const char* errmsg)
{
  bool shutdown = false;

  if (gOFS.sShutdown) {
    shutdown = true;
  }

  if (!shutdown) {
    SetStatus(eos::common::BootStatus::kOpsError);
    SetError(errno ? errno : EIO, errmsg);
  }
}

/*----------------------------------------------------------------------------*/
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
    eos_err("cannot statfs");
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

/*----------------------------------------------------------------------------*/
void
FileSystem::CleanTransactions()
{
  using eos::common::FileId;
  DIR* tdir = opendir(GetTransactionDirectory());

  if (tdir) {
    struct dirent* name;

    while ((name = readdir(tdir))) {
      XrdOucString sname = name->d_name;

      // skipp . & ..
      if (sname.beginswith(".")) {
        continue;
      }

      XrdOucString fulltransactionpath = GetTransactionDirectory();
      fulltransactionpath += "/";
      fulltransactionpath += name->d_name;
      struct stat buf;

      if (!stat(fulltransactionpath.c_str(), &buf)) {
        XrdOucString hexfid = name->d_name;
        XrdOucString localprefix = GetPath().c_str();
        std::string fstPath = FileId::FidPrefix2FullPath(hexfid.c_str(),
                              localprefix.c_str());
        unsigned long long fileid = FileId::Hex2Fid(hexfid.c_str());
        // we allow to keep files open for 1 week
        bool isOpen = gOFS.openedForWriting.isOpen(mLocalId, fileid);

        if ((buf.st_mtime < (time(NULL) - (7 * 86400))) && (!isOpen)) {
          auto fMd = gFmdDbMapHandler.LocalGetFmd(fileid, mLocalId, true);

          if (fMd) {
            auto location_set = fMd->GetLocations();

            if (location_set.count(mLocalId)) {
              // close that transaction and keep the file
              gOFS.Storage->CloseTransaction(mLocalId, fileid);
              continue;
            }
          }

          eos_static_info("action=delete transaction=%llx fstpath=%s",
                          sname.c_str(),
                          fulltransactionpath.c_str());
          // Clean-up this file locally
          XrdOucErrInfo error;
          int retc = gOFS._rem("/CLEANTRANSACTIONS", error, 0, 0,
                               fstPath.c_str(), fileid, mLocalId, true);

          if (retc) {
            eos_static_debug("deletion failed for %s", fstPath.c_str());
          }
        } else {
          eos_static_info("action=keep transaction=%llx fstpath=%s isopen=%d",
                          sname.c_str(), fulltransactionpath.c_str(), isOpen);
        }
      }
    }

    closedir(tdir);
  } else {
    eos_static_err("Unable to open transactiondirectory %s",
                   GetTransactionDirectory());
  }
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SyncTransactions(const char* manager)
{
  using eos::common::FileId;
  bool ok = true;
  DIR* tdir = opendir(GetTransactionDirectory());

  if (tdir) {
    struct dirent* name;

    while ((name = readdir(tdir))) {
      XrdOucString sname = name->d_name;

      // skipp . & ..
      if (sname.beginswith(".")) {
        continue;
      }

      XrdOucString fulltransactionpath = GetTransactionDirectory();
      fulltransactionpath += "/";
      fulltransactionpath += name->d_name;
      struct stat buf;

      if (!stat(fulltransactionpath.c_str(), &buf)) {
        XrdOucString hexfid = name->d_name;
        std::string path = GetPath();
        const char* localprefix = path.c_str();
        std::string fstPath = FileId::FidPrefix2FullPath(hexfid.c_str(), localprefix);
        unsigned long long fid = FileId::Hex2Fid(hexfid.c_str());

        // try to sync this file from the MGM
        if (gFmdDbMapHandler.ResyncMgm(mLocalId, fid, manager)) {
          eos_static_info("msg=\"resync ok\" fsid=%lu fxid=%08llx",
                          mLocalId, fid);
        } else {
          eos_static_err("msg=\"resync failed\" fsid=%lu fxid=%08llx",
                         mLocalId, fid);
          ok = false;
          continue;
        }
      }
    }

    closedir(tdir);
  } else {
    ok = false;
    eos_static_err("Unable to open transactiondirectory %s",
                   GetTransactionDirectory());
  }

  return ok;
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

/*----------------------------------------------------------------------------*/
bool
FileSystem::OpenTransaction(unsigned long long fid)
{
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  tagfile += eos::common::FileId::Fid2Hex(fid).c_str();
  int fd = open(tagfile.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR |
                S_IROTH | S_IRGRP);

  if (fd >= 0) {
    close(fd);
    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::CloseTransaction(unsigned long long fid)
{
  XrdOucString tagfile = GetTransactionDirectory();
  tagfile += "/";
  tagfile += eos::common::FileId::Fid2Hex(fid).c_str();

  if (unlink(tagfile.c_str())) {
    return false;
  }

  return true;
}

/*----------------------------------------------------------------------------*/
void
FileSystem::IoPing()
{
  std::string cmdbw = "eos-iobw ";
  cmdbw += GetPath();
  std::string cmdiops = "eos-iops ";
  cmdiops += GetPath();
  eos_info("\"%s\" \"%s\"", cmdbw.c_str(), cmdiops.c_str());
  seqBandwidth = 0;
  IOPS = 0;

  // ----------------------
  // exclude 'remote' disks
  // ----------------------
  if (GetPath()[0] == '/') {
    std::string bwMeasurement = eos::common::StringConversion::StringFromShellCmd(
                                  cmdbw.c_str());
    std::string iopsMeasurement = eos::common::StringConversion::StringFromShellCmd(
                                    cmdiops.c_str());

    if (
      bwMeasurement.length() &&
      iopsMeasurement.length()) {
      seqBandwidth = strtol(bwMeasurement.c_str(), 0, 10);
      IOPS = atoi(iopsMeasurement.c_str());
    }
  }

  eos_info("bw=%lld iops=%d", seqBandwidth, IOPS);
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

  if (!gFmdDbMapHandler.GetInconsistencyStatistics(mLocalId, tmp_stats,
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
