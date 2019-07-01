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
FileSystem::FileSystem(const common::FileSystemLocator &locator,
                       XrdMqSharedObjectManager* som,
                       qclient::SharedManager *qsom):
  eos::common::FileSystem(locator, som, qsom, true),
  mScanDir()
{
  std::string queuepath = locator.getQueuePath();

  last_blocks_free = 0;
  last_status_broadcast = 0;
  seqBandwidth = 0;
  IOPS = 0;
  transactionDirectory = "";
  mLocalBootStatus = eos::common::BootStatus::kDown;
  mTxDrainQueue = new TransferQueue(&mDrainQueue);
  mTxBalanceQueue = new TransferQueue(&mBalanceQueue);
  mTxExternQueue = new TransferQueue(&mExternQueue);
  mTxMultiplexer.Add(mTxDrainQueue);
  mTxMultiplexer.Add(mTxBalanceQueue);
  mTxMultiplexer.Add(mTxExternQueue);
  mTxMultiplexer.Run();
  mRecoverable = false;
  mFileIO.reset(FileIoPlugin::GetIoObject(mPath));
}

/*----------------------------------------------------------------------------*/
FileSystem::~FileSystem()
{
  mScanDir.release();
  mFileIO.release();
  gFmdDbMapHandler.ShutdownDB(GetId());
  // ----------------------------------------------------------------------------
  // @todo we accept this tiny memory leak to be able to let running
  // transfers callback their queue
  // -> we don't delete them here!
  //  if (mTxDrainQueue) {
  //    delete mTxDrainQueue;
  //  }
  //  if (mTxBalanceQueue) {
  //    delete mTxBalanceQueue;
  //  }
  //  if (mTxExternQueue) {
  //    delete mTxExternQueue;
  //  }
  // ----------------------------------------------------------------------------
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
    return 0;
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
        XrdOucString fstPath;
        eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(), localprefix.c_str(),
                                                fstPath);
        unsigned long long fileid = eos::common::FileId::Hex2Fid(hexfid.c_str());
        // we allow to keep files open for 1 week
        bool isOpen = gOFS.openedForWriting.isOpen(GetId(), fileid);

        if ((buf.st_mtime < (time(NULL) - (7 * 86400))) && (!isOpen)) {
          FmdHelper* fMd = 0;
          fMd = gFmdDbMapHandler.LocalGetFmd(fileid, GetId(), 0, 0, 0, 0, true);

          if (fMd) {
            size_t valid_loc;
            auto location_set = FmdHelper::GetLocations(fMd->mProtoFmd, valid_loc);

            if (location_set.count(GetId())) {
              // close that transaction and keep the file
              gOFS.Storage->CloseTransaction(GetId(), fileid);
              delete fMd;
              continue;
            }

            delete fMd;
          }

          eos_static_info("action=delete transaction=%llx fstpath=%s",
                          sname.c_str(),
                          fulltransactionpath.c_str());
          // -------------------------------------------------------------------------------------------------------
          // clean-up this file locally
          // -------------------------------------------------------------------------------------------------------
          XrdOucErrInfo error;
          int retc = gOFS._rem("/CLEANTRANSACTIONS",
                               error, 0, 0, fstPath.c_str(),
                               fileid, GetId(), true);

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
        XrdOucString fstPath;
        eos::common::FileId::FidPrefix2FullPath(hexfid.c_str(),
                                                localprefix, fstPath);
        unsigned long long fid = eos::common::FileId::Hex2Fid(hexfid.c_str());

        // try to sync this file from the MGM
        if (gFmdDbMapHandler.ResyncMgm(GetId(), fid, manager)) {
          eos_static_info("msg=\"resync ok\" fsid=%lu fid=%08llx",
                          (unsigned long) GetId(),
                          fid);
        } else {
          eos_static_err("msg=\"resync failed\" fsid=%lu fid=%08llx",
                         (unsigned long) GetId(), fid);
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
    mScanDir.reset(new ScanDir(GetPath().c_str(), GetId(), fst_load, true));
    eos_info("msg=\"%s\"", "started 'ScanDir' thread with default parameters");
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


EOSFSTNAMESPACE_END
