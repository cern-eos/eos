// ----------------------------------------------------------------------
// File: FileSystem.hh
// Author: Andreas-Joachim Peters - CERN
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

#ifndef __EOSFST_FILESYSTEM_HH__
#define __EOSFST_FILESYSTEM_HH__

#include "fst/Namespace.hh"
#include "fst/io/FileIoPlugin.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/io/FileIo.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include <vector>
#include <list>
#include <queue>
#include <map>

namespace eos::mq
{
class MessagingRealm;
}

namespace eos::common
{
class Statfs;
}

namespace qclient
{
class SharedHashUpdate;
class SharedHashSubscription;
}

EOSFSTNAMESPACE_BEGIN

class TransferMultiplexer;
class TransferQueue;
class ScanDir;
class Load;

//-------------------------------------------------------------------------------
//! Class FileSystem
//-------------------------------------------------------------------------------
class FileSystem : public eos::common::FileSystem, eos::common::LogId
{
public:
  //! Set of key updates to be tracked at the file system level
  static std::set<std::string> sFsUpdateKeys;

  //-----------------------------------------------------------------------------
  //! Constructor
  //-----------------------------------------------------------------------------
  FileSystem(const common::FileSystemLocator& locator, mq::MessagingRealm* realm);

  //-----------------------------------------------------------------------------
  //! Destructor
  //-----------------------------------------------------------------------------
  ~FileSystem();

  //-----------------------------------------------------------------------------
  //! Set local id as it was published by the MGM the first time, this won't
  //! change throughout the lifetime of this object.
  //-----------------------------------------------------------------------------
  inline void SetLocalId()
  {
    mLocalId = GetId();
  }

  //-----------------------------------------------------------------------------
  //! Get local id value
  //-----------------------------------------------------------------------------
  inline eos::common::FileSystem::fsid_t GetLocalId() const
  {
    return mLocalId;
  }

  //-----------------------------------------------------------------------------
  //! Set local uuid as it was published by the MGM the first time, this won't
  //! change throughout the lifetime of this object.
  //-----------------------------------------------------------------------------
  inline void SetLocalUuid()
  {
    mLocalUuid = GetString("uuid");
  }

  //-----------------------------------------------------------------------------
  //! Get local id value
  //-----------------------------------------------------------------------------
  inline std::string GetLocalUuid() const
  {
    return mLocalUuid;;
  }

  //-----------------------------------------------------------------------------
  //! Configure scanner thread - possibly start the scanner
  //!
  //! @param fst_load file system IO load monitoring object
  //! @param key configuration key
  //! @param value configuration value
  //-----------------------------------------------------------------------------
  void ConfigScanner(Load* fst_load, const std::string& key, long long value);

  inline TransferQueue*
  GetExternQueue()
  {
    return mTxExternQueue;
  }

  void
  SetStatus(eos::common::BootStatus status)
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

  //----------------------------------------------------------------------------
  //
  //----------------------------------------------------------------------------
  eos::common::BootStatus
  GetStatus()
  {
    // we patch this function because we don't want to see the shared information
    // but the 'true' information created locally
    return mLocalBootStatus;
  }

  //----------------------------------------------------------------------------
  //! Broadcast given error message
  //!
  //! @param msg message to be sent
  //----------------------------------------------------------------------------
  void BroadcastError(const char* msg);

  //----------------------------------------------------------------------------
  //! Broadcast given error code and message
  //!
  //! @param errc error code to be sent
  //! @param msg message to be sent
  //----------------------------------------------------------------------------
  void BroadcastError(int errc, const char* errmsg);

  //----------------------------------------------------------------------------
  //! Set given error code and message
  //----------------------------------------------------------------------------
  void SetError(int errc, const char* errmsg);

  //----------------------------------------------------------------------------
  //! Get statfs info about mountpoint
  //----------------------------------------------------------------------------
  std::unique_ptr<eos::common::Statfs> GetStatfs();

  //----------------------------------------------------------------------------
  //! Get file system disk performance metrics eg. IOPS/seq bandwidth
  //----------------------------------------------------------------------------
  void IoPing();

  //----------------------------------------------------------------------------
  //! Get sequential bandwidth
  //----------------------------------------------------------------------------
  inline long long getSeqBandwidth()
  {
    return seqBandwidth;
  }

  //----------------------------------------------------------------------------
  //! Get IOPS
  //----------------------------------------------------------------------------
  inline int getIOPS()
  {
    return IOPS;
  }

  bool condReloadFileIo(std::string iotype)
  {
    if (!mFileIO || mFileIO->GetIoType() != iotype) {
      return false;
    }

    mFileIO.reset(FileIoPlugin::GetIoObject(GetPath().c_str()));
    return true;
  }

  bool getFileIOStats(std::map<std::string, std::string>& map)
  {
    if (!mFileIO) {
      return false;
    }

    // Avoid querying IO stats attributes for certain storage types
    if (mFileIO->GetIoType() == "DavixIo" ||
        mFileIO->GetIoType() == "XrdIo") {
      return false;
    }

    std::string iostats;
    mFileIO->attrGet("sys.iostats", iostats);
    return eos::common::StringConversion::GetKeyValueMap(iostats.c_str(),
           map, "=", ",");
  }

  bool getHealth(std::map<std::string, std::string>& map)
  {
    if (!mFileIO) {
      return false;
    }

    // Avoid querying Health attributes for certain storage types
    if (mFileIO->GetIoType() == "DavixIo" ||
        mFileIO->GetIoType() == "XrdIo") {
      return false;
    }

    // Avoid querying Health attributes for certain storage types
    if (mFileIO->GetIoType() == "DavixIo" ||
        mFileIO->GetIoType() == "XrdIo") {
      return false;
    }

    std::string health;
    mFileIO->attrGet("sys.health", health);
    return eos::common::StringConversion::GetKeyValueMap(health.c_str(),
           map, "=", ",");
  }

  //----------------------------------------------------------------------------
  //! Collect inconsistency statistics about the current file system
  //!
  //! @param prefix optional prefix
  //!
  //! @return map of inconsistency types to counters
  //----------------------------------------------------------------------------
  std::map<std::string, std::string>
  CollectInconsistencyStats(const std::string prefix = "") const;

  //----------------------------------------------------------------------------
  //! Collect orphans registered in the db for the current file system
  //!
  //! @return set of orphan file ids
  //----------------------------------------------------------------------------
  std::set<eos::common::FileId::fileid_t> CollectOrphans() const;

  //----------------------------------------------------------------------------
  //! Update inconsistency info about the current file system
  //----------------------------------------------------------------------------
  void UpdateInconsistencyInfo();

  //----------------------------------------------------------------------------
  //! Get inconsistency sets - this requires the mInconsistencyMutex locked
  //----------------------------------------------------------------------------
  const std::map<std::string, std::set<eos::common::FileId::fileid_t> >&
  GetInconsistencySets() const
  {
    return mInconsistencySets;
  }

  //! Mutex protecting inconsistency stats
  mutable eos::common::RWMutex mInconsistencyMutex;

private:
  //----------------------------------------------------------------------------
  //! Process shared hash update
  //!
  //! @param upd shared hash update
  //----------------------------------------------------------------------------
  void ProcessUpdateCb(qclient::SharedHashUpdate&& upd);

  //! Subscription to underlying shared hash notifications
  std::unique_ptr<qclient::SharedHashSubscription> mSubscription;
  //! Local file system id irrespective of the shared hash status, populated
  //! the first time the id is broadcasted from the mgm
  eos::common::FileSystem::fsid_t mLocalId {0ull};
  //! Local file system uuid irrespective of the shared hash status, populated
  //! the first time the *id* is broadcasted from the mgm
  std::string mLocalUuid;
  std::unique_ptr<eos::fst::ScanDir> mScanDir; ///< Filesystem scanner
  std::unique_ptr<FileIo> mFileIO; ///< File used for statfs calls
  std::unique_ptr<TransferMultiplexer> mTxMultiplexer;
  TransferQueue* mTxExternQueue;
  std::string mTxDirectory;
  unsigned long last_blocks_free;
  time_t last_status_broadcast;
  //! Internal boot state not stored in the shared hash
  std::atomic<eos::common::BootStatus> mLocalBootStatus;
  std::map<std::string, size_t> mInconsistencyStats;
  std::map<std::string, std::set<eos::common::FileId::fileid_t> >
  mInconsistencySets;
  long long seqBandwidth; // measurement of sequential bandwidth
  int IOPS; // measurement of IOPS
  bool mRecoverable; // true if a filesystem was booted and then set to ops error
};

EOSFSTNAMESPACE_END

#endif
