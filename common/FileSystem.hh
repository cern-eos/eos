//------------------------------------------------------------------------------
// File: FileSystem.hh
// Author: Andreas-Joachim Peters - CERN
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
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSCOMMON_FILESYSTEM_HH__
#define __EOSCOMMON_FILESYSTEM_HH__

#include "common/Namespace.hh"
#include "mq/XrdMqSharedObject.hh"
#include <string>
#include <stdint.h>
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif
#include <atomic>

EOSCOMMONNAMESPACE_BEGIN;

//! Values for a boot status
enum class BootStatus {
  kOpsError = -2,
  kBootFailure = -1,
  kDown = 0,
  kBootSent = 1,
  kBooting = 2,
  kBooted = 3
};

//! Values for a drain status
enum class DrainStatus {
  kNoDrain = 0,
  kDrainPrepare = 1,
  kDrainWait = 2,
  kDraining = 3,
  kDrained = 4,
  kDrainStalling = 5,
  kDrainExpired = 6,
  kDrainFailed = 7
};

//! Values describing if a filesystem is online or offline
//! (combination of multiple conditions)
enum class ActiveStatus {
  kUndefined = -1,
  kOffline = 0,
  kOnline = 1
};

#define EOS_TAPE_FSID 65535
#define EOS_TAPE_MODE_T (0x10000000ll)

class TransferQueue;

//------------------------------------------------------------------------------
//! Describes how to physically locate a filesystem:
//! - Host + port of the corresponding FST.
//! - Local path of the filesystem.
//------------------------------------------------------------------------------
class FileSystemLocator {
public:
  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  FileSystemLocator() {}

  //----------------------------------------------------------------------------
  //! Constructor, pass manually individual components
  //----------------------------------------------------------------------------
  FileSystemLocator(const std::string &host, int port,
    const std::string &localpath);

  //----------------------------------------------------------------------------
  //! Try to parse a "queuepath"
  //----------------------------------------------------------------------------
  static bool fromQueuePath(const std::string &queuepath, FileSystemLocator &out);

  //----------------------------------------------------------------------------
  //! Get host
  //----------------------------------------------------------------------------
  std::string getHost() const;

  //----------------------------------------------------------------------------
  //! Get port
  //----------------------------------------------------------------------------
  int getPort() const;

  //----------------------------------------------------------------------------
  //! Get hostport, concatenated together as "host:port"
  //----------------------------------------------------------------------------
  std::string getHostPort() const;

  //----------------------------------------------------------------------------
  //! Get queuepath
  //----------------------------------------------------------------------------
  std::string getQueuePath() const;

  //----------------------------------------------------------------------------
  //! Get local path
  //----------------------------------------------------------------------------
  std::string getLocalPath() const;

private:
  std::string host;
  int32_t port = 0;
  std::string localpath;
};

//------------------------------------------------------------------------------
//! Base Class abstracting the internal representation of a filesystem inside
//! the MGM and FST
//------------------------------------------------------------------------------
class FileSystem
{
protected:
  //! Queue Name/Path    = 'queue' + 'path' e.g. /eos/'host'/fst/data01
  std::string mQueuePath;

  //! Queue Name         = 'queue'          e.g. /eos/'host'/fst
  std::string mQueue;

  //! Filesystem Path e.g. /data01
  std::string mPath;

  //! Indicates that if the filesystem is deleted - the deletion should be
  //! broadcasted or not (only MGMs should broadcast deletion!)
  bool BroadCastDeletion;

  //! Handle to the shared object manager object
  //! Before usage mSom needs a read lock and mHash has to be validated
  //! to avoid race conditions in deletion.
  XrdMqSharedObjectManager* mSom;

  //! Handle to the drain queue
  TransferQueue* mDrainQueue;

  //! Handle to the balance queue
  TransferQueue* mBalanceQueue;

  //! Handle to the extern queue
  TransferQueue* mExternQueue;

  //! boot status stored inside the object not the hash
  BootStatus mInternalBootStatus;

public:
  //------------------------------------------------------------------------------
  //! Struct & Type definitions
  //------------------------------------------------------------------------------

  //! File System ID type
  typedef uint32_t fsid_t;

  //! File System Status type
  typedef int32_t fsstatus_t;

  //! Snapshot Structure of a filesystem

  typedef struct fs_snapshot {
    fsid_t mId;
    std::string mQueue;
    std::string mQueuePath;
    std::string mPath;
    std::string mErrMsg;
    std::string mGroup;
    std::string mUuid;
    std::string mHost;
    std::string mHostPort;
    std::string mProxyGroup;
    std::string mS3Credentials;
    int8_t      mFileStickyProxyDepth;
    std::string mPort;
    std::string mGeoTag;
    std::string mForceGeoTag;
    size_t mPublishTimestamp;
    int mGroupIndex;
    std::string mSpace;
    BootStatus mStatus;
    fsstatus_t mConfigStatus;
    DrainStatus mDrainStatus;
    ActiveStatus mActiveStatus;
    double mBalThresh;
    long long mHeadRoom;
    unsigned int mErrCode;
    time_t mBootSentTime;
    time_t mBootDoneTime;
    time_t mHeartBeatTime;
    double mDiskUtilization;
    double mDiskWriteRateMb;
    double mDiskReadRateMb;
    double mNetEthRateMiB;
    double mNetInRateMiB;
    double mNetOutRateMiB;
    double mWeightRead;
    double mWeightWrite;
    double mNominalFilled;
    double mDiskFilled;
    long long mDiskCapacity;
    long long mDiskFreeBytes;
    long mDiskType;
    long mDiskBsize;
    long mDiskBlocks;
    long mDiskBused;
    long mDiskBfree;
    long mDiskBavail;
    long mDiskFiles;
    long mDiskFused;
    long mDiskFfree;
    long mFiles;
    long mDiskNameLen;
    long mDiskRopen;
    long mDiskWopen;
    long mScanRate; ///< Maximum scan rate in MB/s
    time_t mScanInterval;
    time_t mGracePeriod;
    time_t mDrainPeriod;
    bool mDrainerOn;

    bool hasHeartbeat() const {
      time_t now = time(NULL);

      if ((now - mHeartBeatTime) < 60) {
        // we allow some time drift plus overload delay of 60 seconds
        return true;
      }

      return false;
    }

  } fs_snapshot_t;

  typedef struct host_snapshot {
    std::string mQueue;
    std::string mHost;
    std::string mHostPort;
    std::string mGeoTag;
    size_t mPublishTimestamp;
    ActiveStatus mActiveStatus;
    time_t mHeartBeatTime;
    double mNetEthRateMiB;
    double mNetInRateMiB;
    double mNetOutRateMiB;
    long mGopen; // number of files open as data proxy

    bool hasHeartbeat() const {
      time_t now = time(NULL);

      if ((now - mHeartBeatTime) < 60) {
        // we allow some time drift plus overload delay of 60 seconds
        return true;
      }

      return false;
    }

  } host_snapshot_t;

  //----------------------------------------------------------------------------
  //! Constructor
  //! @param queuepath Named Queue to specify the receiver filesystem of
  //!                  modifications e.g. /eos/<host:port>/fst/<path>
  //! @param queue     Named Queue to specify the receiver of modifications
  //!                  e.g. /eos/<host:port>/fst
  //! @param som       Handle to the shared object manager to store filesystem
  //!                  key-value pairs
  //! @param bc2mgm   If true we broad cast to the management server
  //----------------------------------------------------------------------------
  FileSystem(const char* queuepath, const char* queue,
             XrdMqSharedObjectManager* som, bool bc2mgm = false);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  virtual ~FileSystem();

  //----------------------------------------------------------------------------
  // Enums
  //----------------------------------------------------------------------------

  //! Values for a configuration status
  enum eConfigStatus {
    kUnknown = -1,
    kOff = 0,
    kEmpty,
    kDrainDead,
    kDrain,
    kRO,
    kWO,
    kRW
  };

  //! Value indication the way a boot message should be executed on an FST node
  enum eBootConfig {
    kBootOptional = 0,
    kBootForced = 1,
    kBootResync = 2
  };

  //----------------------------------------------------------------------------
  // Get file system status as a string
  //----------------------------------------------------------------------------
  static const char* GetStatusAsString(BootStatus status);
  static const char* GetDrainStatusAsString(DrainStatus status);
  static const char* GetConfigStatusAsString(int status);

  //----------------------------------------------------------------------------
  //! Parse a string status into the enum value
  //----------------------------------------------------------------------------
  static BootStatus GetStatusFromString(const char* ss);

  //----------------------------------------------------------------------------
  //! Parse a drain status into the enum value
  //----------------------------------------------------------------------------
  static DrainStatus GetDrainStatusFromString(const char* ss);

  //----------------------------------------------------------------------------
  //! Parse a configuration status into the enum value
  //----------------------------------------------------------------------------
  static int GetConfigStatusFromString(const char*  ss);

  //----------------------------------------------------------------------------
  //! Parse an active status into the enum value
  //----------------------------------------------------------------------------
  static ActiveStatus GetActiveStatusFromString(const char*  ss);

  //----------------------------------------------------------------------------
  //! Get the message string for an auto boot request
  //----------------------------------------------------------------------------
  static const char* GetAutoBootRequestString();

  //----------------------------------------------------------------------------
  //! Get the message string to register a filesystem
  //----------------------------------------------------------------------------
  static const char* GetRegisterRequestString();

  //----------------------------------------------------------------------------
  //! Cache Members
  //----------------------------------------------------------------------------
  ActiveStatus cActive; ///< cache value of the active status
  XrdSysMutex cActiveLock; ///< lock protecting the cached active status
  time_t cActiveTime; ///< unix time stamp of last update of the active status
  BootStatus cStatus; ///< cache value of the status
  time_t cStatusTime; ///< unix time stamp of last update of the cached status
  XrdSysMutex cStatusLock; ///< lock protecting the cached status
  std::atomic<fsstatus_t> cConfigStatus; ///< cached value of the config status
  XrdSysMutex cConfigLock; ///< lock protecting the cached config status
  time_t cConfigTime; ///< unix time stamp of last update of the cached config status

  //----------------------------------------------------------------------------
  //! Open transaction to initiate bulk modifications on a file system
  //----------------------------------------------------------------------------
  bool
  OpenTransaction()
  {
    RWMutexReadLock lock(mSom->HashMutex);
    XrdMqSharedHash* hash = nullptr;

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->OpenTransaction();
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Close transaction to finish modifications on a file system
  //----------------------------------------------------------------------------
  bool
  CloseTransaction()
  {
    RWMutexReadLock lock(mSom->HashMutex);
    XrdMqSharedHash* hash = nullptr;

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->CloseTransaction();
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Set a filesystem ID.
  //----------------------------------------------------------------------------
  bool
  SetId(fsid_t fsid)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->Set("id", (long long) fsid);
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Set a key-value pair in a filesystem and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool
  SetString(const char* key, const char* str, bool broadcast = true)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->Set(key, str, broadcast);
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Set a double value by name and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool
  SetDouble(const char* key, double f, bool broadcast = true)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->Set(key, f, broadcast);
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Set a long long value and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool
  SetLongLong(const char* key, long long l, bool broadcast = true)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->Set(key, l, broadcast);
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Remove a key from a filesystem and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool
  RemoveKey(const char* key, bool broadcast = true)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      hash->Delete(key, broadcast);
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Set the filesystem status.
  //----------------------------------------------------------------------------
  bool
  SetStatus(BootStatus status, bool broadcast = true)
  {
    mInternalBootStatus = status;
    return SetString("stat.boot", GetStatusAsString(status), broadcast);
  }

  //----------------------------------------------------------------------------
  //! Set the activation status
  //----------------------------------------------------------------------------
  bool
  SetActiveStatus(ActiveStatus active)
  {
    if (active == ActiveStatus::kOnline) {
      return SetString("stat.active", "online", false);
    } else {
      return SetString("stat.active", "offline", false);
    }
  }

  //----------------------------------------------------------------------------
  //! Set the draining status
  //----------------------------------------------------------------------------
  bool
  SetDrainStatus(DrainStatus status, bool broadcast = true)
  {
    return SetString("stat.drain", GetDrainStatusAsString(status), broadcast);
  }

  //----------------------------------------------------------------------------
  //! Get the activation status via a cache.
  //! This can be used with a small cache which 1s expiration time to avoid too
  //! many lookup's in tight loops.
  //----------------------------------------------------------------------------
  ActiveStatus
  GetActiveStatus(bool cached = false);

  //----------------------------------------------------------------------------
  //! Get the activation status from a snapshot
  //----------------------------------------------------------------------------
  ActiveStatus
  GetActiveStatus(const fs_snapshot_t& snapshot)
  {
    return snapshot.mActiveStatus;
  }

  //----------------------------------------------------------------------------
  //! Get all keys in a vector of strings.
  //----------------------------------------------------------------------------
  bool
  GetKeys(std::vector<std::string>& keys)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      keys = hash->GetKeys();
      return true;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Get the string value by key
  //----------------------------------------------------------------------------
  std::string
  GetString(const char* key)
  {
    std::string skey = key;

    if (skey == "<n>") {
      return "1";
    }

    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      return hash->Get(skey);
    } else {
      return "";
    }
  }

  //----------------------------------------------------------------------------
  //! Get the string value by key.
  //----------------------------------------------------------------------------
  double
  GetAge(const char* key)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      // avoid to return a string with a 0 pointer !
      return hash->GetAgeInSeconds(key);
    } else {
      return 0;
    }
  }

  //----------------------------------------------------------------------------
  //! Get a long long value by key
  //----------------------------------------------------------------------------
  long long
  GetLongLong(const char* key)
  {
    std::string skey = key;

    if (skey == "<n>") {
      return 1;
    }

    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      return hash->GetLongLong(key);
    } else {
      return 0;
    }
  }

  //----------------------------------------------------------------------------
  //! Get a double value by key
  //----------------------------------------------------------------------------
  double
  GetDouble(const char* key)
  {
    XrdMqSharedHash* hash = nullptr;
    RWMutexReadLock lock(mSom->HashMutex);

    if ((hash = mSom->GetObject(mQueuePath.c_str(), "hash"))) {
      return hash->GetDouble(key);
    } else {
      return 0;
    }
  }

  //----------------------------------------------------------------------------
  //! Return handle to the drain queue.
  //----------------------------------------------------------------------------
  TransferQueue*
  GetDrainQueue()
  {
    return mDrainQueue;
  }

  //----------------------------------------------------------------------------
  //! Return handle to the balance queue.
  //----------------------------------------------------------------------------
  TransferQueue*
  GetBalanceQueue()
  {
    return mBalanceQueue;
  }

  //----------------------------------------------------------------------------
  //! Return handle to the external queue.
  //----------------------------------------------------------------------------
  TransferQueue*
  GetExternQueue()
  {
    return mExternQueue;
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem id.
  //----------------------------------------------------------------------------
  fsid_t
  GetId()
  {
    return (fsid_t) GetLongLong("id");
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem queue path.
  //----------------------------------------------------------------------------
  std::string
  GetQueuePath()
  {
    return mQueuePath;
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem queue name
  //----------------------------------------------------------------------------
  std::string
  GetQueue()
  {
    return mQueue;
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem path
  //----------------------------------------------------------------------------
  std::string
  GetPath()
  {
    return mPath;
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem status (via a cache)
  //----------------------------------------------------------------------------
  BootStatus
  GetStatus(bool cached = false);

  //----------------------------------------------------------------------------
  //! Get internal boot status
  //----------------------------------------------------------------------------
  BootStatus
  GetInternalBootStatus()
  {
    return mInternalBootStatus;
  }

  //----------------------------------------------------------------------------
  //! Return the drain status
  //----------------------------------------------------------------------------
  DrainStatus
  GetDrainStatus()
  {
    return GetDrainStatusFromString(GetString("stat.drain").c_str());
  }

  //----------------------------------------------------------------------------
  //! Return the configuration status (via cache)
  //----------------------------------------------------------------------------
  fsstatus_t
  GetConfigStatus(bool cached = false);

  //----------------------------------------------------------------------------
  //! Return the error code variable of that filesystem
  //----------------------------------------------------------------------------
  int
  GetErrCode()
  {
    return atoi(GetString("stat.errc").c_str());
  }

  //------------------------------------------------------------------------------
  //! Snapshots all variables of a filesystem into a snapshot struct
  //!
  //! @param fs snapshot struct to be filled
  //! @param dolock indicates if the shared hash representing the filesystem has
  //!               to be locked or not
  //!
  //! @return true if successful, otherwise false
  //------------------------------------------------------------------------------
  bool SnapShotFileSystem(FileSystem::fs_snapshot_t& fs, bool dolock = true);

  //------------------------------------------------------------------------------
  //! Snapshots all variables of a filesystem into a snapshot struct
  //!
  //! @param fs snapshot struct to be filled
  //! @param dolock indicates if the shared hash representing the filesystem has
  //!               to be locked or not
  //!
  //! @return true if successful, otherwise false
  //------------------------------------------------------------------------------
  static bool SnapShotHost(XrdMqSharedObjectManager* som,
                           const std::string& queue,
                           FileSystem::host_snapshot_t& fs,
                           bool dolock = true);

  //----------------------------------------------------------------------------
  //! Function printing the file system info to the table
  //----------------------------------------------------------------------------
  void Print(TableHeader& table_mq_header, TableData& table_mq_data,
             std::string listformat, const std::string& filter = "");

  //----------------------------------------------------------------------------
  //! Store a configuration key-val pair.
  //! Internally, these keys are not prefixed with 'stat.'
  //!
  //! @param key key string
  //! @param val value string
  //----------------------------------------------------------------------------
  void CreateConfig(std::string& key, std::string& val);
};

EOSCOMMONNAMESPACE_END;

#endif
