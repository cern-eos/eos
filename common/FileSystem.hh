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
#include "common/Locators.hh"
#include "common/table_formatter/TableCell.hh"
#include "mq/SharedHashWrapper.hh"
#include <string>
#include <stdint.h>
#ifndef __APPLE__
#include <sys/vfs.h>
#else
#include <sys/param.h>
#include <sys/mount.h>
#endif
#include <atomic>
#include <list>

namespace eos
{
namespace mq
{
class MessagingRealm;
}
}

namespace qclient
{

class SharedManager;

}

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
  kOnline = 1,
  kOverload = 2
};

//! Values for a configuration status - stored persistently in the
//! filesystem configuration
enum class ConfigStatus {
  kUnknown = -1,
  kOff = 0,
  kEmpty,
  kDrainDead,
  kGroupDrain,
  kDrain,
  kRO,
  kWO,
  kRW
};

inline bool operator<(ConfigStatus one, ConfigStatus two)
{
  return static_cast<int>(one) < static_cast<int>(two);
}

inline bool operator<=(ConfigStatus one, ConfigStatus two)
{
  return static_cast<int>(one) <= static_cast<int>(two);
}

#define EOS_TAPE_FSID 65535
#define EOS_TAPE_MODE_T (0x10000000ll)

class TransferQueue;

//------------------------------------------------------------------------------
//! Contains a batch of key-value updates on the attributes of a filesystem.
//! Build up the batch, then submit it through applyBatch.
//!
//! Note: If the batch mixes durable, transient, and/or local values, three
//! "transactions" will take place:
//!
//! - All durable values will be updated atomically.
//! - All transient values will be updated atomically.
//! - All local values will be updated atomically.
//!
//! However, the entire operation as a whole will NOT be atomic.
//------------------------------------------------------------------------------
class FileSystemUpdateBatch
{
public:
  //----------------------------------------------------------------------------
  //! Type definitions
  //----------------------------------------------------------------------------
  using fsid_t = uint32_t;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemUpdateBatch();

  //----------------------------------------------------------------------------
  //! Set filesystem ID - durable.
  //----------------------------------------------------------------------------
  void setId(fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Set the draining status - local.
  //----------------------------------------------------------------------------
  void setDrainStatusLocal(DrainStatus status);

  //----------------------------------------------------------------------------
  //! Set durable string.
  //!
  //! All observers of this filesystem are guaranteed to receive the update
  //! eventually, and all updates are guaranteed to be applied in the same
  //! order for all observers.
  //!
  //! If two racing setStringDurable are attempted on the same key, only one
  //! will survive, and all observers will agree as to which one survives.
  //----------------------------------------------------------------------------
  void setStringDurable(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Set transient string. Depending on network instabilities,
  //! process restarts, or the phase of the moon, some or all observers of this
  //! filesystem may or may not receive the update.
  //!
  //! Transient updates may be applied out of order, and if multiple subscribers
  //! try to modify the same value, it's possible that observers will not all
  //! converge on a single consistent value.
  //----------------------------------------------------------------------------
  void setStringTransient(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Set local string. This node, and only this node will store the value,
  //! and only until process restart.
  //----------------------------------------------------------------------------
  void setStringLocal(const std::string& key, const std::string& value);

  //----------------------------------------------------------------------------
  //! Set durable int64_t - serialize as string automatically.
  //----------------------------------------------------------------------------
  void setLongLongDurable(const std::string& key, int64_t value);

  //----------------------------------------------------------------------------
  //! Set transient int64_t - serialize as string automatically.
  //----------------------------------------------------------------------------
  void setLongLongTransient(const std::string& key, int64_t value);

  //----------------------------------------------------------------------------
  //! Set local int64_t - serialize as string automatically.
  //----------------------------------------------------------------------------
  void setLongLongLocal(const std::string& key, int64_t value);

  //----------------------------------------------------------------------------
  //! Get MQ update batch
  //----------------------------------------------------------------------------
  const mq::SharedHashWrapper::Batch& getBatch() const;

private:
  mq::SharedHashWrapper::Batch mBatch;
};

//------------------------------------------------------------------------------
//! Describes how to locate an FST: host + port.
//------------------------------------------------------------------------------
class FstLocator
{
public:
  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  FstLocator();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FstLocator(const std::string& host, int port);

  //----------------------------------------------------------------------------
  //! Get host
  //----------------------------------------------------------------------------
  std::string getHost() const;

  //----------------------------------------------------------------------------
  //! Get port
  //----------------------------------------------------------------------------
  int getPort() const;

  //----------------------------------------------------------------------------
  //! Get queuepath
  //----------------------------------------------------------------------------
  std::string getQueuePath() const;

  //----------------------------------------------------------------------------
  //! Get host:port
  //----------------------------------------------------------------------------
  std::string getHostPort() const;

  //----------------------------------------------------------------------------
  //! Try to parse from queuepath
  //----------------------------------------------------------------------------
  static bool fromQueuePath(const std::string& queuepath, FstLocator& out);

private:
  std::string mHost;
  int mPort;
};

//------------------------------------------------------------------------------
//! Describes a FileSystem Group:
//! - Space
//! - Index
//------------------------------------------------------------------------------
class GroupLocator
{
public:
  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  GroupLocator();

  //----------------------------------------------------------------------------
  //! Get group (space.index)
  //----------------------------------------------------------------------------
  std::string getGroup() const;

  //----------------------------------------------------------------------------
  //! Get space
  //----------------------------------------------------------------------------
  std::string getSpace() const;

  //----------------------------------------------------------------------------
  //! Get index
  //----------------------------------------------------------------------------
  int getIndex() const;

  //----------------------------------------------------------------------------
  //! Parse full group (space.index)
  //!
  //! NOTE: In case parsing fails, "out" will still be filled
  //! with "description.0" to match legacy behaviour.
  //----------------------------------------------------------------------------
  static bool parseGroup(const std::string& description, GroupLocator& out);

private:
  std::string mGroup;
  std::string mSpace;
  int mIndex = 0;
};

//------------------------------------------------------------------------------
//! Describes critical parameters of a FileSystem, which are necessary to have
//! when registering a filesystem on the MGM.
//!
//! A FileSystemLocator can physically locate a FileSystem, but we still can't
//! operate it on the MGM without knowing more information. (id, group, uuid)
//------------------------------------------------------------------------------
class FileSystemCoreParams
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemCoreParams(uint32_t id, const FileSystemLocator& fsLocator,
                       const GroupLocator& grpLocator, const std::string& uuid,
                       ConfigStatus cfg);

  //----------------------------------------------------------------------------
  //! Get locator
  //----------------------------------------------------------------------------
  const FileSystemLocator& getLocator() const;

  //----------------------------------------------------------------------------
  //! Get group locator
  //----------------------------------------------------------------------------
  const GroupLocator& getGroupLocator() const;

  //----------------------------------------------------------------------------
  //! Get id
  //----------------------------------------------------------------------------
  uint32_t getId() const;

  //----------------------------------------------------------------------------
  //! Get uuid
  //----------------------------------------------------------------------------
  std::string getUuid() const;

  //----------------------------------------------------------------------------
  //! Get current ConfigStatus
  //----------------------------------------------------------------------------
  ConfigStatus getConfigStatus() const;

  //----------------------------------------------------------------------------
  //! Get queuepath
  //----------------------------------------------------------------------------
  std::string getQueuePath() const;

  //----------------------------------------------------------------------------
  //! Get host
  //----------------------------------------------------------------------------
  std::string getHost() const;

  //----------------------------------------------------------------------------
  //! Get hostport
  //----------------------------------------------------------------------------
  std::string getHostPort() const;

  //----------------------------------------------------------------------------
  //! Get FST queue
  //----------------------------------------------------------------------------
  std::string getFSTQueue() const;

  //----------------------------------------------------------------------------
  //! Get group (space.index)
  //----------------------------------------------------------------------------
  std::string getGroup() const;

  //----------------------------------------------------------------------------
  //! Get space
  //----------------------------------------------------------------------------
  std::string getSpace() const;

private:
  uint32_t mFsId;
  FileSystemLocator mLocator;
  GroupLocator mGroup;
  std::string mUuid;
  ConfigStatus mConfigStatus;
};


//------------------------------------------------------------------------------
//! Base Class abstracting the internal representation of a filesystem inside
//! the MGM and FST
//------------------------------------------------------------------------------
class FileSystem
{
protected:
  //! This filesystem's locator object
  FileSystemLocator mLocator;
  //! Locator for shared hash
  SharedHashLocator mHashLocator;
  //! Indicates that if the filesystem is deleted - the deletion should be
  //! broadcasted or not (only MGMs should broadcast deletion!)
  bool BroadCastDeletion;
  //! This filesystem's messaging realm
  mq::MessagingRealm* mRealm;
  //! Handle to the balance queue
  TransferQueue* mBalanceQueue;
  //! Handle to the extern queue
  TransferQueue* mExternQueue;
  //! boot status stored inside the object not the hash
  BootStatus mInternalBootStatus;
public:
  //----------------------------------------------------------------------------
  //! Struct & Type definitions
  //----------------------------------------------------------------------------

  //! File System ID type
  typedef uint32_t fsid_t;

  //! Snapshot Structure of a filesystem
  struct fs_snapshot_t {
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
    int32_t mPort;
    std::string mGeoTag;
    std::string mForceGeoTag;
    size_t mPublishTimestamp;
    int mGroupIndex;
    std::string mSpace;
    BootStatus mStatus;
    ConfigStatus mConfigStatus;
    DrainStatus mDrainStatus;
    ActiveStatus mActiveStatus;
    double mBalThresh;
    long long mHeadRoom;
    unsigned int mErrCode;
    time_t mBootSentTime;
    time_t mBootDoneTime;
    double mDiskUtilization;
    double mDiskWriteRateMb;
    double mDiskReadRateMb;
    double mNetEthRateMiB;
    double mNetInRateMiB;
    double mNetOutRateMiB;
    double mNominalFilled;
    double mDiskFilled;
    long long mDiskCapacity;
    long long mDiskFreeBytes;
    long mDiskType;
    long mDiskBsize;
    long mDiskBlocks;
    // @todo (esindril) to be dropped
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
    long mMaxDiskRopen;
    long mMaxDiskWopen;
    long mScanIoRate; ///< Maximum scan rate in MB/s
    long mScanEntryInterval; ///< Time after which a scanned file is rescanned
    long mScanDiskInterval; ///< Time after which the disk scanner runs again
    long mScanNsInterval; ///< Time after which the disk scanner runs again
    long mScanNsRate; ///< Max ns scan rate in entries/s
    ///< Time in seconds after which fsck inconsistencies are refreshed
    long mFsckRefreshInterval;
    time_t mGracePeriod;
    time_t mDrainPeriod;

    //--------------------------------------------------------------------------
    //! Get active status
    //--------------------------------------------------------------------------
    ActiveStatus GetActiveStatus() const
    {
      return mActiveStatus;
    }

    //--------------------------------------------------------------------------
    //! Empty constructor
    //--------------------------------------------------------------------------
    fs_snapshot_t();

    //--------------------------------------------------------------------------
    //! "Absorb" all information contained within coreParams into this object.
    //! Fields which are not present in coreParams (ie mNetInRateMiB) remain
    //! unchanged.
    //--------------------------------------------------------------------------
    void fillFromCoreParams(const FileSystemCoreParams& coreParams);
  };

  struct host_snapshot_t {
    std::string mQueue;
    std::string mHost;
    std::string mHostPort;
    std::string mGeoTag;
    std::string mProxyGroups;
    size_t mPublishTimestamp;
    ActiveStatus mActiveStatus;
    double mNetEthRateMiB;
    double mNetInRateMiB;
    double mNetOutRateMiB;
  };

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
  FileSystem(const FileSystemLocator& locator, mq::MessagingRealm* realm,
             bool b2mgm = false);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  virtual ~FileSystem();

  //----------------------------------------------------------------------------
  // Get underlying hash locator
  //----------------------------------------------------------------------------
  SharedHashLocator getHashLocator() const;

  //----------------------------------------------------------------------------
  // Enums
  //----------------------------------------------------------------------------

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
  static const char* GetConfigStatusAsString(ConfigStatus status);

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
  static ConfigStatus GetConfigStatusFromString(const char*  ss);

  //----------------------------------------------------------------------------
  //! Parse an active status into the enum value
  //----------------------------------------------------------------------------
  static ActiveStatus GetActiveStatusFromString(const char*  ss);

  //----------------------------------------------------------------------------
  //! Get the message string to register a filesystem
  //----------------------------------------------------------------------------
  static const char* GetRegisterRequestString();

  //----------------------------------------------------------------------------
  //! Convert input to file system id
  //!
  //! @param value input string
  //!
  //! @return file system id if conversion successful, otherwise 0
  //----------------------------------------------------------------------------
  static eos::common::FileSystem::fsid_t
  ConvertToFsid(const std::string& value);

  //----------------------------------------------------------------------------
  //! Serializes hash contents as follows 'key1=val1 key2=val2 ... keyn=valn'
  //! but return only keys that don't start with filter_prefix. If specified,
  //! the string values will be curl encoded
  //!
  //! @param contents key value pairs
  //! @param filter_prefix prefix used for filtering keys
  //!
  //! @return string representation of the content for the hash
  //----------------------------------------------------------------------------
  static std::string
  SerializeWithFilter(const std::map<std::string, std::string>& contents,
                      std::list<std::string> filter_prefixes);

  //----------------------------------------------------------------------------
  //! Cache Members
  //----------------------------------------------------------------------------
  ActiveStatus cActive; ///< cache value of the active status
  XrdSysMutex cActiveLock; ///< lock protecting the cached active status
  time_t cActiveTime; ///< unix time stamp of last update of the active status
  BootStatus cStatus; ///< cache value of the status
  time_t cStatusTime; ///< unix time stamp of last update of the cached status
  XrdSysMutex cStatusLock; ///< lock protecting the cached status
  std::atomic<ConfigStatus> cConfigStatus; ///< cached value of the config status
  XrdSysMutex cConfigLock; ///< lock protecting the cached config status
  time_t cConfigTime; ///< unix time stamp of last update of the cached config status

  //----------------------------------------------------------------------------
  //! Apply the given batch of updates
  //----------------------------------------------------------------------------
  bool applyBatch(const FileSystemUpdateBatch& batch);

  //----------------------------------------------------------------------------
  //! Apply the given core parameters
  //----------------------------------------------------------------------------
  bool applyCoreParams(const FileSystemCoreParams& params);

  //----------------------------------------------------------------------------
  //! Set a single local long long
  //----------------------------------------------------------------------------
  bool setLongLongLocal(const std::string& key, int64_t value);

  //----------------------------------------------------------------------------
  //! Set a filesystem ID.
  //----------------------------------------------------------------------------
  bool
  SetId(fsid_t fsid)
  {
    return SetString("id", std::to_string(fsid).c_str());
  }

  //----------------------------------------------------------------------------
  //! Set a key-value pair in a filesystem and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool SetString(const char* key, const char* str, bool broadcast = true);

  //----------------------------------------------------------------------------
  //! Set a double value by name and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool
  SetDouble(const char* key, double f)
  {
    return SetString(key, std::to_string(f).c_str());
  }

  //----------------------------------------------------------------------------
  //! Set a long long value and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool
  SetLongLong(const char* key, long long l, bool broadcast = true)
  {
    return SetString(key, std::to_string(l).c_str(), broadcast);
  }

  //----------------------------------------------------------------------------
  //! Remove a key from a filesystem and evt. broadcast it.
  //----------------------------------------------------------------------------
  bool RemoveKey(const char* key, bool broadcast = true);

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
    } else if (active == ActiveStatus::kOverload) {
      return SetString("stat.active", "overload", false);
    } else {
      return SetString("stat.active", "offline", false);
    }
  }

  //----------------------------------------------------------------------------
  //! Set the draining status
  //----------------------------------------------------------------------------
  bool
  SetDrainStatus(DrainStatus status)
  {
    return SetString("local.drain", GetDrainStatusAsString(status), false);
  }

  //----------------------------------------------------------------------------
  //! Get the activation status via a cache.
  //! This can be used with a small cache which 1s expiration time to avoid too
  //! many lookup's in tight loops.
  //----------------------------------------------------------------------------
  ActiveStatus
  GetActiveStatus(bool cached = false);

  //----------------------------------------------------------------------------
  //! Get all keys in a vector of strings.
  //----------------------------------------------------------------------------
  bool GetKeys(std::vector<std::string>& keys);

  //----------------------------------------------------------------------------
  //! Get the string value by key
  //----------------------------------------------------------------------------
  std::string GetString(const char* key);

  //----------------------------------------------------------------------------
  //! Get a long long value by key
  //----------------------------------------------------------------------------
  long long GetLongLong(const char* key);

  //----------------------------------------------------------------------------
  //! Get a double value by key
  //----------------------------------------------------------------------------
  double GetDouble(const char* key);

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
    return mLocator.getQueuePath();
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem host
  //----------------------------------------------------------------------------
  std::string
  GetHost()
  {
    return mLocator.getHost();
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem queue name
  //----------------------------------------------------------------------------
  std::string
  GetQueue()
  {
    return mLocator.getFSTQueue();
  }

  //----------------------------------------------------------------------------
  //! Return the filesystem path
  //----------------------------------------------------------------------------
  std::string
  GetPath()
  {
    return mLocator.getStoragePath();
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
    return GetDrainStatusFromString(GetString("local.drain").c_str());
  }

  //----------------------------------------------------------------------------
  //! Return the configuration status (via cache)
  //----------------------------------------------------------------------------
  ConfigStatus GetConfigStatus(bool cached = false);

  //----------------------------------------------------------------------------
  //! Retrieve FileSystem's core parameters
  //----------------------------------------------------------------------------
  FileSystemCoreParams getCoreParams();

  //----------------------------------------------------------------------------
  //! Snapshots all variables of a filesystem into a snapshot struct
  //!
  //! @param fs snapshot struct to be filled
  //! @param dolock indicates if the shared hash representing the filesystem has
  //!               to be locked or not
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SnapShotFileSystem(FileSystem::fs_snapshot_t& fs, bool dolock = true);

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

  //----------------------------------------------------------------------------
  //! Get heartbeatTime
  //----------------------------------------------------------------------------
  time_t getLocalHeartbeatTime() const;

  //----------------------------------------------------------------------------
  //! Get local heartbeat delta
  //----------------------------------------------------------------------------
  int getLocalHeartbeatDelta() const;

  //----------------------------------------------------------------------------
  //! Set heartbeatTime
  //----------------------------------------------------------------------------
  void setLocalHeartbeatTime(time_t t);

  //----------------------------------------------------------------------------
  //! Check if local heartbeat is recent enough
  //----------------------------------------------------------------------------
  bool hasHeartbeat() const;

  //----------------------------------------------------------------------------
  //! Delete shared hash object corresponding to this file system and also
  //! broadcast the message. This should be called only when an explicit removal
  //! of the file system is request though "fs rm"
  //----------------------------------------------------------------------------
  void DeleteSharedHash();

  //--------------------------------------------------------------------------
  //! Get used bytes
  //--------------------------------------------------------------------------
  uint64_t GetUsedbytes();

  //--------------------------------------------------------------------------
  //! Get space name
  //--------------------------------------------------------------------------
  std::string GetSpace();

};

EOSCOMMONNAMESPACE_END;

#endif
