#ifndef __EOSCOMMON_FILESYSTEM_HH__
#define __EOSCOMMON_FILESYSTEM_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Exception.hh"
#include "common/StringConversion.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <stdint.h>
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

class FileSystem {
private:
  std::string mQueuePath;  // = <queue> + <path> e.g. /eos/<host>/fst/data01
  std::string mQueue;      // = <queue>          e.g. /eos/<host>/fst
  std::string mPath;       // = <queuepath> - <queue> e.g. /data01

  XrdMqSharedHash* mHash;  // before usage mSom needs a read lock and mHash has to be validated to avoid race conditions in deletion
  XrdMqSharedObjectManager* mSom;
  XrdSysMutex constructorLock;

public:
  //------------------------------------------------------------------------
  // Struct & Type definitions
  //------------------------------------------------------------------------
  typedef uint32_t fsid_t;
  typedef int32_t fsstatus_t;

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
    std::string mPort;
    int         mGroupIndex;
    std::string mSpace;
    fsstatus_t  mStatus;
    fsstatus_t  mConfigStatus;
    fsstatus_t  mDrainStatus;
    unsigned int mErrCode;
    time_t mBootSentTime;
    time_t mBootDoneTime;
    time_t mHeartBeatTime;
    double mDiskLoad;
    double mDiskWriteRateMb;
    double mDiskReadRateMb;
    double mNetLoad;
    double mWeightRead;
    double mWeightWrite;
    long   mDiskType;
    long   mDiskBsize;
    long   mDiskBlocks;
    long   mDiskBfree;
    long   mDiskBavail;
    long   mDiskFiles;
    long   mDiskFfree;
    long   mDiskNameLen;
    long   mDiskRopen;
    long   mDiskWopen;
  } fs_snapshot_t;

  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  FileSystem(const char* queuepath, const char* queue, XrdMqSharedObjectManager* som);

  virtual ~FileSystem();

  //------------------------------------------------------------------------
  // Enums
  //------------------------------------------------------------------------

  enum eBootStatus   { kOpsError=-2, kBootFailure=-1, kDown=0, kBootSent, kBooting=2, kBooted=3};
  enum eConfigStatus { kUnknown=-1, kOff=0, kDrain, kRO, kWO, kRW};
  enum eDrainStatus  { kNoDrain=0,     kDraining=1, kDrained=1};

  //------------------------------------------------------------------------
  //! Conversion Functions
  //------------------------------------------------------------------------
  static const char* GetStatusAsString(int status);
  static const char* GetDrainStatusAsString(int status);
  static const char* GetConfigStatusAsString(int status);
  static         int GetStatusFromString(const char* ss);
  static         int GetDrainStatusFromString(const char* ss);
  static         int GetConfigStatusFromString(const char* ss);
  static const char* GetAutoBootRequestString();

  //------------------------------------------------------------------------
  //! Setter Functions
  //------------------------------------------------------------------------

  bool SetId(fsid_t fsid) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      mHash->SetLongLong("id",(long long)fsid);
      return true;
    } else {
      return false;
    }
  }

  bool SetString(const char* key, const char* str) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      mHash->Set(key,str);
      return true;
    } else {
      return false;
    }
  }

  bool SetDouble(const char* key, double f) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      mHash->SetDouble(key,f);
      return true;
    } else {
      return false;
    }
  }

  bool SetLongLong(const char* key, long long l) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      mHash->SetLongLong(key,l);
      return true;
    } else {
      return false;
    }
  }

  bool SetStatus(fsstatus_t status) {
    
    return SetString("status", GetStatusAsString(status));
  }

  bool SetDrainStatus(fsstatus_t status) {
    return SetString("drainstatus", GetDrainStatusAsString(status));
  }

  bool SetConfigStatus(fsstatus_t status) {
    return SetString("configstatus", GetConfigStatusAsString(status));
  }

  //------------------------------------------------------------------------
  //! Getter Functions throwing exceptions
  //------------------------------------------------------------------------  

  std::string GetString(const char* key) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      return mHash->Get(key);
    } else {
      Exception ex( ENODATA );
      ex.getMessage() << "no string data stored for this key";
      throw ex;
    }
  }
  
  long long GetLongLong(const char* key) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      return mHash->GetLongLong(key);
    } else {
      Exception ex( ENODATA );
      ex.getMessage() << "no long long data stored for this key";
      throw ex;
    }
  }

  double GetDouble(const char* key) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      return mHash->GetDouble(key);
    } else {
      Exception ex( ENODATA );
      ex.getMessage() << "no double data stored for this key";
      throw ex;
    }
  }

  fsid_t GetId() {
    return (fsid_t) GetLongLong("id");
  }

  std::string GetQueuePath() {
    return mQueuePath;
  }

  std::string GetQueue() {
    return mQueue;
  }

  fsstatus_t GetStatus() {
    return GetStatusFromString(GetString("status").c_str());
  }
  
  fsstatus_t GetDrainStatus() {
    return GetDrainStatusFromString(GetString("drainstatus").c_str());
  }

  fsstatus_t GetConfigStatus() { 
    return GetConfigStatusFromString(GetString("configstatus").c_str());
  }

  //------------------------------------------------------------------------
  //! Snapshot filesystem
  //! - this creates a copy of the present state into a snapshot struct
  //------------------------------------------------------------------------
  
  bool SnapShotFileSystem(FileSystem::fs_snapshot_t &fs);

  //------------------------------------------------------------------------
  void Print(std::string &out, std::string listformat) {
    XrdMqRWMutexReadLock lock(mSom->HashMutex);
    if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
      mHash->Print(out, listformat);
    }
  }

  //------------------------------------------------------------------------
  //! Create Config
  //! - this creates the config string representation of this file system
  //------------------------------------------------------------------------
  void CreateConfig(std::string &key, std::string &val);
};

EOSCOMMONNAMESPACE_END

#endif
