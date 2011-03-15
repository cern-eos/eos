/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/FileSystem.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
FileSystem::FileSystem(const char* queuepath, const char* queue, XrdMqSharedObjectManager* som)
{
  mQueuePath = queuepath;
  mQueue     = queue;
  mPath      = queuepath;
  mPath.erase(0, mQueue.length());
  mSom       = som;
  if (mSom) {
    mSom->HashMutex.LockRead();
    if (! (mHash = mSom->GetObject(mQueuePath.c_str(),"hash")) ) {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      if (mSom->CreateSharedHash(mQueuePath.c_str(), mQueue.c_str(),som)) {
        mSom->HashMutex.LockRead();
        mHash = mSom->GetObject(mQueuePath.c_str(),"hash");
        if (mHash) {
          mHash->Set("queue",mQueue);
          mHash->Set("queuepath",mQueuePath);
          mHash->Set("path",mPath);
        }           
        
        mSom->HashMutex.UnLockRead();
      } else {
        mHash = 0;
      }
    } else {
      mSom->HashMutex.UnLockRead();
    }
  } else {
    mHash = 0;
  }
}

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetStatusAsString(int status)
{
  if (status == kDown) return "down";
  if (status == kOpsError) return "opserror";
  if (status == kBootFailure) return "bootfailure";
  if (status == kBootSent) return "bootsent";
  if (status == kBooting) return "booting";
  if (status == kBooted) return "booted";
  return "unknown";
}

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetDrainStatusAsString(int status)
{
  if (status == kNoDrain) return "nodrain";
  if (status == kDraining) return "draining";
  if (status == kDrained) return "drained";
  return "unknown";
}

/*----------------------------------------------------------------------------*/
int
FileSystem::GetStatusFromString(const char* ss)
{
  if (!ss) 
    return kDown;
  
  if (!strcmp(ss,"down")) return kDown;
  if (!strcmp(ss,"opserror")) return kOpsError;
  if (!strcmp(ss,"bootfailure")) return kBootFailure;
  if (!strcmp(ss,"bootsent")) return kBootSent;
  if (!strcmp(ss,"booting")) return kBooting;
  if (!strcmp(ss,"booted")) return kBooted;
  return kDown;
}


/*----------------------------------------------------------------------------*/
int
FileSystem::GetConfigStatusFromString(const char* ss) 
{
  if (!ss) 
    return kDown;
  
  if (!strcmp(ss,"unknown")) return kUnknown;
  if (!strcmp(ss,"off")) return kOff;
  if (!strcmp(ss,"drain")) return kDrain;
  if (!strcmp(ss,"ro")) return kRO;
  if (!strcmp(ss,"wo")) return kWO;
  if (!strcmp(ss,"rw")) return kRW;
  return kUnknown;
}

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetAutoBootRequestString() 
{
  return "mgm.cmd=bootreq";
}


/*----------------------------------------------------------------------------*/
bool 
FileSystem::SnapShotFileSystem(FileSystem::fs_snapshot_t &fs) {
  XrdMqRWMutexReadLock lock(mSom->HashMutex);
  if ( (mHash = mSom->GetObject(mQueuePath.c_str(),"hash"))) {
    fs.mId = (fsid_t) mHash->GetUInt("id");
    fs.mQueue         = mQueue;
    fs.mQueuePath     = mQueuePath;
    fs.mGroup         = mHash->Get("schedgroup");
    std::string::size_type dpos=0;
    if ( ( dpos = fs.mGroup.find(".") ) != std::string::npos) {
      std::string s = fs.mGroup;
      s.erase(0,dpos+1);
      fs.mGroupIndex  = atoi(s.c_str());
    } else {
      fs.mGroupIndex  = 0;
    }
    fs.mSpace         = fs.mGroup;
    fs.mSpace.erase(dpos);
    fs.mPath          = mPath;
    fs.mErrMsg        = mHash->Get("errmsg");
    fs.mStatus        = mHash->GetLongLong("status");
    fs.mConfigStatus  = mHash->GetLongLong("configstatus");
    fs.mDrainStatus   = mHash->GetLongLong("drainstatus");
    fs.mErrCode       = (unsigned int)mHash->GetLongLong("errc");
    fs.mBootSentTime  = (time_t) mHash->GetLongLong("bootsenttime");
    fs.mBootDoneTime  = (time_t) mHash->GetLongLong("bootdonetime");
    fs.mHeartBeatTime = (time_t) mHash->GetLongLong("heartbeattime");
    fs.mDiskLoad      = mHash->GetDouble("diskload");
    fs.mNetLoad         = mHash->GetDouble("netload");
    fs.mDiskWriteRateMb = mHash->GetDouble("diskwriteratemb");
    fs.mDiskReadRateMb  = mHash->GetDouble("diskreadratemb");
    fs.mDiskType        = (long) mHash->GetLongLong("statfs.type");
    fs.mDiskBsize       = (long) mHash->GetLongLong("statfs.bsize");
    fs.mDiskBlocks      = (long) mHash->GetLongLong("statfs.blocks");
    fs.mDiskBfree       = (long) mHash->GetLongLong("statfs.bfree");
    fs.mDiskBavail      = (long) mHash->GetLongLong("statfs.bavail");
    fs.mDiskFiles       = (long) mHash->GetLongLong("statfs.files");
    fs.mDiskFfree       = (long) mHash->GetLongLong("statfs.ffree");
    fs.mDiskNameLen     = (long) mHash->GetLongLong("statfs.namelen");
    fs.mDiskRopen       = (long) mHash->GetLongLong("ropen");
    fs.mDiskWopen       = (long) mHash->GetLongLong("wopen");
    fs.mWeightRead      = 1.0;
    fs.mWeightWrite     = 1.0;
    return true;
  } else {
    fs.mId = 0;
    fs.mQueue         = "";
    fs.mQueuePath     = "";
    fs.mPath          = "";
    fs.mErrMsg        = "";
    fs.mStatus        = 0;
    fs.mConfigStatus  = 0;
    fs.mDrainStatus   = 0;
    fs.mErrCode       = 0;
    fs.mBootSentTime  = 0;
    fs.mBootDoneTime  = 0;
    fs.mHeartBeatTime = 0;
    fs.mDiskLoad      = 0;
    fs.mNetLoad         = 0;
    fs.mDiskWriteRateMb = 0;
    fs.mDiskReadRateMb  = 0;
    fs.mDiskType        = 0;
    fs.mDiskBsize       = 0;
    fs.mDiskBlocks      = 0;
    fs.mDiskBfree       = 0;
    fs.mDiskBavail      = 0;
    fs.mDiskFiles       = 0;
    fs.mDiskFfree       = 0;
    fs.mDiskNameLen     = 0;
    fs.mDiskRopen       = 0;
    fs.mDiskWopen       = 0;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END





