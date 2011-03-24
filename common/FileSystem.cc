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
	  mHash->OpenTransaction();
          mHash->Set("queue",mQueue);
          mHash->Set("queuepath",mQueuePath);
          mHash->Set("path",mPath);
	  std::string hostport = eos::common::StringConversion::GetStringHostPortFromQueue(mQueue.c_str());
	  size_t ppos=hostport.find(":");
	  std::string host = hostport;
	  std::string port = hostport;
	  if (ppos != std::string::npos) {
	    host.erase(ppos);
	    port.erase(0,ppos+1);
	  } else {
	    port = "1094";
	  }
	  mHash->Set("hostport",hostport);
	  mHash->Set("host",host);
	  mHash->Set("port",port);
	  mHash->CloseTransaction();
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
FileSystem::~FileSystem()
{
  // remove the shared hash of this file system
 if (mSom) {
    mSom->DeleteSharedHash(mQueuePath.c_str());
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
void
FileSystem::CreateConfig(std::string &key, std::string &val)
{
  key = val ="";
  fs_snapshot_t fs;

  XrdMqRWMutexReadLock lock(mSom->HashMutex);

  key = mQueuePath;
  val = mHash->StoreAsString("stat.");
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
    fs.mUuid          = mHash->Get("uuid");
    fs.mHost          = mHash->Get("host");
    fs.mHostPort      = mHash->Get("hostport");
    fs.mPort          = mHash->Get("port");

    std::string::size_type dpos=0;
    if ( ( dpos = fs.mGroup.find(".") ) != std::string::npos) {
      std::string s = fs.mGroup;
      s.erase(0,dpos+1);
      fs.mGroupIndex  = atoi(s.c_str());
    } else {
      fs.mGroupIndex  = 0;
    }
    fs.mSpace         = fs.mGroup;
    if (dpos != std::string::npos)
      fs.mSpace.erase(dpos);
    fs.mPath          = mPath;
    fs.mErrMsg        = mHash->Get("stat.errmsg");
    fs.mStatus        = mHash->GetLongLong("status");
    fs.mConfigStatus  = mHash->GetLongLong("configstatus");
    fs.mDrainStatus   = mHash->GetLongLong("drainstatus");
    fs.mErrCode       = (unsigned int)mHash->GetLongLong("stat.errc");
    fs.mBootSentTime  = (time_t) mHash->GetLongLong("stat.bootsenttime");
    fs.mBootDoneTime  = (time_t) mHash->GetLongLong("stat.bootdonetime");
    fs.mHeartBeatTime = (time_t) mHash->GetLongLong("stat.heartbeattime");
    fs.mDiskLoad      = mHash->GetDouble("stat.diskload");
    fs.mNetLoad         = mHash->GetDouble("stat.netload");
    fs.mDiskWriteRateMb = mHash->GetDouble("stat.diskwriteratemb");
    fs.mDiskReadRateMb  = mHash->GetDouble("stat.diskreadratemb");
    fs.mDiskType        = (long) mHash->GetLongLong("stat.statfs.type");
    fs.mDiskBsize       = (long) mHash->GetLongLong("stat.statfs.bsize");
    fs.mDiskBlocks      = (long) mHash->GetLongLong("stat.statfs.blocks");
    fs.mDiskBfree       = (long) mHash->GetLongLong("stat.statfs.bfree");
    fs.mDiskBavail      = (long) mHash->GetLongLong("stat.statfs.bavail");
    fs.mDiskFiles       = (long) mHash->GetLongLong("stat.statfs.files");
    fs.mDiskFfree       = (long) mHash->GetLongLong("stat.statfs.ffree");
    fs.mDiskNameLen     = (long) mHash->GetLongLong("stat.statfs.namelen");
    fs.mDiskRopen       = (long) mHash->GetLongLong("stat.ropen");
    fs.mDiskWopen       = (long) mHash->GetLongLong("stat.wopen");
    fs.mWeightRead      = 1.0;
    fs.mWeightWrite     = 1.0;
    return true;
  } else {
    fs.mId = 0;
    fs.mQueue         = "";
    fs.mQueuePath     = "";
    fs.mPath          = "";
    fs.mUuid          = "";
    fs.mHost          = "";
    fs.mHostPort      = "";
    fs.mPort          = "";
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





