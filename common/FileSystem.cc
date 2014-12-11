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

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/FileSystem.hh"
#include "common/Logging.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN;

/*----------------------------------------------------------------------------*/
//! Constructor
/*----------------------------------------------------------------------------*/
/** 
 * Constructor of a filesystem object.
 * 
 * @param queuepath Named Queue to specify the receiver filesystem of modifications e.g. /eos/<host:port>/fst/<path>
 * @param queue     Named Queue to specify the reciever of modifications e.g. /eos/<host:port>/fst
 * @param som       Handle to the shared obejct manager to store filesystem key-value pairs
 * @param bc2mgm    If true we broad cast to the management server
 */

/*----------------------------------------------------------------------------*/
FileSystem::FileSystem (const char* queuepath, const char* queue, XrdMqSharedObjectManager* som, bool bc2mgm)
{
  XrdSysMutexHelper cLock(mConstructorLock);
  mQueuePath = queuepath;
  mQueue = queue;
  mPath = queuepath;
  mPath.erase(0, mQueue.length());
  mSom = som;
  mInternalBootStatus = kDown;
  PreBookedSpace = 0;
  cActive = 0;
  cStatus = 0;
  cConfigStatus = 0;
  cActiveTime = 0;
  cStatusTime = 0;
  cConfigTime = 0;

  std::string broadcast = queue;
  if (bc2mgm)
    broadcast = "/eos/*/mgm";

  if (mSom)
  {
    mSom->HashMutex.LockRead();
    if (!(mHash = mSom->GetObject(mQueuePath.c_str(), "hash")))
    {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      mSom->CreateSharedHash(mQueuePath.c_str(), broadcast.c_str(), som);
      mSom->HashMutex.LockRead();
      mHash = mSom->GetObject(mQueuePath.c_str(), "hash");
      if (mHash)
      {
        mHash->OpenTransaction();
        mHash->Set("queue", mQueue);
        mHash->Set("queuepath", mQueuePath);
        mHash->Set("path", mPath);
        std::string hostport = eos::common::StringConversion::GetStringHostPortFromQueue(mQueue.c_str());
        if (hostport.length())
        {
          size_t ppos = hostport.find(":");
          std::string host = hostport;
          std::string port = hostport;
          if (ppos != std::string::npos)
          {
            host.erase(ppos);
            port.erase(0, ppos + 1);
          }
          else
          {
            port = "1094";
          }
          mHash->Set("hostport", hostport);
          mHash->Set("host", host);
          mHash->Set("port", port);
          mHash->Set("configstatus", "down");
        }
        else
        {
          eos_static_crit("there is no hostport defined for queue %s\n", mQueue.c_str());
        }
        mHash->CloseTransaction();
      }
      mSom->HashMutex.UnLockRead();
    }
    else
    {
      mHash->SetBroadCastQueue(broadcast.c_str());
      mHash->OpenTransaction();
      mHash->Set("queue", mQueue);
      mHash->Set("queuepath", mQueuePath);
      mHash->Set("path", mPath);
      std::string hostport = eos::common::StringConversion::GetStringHostPortFromQueue(mQueue.c_str());
      if (hostport.length())
      {
        size_t ppos = hostport.find(":");
        std::string host = hostport;
        std::string port = hostport;
        if (ppos != std::string::npos)
        {
          host.erase(ppos);
          port.erase(0, ppos + 1);
        }
        else
        {
          port = "1094";
        }
        mHash->Set("hostport", hostport);
        mHash->Set("host", host);
        mHash->Set("port", port);
      }
      else
      {
        eos_static_crit("there is no hostport defined for queue %s\n", mQueue.c_str());
      }
      mHash->CloseTransaction();

      mSom->HashMutex.UnLockRead();
    }
    mDrainQueue = new TransferQueue(mQueue.c_str(), mQueuePath.c_str(), "drainq", this, mSom, bc2mgm);
    mBalanceQueue = new TransferQueue(mQueue.c_str(), mQueuePath.c_str(), "balanceq", this, mSom, bc2mgm);
    mExternQueue = new TransferQueue(mQueue.c_str(), mQueuePath.c_str(), "externq", this, mSom, bc2mgm);
  }
  else
  {
    mHash = 0;
    mDrainQueue = 0;
    mBalanceQueue = 0;
    mExternQueue = 0;
  }
  if (bc2mgm)
    BroadCastDeletion = false;
  else
    BroadCastDeletion = true;
}

/*----------------------------------------------------------------------------*/
/** 
 * Destructor
 * 
 */

/*----------------------------------------------------------------------------*/
FileSystem::~FileSystem ()
{
  XrdSysMutexHelper cLock(mConstructorLock);
  // remove the shared hash of this file system
  if (mSom)
  {
    mSom->DeleteSharedHash(mQueuePath.c_str(), BroadCastDeletion);
  }

  if (mDrainQueue)
    delete mDrainQueue;
  if (mBalanceQueue)
    delete mBalanceQueue;
  if (mExternQueue)
    delete mExternQueue;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return the given status as a string
 * 
 * @param status status to convert into a string
 * 
 * @return string representation of status
 */

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetStatusAsString (int status)
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
/** 
 * Return given drain status as a string
 * 
 * @param status drain status to convert into a string
 * 
 * @return string representation of drain status
 */

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetDrainStatusAsString (int status)
{
  if (status == kNoDrain) return "nodrain";
  if (status == kDrainPrepare) return "prepare";
  if (status == kDrainWait) return "waiting";
  if (status == kDraining) return "draining";
  if (status == kDrained) return "drained";
  if (status == kDrainStalling) return "stalling";
  if (status == kDrainExpired) return "expired";
  if (status == kDrainLostFiles) return "lostfiles";
  return "unknown";
}

/*----------------------------------------------------------------------------*/
/** 
 * Return given configuration status as a string
 * 
 * @param status configuration status
 * 
 * @return string representation of configuration status
 */

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetConfigStatusAsString (int status)
{
  if (status == kUnknown) return "unknown";
  if (status == kOff) return "off";
  if (status == kEmpty) return "empty";
  if (status == kDrainDead) return "draindead";
  if (status == kDrain) return "drain";
  if (status == kRO) return "ro";
  if (status == kWO) return "wo";
  if (status == kRW) return "rw";
  return "unknown";
}

/*----------------------------------------------------------------------------*/
/** 
 * Get the status from a string representation
 * 
 * @param ss string representation of status
 * 
 * @return status as int
 */

/*----------------------------------------------------------------------------*/
int
FileSystem::GetStatusFromString (const char* ss)
{
  if (!ss)
    return kDown;

  if (!strcmp(ss, "down")) return kDown;
  if (!strcmp(ss, "opserror")) return kOpsError;
  if (!strcmp(ss, "bootfailure")) return kBootFailure;
  if (!strcmp(ss, "bootsent")) return kBootSent;
  if (!strcmp(ss, "booting")) return kBooting;
  if (!strcmp(ss, "booted")) return kBooted;
  return kDown;
}


/*----------------------------------------------------------------------------*/
/** 
 * Return configuration status from a string representation
 * 
 * @param ss string representation of configuration status
 * 
 * @return configuration status as int
 */

/*----------------------------------------------------------------------------*/
int
FileSystem::GetConfigStatusFromString (const char* ss)
{
  if (!ss)
    return kDown;

  if (!strcmp(ss, "unknown")) return kUnknown;
  if (!strcmp(ss, "off")) return kOff;
  if (!strcmp(ss, "empty")) return kEmpty;
  if (!strcmp(ss, "draindead")) return kDrainDead;
  if (!strcmp(ss, "drain")) return kDrain;
  if (!strcmp(ss, "ro")) return kRO;
  if (!strcmp(ss, "wo")) return kWO;
  if (!strcmp(ss, "rw")) return kRW;
  return kUnknown;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return drains status from string representation
 * 
 * @param ss string representation of drain status
 * 
 * @return drain status as int
 */

/*----------------------------------------------------------------------------*/
int
FileSystem::GetDrainStatusFromString (const char* ss)
{
  if (!ss)
    return kNoDrain;

  if (!strcmp(ss, "nodrain")) return kNoDrain;
  if (!strcmp(ss, "prepare")) return kDrainPrepare;
  if (!strcmp(ss, "wait")) return kDrainWait;
  if (!strcmp(ss, "draining"))return kDraining;
  if (!strcmp(ss, "stalling"))return kDrainStalling;
  if (!strcmp(ss, "drained")) return kDrained;
  if (!strcmp(ss, "expired")) return kDrainExpired;
  if (!strcmp(ss, "lostfiles")) return kDrainLostFiles;

  return kNoDrain;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return active status from a string representation
 * 
 * @param ss string representation of active status
 * 
 * @return active status as fsactive_t
 */

/*----------------------------------------------------------------------------*/
FileSystem::fsactive_t
FileSystem::GetActiveStatusFromString (const char* ss)
{
  if (!ss)
    return kOffline;
  if (!strcmp(ss, "online")) return kOnline;
  if (!strcmp(ss, "offline")) return kOffline;
  return kOffline;
}

/*----------------------------------------------------------------------------*/
/** 
 * Return boot request string
 * 
 * 
 * @return boot request string
 */

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetAutoBootRequestString ()
{
  return "mgm.cmd=bootreq";
}

/*----------------------------------------------------------------------------*/
/** 
 * Return register request string 
 * 
 * 
 * @return register request string
 */

/*----------------------------------------------------------------------------*/
const char*
FileSystem::GetRegisterRequestString ()
{
  return "mgm.cmd=register";
}

/*----------------------------------------------------------------------------*/
/** 
 * Store a configuration key-val pair. Internally these keys are prefixed with 'stat.'
 * 
 * @param key key string
 * @param val value string
 */

/*----------------------------------------------------------------------------*/
void
FileSystem::CreateConfig (std::string &key, std::string &val)
{
  key = val = "";
  fs_snapshot_t fs;

  XrdMqRWMutexReadLock lock(mSom->HashMutex);

  key = mQueuePath;
  val = mHash->StoreAsString("stat.");
}

/*----------------------------------------------------------------------------*/
/** 
 * Snapshots all variables of a filesystem into a snapsthot struct
 * 
 * @param fs Snapshot struct to be filled 
 * @param dolock Indicates if the shared hash representing the filesystme has to be locked or not
 * 
 * @return true if successful - false
 */

/*----------------------------------------------------------------------------*/
bool
FileSystem::SnapShotFileSystem (FileSystem::fs_snapshot_t &fs, bool dolock)
{
  if (dolock)
  {
    mSom->HashMutex.LockRead();
  }
  if ((mHash = mSom->GetObject(mQueuePath.c_str(), "hash")))
  {
    fs.mId = (fsid_t) mHash->GetUInt("id");
    fs.mQueue = mQueue;
    fs.mQueuePath = mQueuePath;
    fs.mGroup = mHash->Get("schedgroup");
    fs.mUuid = mHash->Get("uuid");
    fs.mHost = mHash->Get("host");
    fs.mHostPort = mHash->Get("hostport");
    fs.mPort = mHash->Get("port");

    std::string::size_type dpos = 0;
    if ((dpos = fs.mGroup.find(".")) != std::string::npos)
    {
      std::string s = fs.mGroup;
      s.erase(0, dpos + 1);
      fs.mGroupIndex = atoi(s.c_str());
    }
    else
    {
      fs.mGroupIndex = 0;
    }
    fs.mSpace = fs.mGroup;
    if (dpos != std::string::npos)
      fs.mSpace.erase(dpos);
    fs.mPath = mPath;
    fs.mErrMsg = mHash->Get("stat.errmsg");
    fs.mGeoTag = mHash->Get("stat.geotag");
    fs.mPublishTimestamp = (size_t)mHash->GetLongLong("stat.publishtimestamp");
    fs.mStatus = GetStatusFromString(mHash->Get("stat.boot").c_str());
    fs.mConfigStatus = GetConfigStatusFromString(mHash->Get("configstatus").c_str());
    fs.mDrainStatus = GetDrainStatusFromString(mHash->Get("stat.drain").c_str());
    fs.mActiveStatus = GetActiveStatusFromString(mHash->Get("stat.active").c_str());
    fs.mBalRunning = (mHash->Get("stat.balancing.running")=="1"?true:false);
    fs.mHeadRoom = mHash->GetLongLong("headroom");
    fs.mErrCode = (unsigned int) mHash->GetLongLong("stat.errc");
    fs.mBootSentTime = (time_t) mHash->GetLongLong("stat.bootsenttime");
    fs.mBootDoneTime = (time_t) mHash->GetLongLong("stat.bootdonetime");
    fs.mHeartBeatTime = (time_t) mHash->GetLongLong("stat.heartbeattime");
    fs.mDiskUtilization = mHash->GetDouble("stat.disk.load");
    fs.mNetEthRateMiB = mHash->GetDouble("stat.net.ethratemib");
    fs.mNetInRateMiB = mHash->GetDouble("stat.net.inratemib");
    fs.mNetOutRateMiB = mHash->GetDouble("stat.net.outratemib");
    fs.mDiskWriteRateMb = mHash->GetDouble("stat.disk.writeratemb");
    fs.mDiskReadRateMb = mHash->GetDouble("stat.disk.readratemb");
    fs.mDiskType = (long) mHash->GetLongLong("stat.statfs.type");
    fs.mDiskFreeBytes = mHash->GetLongLong("stat.statfs.freebytes");
    fs.mDiskCapacity = mHash->GetLongLong("stat.statfs.capacity");
    fs.mDiskBsize = (long) mHash->GetLongLong("stat.statfs.bsize");
    fs.mDiskBlocks = (long) mHash->GetLongLong("stat.statfs.blocks");
    fs.mDiskBfree = (long) mHash->GetLongLong("stat.statfs.bfree");
    fs.mDiskBused = (long) mHash->GetLongLong("stat.statfs.bused");
    fs.mDiskBavail = (long) mHash->GetLongLong("stat.statfs.bavail");
    fs.mDiskFiles = (long) mHash->GetLongLong("stat.statfs.files");
    fs.mDiskFfree = (long) mHash->GetLongLong("stat.statfs.ffree");
    fs.mDiskFused = (long) mHash->GetLongLong("stat.statfs.fused");
    fs.mDiskFilled = (double) mHash->GetDouble("stat.statfs.filled");
    fs.mNominalFilled = (double) mHash->GetDouble("stat.nominal.filled");

    fs.mFiles = (long) mHash->GetLongLong("stat.usedfiles");
    fs.mDiskNameLen = (long) mHash->GetLongLong("stat.statfs.namelen");
    fs.mDiskRopen = (long) mHash->GetLongLong("stat.ropen");
    fs.mDiskWopen = (long) mHash->GetLongLong("stat.wopen");
    fs.mWeightRead = 1.0;
    fs.mWeightWrite = 1.0;

    fs.mScanInterval = (time_t) mHash->GetLongLong("scaninterval");
    fs.mGracePeriod = (time_t) mHash->GetLongLong("graceperiod");
    fs.mDrainPeriod = (time_t) mHash->GetLongLong("drainperiod");

    if (dolock)
    {
      mSom->HashMutex.UnLockRead();
    }
    return true;
  }
  else
  {
    if (dolock)
    {
      mSom->HashMutex.UnLockRead();
    }
    fs.mId = 0;
    fs.mQueue = "";
    fs.mQueuePath = "";
    fs.mGroup = "";
    fs.mPath = "";
    fs.mUuid = "";
    fs.mHost = "";
    fs.mHostPort = "";
    fs.mPort = "";
    fs.mErrMsg = "";
    fs.mGeoTag ="";
    fs.mPublishTimestamp = 0;
    fs.mStatus = 0;
    fs.mConfigStatus = 0;
    fs.mDrainStatus = 0;
    fs.mBalRunning = false;
    fs.mHeadRoom = 0;
    fs.mErrCode = 0;
    fs.mBootSentTime = 0;
    fs.mBootDoneTime = 0;
    fs.mHeartBeatTime = 0;
    fs.mDiskUtilization = 0;
    fs.mNetEthRateMiB = 0;
    fs.mNetInRateMiB = 0;
    fs.mNetOutRateMiB = 0;
    fs.mDiskWriteRateMb = 0;
    fs.mDiskReadRateMb = 0;
    fs.mDiskType = 0;
    fs.mDiskBsize = 0;
    fs.mDiskBlocks = 0;
    fs.mDiskBfree = 0;
    fs.mDiskBused = 0;
    fs.mDiskBavail = 0;
    fs.mDiskFiles = 0;
    fs.mDiskFfree = 0;
    fs.mDiskFused = 0;
    fs.mFiles = 0;
    fs.mDiskNameLen = 0;
    fs.mDiskRopen = 0;
    fs.mDiskWopen = 0;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
/** 
 * Store a given statfs struct into the hash representation
 * 
 * @param statfs struct to read
 * 
 * @return true if successful otherwise false
 *
 */

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetStatfs (struct statfs* statfs)
{
  if (!statfs)
    return false;

  bool success = true;
  success &= SetLongLong("stat.statfs.type", statfs->f_type);
  success &= SetLongLong("stat.statfs.bsize", statfs->f_bsize);
  success &= SetLongLong("stat.statfs.blocks", statfs->f_blocks);
  success &= SetLongLong("stat.statfs.bfree", statfs->f_bfree);
  success &= SetLongLong("stat.statfs.bavail", statfs->f_bavail);
  success &= SetLongLong("stat.statfs.files", statfs->f_files);
  success &= SetLongLong("stat.statfs.ffree", statfs->f_ffree);

#ifdef __APPLE__
  success &= SetLongLong("stat.statfs.namelen", MNAMELEN);
#else
  success &= SetLongLong("stat.statfs.namelen", statfs->f_namelen);
#endif

  return success;
}

/*----------------------------------------------------------------------------*/
/** 
 * Try if one can reserve <bookingspace> in this filesystem.
 * 
 * @param fs Snapshot of the filesystem.
 * @param bookingsize Size to be reserved
 * 
 * @return true if there is enough space - false if not
 */

/*----------------------------------------------------------------------------*/
bool
FileSystem::ReserveSpace (fs_snapshot_t &fs, unsigned long long bookingsize)
{
  long long headroom = fs.mHeadRoom;
  long long freebytes = fs.mDiskFreeBytes;
  long long prebooked = GetPrebookedSpace();

  // guarantee that we don't overbook the filesystem and we keep <headroom> free
  if ((unsigned long long) (freebytes - prebooked) > ((unsigned long long) headroom + bookingsize))
  {
    // there is enough space
    return true;
  }
  else
  {
    return false;
  }
}


/*----------------------------------------------------------------------------*/
/** 
 * Check if the filesystem has a valid heartbeat
 * 
 * @param fs Snapshot of the filesystem
 * 
 * @return true if filesystem got a heartbeat during the last 300 seconds - otherwise false
 */

/*----------------------------------------------------------------------------*/
bool
FileSystem::HasHeartBeat (fs_snapshot_t &fs)
{
  time_t now = time(NULL);
  time_t hb = fs.mHeartBeatTime;
  if ((now - hb) < 60)
  {
    // we allow some time drift plus overload delay of 60 seconds
    return true;
  }
  return false;
}

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END;





