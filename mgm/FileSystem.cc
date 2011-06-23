/*----------------------------------------------------------------------------*/
#include "mgm/FileSystem.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
FileSystem::StartDrainJob() 
{
  //----------------------------------------------------------------
  //! start a drain job after stat.errc!=0 (e.g. opserror)
  //----------------------------------------------------------------

  // check if there is already a drainjob
  drainJobMutex.Lock();
  if (drainJob) {
    drainJobMutex.UnLock();
    return false;
  }

  // no drain job
  drainJob = new DrainJob(GetId(),true);
  drainJobMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::StopDrainJob()
{
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ( (isstatus == kDrainDead) || (isstatus == kDrain) ) {
    // if this is in drain mode, we leave the drain job
    return false;
  }
  
  drainJobMutex.Lock();
  if (drainJob) {
    delete drainJob;
    drainJob = 0;
    SetDrainStatus(eos::common::FileSystem::kNoDrain);
    drainJobMutex.UnLock();
    return true;
  }
  drainJobMutex.UnLock();
  return false;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetConfigStatus(eos::common::FileSystem::fsstatus_t status)
{
  //----------------------------------------------------------------
  //! catch any status change from/to 'drain' or 'draindead' 
  //----------------------------------------------------------------

  // check the current status
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ( (isstatus == kDrainDead) || (isstatus == kDrain) ) {
    // stop draining
    drainJobMutex.Lock();
    if (drainJob) {
      delete drainJob;
      drainJob = 0;
      drainJobMutex.UnLock();
      SetDrainStatus(eos::common::FileSystem::kNoDrain);
    } else {
      drainJobMutex.UnLock();
    }
  }

  if ( (status == kDrain) || (status == kDrainDead) ) {
    // create a drain job
    drainJobMutex.Lock();
    drainJob = new DrainJob(GetId());
    drainJobMutex.UnLock();
  } else {
    SetDrainStatus(eos::common::FileSystem::kNoDrain);
  }

  return eos::common::FileSystem::SetConfigStatus(status);
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetString(const char* key, const char* str, bool broadcast)
{
  std::string skey=key;
  std::string sval=str;
  if (skey == "configstatus") {
    return SetConfigStatus(GetConfigStatusFromString(str));
  }
  
  return eos::common::FileSystem::SetString(key,str,broadcast);
}

EOSMGMNAMESPACE_END
 
