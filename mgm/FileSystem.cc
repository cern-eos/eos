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
  if (drainJob) 
    return false;

  // no drain job
  drainJob = new DrainJob(GetId(),true);
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

  if (drainJob) {
    delete drainJob;
    drainJob = 0;
    SetDrainStatus(eos::common::FileSystem::kNoDrain);
    return true;
  }
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
    if (drainJob) {
      delete drainJob;
      drainJob = 0;
      SetDrainStatus(eos::common::FileSystem::kNoDrain);
    }
  }

  if ( (status == kDrain) || (status == kDrainDead) ) {
    // create a drain job
    drainJob = new DrainJob(GetId());
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
 
