/*----------------------------------------------------------------------------*/
#include "mgm/FileSystem.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

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
 
