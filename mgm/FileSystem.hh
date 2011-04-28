#ifndef __EOSMGM_FILESYSTEM_HH__
#define __EOSMGM_FILESYSTEM_HH__

/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "mgm/DrainJob.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN
/*----------------------------------------------------------------------------*/
class FileSystem : public eos::common::FileSystem {
private:
  DrainJob* drainJob;
public:
  FileSystem(const char* queuepath, const char* queue, XrdMqSharedObjectManager* som) : eos::common::FileSystem(queuepath,queue,som) {
    drainJob=0;
  }
  virtual ~FileSystem() {
    if (drainJob) {
      delete drainJob;
      drainJob=0;
    }
  }

  bool SetConfigStatus(eos::common::FileSystem::fsstatus_t status); // this method is overwritten to catch any status change to/from 'drain' or 'draindead'

  bool SetString(const char* key, const char* str, bool broadcast=true); // see above

  bool StartDrainJob(); // starts a drain job with the opserror flag - this is triggered by stat.errc!= 0 via the FsListener Thread
  bool StopDrainJob();  // stops  a drain job with the opserror flag - this is triggered by stat.errc = 0 via the FsListener Thread
};

EOSMGMNAMESPACE_END

#endif
