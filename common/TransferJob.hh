#ifndef __EOSCOMMON_TRANSFERJOB_HH__
#define __EOSCOMMON_TRANSFERJOB_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <stdint.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class TransferJob {
private:
  XrdOucEnv* mJob;
public:
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  TransferJob(const char* description);
  virtual ~TransferJob();

  const char* GetEnv(); // returns the env string
  
  const char* AddSignature(const char* signature); // adds the env signature to the internal mJob representation
};

EOSCOMMONNAMESPACE_END

#endif
