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
  XrdOucString mEncodedEnv;
  
public:
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  TransferJob(const char* description);
  virtual ~TransferJob();

  const char* GetSealed(); // returns the sealed env string
  
  static TransferJob* Create(const char* sealeddescription);

  XrdOucEnv* GetEnv() ; // return XrdOucEnv object

  void Replace(const char* description); // this is used to replace the job content with a decoded capability

  void PrintOut(XrdOucString &out);
};

EOSCOMMONNAMESPACE_END

#endif
