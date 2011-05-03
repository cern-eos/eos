/*----------------------------------------------------------------------------*/
#include "common/TransferJob.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

TransferJob::TransferJob(const char* description)
{
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------
  mJob = new XrdOucEnv(description);
}

TransferJob::~TransferJob() {
  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  if (mJob)
    delete mJob;
}

const char*
TransferJob::GetEnv() 
{
  //------------------------------------------------------------------------
  //! Return the env representation of a job
  //------------------------------------------------------------------------
  if (!mJob)
    return 0;

  int envlen=0;
  return mJob->Env(envlen);
}

const char*
TransferJob::AddSignature(const char* signature)
{
  //------------------------------------------------------------------------
  //! Add a signature built externally to a job
  //------------------------------------------------------------------------
  if (!mJob)
    return 0;

  int envlen=0;
  
  std::string allenv = mJob->Env(envlen);
  allenv += "&";
  allenv += signature;
  XrdOucEnv* signedJob = new XrdOucEnv(allenv.c_str());
  delete mJob;
  mJob = signedJob;
  return mJob->Env(envlen);
}

EOSCOMMONNAMESPACE_END

