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
  if (description)
    mJob = new XrdOucEnv(description);
  else
    mJob = 0;
}

TransferJob::~TransferJob() {
  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  if (mJob)
    delete mJob;
}

const char*
TransferJob::GetSealed() 
{
  //------------------------------------------------------------------------
  //! Return the env representation of a job
  //------------------------------------------------------------------------
  if (!mJob)
    return 0;

  int envlen=0;
  mEncodedEnv = mJob->Env(envlen);
  while (mEncodedEnv.replace("&","#@#")) {};
  return mEncodedEnv.c_str();
}

TransferJob*
TransferJob::Create(const char* sealeddescription)
{
  //------------------------------------------------------------------------
  //! Return a job created from a sealed description as found in shared queues
  //------------------------------------------------------------------------
  if (!sealeddescription)
    return 0;

  XrdOucString s = sealeddescription;
  while (s.replace("#@#","&")) {};
  return new TransferJob(s.c_str());
}

XrdOucEnv*
TransferJob::GetEnv() 
{
  //------------------------------------------------------------------------
  //! Return the env representation of a job
  //------------------------------------------------------------------------
  return mJob;
}

void 
TransferJob::Replace(const char* description)
{
  //------------------------------------------------------------------------
  //! Replace the XrdOucEnv description externally
  //------------------------------------------------------------------------
  if (mJob) {
    delete mJob;
  }

  mJob = new XrdOucEnv(description);
}

void 
TransferJob::PrintOut(XrdOucString &out)
{
  std::vector<std::string>tokens;
  std::string delimiter="&";
  int envlen=0;
  std::string description = mJob->Env(envlen);
  eos::common::StringConversion::Tokenize(description, tokens, delimiter);
  for (size_t i=0; i< tokens.size(); i++) {
    out += tokens[i].c_str();
    out += " ";
  }
}


EOSCOMMONNAMESPACE_END

