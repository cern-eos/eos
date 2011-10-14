// ----------------------------------------------------------------------
// File: TransferJob.cc
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

