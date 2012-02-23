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
/*----------------------------------------------------------------------------*/
/** 
 * Constructor
 * 
 * @param description string describing a transfer
 */
/*----------------------------------------------------------------------------*/
TransferJob::TransferJob(const char* description)
{
  if (description)
    mJob = new XrdOucEnv(description);
  else
    mJob = 0;
}

/*----------------------------------------------------------------------------*/
//! Destructor
/*----------------------------------------------------------------------------*/
TransferJob::~TransferJob() {
  if (mJob)
    delete mJob;
}

/*----------------------------------------------------------------------------*/
/** 
 * Get the env representation of a transfer job
 * 
 * 
 * @return pointer to string describing env representation
 */
/*----------------------------------------------------------------------------*/
const char*
TransferJob::GetSealed() 
{
  if (!mJob)
    return 0;

  int envlen=0;
  mEncodedEnv = mJob->Env(envlen);
  while (mEncodedEnv.replace("&","#@#")) {};
  return mEncodedEnv.c_str();
}

/*----------------------------------------------------------------------------*/
/** 
 * Factor function to create a transfer job from a string description
 * 
 * @param sealeddescription return's a job object created from a sealed description as found in shared queues
 * 
 * @return pointer to transfer job object
 */
/*----------------------------------------------------------------------------*/
TransferJob*
TransferJob::Create(const char* sealeddescription)
{
  if (!sealeddescription)
    return 0;

  XrdOucString s = sealeddescription;
  while (s.replace("#@#","&")) {};
  return new TransferJob(s.c_str());
}

/*----------------------------------------------------------------------------*/
//! Return the env representation of a job
/*----------------------------------------------------------------------------*/
XrdOucEnv*
TransferJob::GetEnv() 
{
  return mJob;
}

/*----------------------------------------------------------------------------*/
//! Allows to replace the XrdOucEnv description externally
//! This is used to replace a symmetric key encoded contents with the human readable contents
/*----------------------------------------------------------------------------*/
void 
TransferJob::Replace(const char* description)
{
  if (mJob) {
    delete mJob;
  }

  mJob = new XrdOucEnv(description);
}

/*----------------------------------------------------------------------------*/
//! Print a transfer job env description as key-val pairs
/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

