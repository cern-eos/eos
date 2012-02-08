// ----------------------------------------------------------------------
// File: TransferJob.hh
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

/**
 * @file   TransferJob.hh
 * 
 * @brief  Base class for transfer jobs.
 * 
 * 
 */

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

/*----------------------------------------------------------------------------*/
//! Class abstracting a transfer job for FST and MGM
//! Main purpose is to encode a transfer job into a safe text string
/*----------------------------------------------------------------------------*/
class TransferJob {
private:
  XrdOucEnv* mJob;             //< Description of a transfer
  XrdOucString mEncodedEnv;    //< Encoded string version of a transfer
  
public:
  // ---------------------------------------------------------------------------
  //! Concstructor
  // ---------------------------------------------------------------------------
  TransferJob(const char* description);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  virtual ~TransferJob();

  // ---------------------------------------------------------------------------
  //! Returns a 'sealed' env string - e.g. & is forbidden in the messaging framework and therefore replaced
  // ---------------------------------------------------------------------------
  const char* GetSealed();
  
  // ---------------------------------------------------------------------------
  //! Factory function for a transferjob
  // ---------------------------------------------------------------------------
  static TransferJob* Create(const char* sealeddescription);

  // ---------------------------------------------------------------------------
  //! Return XrdOucEnv repesentation of a transfer job
  // ---------------------------------------------------------------------------
  XrdOucEnv* GetEnv() ; 

  // ---------------------------------------------------------------------------
  //! Function to replace the contents of a transfer job.
  //! This is used to replace the job content with a decoded capability.
  // ---------------------------------------------------------------------------
  void Replace(const char* description); 

  // ---------------------------------------------------------------------------
  //! Print the job details into out
  // ---------------------------------------------------------------------------
  void PrintOut(XrdOucString &out);
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
