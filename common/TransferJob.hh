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
