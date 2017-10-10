// ----------------------------------------------------------------------
// File: DrainTransfer.hh
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_DRAINTRANSFERJOB_HH__
#define __EOSMGM_DRAINTRANSFERJOB_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileId.hh"
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "common/FileId.hh"
#include "common/SecEntity.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClCopyProcess.hh"
#include "Xrd/XrdScheduler.hh"
#include <vector>
#include <string>
#include <cstring>

/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/**
 * @file DrainTransferJob.hh
 * 
 * @brief Class implementing a third party copy transfer for drain
 * 
 */

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Class implementing the third party copy transfer, takes as input the 
 *
 * source file id and the destination filesystem
 */
/*----------------------------------------------------------------------------*/
class DrainTransferJob : XrdJob, public eos::common::LogId 
{
public:
  
  enum Status { OK, Running, Failed, Ready};

  // ---------------------------------------------------------------------------
  /**
   * @brief Constructor
   * @param fileId the file id
   */
  // ---------------------------------------------------------------------------

  DrainTransferJob (eos::common::FileId::fileid_t fileId, 
                              eos::common::FileSystem::fsid_t fsIdS,
                              eos::common::FileSystem::fsid_t fsIdT=0) {
    mFileId = fileId;
    mFsIdSource = fsIdS;
    mFsIdTarget = fsIdT;
  }

  // ---------------------------------------------------------------------------
  // Destructor
  // ---------------------------------------------------------------------------
  virtual ~DrainTransferJob (); 

  void DoIt();
  
  inline DrainTransferJob::Status GetStatus() { return mStatus;}

  void SetTargetFS(eos::common::FileSystem::fsid_t fsIdT);

  inline eos::common::FileId::fileid_t GetFileId() { return mFileId;}

  inline eos::common::FileSystem::fsid_t GetSourceFS() { return mFsIdSource;}

  inline eos::common::FileSystem::fsid_t GetTargetFS() { return mFsIdTarget;}
 
  inline void SetStatus(DrainTransferJob::Status status) {  mStatus= status;}

  inline void SetErrorString (std::string& error) { mErrorString= error;}

  inline  std::string& GetErrorString() { return mErrorString;}
  
  void ReportError(std::string& error);
private:
  /// file id for the given file to transfer
  eos::common::FileId::fileid_t mFileId;
  // destination fs
  eos::common::FileSystem::fsid_t mFsIdSource, mFsIdTarget;
  std::string mSourcePath;
  Status mStatus;
  std::string mErrorString;

};

EOSMGMNAMESPACE_END

#endif
