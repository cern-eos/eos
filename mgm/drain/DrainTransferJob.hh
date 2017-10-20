//------------------------------------------------------------------------------
//! @file DrainTransfer.hh
//! @author Andrea Manzi - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include "mgm/Namespace.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include <thread>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing the third party copy transfer, takes as input the
//! file id and the destination filesystem
//------------------------------------------------------------------------------
class DrainTransferJob: public eos::common::LogId
{
public:
  //! Status of a drain transfer job
  enum Status { OK, Running, Failed, Ready};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fileId the file id
  //! @param fsIdT source file system id
  //! @param fsIdT target file system id
  //----------------------------------------------------------------------------
  DrainTransferJob(eos::common::FileId::fileid_t fileId,
                   eos::common::FileSystem::fsid_t fsIdS,
                   eos::common::FileSystem::fsid_t fsIdT = 0)
    : mFileId(fileId), mFsIdSource(fsIdS), mFsIdTarget(fsIdT), mThread() {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DrainTransferJob();

  //----------------------------------------------------------------------------
  //! Start thread doing the draining
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Log error message and save it
  //!
  //! @param error error message
  //----------------------------------------------------------------------------
  void ReportError(const std::string& error);

  inline void SetTargetFS(eos::common::FileSystem::fsid_t fsIdT)
  {
    mFsIdTarget = fsIdT;
  }

  inline void SetStatus(DrainTransferJob::Status status)
  {
    mStatus = status;
  }

  inline DrainTransferJob::Status GetStatus() const
  {
    return mStatus;
  }

  inline eos::common::FileId::fileid_t GetFileId() const
  {
    return mFileId;
  }

  inline eos::common::FileSystem::fsid_t GetSourceFS() const
  {
    return mFsIdSource;
  }

  inline eos::common::FileSystem::fsid_t GetTargetFS() const
  {
    return mFsIdTarget;
  }

  inline const std::string& GetErrorString() const
  {
    return mErrorString;
  }

private:
  //----------------------------------------------------------------------------
  //! Method executed by the drainer thread where all the work is done
  //----------------------------------------------------------------------------
  void DoIt();

  eos::common::FileId::fileid_t mFileId; ///< File id to transfer
  ///! Source and destination file system
  eos::common::FileSystem::fsid_t mFsIdSource, mFsIdTarget;
  std::thread mThread; ///< Thread doing the draining
  std::string mErrorString; ///< Error message
  Status mStatus; ///< Status of the drain job
};

EOSMGMNAMESPACE_END
