//------------------------------------------------------------------------------
//! @file BalancerJob.hh
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
#include "proto/FileMd.pb.h"
#include <thread>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing the third party copy transfer, takes as input the
//! file id and the destination filesystem
//------------------------------------------------------------------------------
class BalancerJob: public eos::common::LogId
{
public:
  //status of the Balancer Job
  enum Status {OK, Running, Failed, Ready};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fid the file id
  //! @param fsid_src source file system id
  //! @param fsid_trg target file system id
  //----------------------------------------------------------------------------
  BalancerJob(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid_src,
                   eos::common::FileSystem::fsid_t fsid_trg = 0):
    mFileId(fid), mFsIdSource(fsid_src), mFsIdTarget(fsid_trg), mThread(),
    mStatus(OK) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~BalancerJob();

  //----------------------------------------------------------------------------
  //! Start thread doing the transfer
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Log error message and save it
  //!
  //! @param error error message
  //----------------------------------------------------------------------------
  void ReportError(const std::string& error);

  //----------------------------------------------------------------------------
  //! Set the taget file system
  //----------------------------------------------------------------------------
  inline void SetTargetFS(eos::common::FileSystem::fsid_t fsid_trg)
  {
    mFsIdTarget = fsid_trg;
  }

  inline void SetStatus(BalancerJob::Status status)
  {
    mStatus = status;
  }

  inline BalancerJob::Status GetStatus() const
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


  //----------------------------------------------------------------------------
  //! Struct holding info about a file to be balanced
  //----------------------------------------------------------------------------
  struct FileBalanceInfo {
    std::string mFullPath;
    eos::ns::FileMdProto mProto;
  };

private:
  //----------------------------------------------------------------------------
  //! Method executed by the balancer thread where all the work is done
  //----------------------------------------------------------------------------
  void DoIt();

  //----------------------------------------------------------------------------
  //! Get file metadata info. Depending on the MGM configuration this will use
  //! the in-memory approach with namespace locking or the qclient to connect
  //! directy to QDB without any locking.
  //!
  //! @return file balance info object or throws and MDException
  //----------------------------------------------------------------------------
  static FileBalanceInfo GetFileInfo(eos::common::FileId::fileid_t fileId);

  //----------------------------------------------------------------------------
  //! Build TPC source url
  //!
  //! @param fbalance file to be balanced info
  //! @param fs source file system snapshot
  //!
  //! @return XrdCl source URL
  //----------------------------------------------------------------------------
  XrdCl::URL BuildTpcSrc(const FileBalanceInfo& fbalance,
                         const eos::common::FileSystem::fs_snapshot& fs);

  //----------------------------------------------------------------------------
  //! Build TPC destination url
  //!
  //! @param fbalanced file to be balanced info
  //! @param fs source file system snapshot
  //!
  //! @return XrdCl destination URL
  //----------------------------------------------------------------------------
  XrdCl::URL BuildTpcDst(const FileBalanceInfo& fbalance,
                         const eos::common::FileSystem::fs_snapshot& fs);


  eos::common::FileId::fileid_t mFileId; ///< File id to transfer
  ///! Source and destination file system
  eos::common::FileSystem::fsid_t mFsIdSource, mFsIdTarget;
  std::thread mThread; ///< Thread doing the balancing
  std::string mErrorString; ///< Error message
  Status mStatus; ///< Status of the balancing job
};

EOSMGMNAMESPACE_END
