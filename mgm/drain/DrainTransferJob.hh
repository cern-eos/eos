//------------------------------------------------------------------------------
//! @file DrainTransfer.hh
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
#include "XrdCl/XrdClCopyProcess.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class DrainProgressHandler used to monitor the progress of the current
//! drain transfers but also more importantly to cancel gracefully a running
//! transfer.
//------------------------------------------------------------------------------
class DrainProgressHandler: public XrdCl::CopyProgressHandler,
  public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Notify when a new job is about to start
  //!
  //! @param jobNum         the job number of the copy job concerned
  //! @param jobTotal       total number of jobs being processed
  //! @param source         the source url of the current job
  //! @param destination    the destination url of the current job
  //----------------------------------------------------------------------------
  void BeginJob(uint16_t jobNum, uint16_t jobTotal, const XrdCl::URL* source,
                const XrdCl::URL* destination) override
  {
    eos_info("msg=\"starting copy job %i src=%s dst=%s", jobNum,
             source->GetURL().c_str(), destination->GetURL().c_str());
  }

  //----------------------------------------------------------------------------
  //! Notify when the previous job has finished
  //!
  //! @param jobNum job number
  //! @param result result of the job
  //----------------------------------------------------------------------------
  void EndJob(uint16_t jobNum, const XrdCl::PropertyList* result) override
  {
    eos_info("msg=\"job=%i finished\"");
  }

  //----------------------------------------------------------------------------
  //! Notify about the progress of the current job
  //!
  //! @param jobNum         job number
  //! @param bytesProcessed bytes processed by the current job
  //! @param bytesTotal     total number of bytes to be processed by job
  //----------------------------------------------------------------------------
  void JobProgress(uint16_t jobNum, uint64_t bytesProcessed,
                   uint64_t bytesTotal) override
  {
    eos_info("msg=\"progress job=%i percentage=%.02f\%\"", jobNum,
             (1.0 - (1.0 * (bytesTotal - bytesProcessed) / bytesTotal)) * 100);
  };

  //----------------------------------------------------------------------------
  //! Determine whether the job should be canceled - this is used internally
  //! by the XrdCl::CopyProcess.
  //----------------------------------------------------------------------------
  bool ShouldCancel(uint16_t jobNum) override
  {
    return mDoCancel;
  }

  //----------------------------------------------------------------------------
  //! Mark drain job to be cancelled
  //----------------------------------------------------------------------------
  void DoCancel()
  {
    mDoCancel = true;
  }

private:
  std::atomic<bool> mDoCancel {false}; ///< Mark if job should be cancelled
};

//------------------------------------------------------------------------------
//! Class implementing the third party copy transfer, takes as input the
//! file id and the destination filesystem
//------------------------------------------------------------------------------
class DrainTransferJob: public eos::common::LogId
{
public:
  //! Status of a drain transfer job
  enum class Status {OK, Running, Failed, Ready};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fid the file id
  //! @param fsid_src source file system id
  //! @param fsid_trg target file system id
  //----------------------------------------------------------------------------
  DrainTransferJob(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid_src,
                   eos::common::FileSystem::fsid_t fsid_trg = 0):
    mFileId(fid), mFsIdSource(fsid_src), mFsIdTarget(fsid_trg),
    mStatus(Status::Ready), mRainReconstruct(false) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~DrainTransferJob() = default;

  //----------------------------------------------------------------------------
  //! Execute a third-party transfer
  //----------------------------------------------------------------------------
  void DoIt();

  //----------------------------------------------------------------------------
  //! Cancel ongoing TPC transfer
  //----------------------------------------------------------------------------
  void Cancel()
  {
    mProgressHandler.DoCancel();
  }

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

  inline void SetStatus(DrainTransferJob::Status status)
  {
    mStatus = status;
  }

  inline DrainTransferJob::Status GetStatus() const
  {
    return mStatus.load();
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
  //! Struct holding info about a file to be drained
  //----------------------------------------------------------------------------
  struct FileDrainInfo {
    std::string mFullPath;
    eos::ns::FileMdProto mProto;
  };

  //----------------------------------------------------------------------------
  //! Get file metadata info. Depending on the MGM configuration this will use
  //! the in-memory approach with namespace locking or the qclient to connect
  //! directy to QDB without any locking.
  //!
  //! @return file drain info object or throws and MDException
  //----------------------------------------------------------------------------
  FileDrainInfo GetFileInfo() const;

  //----------------------------------------------------------------------------
  //! Build TPC source url
  //!
  //! @param fdrain file to be drained info
  //! @param log_id transfer log id
  //!
  //! @return XrdCl source URL
  //----------------------------------------------------------------------------
  XrdCl::URL BuildTpcSrc(const FileDrainInfo& fdrain,
                         const std::string& log_id);

  //----------------------------------------------------------------------------
  //! Build TPC destination url
  //!
  //! @param fdrain file to be drained info
  //! @param log_id transfer log id
  //!
  //! @return XrdCl destination URL
  //----------------------------------------------------------------------------
  XrdCl::URL BuildTpcDst(const FileDrainInfo& fdrain,
                         const std::string& log_id);

  //----------------------------------------------------------------------------
  //! Select destiantion file system for current transfer
  //!
  //! @param fdrain file to drain metadata info
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SelectDstFs(const FileDrainInfo& fdrain);

  //----------------------------------------------------------------------------
  //! Drain 0-size file
  //!
  //! @param fdrain file to drain metadata info
  //!
  //! @return drain status
  //----------------------------------------------------------------------------
  Status DrainZeroSizeFile(const FileDrainInfo& fdrain);

  //----------------------------------------------------------------------------
  //! Estimate TCP transfer timeout based on file size but not shorter than
  //! 30 min.
  //!
  //! @param fsize file size
  //!
  //! @return timeout value in seconds
  //----------------------------------------------------------------------------
  std::chrono::seconds EstimateTpcTimeout(const uint64_t fsize) const;

  eos::common::FileId::fileid_t mFileId; ///< File id to transfer
  ///! Source and destination file system
  eos::common::FileSystem::fsid_t mFsIdSource, mFsIdTarget;
  std::string mErrorString; ///< Error message
  std::atomic<Status> mStatus; ///< Status of the drain job
  std::set<eos::common::FileSystem::fsid_t> mTriedSrcs; ///< Tried src
  bool mRainReconstruct; ///< Flag to mark a rain reconstruction
  DrainProgressHandler mProgressHandler; ///< TPC progress handler
};

EOSMGMNAMESPACE_END
