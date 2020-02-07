//------------------------------------------------------------------------------
//! @file DrainTransfer.hh
//! @author Elvin Sindrilaru - CERN
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

//! Forward declaration
class DrainTransferJob;

//------------------------------------------------------------------------------
//! Class DrainProgressHandler used to monitor the progress of the current
//! drain transfers but also more importantly to cancel gracefully a running
//! transfer.
//------------------------------------------------------------------------------
class DrainProgressHandler: public XrdCl::CopyProgressHandler,
  public eos::common::LogId
{
  friend class DrainTransferJob;

public:

//----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DrainProgressHandler():
    mDoCancel{false}, mProgress{0}, mBytesTransferred{0ull},
    mStartTimestampSec(std::chrono::duration_cast<std::chrono::seconds>
                       (std::chrono::system_clock::now().time_since_epoch()).count())
  {}

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
    using namespace std::chrono;
    mStartTimestampSec = duration_cast<seconds>
                         (system_clock::now().time_since_epoch()).count();
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
    mBytesTransferred = bytesProcessed;
    mProgress = static_cast<int>((1.0 - (1.0 * (bytesTotal - bytesProcessed) /
                                         bytesTotal))  * 100);
  }

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
  std::atomic<bool> mDoCancel; ///< Mark if job should be cancelled
  std::atomic<int> mProgress; ///< Progress percentage
  std::atomic<uint64_t> mBytesTransferred; ///< Amount of data transferred
  std::atomic<uint64_t> mStartTimestampSec; ///< Start timestamp in seconds
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
  //! @param exclude_srcs set of fs ids which are to be excluded as sources
  //! @param exclude_dsts set of fs ids which are to be excluded as dest.
  //! @param drop_src mark if source replica should be dropped if operation
  //!        is successful (default true)
  //! @param app_tag application tag for easy classification of job types
  //----------------------------------------------------------------------------
  DrainTransferJob(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid_src,
                   eos::common::FileSystem::fsid_t fsid_trg = 0,
                   std::set<eos::common::FileSystem::fsid_t> exclude_srcs = {},
                   std::set<eos::common::FileSystem::fsid_t> exclude_dsts = {},
                   bool drop_src = true,
                   const std::string& app_tag = "drain"):
    mAppTag(app_tag), mFileId(fid), mFsIdSource(fsid_src), mFsIdTarget(fsid_trg),
    mTxFsIdSource(fsid_src), mStatus(Status::Ready), mRainReconstruct(false),
    mDropSrc(drop_src)
  {
    mTriedSrcs.insert(exclude_srcs.begin(), exclude_srcs.end());
    mExcludeDsts.insert(mExcludeDsts.begin(), exclude_dsts.begin(),
                        exclude_dsts.end());
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DrainTransferJob() = default;

  //----------------------------------------------------------------------------
  //! Execute a third-party transfer
  //----------------------------------------------------------------------------
  virtual void DoIt() noexcept;

  //----------------------------------------------------------------------------
  //! Cancel ongoing TPC transfer
  //----------------------------------------------------------------------------
  inline void Cancel()
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
  //! Set drain transfer status
  //!
  //! @param status new drain transfer status
  //----------------------------------------------------------------------------
  inline void SetStatus(DrainTransferJob::Status status)
  {
    mStatus = status;
  }

  //----------------------------------------------------------------------------
  //! Get drain transfer status
  //----------------------------------------------------------------------------
  virtual DrainTransferJob::Status GetStatus() const
  {
    return mStatus.load();
  }

  //----------------------------------------------------------------------------
  //! Get drain job info based on the requested tags
  //!
  //! @param tags set of tags that the requestor would like to get inf about
  //!
  //! @param map of tags to corresponding information collected from the job
  //----------------------------------------------------------------------------
  std::list<std::string>
  GetInfo(const std::list<std::string>& tags) const;

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif

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
  //! Update MGM stats depending on the type of transfer. The generic
  //! DrainTransferJob can be used in different components eg. fsck. For the
  //! time being this will update the MGM statistics only for the drainer.
  //----------------------------------------------------------------------------
  void UpdateMgmStats();

  std::string mAppTag; ///< Application tag for the transfer
  const eos::common::FileId::fileid_t mFileId; ///< File id to transfer
  //! Source and destination file system
  std::atomic<eos::common::FileSystem::fsid_t> mFsIdSource;
  std::atomic<eos::common::FileSystem::fsid_t> mFsIdTarget;
  //! Actual file system id used for the current drain transfer, can point to
  //! the file system of a replica of the file
  std::atomic<eos::common::FileSystem::fsid_t> mTxFsIdSource;
  std::string mErrorString; ///< Error message
  std::atomic<Status> mStatus; ///< Status of the drain job
  std::set<eos::common::FileSystem::fsid_t> mTriedSrcs; ///< Tried src
  std::vector<eos::common::FileSystem::fsid_t> mExcludeDsts; ///< Excluded dest.
  bool mRainReconstruct; ///< Mark rain reconstruction
  bool mDropSrc; ///< Mark if source replicas should be dropped
  DrainProgressHandler mProgressHandler; ///< TPC progress handler
};

EOSMGMNAMESPACE_END
