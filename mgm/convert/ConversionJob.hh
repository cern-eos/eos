//------------------------------------------------------------------------------
// File: ConversionJob.hh
// Author: Mihai Patrascoiu - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/convert/ConversionInfo.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/FileSystem.hh"
#include "namespace/interface/IFileMD.hh"
#include "XrdCl/XrdClCopyProcess.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class ConversionJob;

//------------------------------------------------------------------------------
//! @brief Class used for monitoring the progress of a running
//! Conversion Job. Cancellation is also allowed via this class.
//------------------------------------------------------------------------------
class ConversionProgressHandler : public XrdCl::CopyProgressHandler,
  public eos::common::LogId
{
public:
  friend class ConversionJob;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConversionProgressHandler() :
    mCancel(false), mProgress(0), mBytesProcessed(0),
    mStartTimestamp(0) {}

  //----------------------------------------------------------------------------
  //! Notify when a new job is about to start
  //!
  //! @param jobNum         the job number of the copy job concerned
  //! @param jobTotal       total number of jobs being processed
  //! @param source         the source url of the current job
  //! @param destination    the destination url of the current job
  //----------------------------------------------------------------------------
  void BeginJob(uint16_t jobNum, uint16_t jobTotal,
                const XrdCl::URL* source,
                const XrdCl::URL* destination) override
  {
    mStartTimestamp = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count();
  }

  //----------------------------------------------------------------------------
  //! Notify about the progress of the current job
  //!
  //! @param jobNum         job number
  //! @param bytesProcessed bytes processed by the current job
  //! @param bytesTotal     total number of bytes to be processed
  //----------------------------------------------------------------------------
  void JobProgress(uint16_t jobNum, uint64_t bytesProcessed,
                   uint64_t bytesTotal) override
  {
    mBytesProcessed = bytesProcessed;
    mProgress = static_cast<int>(100.0 * bytesProcessed / bytesTotal);
  }

  //----------------------------------------------------------------------------
  //! Determine whether the job should be cancelled
  //----------------------------------------------------------------------------
  bool ShouldCancel(uint16_t jobNum) override
  {
    return mCancel;
  }

  //----------------------------------------------------------------------------
  //! Trigger job cancellation
  //----------------------------------------------------------------------------
  inline void Cancel()
  {
    mCancel = true;
  }

private:
  std::atomic<bool> mCancel; ///< Flag whether job should be cancelled
  std::atomic<int> mProgress; ///< Job completion percentage
  std::atomic<uint64_t> mBytesProcessed; ///< Bytes processed by job so far
  std::atomic<uint64_t> mStartTimestamp; ///< Timestamp of job start
};


//------------------------------------------------------------------------------
//! @brief Class executing a third-party conversion job
//------------------------------------------------------------------------------
class ConversionJob : public eos::common::LogId
{
public:
  //! Possible status of a conversion job
  enum class Status { DONE, RUNNING, PENDING, FAILED };

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fid the file id to convert
  //----------------------------------------------------------------------------
  ConversionJob(const eos::IFileMD::id_t fid,
                const ConversionInfo& conversion_info);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConversionJob();

  //----------------------------------------------------------------------------
  //! Execute a third-party copy
  //----------------------------------------------------------------------------
  void DoIt() noexcept;

  //----------------------------------------------------------------------------
  //! Cancel running third-party copy
  //----------------------------------------------------------------------------
  inline void Cancel()
  {
    mProgressHandler.Cancel();
  }

  //----------------------------------------------------------------------------
  //! Get the conversion job status
  //----------------------------------------------------------------------------
  inline ConversionJob::Status GetStatus() const
  {
    return mStatus.load();
  }

  //----------------------------------------------------------------------------
  //! Get the conversion string
  //----------------------------------------------------------------------------
  inline std::string GetConversionString() const
  {
    return mConversionInfo.ToString();
  }

  //----------------------------------------------------------------------------
  //! Get the conversion job file id
  //----------------------------------------------------------------------------
  inline eos::IFileMD::id_t GetFid() const
  {
    assert(mFid == mConversionInfo.mFid);
    return mFid;
  }

  //----------------------------------------------------------------------------
  //! Get the conversion error message
  //----------------------------------------------------------------------------
  inline std::string GetErrorMsg() const
  {
    return mErrorString;
  }

private:
  //----------------------------------------------------------------------------
  //! Merge origial and the newly converted one so that the initial file
  //! identifier and all the rest of the metadata information is preserved.
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Merge();

  //----------------------------------------------------------------------------
  //! Log the error message, store it and set the job as failed
  //!
  //! @param errmsg the error message
  //! @param details additional details
  //----------------------------------------------------------------------------
  void HandleError(const std::string& emsg, const std::string& details = "");

  //----------------------------------------------------------------------------
  //! Construct CGI from conversion info
  //!
  //! @param info conversion info to process
  //!
  //! @return string containing conversion CGI
  //----------------------------------------------------------------------------
  std::string ConversionCGI(const ConversionInfo& info) const;

  const eos::IFileMD::id_t mFid; ///< Conversion file id
  const ConversionInfo mConversionInfo; ///< Conversion details
  std::string mSourcePath; ///< Path of file to be converted
  std::string mConversionPath; ///< Path of newly converted file
  std::atomic<Status> mStatus; ///< Conversion job status
  std::string mErrorString; ///< Error message
  ConversionProgressHandler mProgressHandler; ///< Conversion progress handler
};

EOSMGMNAMESPACE_END
