//------------------------------------------------------------------------------
// File: ConverterDriver.hh
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

#include "common/Logging.hh"
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/convert/ConversionJob.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class ConversionJob;

//------------------------------------------------------------------------------
//! @brief Class running the conversion threadpool
//------------------------------------------------------------------------------
class ConverterDriver : public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConverterDriver(const eos::QdbContactDetails& qdb_details) :
    mQdbHelper(qdb_details), mIsRunning(false),
    mThreadPool(std::thread::hardware_concurrency(), cDefaultMaxThreadPoolSize,
                10, 6, 5, "converter_engine"),
    mMaxThreadPoolSize(cDefaultMaxThreadPoolSize)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConverterDriver()
  {
    Stop();
  }

  //----------------------------------------------------------------------------
  //! Start converter thread
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Stop converter thread and all running conversion jobs
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Get thread pool info
  //!
  //! @return string thread pool info
  //----------------------------------------------------------------------------
  inline std::string GetThreadPoolInfo() const
  {
    return mThreadPool.GetInfo();
  }

  //----------------------------------------------------------------------------
  //! Get thread pool max size
  //!
  //! @return thread pool max size
  //----------------------------------------------------------------------------
  inline uint32_t GetMaxThreadPoolSize() const
  {
    return mMaxThreadPoolSize.load();
  }

  //----------------------------------------------------------------------------
  //! Set maximum size of the converter thread pool
  //!
  //! @param max maximum threadpool value
  //----------------------------------------------------------------------------
  inline void SetMaxThreadPoolSize(uint32_t max)
  {
    mThreadPool.SetMaxThreads(max);
    mMaxThreadPoolSize = max;
  }

private:
  using JobInfoT = std::pair<eos::IFileMD::id_t, std::string>;

  struct QdbHelper {
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    QdbHelper(const eos::QdbContactDetails& qdb_details) :
      mJobsRetrieved{false}, mTimestamp()
    {
      mQcl = std::make_unique<qclient::QClient>(qdb_details.members,
                                                qdb_details.constructOptions());
    }

    //--------------------------------------------------------------------------
    //! Retrieve the next batch of conversion jobs from QuarkDB.
    //! In case of a failed attempt, avoid repeating the request
    //! for cRequestIntervalTime seconds.
    //!
    //! @return list of conversion job strings
    //--------------------------------------------------------------------------
    std::list<JobInfoT> RetrieveJobsBatch();

    //--------------------------------------------------------------------------
    //! Remove conversion job by id from the pending jobs queue in QuarkDB.
    //!
    //! @param id the conversion job id to remove
    //! @return true if operation succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool RemovePendingJob(const eos::IFileMD::id_t& id) const;

    //--------------------------------------------------------------------------
    //! Add conversion job to the queue of failed jobs in QuarkDB.
    //!
    //! @param jobinfo the failed conversion job details
    //! @return true if operation succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool AddFailedJob(const JobInfoT& jobinfo) const;

    ///< QDB conversion hash keys
    const std::string kConversionPendingHashKey = "eos-conversion-jobs-pending";
    const std::string kConversionFailedHashKey = "eos-conversion-jobs-failed";
    static constexpr unsigned int cBatchSize{50}; ///< Batch size constant
    ///< Wait-time before repeating a request constant
    static constexpr unsigned int cRequestIntervalTime{60};

  private:
    //--------------------------------------------------------------------------
    //! Parse a Redis reply containing an array of conversion data
    //! and fill the given list with jobs info
    //!
    //! @param reply the Redis reply object
    //! @param jobs the list of jobs info to fill
    //! @return true if parsing succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool ParseJobsFromReply(const qclient::Reply* const reply,
                            std::list<JobInfoT>& jobs) const;

    std::unique_ptr<qclient::QClient> mQcl; ///< Internal QClient object
    bool mJobsRetrieved; ///< Flag status of last request
    ///< Timestamp of last request
    std::chrono::steady_clock::time_point mTimestamp;
  };

  //----------------------------------------------------------------------------
  //! Converter engine thread monitoring
  //!
  //! @param assistant converter thread
  //----------------------------------------------------------------------------
  void Convert(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Handle jobs based on status
  //----------------------------------------------------------------------------
  void HandleRunningJobs();

  //----------------------------------------------------------------------------
  //! Signal all conversion jobs to stop
  //----------------------------------------------------------------------------
  void JoinAllConversionJobs();

  //----------------------------------------------------------------------------
  //! Get number of running jobs
  //----------------------------------------------------------------------------
  inline uint64_t NumRunningJobs() const
  {
    eos::common::RWMutexReadLock rlock(mJobsMutex);
    return mJobsRunning.size();
  }

  //----------------------------------------------------------------------------
  //! Get number of failed jobs
  //----------------------------------------------------------------------------
  inline uint64_t NumFailedJobs() const
  {
    eos::common::RWMutexReadLock rlock(mJobsMutex);
    return mJobsFailed.size();
  }

  static constexpr unsigned int cDefaultMaxThreadPoolSize{400};
  AssistedThread mThread; ///< Thread controller object
  QdbHelper mQdbHelper; ///< QuarkDB helper object
  std::atomic<bool> mIsRunning; ///< Mark if converter is running
  eos::common::ThreadPool mThreadPool; ///< Thread pool for conversion jobs
  std::atomic<unsigned int> mMaxThreadPoolSize; ///< Max threadpool size
  ///< Collection of running conversion jobs
  std::list<std::shared_ptr<ConversionJob>> mJobsRunning;
  ///< Collection of failed conversion jobs
  std::set<std::shared_ptr<ConversionJob>> mJobsFailed;
  ///< RWMutex protecting the jobs collections
  mutable eos::common::RWMutex mJobsMutex;
};

EOSMGMNAMESPACE_END
