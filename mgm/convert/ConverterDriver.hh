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
#include "namespace/ns_quarkdb/qclient/include/qclient/structures/QHash.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class ConversionJob;

//------------------------------------------------------------------------------
//! @brief Class running the conversion threadpool
//------------------------------------------------------------------------------
class ConverterDriver : public eos::common::LogId
{
public:
  using JobInfoT = std::pair<eos::IFileMD::id_t, std::string>;
  using JobFailedT = std::pair<std::string, std::string>;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConverterDriver(const eos::QdbContactDetails& qdb_details) :
    mQdbHelper(qdb_details), mIsRunning(false),
    mThreadPool(std::thread::hardware_concurrency(), cDefaultMaxThreadPoolSize,
                10, 5, 3, "converter"),
    mMaxThreadPoolSize(cDefaultMaxThreadPoolSize), mTimestamp()
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
  //! Schedule a conversion job with the given ID and conversion info.
  //!
  //! @param id the job id
  //! @param conversion_info the conversion info string
  //! @return true if scheduling succeeded, false otherwise
  //----------------------------------------------------------------------------
  bool ScheduleJob(const eos::IFileMD::id_t& id,
                   const std::string& conversion_info);

  //----------------------------------------------------------------------------
  //! Get running state info
  //----------------------------------------------------------------------------
  inline bool IsRunning() const
  {
    return mIsRunning;
  }

  //----------------------------------------------------------------------------
  //! Get thread pool info
  //----------------------------------------------------------------------------
  inline std::string GetThreadPoolInfo() const
  {
    return mThreadPool.GetInfo();
  }

  //----------------------------------------------------------------------------
  //! Get thread pool max size
  //----------------------------------------------------------------------------
  inline uint32_t GetMaxThreadPoolSize() const
  {
    return mMaxThreadPoolSize.load();
  }

  //----------------------------------------------------------------------------
  //! Get number of running jobs
  //----------------------------------------------------------------------------
  inline uint64_t NumRunningJobs() const
  {
    eos::common::RWMutexReadLock rlock(mJobsMutex);
    return mJobsRunning.size();
  }

  //----------------------------------------------------------------------------
  //! Get number of pending jobs stored in QuarkDB
  //----------------------------------------------------------------------------
  inline uint64_t NumPendingJobs()
  {
    return mPendingJobs.size();
  }

  //----------------------------------------------------------------------------
  //! Get number of failed jobs stored in QuarkDB
  //----------------------------------------------------------------------------
  inline uint64_t NumQdbFailedJobs()
  {
    return mQdbHelper.NumFailedJobs();
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

  //----------------------------------------------------------------------------
  //! Get list of pending jobs
  //!
  //! @return list of pending jobs
  //----------------------------------------------------------------------------
  inline std::list<JobInfoT> GetPendingJobs()
  {
    return mQdbHelper.GetPendingJobs();
  }

  //----------------------------------------------------------------------------
  //! Get list of failed jobs
  //!
  //! @return list of failed jobs
  //----------------------------------------------------------------------------
  inline std::list<JobFailedT> GetFailedJobs()
  {
    return mQdbHelper.GetFailedJobs();
  }

  //----------------------------------------------------------------------------
  //! Clear list of pending jobs
  //----------------------------------------------------------------------------
  void ClearPendingJobs()
  {
    return mQdbHelper.ClearPendingJobs();
  }

  //----------------------------------------------------------------------------
  //! Clear list of failed jobs
  //----------------------------------------------------------------------------
  void ClearFailedJobs()
  {
    return mQdbHelper.ClearFailedJobs();
  }

private:
  struct QdbHelper {
    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    QdbHelper(const eos::QdbContactDetails& qdb_details)
    {
      mQcl = std::make_unique<qclient::QClient>(qdb_details.members,
             qdb_details.constructOptions());
      mQHashPending = qclient::QHash(*mQcl, kConversionPendingHashKey);
      mQHashFailed = qclient::QHash(*mQcl, kConversionFailedHashKey);
    }

    //--------------------------------------------------------------------------
    //! Returns a QuarkDB iterator for the pending jobs hash.
    //!
    //! @return the pending jobs hash iterator
    //--------------------------------------------------------------------------
    inline qclient::QHash::Iterator PendingJobsIterator()
    {
      return mQHashPending.getIterator(cBatchSize, "0");
    }

    //--------------------------------------------------------------------------
    //! Get list of pending jobs
    //!
    //! @return list of pending jobs
    //--------------------------------------------------------------------------
    std::list<JobInfoT> GetPendingJobs();

    //--------------------------------------------------------------------------
    //! Get list of failed jobs
    //!
    //! @return list of failed jobs
    //--------------------------------------------------------------------------
    std::list<JobFailedT> GetFailedJobs();

    //--------------------------------------------------------------------------
    //! Clear list of pending jobs
    //--------------------------------------------------------------------------
    void ClearPendingJobs();

    //--------------------------------------------------------------------------
    //! Clear list of failed jobs
    //--------------------------------------------------------------------------
    void ClearFailedJobs();

    //--------------------------------------------------------------------------
    //! Add conversion job to the queue of pending jobs in QuarkDB.
    //!
    //! @param jobinfo the pending conversion job details
    //! @return true if operation succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool AddPendingJob(const JobInfoT& jobinfo);

    //--------------------------------------------------------------------------
    //! Add conversion job to the queue of failed jobs in QuarkDB.
    //!
    //! @param jobinfo the failed conversion job details
    //! @return true if operation succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool AddFailedJob(const std::shared_ptr<ConversionJob>& job);

    //--------------------------------------------------------------------------
    //! Remove conversion job by id from the pending jobs queue in QuarkDB.
    //!
    //! @param id the conversion job id to remove
    //! @return true if operation succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool RemovePendingJob(const eos::IFileMD::id_t& id);

    //--------------------------------------------------------------------------
    //! Returns the number of failed jobs or -1 in case of failed operation
    //--------------------------------------------------------------------------
    int64_t NumFailedJobs();

    //! QDB conversion hash keys
    const std::string kConversionPendingHashKey = "eos-conversion-jobs-pending";
    const std::string kConversionFailedHashKey = "eos-conversion-jobs-failed";
    static constexpr unsigned int cBatchSize{1000}; ///< Batch size constant

  private:
    std::unique_ptr<qclient::QClient> mQcl; ///< Internal QClient object
    qclient::QHash mQHashPending; ///< QDB pending jobs hash object
    qclient::QHash mQHashFailed; ///< QDB failed jobs hash object
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
  //! Submit pending jobs from QDB
  //----------------------------------------------------------------------------
  void SubmitQdbPending(ThreadAssistant& assistant);

  //! Wait-time between jobs requests constant
  static constexpr unsigned int cDefaultRequestIntervalSec{60};
  //! Default maximum thread pool size constant
  static constexpr unsigned int cDefaultMaxThreadPoolSize{100};
  //! Max queue size from the thread pool when we delay new jobs
  static constexpr unsigned int cDefaultMaxQueueSize{1000};
  AssistedThread mThread; ///< Thread controller object
  QdbHelper mQdbHelper; ///< QuarkDB helper object
  std::atomic<bool> mIsRunning; ///< Mark if converter is running
  eos::common::ThreadPool mThreadPool; ///< Thread pool for conversion jobs
  std::atomic<unsigned int> mMaxThreadPoolSize; ///< Max threadpool size
  //! Timestamp of last jobs request
  std::chrono::steady_clock::time_point mTimestamp;
  //! Collection of running conversion jobs
  std::list<std::shared_ptr<ConversionJob>> mJobsRunning;
  //! RWMutex protecting the jobs collections
  mutable eos::common::RWMutex mJobsMutex;
  ///! Pending jobs in memory
  eos::common::ConcurrentQueue<JobInfoT> mPendingJobs;
};

EOSMGMNAMESPACE_END
