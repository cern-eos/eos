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

#include <map>
#include "common/Logging.hh"
#include "common/ObserverMgr.hh"
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/config/GlobalConfigStore.hh"
#include "mgm/convert/ConversionJob.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/structures/QHash.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class ConversionJob;
enum class ConversionJobStatus;
static const std::string kConverterMaxThreads {"converter-max-threads"};
static const std::string kConverterMaxQueueSize {"converter-max-queuesize"};
//------------------------------------------------------------------------------
//! @brief Class running the conversion threadpool
//------------------------------------------------------------------------------
class ConverterDriver : public eos::common::LogId
{
public:
  using JobInfoT = std::pair<eos::IFileMD::id_t, std::string>;
  using JobFailedT = std::pair<std::string, std::string>;
  using JobStatusT = ConversionJobStatus;
  using ObserverT = eos::common::ObserverMgr<JobStatusT, std::string>;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConverterDriver(const eos::QdbContactDetails& qdb_details) :
    mQdbHelper(qdb_details), mIsRunning(false),
    mThreadPool(std::thread::hardware_concurrency(), cDefaultMaxThreadPoolSize,
                10, 5, 3, "converter"),
    mMaxThreadPoolSize(cDefaultMaxThreadPoolSize),
    mMaxQueueSize(cDefaultMaxQueueSize), mTimestamp(),
    mObserverMgr(std::make_unique<ObserverT>(4)),
    mConfigStore(std::make_unique<GlobalConfigStore>(&FsView::gFsView))
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
  inline uint64_t NumFailedJobs()
  {
    return mFailed.load();
  }

  //----------------------------------------------------------------------------
  //! Get max queue size
  //----------------------------------------------------------------------------
  inline uint32_t GetMaxQueueSize() const
  {
    return mMaxQueueSize.load();
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
    mConfigStore->save(kConverterMaxThreads, std::to_string(max));
  }

  //----------------------------------------------------------------------------
  //! Set maximum queue size
  //!
  //! @param max maximum (submitted) queue size
  //----------------------------------------------------------------------------
  inline void SetMaxQueueSize(uint32_t max)
  {
    mMaxQueueSize = max;
    mConfigStore->save(kConverterMaxQueueSize, std::to_string(max));
  }

  //----------------------------------------------------------------------------
  //! Get list of pending jobs
  //!
  //! @return list of pending jobs
  //----------------------------------------------------------------------------
  inline std::vector<JobInfoT> GetPendingJobs()
  {
    return mQdbHelper.GetPendingJobs();
  }

  //----------------------------------------------------------------------------
  //! Clear list of pending jobs
  //----------------------------------------------------------------------------
  void ClearPendingJobs()
  {
    return mQdbHelper.ClearPendingJobs();
  }

  //----------------------------------------------------------------------------
  //! Get Observer Mgr, useful for other threads to register observers
  //----------------------------------------------------------------------------
  auto getObserverMgr()
  {
    return mObserverMgr.get();
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
    std::vector<ConverterDriver::JobInfoT> GetPendingJobs();

    //--------------------------------------------------------------------------
    //! Get list of failed jobs
    //!
    //! @return list of failed jobs
    //--------------------------------------------------------------------------
    std::vector<ConverterDriver::JobFailedT> GetFailedJobs();

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
    //! Remove conversion job by id from the pending jobs queue in QuarkDB.
    //!
    //! @param id the conversion job id to remove
    //! @return true if operation succeeded, false otherwise
    //--------------------------------------------------------------------------
    bool RemovePendingJob(const eos::IFileMD::id_t& id);

    //! QDB conversion hash keys
    const std::string kConversionPendingHashKey = "eos-conversion-jobs-pending";
    static constexpr unsigned int cBatchSize{1000}; ///< Batch size constant

  private:
    std::unique_ptr<qclient::QClient> mQcl; ///< Internal QClient object
    qclient::QHash mQHashPending; ///< QDB pending jobs hash object
    qclient::QHash mQHashFailed; ///< QDB failed jobs hash object
  };

  //----------------------------------------------------------------------------
  //! Initialize the saved value of config values like max threads/queue size
  //! from the config store
  //----------------------------------------------------------------------------
  void InitConfig();

  //----------------------------------------------------------------------------
  //! Converter engine thread monitoring
  //!
  //! @param assistant converter thread
  //----------------------------------------------------------------------------
  void Convert(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Signal all conversion jobs to stop
  //----------------------------------------------------------------------------
  void JoinAllConversionJobs();

  void PopulatePendingJobs();

  void HandlePostJobRun(std::shared_ptr<ConversionJob> job);

  //! Wait-time between jobs requests constant
  static constexpr unsigned int cDefaultRequestIntervalSec{60};
  //! Default maximum thread pool size constant
  static constexpr unsigned int cDefaultMaxThreadPoolSize{100};
  //! Max queue size from the thread pool when we delay new jobs
  static constexpr unsigned int cDefaultMaxQueueSize{1000};

  AssistedThread mThread; ///< Thread controller object
  QdbHelper mQdbHelper; ///< QuarkDB helper object
  std::atomic<bool> mIsRunning; ///< Mark if converter is running
  std::atomic<uint64_t> mFailed; ///< Number of failed jobs
  eos::common::ThreadPool mThreadPool; ///< Thread pool for conversion jobs
  std::atomic<unsigned int> mMaxThreadPoolSize; ///< Max threadpool size
  std::atomic<unsigned int> mMaxQueueSize; ///< Max submitted queue size
  //! Timestamp of last jobs request
  std::chrono::steady_clock::time_point mTimestamp;
  //! Collection of running conversion jobs
  std::map<eos::IFileMD::id_t, std::shared_ptr<ConversionJob>> mJobsRunning;
  //! RWMutex protecting the jobs collections
  mutable eos::common::RWMutex mJobsMutex;
  ///! Pending jobs in memory
  eos::common::ConcurrentQueue<JobInfoT> mPendingJobs;
  std::unique_ptr<ObserverT> mObserverMgr;
  std::unique_ptr<common::ConfigStore> mConfigStore;
};

EOSMGMNAMESPACE_END
