//------------------------------------------------------------------------------
// File: ConverterEngine.hh
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
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/convert/ConversionJob.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/structures/QHash.hh"
#include <XrdOuc/XrdOucCallBack.hh>

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class ConversionJob;
enum class ConversionJobStatus;
//------------------------------------------------------------------------------
//! @brief Class running the conversion threadpool
//------------------------------------------------------------------------------
class ConverterEngine : public eos::common::LogId
{
public:
  using JobInfoT =
    std::tuple<eos::IFileMD::id_t, std::string, std::shared_ptr<XrdOucCallBack>>;
  using JobFailedT = std::pair<std::string, std::string>;
  using JobStatusT = ConversionJobStatus;
  using ObserverT = eos::common::ObserverMgr<JobStatusT, std::string>;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ConverterEngine(const eos::QdbContactDetails& qdb_details) :
    mQdbHelper(qdb_details), mIsRunning(false), mFailed(0),
    mThreadPool(std::thread::hardware_concurrency(), 100,
                10, 5, 3, "converter"),
    mMaxQueueSize(1000), mTimestamp(),
    mObserverMgr(std::make_unique<ObserverT>(4))
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ConverterEngine()
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
  //! @param err_msg error message in case of failure
  //! @param callback shared pointer to a callback object - default is nullptr
  //!
  //! @return true if scheduling succeeded, false otherwise
  //----------------------------------------------------------------------------
  bool ScheduleJob(const eos::IFileMD::id_t& id,
                   const std::string& conversion_info,
                   std::string& err_msg,
                   std::shared_ptr<XrdOucCallBack> callback = nullptr);

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

  //----------------------------------------------------------------------------
  //! Apply global configuration relevant for the converter
  //----------------------------------------------------------------------------
  void ApplyConfig();

  //----------------------------------------------------------------------------
  //! Make configuration change
  //!
  //! @param key input key
  //! @param val input value
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetConfig(const std::string& key, const std::string& val);

  //----------------------------------------------------------------------------
  //! Serialize converter configuration
  //!
  //! @return string representing converter configuration
  //----------------------------------------------------------------------------
  std::string SerializeConfig() const;

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
    std::vector<ConverterEngine::JobInfoT> GetPendingJobs();

    //--------------------------------------------------------------------------
    //! Clear list of pending jobs
    //--------------------------------------------------------------------------
    void ClearPendingJobs();

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
  };

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

  //----------------------------------------------------------------------------
  //! Method to collect and queue pending jobs from the QDB backend
  //----------------------------------------------------------------------------
  void PopulatePendingJobs();

  //----------------------------------------------------------------------------
  //! Cleanup handle after a job is run - remove the job from the list of
  //! pending jobs and clean up the conversion file in /eos/.../proc/conversion
  //!
  //! @param job finished job
  //----------------------------------------------------------------------------
  void HandlePostJobRun(std::shared_ptr<ConversionJob> job);

  //----------------------------------------------------------------------------
  //! Store configuration
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool StoreConfig();

  AssistedThread mThread; ///< Thread controller object
  QdbHelper mQdbHelper; ///< QuarkDB helper object
  std::atomic<bool> mIsRunning; ///< Mark if converter is running
  std::atomic<uint64_t> mFailed; ///< Number of failed jobs
  eos::common::ThreadPool mThreadPool; ///< Thread pool for conversion jobs
  std::atomic<unsigned int> mMaxQueueSize; ///< Max submitted queue size
  //! Timestamp of last jobs request
  std::chrono::steady_clock::time_point mTimestamp;
  //! Collection of running conversion jobs
  std::map<eos::IFileMD::id_t, std::shared_ptr<ConversionJob>> mJobsRunning;
  //! RWMutex protecting the jobs collections
  mutable eos::common::RWMutex mJobsMutex;
  //! Pending jobs in memory
  eos::common::ConcurrentQueue<JobInfoT> mPendingJobs;
  std::unique_ptr<ObserverT> mObserverMgr;
};

EOSMGMNAMESPACE_END
