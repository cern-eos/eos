//------------------------------------------------------------------------------
//! file DrainFS.hh
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
#include "mgm/FileSystem.hh"
#include "namespace/interface/IFsView.hh"
#include "common/Logging.hh"
#include <thread>
#include <future>
#include <list>

//! Forward declarations
namespace eos
{
namespace common
{
class ThreadPool;
}
}

EOSMGMNAMESPACE_BEGIN

class DrainTransferJob;
class TableFormatterBase;

//------------------------------------------------------------------------------
//! @brief Class implementing the draining of a filesystem
//------------------------------------------------------------------------------
class DrainFs: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! State of file system drain operation
  //----------------------------------------------------------------------------
  enum class State {Done, Failed, Running, Rerun};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param thread_pool drain thread pool to use for jobs
  //! @param fs_view file system view
  //! @param src_fsid filesystem id to drain
  //! @param dst_fsid file system where to drain
  //----------------------------------------------------------------------------
  DrainFs(eos::common::ThreadPool& thread_pool, eos::IFsView* fs_view,
          eos::common::FileSystem::fsid_t src_fsid,
          eos::common::FileSystem::fsid_t dst_fsid = 0);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DrainFs();

  //----------------------------------------------------------------------------
  //! Signal an ongoing drain to stop
  //---------------------------------------------------------------------------
  inline void SignalStop()
  {
    mDrainStop = true;
  }

  //---------------------------------------------------------------------------
  //! Get drain status
  //---------------------------------------------------------------------------
  inline eos::common::DrainStatus GetDrainStatus() const
  {
    return mStatus;
  }

  //---------------------------------------------------------------------------
  //! Get the file system id
  //---------------------------------------------------------------------------
  inline const eos::common::FileSystem::fsid_t GetFsId() const
  {
    return mFsId;
  }

  //----------------------------------------------------------------------------
  //! Method draining the file system
  //!
  //! @return status of the file system at the end
  //----------------------------------------------------------------------------
  State DoIt();

  //----------------------------------------------------------------------------
  //! Set future holding the result of the drain
  //!
  //! @param future future object
  //----------------------------------------------------------------------------
  inline void SetFuture(std::future<State>&& future)
  {
    std::swap(mFuture, future);
  }

  //----------------------------------------------------------------------------
  //! Check if drain fs is still running by inspecting the future object
  //!
  //! @return true if running, otherwise false
  //----------------------------------------------------------------------------
  inline bool IsRunning() const
  {
    return (mFuture.valid() && (mFuture.wait_for(std::chrono::seconds(0)) !=
                                std::future_status::ready));
  }

  //----------------------------------------------------------------------------
  //! Populate table with drain jobs info corresponding to the current fs
  //!
  //! @param table table objec
  //! @param show_errors if true then display only failed transfers
  //! @param itags list of internal tags for info collection
  //!
  //! @note: Table header tags must match the order of the internal tags
  //----------------------------------------------------------------------------
  void PrintJobsTable(TableFormatterBase& table, bool show_errors,
                      const std::list<std::string>& itags) const;

private:
  //----------------------------------------------------------------------------
  //! Reset drain counters and status
  //----------------------------------------------------------------------------
  void ResetCounters();

  //----------------------------------------------------------------------------
  //! Get space defined drain variables i.e. number of retires, number of
  //! transfers per fs, etc.
  //!
  //! @param space space name
  //! @note method must be called with a lock on gFsView.ViewMutex
  //----------------------------------------------------------------------------
  void GetSpaceConfiguration(const std::string& space);

  //---------------------------------------------------------------------------
  //! Prepare the file system for drain i.e. delay the start by the configured
  //! amount of timem, set the status
  //!
  //! @return true if successful, otherwise false
  //---------------------------------------------------------------------------
  bool PrepareFs();

  //---------------------------------------------------------------------------
  //! Update the file system state to draining
  //!
  //! @return true if successful, otherwise false
  //---------------------------------------------------------------------------
  bool MarkFsDraining();

  //---------------------------------------------------------------------------
  //! Collect and prepare all the drain jobs
  //!
  //! @returns number of drain jobs prepared
  //---------------------------------------------------------------------------
  uint64_t CollectDrainJobs();

  //---------------------------------------------------------------------------
  //! Update progress of the drain
  //!
  //! @return progress state of the drain job
  //---------------------------------------------------------------------------
  State UpdateProgress();

  //----------------------------------------------------------------------------
  //! Handle running jobs
  //----------------------------------------------------------------------------
  void HandleRunningJobs();

  //----------------------------------------------------------------------------
  //! Mark file system drain as failed
  //----------------------------------------------------------------------------
  void FailedDrain();

  //---------------------------------------------------------------------------
  //! Mark file system drain as successful
  //---------------------------------------------------------------------------
  void SuccessfulDrain();

  //----------------------------------------------------------------------------
  //! Stop ongoing drain jobs - must be called by the same thread supervising
  //! the draining.
  //----------------------------------------------------------------------------
  void StopJobs();

  //----------------------------------------------------------------------------
  //! Wait until namespace is booted or drain stop is requested
  //----------------------------------------------------------------------------
  void WaitUntilNamespaceIsBooted() const;

  //----------------------------------------------------------------------------
  //! Get number of running jobs
  //----------------------------------------------------------------------------
  inline uint64_t NumRunningJobs() const
  {
    eos::common::RWMutexReadLock rd_lock(mJobsMutex);
    return mJobsRunning.size();
  }

  //----------------------------------------------------------------------------
  //! Get number of failed jobs
  //----------------------------------------------------------------------------
  inline uint64_t NumFailedJobs() const
  {
    eos::common::RWMutexReadLock rd_lock(mJobsMutex);
    return mJobsFailed.size();
  }

  constexpr static std::chrono::seconds sRefreshTimeout {60};
  constexpr static std::chrono::seconds sStallTimeout {600};
  eos::IFsView* mNsFsView; ///< File system view
  eos::common::FileSystem::fsid_t mFsId; ///< Drain source fsid
  eos::common::FileSystem::fsid_t mTargetFsId; /// Drain target fsid
  eos::common::DrainStatus mStatus;
  bool mDidRerun; ///< Flag if a rerun was already tried
  std::atomic<bool> mDrainStop; ///< Flag to cancel an ongoing draining
  std::atomic<std::uint32_t> mMaxJobs; ///< Max number of drain jobs
  std::chrono::seconds mDrainPeriod; ///< Allowed time for file system to drain
  std::chrono::time_point<std::chrono::steady_clock> mDrainStart;
  std::chrono::time_point<std::chrono::steady_clock> mDrainEnd;
  //! Collection of failed drain jobs
  std::set<std::shared_ptr<DrainTransferJob>> mJobsFailed;
  //! Collection of running drain jobs
  std::list<std::shared_ptr<DrainTransferJob>> mJobsRunning;
  mutable eos::common::RWMutex mJobsMutex; ///< RW mutex protecting job lists
  eos::common::ThreadPool& mThreadPool;
  std::future<State> mFuture;
  uint64_t mTotalFiles; ///< Total number of files to drain
  uint64_t mPending; ///< Current num. of pending files to drain
  uint64_t mLastPending; ///< Previous num. of pending files to drain
  //! Last timestamp when drain progress was recorded
  std::chrono::time_point<std::chrono::steady_clock> mLastProgressTime;
  //! Last timestamp when drain status was updated
  std::chrono::time_point<std::chrono::steady_clock> mLastUpdateTime;
};

EOSMGMNAMESPACE_END
