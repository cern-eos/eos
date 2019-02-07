//------------------------------------------------------------------------------
//! file DrainFS.hh
//! @uthor Andrea Manzi - CERN
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
#include "mgm/FileSystem.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "namespace/interface/IFsView.hh"
#include "common/Logging.hh"
#include <thread>
#include <future>
#include <map>

namespace eos
{
namespace common
{
class ThreadPool;
}
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Class implementing the draining of a filesystem
//------------------------------------------------------------------------------
class DrainFs: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! State of file system drain operation
  //----------------------------------------------------------------------------
  enum class State {Done, Expired, Failed, Running, Stopped, Rerun};

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
  //! Signal the stop of the file system drain
  //---------------------------------------------------------------------------
  void SignalStop();

  //----------------------------------------------------------------------------
  //! Get the list of failed drain jobs
  //----------------------------------------------------------------------------
  inline const std::list<std::shared_ptr<DrainTransferJob>>&
      GetFailedJobs() const
  {
    return mJobsFailed;
  }

  //---------------------------------------------------------------------------
  //! Get drain status
  //---------------------------------------------------------------------------
  inline eos::common::FileSystem::eDrainStatus GetDrainStatus() const
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
  //! Stop draining
  //----------------------------------------------------------------------------
  void Stop();

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

  constexpr static std::chrono::seconds sRefreshTimeout {60};
  constexpr static std::chrono::seconds sStallTimeout {600};
  eos::IFsView* mNsFsView; ///< File system view
  eos::common::FileSystem::fsid_t mFsId; ///< Drain source fsid
  eos::common::FileSystem::fsid_t mTargetFsId; /// Drain target fsid
  eos::common::FileSystem::eDrainStatus mStatus;
  std::atomic<bool> mDrainStop; ///< Flag to cancel an ongoing draining
  std::atomic<std::uint32_t> mMaxRetries; ///< Max number of retries
  std::atomic<std::uint32_t> mMaxJobs; ///< Max number of drain jobs
  std::chrono::seconds mDrainPeriod; ///< Allowed time for file system to drain
  std::chrono::time_point<std::chrono::steady_clock> mDrainStart;
  std::chrono::time_point<std::chrono::steady_clock> mDrainEnd;
  //! Collection of drain jobs to run
  std::list<std::shared_ptr<DrainTransferJob>> mJobsPending;
  //! Collection of failed drain jobs
  std::list<std::shared_ptr<DrainTransferJob>> mJobsFailed;
  //! Collection of running drain jobs
  std::list<std::shared_ptr<DrainTransferJob>> mJobsRunning;
  eos::common::ThreadPool& mThreadPool;
  std::future<State> mFuture;
  uint64_t mTotalFiles; ///< Total number of files to drain
  uint64_t mPending; ///< Current num. of pending files to drain
  uint64_t mLastPending; ///< Previous num. of pending files to drain
  //! Last timestamp when drain progress was recorded
  std::chrono::time_point<std::chrono::steady_clock> mLastProgressTime;
  //! Last timestamp when drain status was updated
  std::chrono::time_point<std::chrono::steady_clock> mLastUpdateTime;
  std::string mSpace; ///< Space name to which fs is attached
};

EOSMGMNAMESPACE_END
