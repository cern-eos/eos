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
#include "common/Logging.hh"
#include <thread>

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class DrainTransferJob;

//------------------------------------------------------------------------------
//! @brief Class implementing the draining of a filesystem
//------------------------------------------------------------------------------
class DrainFS: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fs_id filesystem id
  //----------------------------------------------------------------------------
  DrainFS(eos::common::FileSystem::fsid_t fs_id,
          eos::common::FileSystem::fsid_t target_fs_id = 0):
    mFsId(fs_id), mTargetFsId(target_fs_id),
    mDrainStatus(eos::common::FileSystem::kNoDrain), mTotalFiles(0),
    mDrainPeriod(0)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~DrainFS();

  //----------------------------------------------------------------------------
  //! Stop draining attached file system
  //---------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Get the list of failed drain jobs
  //----------------------------------------------------------------------------
  inline const std::list<shared_ptr<DrainTransferJob>>& GetFailedJobs() const
  {
    return mJobsFailed;
  }

  //---------------------------------------------------------------------------
  //! Get drain status
  //---------------------------------------------------------------------------
  inline eos::common::FileSystem::eDrainStatus GetDrainStatus() const
  {
    return mDrainStatus;
  }
  //---------------------------------------------------------------------------
  //! Get the FS id
  //---------------------------------------------------------------------------
  inline const eos::common::FileSystem::fsid_t GetFsId() const
  {
    return mFsId;
  }

  //----------------------------------------------------------------------------
  //! Start thread supervising the draining
  //----------------------------------------------------------------------------
  void Start()
  {
    mThread = std::thread(&DrainFS::DoIt, this);
  }

private:
  //----------------------------------------------------------------------------
  //! State of the drain job
  //----------------------------------------------------------------------------
  enum class State {DONE, EXPIRED, FAILED, CONTINUE};

  //----------------------------------------------------------------------------
  //! Method draining the file system
  //----------------------------------------------------------------------------
  void DoIt();

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
  //! Clean up when draining is completed
  //---------------------------------------------------------------------------
  void CompleteDrain();

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

  eos::common::FileSystem::fsid_t mFsId; ///< Drain source fsid
  eos::common::FileSystem::fsid_t mTargetFsId; /// Drain target fsid
  eos::common::FileSystem::eDrainStatus mDrainStatus;
  std::thread mThread; ///< Thread supervising the draining
  bool mDrainStop = false; ///< Flag to cancel an ongoing draining
  int mMaxRetries = 1; ///< Max number of retries
  unsigned int maxParallelJobs = 10; ///< Max number of parallel drain jobs
  uint64_t mTotalFiles; ///< Total number of files to drain
  std::chrono::seconds mDrainPeriod; ///< Allowed time for file system to drain
  std::chrono::time_point<std::chrono::steady_clock> mDrainStart;
  std::chrono::time_point<std::chrono::steady_clock> mDrainEnd;
  //! Collection of drain jobs to run
  std::list<shared_ptr<DrainTransferJob>> mJobsPending;
  //! Collection of failed drain jobs
  std::list<shared_ptr<DrainTransferJob>> mJobsFailed;
  //! Collection of running drain jobs
  std::list<shared_ptr<DrainTransferJob>> mJobsRunning;

};

EOSMGMNAMESPACE_END
