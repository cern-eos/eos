//------------------------------------------------------------------------------
//! @file Drainer.hh
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
#include "common/Logging.hh"
#include "common/ThreadPool.hh"
#include "common/AssistedThread.hh"
#include "common/FileSystem.hh"
#include <list>

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class DrainTransferJob;
class TableFormatterBase;
class FileSystem;
class DrainFs;

//------------------------------------------------------------------------------
//! @brief Class running the centralized draining
//------------------------------------------------------------------------------
class Drainer: public eos::common::LogId
{
public:

  //! Map node to map of draining file systems and their associated futures
  using DrainMap = std::map<std::string,
        std::set<std::shared_ptr<eos::mgm::DrainFs>>>;
  //! Drain job table header information - each pair represents the header
  //! tag to be displayed when the table is printed to the client, and the
  //! correspnding internal tag used when collecting information in the
  //! DrainTransferJob class
  using DrainHdrInfo = std::list<std::pair<std::string, std::string>>;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Drainer();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Drainer();

  //----------------------------------------------------------------------------
  //! Start drainer thread
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Stop running thread and implicitly all running drain jobs
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Start  of a given file system
  //! @note This method must be called with a read lock on the FsView::ViewMutex
  //!
  //! @param fs file system object
  //! @param dst_fsid destination file system, 0 if not chosen
  //! @param err output error message
  //!
  //! @return true if drain started successfully, otherwise false
  //----------------------------------------------------------------------------
  bool StartFsDrain(eos::mgm::FileSystem* fs,
                    eos::common::FileSystem::fsid_t dst_fsid, std::string& err);

  //----------------------------------------------------------------------------
  //! Stop draining of a given file system
  //! @note This method must be called with a read lock on the FsView::ViewMutex
  //!
  //! @param fs file system object
  //! @param err output error message
  //!
  //! @return true if drain stopped successfully, otherwise false
  //----------------------------------------------------------------------------
  bool StopFsDrain(eos::mgm::FileSystem* fs, std::string& err);

  //----------------------------------------------------------------------------
  //! Set max size of thread pool used for draining
  //!
  //! @param max max value
  //----------------------------------------------------------------------------
  void SetMaxThreadPoolSize(uint64_t max)
  {
    mThreadPool.SetMaxThreads(max);
  }

  //----------------------------------------------------------------------------
  //! Get thread pool info
  //!
  //! @return string summary for the thread pool
  //----------------------------------------------------------------------------
  std::string GetThreadPoolInfo() const
  {
    return mThreadPool.GetInfo();
  }

  //----------------------------------------------------------------------------
  //! Get drain jobs info (global or specific to an fsid)
  //!
  //! @param out output string
  //! @param hdr_info map of header tags to internal tags used for collecting
  //!        info about drain transfers
  //! @param fsid file system for which to display the drain status or 0 for
  //!        displaying all drain activities in the system
  //! @param err output error string
  //! @param show_errors if true then display only failed transfers
  //! @param monitor_format if true then display in monitoring format
  //!
  //! @return true if successful, or false if there was any issue in collecting
  //!        the requested information
  //----------------------------------------------------------------------------
  bool GetJobsInfo(std::string& out, const DrainHdrInfo& hdr_info,
                   unsigned int fsid, bool only_failed = false,
                   bool monitor_fmt = false) const;

  //----------------------------------------------------------------------------
  //! Get the maximum number of file systems that can be drained in parallel
  //! on the same node.
  //!
  //! @return max number of file systems to be drained in parallel per node
  //----------------------------------------------------------------------------
  unsigned int MaxDrainFsInParallel(const std::string& space) const;

private:
  using ListPendingT = std::list<std::pair<eos::common::FileSystem::fsid_t,
        eos::common::FileSystem::fsid_t>>;

  //----------------------------------------------------------------------------
  //! Method doing the drain monitoring
  //!
  //! @param assistant thread running the job
  //----------------------------------------------------------------------------
  void Drain(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Handle queued draining requests
  //----------------------------------------------------------------------------
  void HandleQueued();

  //----------------------------------------------------------------------------
  //! Signal all drain file systems to stop and wait for them
  //----------------------------------------------------------------------------
  void WaitForAllDrainToStop();

  //----------------------------------------------------------------------------
  //! Update drain relevant configuration from the space view
  //----------------------------------------------------------------------------
  void UpdateFromSpaceConfig();

  std::atomic<bool> mIsRunning; ///< Mark if drainer is running
  AssistedThread mThread; ///< Thread updating the drain configuration
  //! Contains per space the max allowed fs draining per node
  std::map<std::string, int> mCfgMap;
  DrainMap mDrainFs; ///< Map of nodes to file systems draining
  mutable eos::common::RWMutex mDrainMutex; ///< Mutex protecting the drain map
  mutable XrdSysMutex mCfgMutex; ///< Mutex for drain config updates
  eos::common::ThreadPool mThreadPool; ///< Thread pool for drain jobs
  ListPendingT mPending; ///< Queue of pending file systems to be drained
};

EOSMGMNAMESPACE_END
