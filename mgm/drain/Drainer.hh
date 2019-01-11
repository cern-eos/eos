//------------------------------------------------------------------------------
//! @file Drainer.hh
//! @author Andrea Manzi - CERN
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
#include "common/Logging.hh"
#include "common/ThreadPool.hh"
#include "common/AssistedThread.hh"
#include "mgm/drain/DrainFs.hh"

EOSMGMNAMESPACE_BEGIN

//! Forward declaration
class DrainTransferJob;
class TableFormatterBase;
class FileSystem;

//------------------------------------------------------------------------------
//! @brief Class running the centralized draining
//------------------------------------------------------------------------------
class Drainer: public eos::common::LogId
{
public:

  //! Map node to map of draining file systems and their associated futures
  typedef
  std::map<std::string, std::set<std::shared_ptr<eos::mgm::DrainFs>>> DrainMap;

  //----------------------------------------------------------------------------
  // Service thread static startup function
  //----------------------------------------------------------------------------
  static void* StaticDrainer(void*);

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
  //! Stop running thread
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

  // @todo (esindril): to review

  //----------------------------------------------------------------------------
  //! Get draining status (global or specific to a fsid)
  //!
  //! @param env
  //! @param out
  //! @param err
  //----------------------------------------------------------------------------
  bool GetDrainStatus(unsigned int fsId, XrdOucString&, XrdOucString&);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  unsigned int GetSpaceConf(const std::string& space);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void PrintTable(TableFormatterBase&, std::string, DrainFs* fs);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  void PrintJobsTable(TableFormatterBase&, DrainTransferJob*);

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

  AssistedThread mThread; ///< Thread updating the drain configuration
  //! Contains per space the max allowed fs draining per node
  std::map<std::string, int> mCfgMap;
  DrainMap mDrainFs; ///< Map of nodes to file systems draining
  XrdSysMutex mDrainMutex; ///< Mutex protecting the drain map
  XrdSysMutex mCfgMutex; ///< Mutex for drain config updates
  eos::common::ThreadPool mThreadPool; ///< Thread pool for drain jobs
  ListPendingT mPending; ///< Queue of pending jobs
};

EOSMGMNAMESPACE_END
