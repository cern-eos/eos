//------------------------------------------------------------------------------
//! file BalancerGroup.hh
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
#include <thread>
#include "mgm/Namespace.hh"
#include "mgm/FileSystem.hh"
#include "common/Logging.hh"
#include "mgm/balancer/BalancerJob.hh"
#include "common/ThreadPool.hh"
#include <chrono>

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//! @brief Class implementing the balancing of a group
//------------------------------------------------------------------------------
class BalancerGroup: public eos::common::LogId
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param groupName groupName
  //  @param spaceName spaceName
  //----------------------------------------------------------------------------
  BalancerGroup(const std::string& groupName, const std::string& spaceName)
  {
    mGroup = groupName;
    mSpace = spaceName;
    mThread = std::thread(&BalancerGroup::Balance, this);
  }

  BalancerGroup() = delete;
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~BalancerGroup();

  //----------------------------------------------------------------------------
  //! Stop balancing the group
  //---------------------------------------------------------------------------
  inline void BalancerGroupStop() {
    mBalanceStop = true;
  }
  //----------------------------------------------------------------------------
  // Start balancing the group
  //-----------------------------------------------------------------------
  inline void BalancerGroupStart() {
    mBalanceStop = false;
  }
  //----------------------------------------------------------------------------
  // Check if the balancer is running
  // ---------------------------------------------------------------------------
  inline bool const isBalancerGroupRunning() const
  {
    return !mBalanceStop;
  }
  //---------------------------------------------------------------------------
  //! Get the group name
  //---------------------------------------------------------------------------
  inline const std::string GetGroupName() const
  {
    return mGroup;
  }

  //---------------------------------------------------------------------------
  //! Get the space name
  //---------------------------------------------------------------------------
  inline const std::string GetSpanceName() const
  {
    return mSpace;
  }


private:
  //----------------------------------------------------------------------------
  // Thread loop implementing the Balance 
  //----------------------------------------------------------------------------
  void* Balance();

  //----------------------------------------------------------------------------
  // Thread pool
  //----------------------------------------------------------------------------
  eos::common::ThreadPool mThreadPool;

  //----------------------------------------------------------------------------
  //! Select source file system 
  //!
  //! @return if successful then source file system, othewise 0
  //---------------------------------------------------------------------------- 
  eos::common::FileSystem::fsid_t SelectSourceFS();

  //----------------------------------------------------------------------------
  //! Select a bunch of files to balance from the fs
  //!
  //! @return set of file to balance
  std::set<eos::common::FileId::fileid_t> SelectFilesToBalance(eos::common::FileSystem::fsid_t);


  uint64_t CollectBalanceJobs(eos::common::FileSystem::fsid_t, const std::set<eos::common::FileId::fileid_t>&);
  //----------------------------------------------------------------------------
  //! Set initial balancer counters and status
  //----------------------------------------------------------------------------
  void SetInitialCounters();
  
  void GetSpaceConfiguration();
 
  uint64_t CollectBalancingJobs(); 

  std::thread mThread; ///< Thread supervisioning the group balancing

  std::string mSpace; ///< Space where fs resides
  std::string mGroup; ///< Group where fs resided
  //! Collection of balancer jobs to run
  std::vector<shared_ptr<BalancerJob>> mJobsPending;
  //! Collection of failed balancer jobs
  std::vector<shared_ptr<BalancerJob>> mJobsFailed;
  //! Collection of running balancer jobs
  std::map<std::shared_ptr<BalancerJob>, std::future<void>> mJobsRunning;
  bool mBalanceStop = false; ///< Flag to cancel an ongoing draining
  unsigned int maxParallelJobs = 10; ///< Max number of parallel balancer jobs
  unsigned int filesToBalance = 100; ///<Max Files to balance per each iteration //to parametrize
};

EOSMGMNAMESPACE_END
