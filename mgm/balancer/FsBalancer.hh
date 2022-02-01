//------------------------------------------------------------------------------
//! @file FsBalancer.hh
//! @author Elvin Sindrilaru <esindril@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "common/ThreadPool.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FsBalancer taking care of balancing the data between file systems
//! inside groups for a given space
//------------------------------------------------------------------------------
class FsBalancer: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param space_name space that balancer it attached to
  //----------------------------------------------------------------------------
  FsBalancer(const std::string& space_name):
    mSpaceName(space_name), mThreshold(5), mTxNumPerNode(2), mTxRatePerNode(25)
  {
    mThread.reset(&FsBalancer::Balance, this);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FsBalancer()
  {
    mThread.join();
  }

  //----------------------------------------------------------------------------
  //! Set max size of the thread pool used for balancing
  //----------------------------------------------------------------------------
  inline void SetMaxThreadPoolSize(unsigned int max)
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
  //! Signal balancer to perform a configuration update
  //----------------------------------------------------------------------------
  inline void SignalConfigUpdate()
  {
    mDoConfigUpdate = true;
  }

  //----------------------------------------------------------------------------
  //! Loop handling balancing jobs
  //----------------------------------------------------------------------------
  void Balance(ThreadAssistant& assistant) noexcept;

private:
  AssistedThread mThread;
  std::string mSpaceName; ///< Name of the space balancer belongs to
  std::atomic<bool> mDoConfigUpdate {true}; ///< Signal a config update
  bool mIsEnabled {true};
  //! Threshold value that represents distance form the average from which
  //! file systems are considered for balancing
  double mThreshold;
  unsigned int mTxNumPerNode; ///< Number of concurrent transfers per node
  unsigned int mTxRatePerNode; ///< Max transfer rate per node MB/s
  eos::common::ThreadPool mThreadPool; ///< Thread pool for balancing jobs

  //----------------------------------------------------------------------------
  //! Update balancer config based on the info registered at the space
  //----------------------------------------------------------------------------
  void ConfigUpdate();

};

EOSMGMNAMESPACE_END
