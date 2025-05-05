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
#include "mgm/balancer/FsBalancerStats.hh"
#include "namespace/interface/IFileMD.hh"

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
    mSpaceName(space_name), mThreshold(10), mTxNumPerNode(2),
    mTxRatePerNode(25), mBalanceStats(space_name),
    mThreadPool(10, 100, 10 , 6, 5, "balance")
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
    mDoConfigUpdate.store(true, std::memory_order_release);
  }

  //----------------------------------------------------------------------------
  //! Loop handling balancing jobs
  //----------------------------------------------------------------------------
  void Balance(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Account started (queued) transfers by reserving the slot on the
  //! corresponding endpoints
  //!
  //! @param src source balancer info
  //! @param dst destination balancer info
  //----------------------------------------------------------------------------
  void TakeTxSlot(const FsBalanceInfo& src, const FsBalanceInfo& dst);

  //----------------------------------------------------------------------------
  //! Account finished transfers by freeing up slot and un-tracking the file
  //! identifier
  //!
  //! @param fid file identifier
  //! @param src source balancer info object
  //! @param dst destnation balancer info object
  //----------------------------------------------------------------------------
  void FreeTxSlot(eos::IFileMD::id_t fid, FsBalanceInfo src, FsBalanceInfo dst);

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif
  AssistedThread mThread; ///< Main balancer thread
  std::string mSpaceName; ///< Name of the space balancer belongs to
  std::atomic<bool> mDoConfigUpdate {true}; ///< Signal a config update
  bool mIsEnabled {true};
  //! Threshold value that represents distance form the average from which
  //! file systems are considered for balancing
  double mThreshold;
  unsigned int mTxNumPerNode; ///< Number of concurrent transfers per node
  unsigned int mTxRatePerNode; ///< Max transfer rate per node MB/s
  FsBalancerStats mBalanceStats; ///< Balancer stats
  eos::common::ThreadPool mThreadPool; ///< Thread pool for balancing jobs
  unsigned int mMaxQueuedJobs {1000}; ///< Max number of queued jobs
  unsigned int mMaxThreadPoolSize {100}; ///< Max number of threads
  std::atomic<uint64_t> mRunningJobs {0}; ///< Number of running/queued jobs
  std::chrono::seconds mUpdInterval {60}; ///< Balance stats update interval

  //----------------------------------------------------------------------------
  //! Update balancer config based on the info registered at the space
  //----------------------------------------------------------------------------
  void ConfigUpdate();

  //----------------------------------------------------------------------------
  //! Get iterator to a random start element in the vector
  //!
  //! @param vect vector object
  //----------------------------------------------------------------------------
  template<typename Vect>
  static typename Vect::iterator GetRandomIter(Vect& vect);

  //----------------------------------------------------------------------------
  //! Get file identifier to balance from the given source file system
  //!
  //! @param src source file system obj
  //! @param set_dsts set of suitable destination file systems
  //! @param dst selected destination file system obj
  //!
  //! @return file identifier or 0ull if no file found
  //----------------------------------------------------------------------------
  eos::IFileMD::id_t
  GetFileToBalance(const FsBalanceInfo& src,
                   const std::set<FsBalanceInfo>& set_dsts, FsBalanceInfo& dst);
};

//------------------------------------------------------------------------------
//! Get iterator to a random start element in the vector
//----------------------------------------------------------------------------
template<typename Vect>
typename Vect::iterator
FsBalancer::GetRandomIter(Vect& vect)
{
  typename Vect::iterator iter = vect.begin();
  const size_t size = vect.size();

  if (!size) {
    return iter;
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(1, size);
  size_t index = dist(gen);

  while (index > 1) {
    --index;
    ++iter;
  }

  return iter;
}

EOSMGMNAMESPACE_END
