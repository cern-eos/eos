//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Container subtree accounting
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
#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "common/RWMutex.hh"
#include <mutex>
#include <thread>
#include <vector>
#include <utility>
#include <unordered_map>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Container subtree accounting listener
//------------------------------------------------------------------------------
class ContainerAccounting : public IFileMDChangeListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param svc container metadata service
  //! @param ns_mutex global (MGM) namespace mutex
  //! @param update_interval interval in seconds when updates are propagated
  //----------------------------------------------------------------------------
  ContainerAccounting(IContainerMDSvc* svc, eos::common::RWMutex* ns_mutex,
                      int32_t update_interval = 5);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ContainerAccounting();

  //----------------------------------------------------------------------------
  //! Delete copy/move constructor and assignment operators
  //----------------------------------------------------------------------------
  ContainerAccounting(const ContainerAccounting& other) = delete;
  ContainerAccounting& operator=(const ContainerAccounting& other) = delete;
  ContainerAccounting(ContainerAccounting&& other) = delete;
  ContainerAccounting& operator=(ContainerAccounting&& other) = delete;

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e);

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  virtual void
  fileMDRead(IFileMD* obj) {}

  //----------------------------------------------------------------------------
  //! Recheck the current file object and make any modifications necessary so
  //! that the information is consistent in the back-end KV store.
  //!
  //! @param file file object to be checked
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool
  fileMDCheck(IFileMD* file)
  {
    return true;
  }

  //----------------------------------------------------------------------------
  //! Add tree
  //!
  //! @param obj container where tree si added
  //! @param dsize size of the subtree adeded
  //----------------------------------------------------------------------------
  void AddTree(IContainerMD* obj, int64_t dsize);

  //----------------------------------------------------------------------------
  //! Remove tree
  //!
  //! @param obj container where tree si added
  //! @param dsize size of the subtree adeded
  //----------------------------------------------------------------------------
  void RemoveTree(IContainerMD* obj, int64_t dsize);

  //----------------------------------------------------------------------------
  //! Queue info for update
  //!
  //! @param pid container id
  //! @param dsize size change
  //----------------------------------------------------------------------------
  void QueueForUpdate(IContainerMD::id_t pid, int64_t dsize);

  //----------------------------------------------------------------------------
  //! Propagate updates in the hierarchical structure. Method ran by the
  //! asynchronous thread.
  //----------------------------------------------------------------------------
  void PropagateUpdates();

private:

  //! Update structure containin the nodes that need an update. We try to
  //! optimise the number of updates to the backend by computing the final
  //! size deltas from a number of individual updates.
  struct UpdateT {
    std::unordered_map<IContainerMD::id_t, int64_t> mMap; ///< Map updates
  };

  //! Vector of two elements containing the batch which is currently being
  //! accumulated and the batch which is being commited to the namespace by the
  //! asynchronous thread
  std::vector<UpdateT> mBatch;
  std::mutex mMutexBatch; ///< Mutex protecting access to the updates batch
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index o the batch committing updates
  std::thread mThread; ///< Thread updating the namespace
  std::atomic<bool> mShutdown; ///< Flag to shutdown the async thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< container MD service
  eos::common::RWMutex* gNsRwMutex; ///< Global (MGM) name RW mutex
};

EOSNSNAMESPACE_END
