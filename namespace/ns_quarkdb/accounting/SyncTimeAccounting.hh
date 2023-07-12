//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Synchronous mtime propagation listener
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
#include "namespace/MDException.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "common/AssistedThread.hh"
#include <mutex>
#include <list>
#include <unordered_map>
#include <atomic>
#include "namespace/interface/INamespaceStats.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Synchronous mtime propagation listener
//------------------------------------------------------------------------------
class QuarkSyncTimeAccounting : public IContainerMDChangeListener,
  public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param svc container meta-data service
  //! @param update_interval interval in seconds when updates are propagated
  //----------------------------------------------------------------------------
  QuarkSyncTimeAccounting(IContainerMDSvc* svc,
                          uint32_t update_interval = 5);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkSyncTimeAccounting();

  //----------------------------------------------------------------------------
  //! Delete copy constructor and assignment operator
  //----------------------------------------------------------------------------
  QuarkSyncTimeAccounting(const QuarkSyncTimeAccounting& other) = delete;
  QuarkSyncTimeAccounting& operator=(const QuarkSyncTimeAccounting& other) =
    delete;
  QuarkSyncTimeAccounting(QuarkSyncTimeAccounting&& other) = delete;
  QuarkSyncTimeAccounting& operator=(QuarkSyncTimeAccounting&& other) = delete;

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //!
  //! @param obj container object pointer
  //! @param type action type
  //----------------------------------------------------------------------------
  void containerMDChanged(IContainerMD* obj, Action type);

  //----------------------------------------------------------------------------
  //! Propagate updates in the hierarchical structure. Method ran by the
  //! asynchronous thread.
  //!
  //! @param assistant thread doing the propagation
  //----------------------------------------------------------------------------
  void PropagateUpdates(ThreadAssistant* assistant = nullptr);

  //----------------------------------------------------------------------------
  //! Queue container info for update
  //!
  //! @param obj container id
  //----------------------------------------------------------------------------
  void QueueForUpdate(IContainerMD::id_t id);

  //----------------------------------------------------------------------------
  //! Sets the object that allows to communicate some execution timing statistics
  //!
  //! @param namespaceStats the object that will allow to communicate some execution
  //! timing statistics
  //----------------------------------------------------------------------------
  void setNamespaceStats(INamespaceStats * namespaceStats);

private:

  //----------------------------------------------------------------------------
  //! Propagate updates in the hierarchical structure. Method ran by the
  //! asynchronous thread.
  //!
  //! @param assistant thread doing the propagation
  //----------------------------------------------------------------------------
  void AssistedPropagateUpdates(ThreadAssistant& assistant) noexcept;

  //! Update structure containing a list of the nodes that need an update in
  //! the order that the updates need to be applied and also a map used for
  //! filtering out multiple updates to the same container ID. Try to optimise
  //! the number of updates to a container by keeping only the last one.
  struct UpdateT {
    std::list<IContainerMD::id_t> mLstUpd; ///< Ordered list of updates
    //! Map used for fast search/insert operations
    std::unordered_map<IContainerMD::id_t,
        std::list<IContainerMD::id_t>::iterator > mMap;

    void Clean()
    {
      mLstUpd.clear();
      mMap.clear();
    }
  };

  //! Vector of two elements containing the batch which is currently being
  //! accumulated and the batch which is being committed to the namespace by the
  //! asynchronous thread
  std::vector<UpdateT> mBatch;
  std::mutex mMutexBatch; ///< Mutex protecting access to the updates batch
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index of the batch committing updates
  AssistedThread mThread; ///< Thread updating the namespace
  std::atomic<bool> mShutdown; ///< Flag to shutdown async thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< Container meta-data service
  INamespaceStats * mNamespaceStats;
};

EOSNSNAMESPACE_END
