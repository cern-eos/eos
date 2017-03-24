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
#include "common/RWMutex.hh"
#include <mutex>
#include <thread>
#include <list>
#include <map>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Synchronous mtime propagation listener
//------------------------------------------------------------------------------
class SyncTimeAccounting : public IContainerMDChangeListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param svc container meta-data service
  //! @param ns_mutex global namespace view mutex
  //----------------------------------------------------------------------------
  SyncTimeAccounting(IContainerMDSvc* svc, eos::common::RWMutex* ns_mutex);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~SyncTimeAccounting();

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //!
  //! @param obj container object pointer
  //! @param type action type
  //----------------------------------------------------------------------------
  void containerMDChanged(IContainerMD* obj, Action type);

private:
  //----------------------------------------------------------------------------
  //! Queue container info for update
  //!
  //! @param obj container object
  //----------------------------------------------------------------------------
  void QueueForUpdate(IContainerMD* obj);

  //----------------------------------------------------------------------------
  //! Propagate updates in the hierarchical structure. Method ran by the
  //! asynchronous thread.
  //----------------------------------------------------------------------------
  void PropagateUpdates();

  //----------------------------------------------------------------------------
  //! Get hierarchy level of the current container. Root is 0.
  //!
  //! @param obj container object
  //!
  //! @return level in the hierarchy
  //----------------------------------------------------------------------------
  uint16_t GetLevel(IContainerMD* obj);

  //! Node information
  struct NodeInfoT {
    //! Constructor
    NodeInfoT(IContainerMD::id_t id, uint16_t level):
      mId(id), mLevel(level)
    {}

    IContainerMD::id_t mId; ///< Continer id
    uint16_t mLevel; ///< Level in the hierarchy of the container
  };

  //! Update structure containing a list of the nodes that need an update in
  //! the order that the updates need to be applied and also a map used for
  //! filtering out multiple updates to the same container ID. Try to optimise
  //! the number of updates to a container by keeping only the last one.
  struct UpdateT {
    std::list<NodeInfoT> mLstUpd; ///< Ordered list of updates
    //! Map used for fast search operations
    std::map<IContainerMD::id_t, std::list<NodeInfoT>::iterator > mMap;

    void Clean()
    {
      mLstUpd.clear();
      mMap.clear();
    }
  };

  //! Vector of two elements containing the batch which is currently begin
  //! accumulated and the batch which is being commited to the namespace by the
  //! asynchronous thread
  std::vector<UpdateT> mBatch;
  uint8_t mAccumutateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index of the batch committing updates
  std::mutex mMutexBatch; ///< Mutex protecting acces to the updates batch
  std::thread mThread; ///< Thread updating the namespace
  IContainerMDSvc* mContainerMDSvc; ///< Container meta-data service
  eos::common::RWMutex* gNsRwMutex; ///< Global(MGM) namespace RW mutex
  bool mShutdown; ///< Flag to shutdown async thread
};

EOSNSNAMESPACE_END
