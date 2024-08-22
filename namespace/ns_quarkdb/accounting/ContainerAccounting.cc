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

#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include <iostream>
#include <chrono>

EOSNSNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
QuarkContainerAccounting::QuarkContainerAccounting(IContainerMDSvc* svc, int32_t update_interval)
  : mAccumulateIndx(0), mCommitIndx(1),
    mUpdateIntervalSec(update_interval), mContainerMDSvc(svc)
{
  mBatch.resize(2);

  // If update interval is 0 then we disable async updates
  if (mUpdateIntervalSec) {
    mThread.reset(&QuarkContainerAccounting::AssistedPropagateUpdates, this);
    mQueueForUpdateThread.reset(&QuarkContainerAccounting::AssistedQueueForUpdate,this);
  }
}

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
QuarkContainerAccounting::~QuarkContainerAccounting()
{
  // Stop the AsyncQueueUpdate thread by queuing containerID = 0
  mIdSizeToUpdateQueue.emplace(0,0);
  if (mUpdateIntervalSec) {
    mThread.join();
    mQueueForUpdateThread.join();
  }
}

//----------------------------------------------------------------------------
// Notifications about changes in the main view
//----------------------------------------------------------------------------
void
QuarkContainerAccounting::fileMDChanged(IFileMDChangeListener::Event* e)
{
  switch (e->action) {
  // We are only interested in SizeChange events
  case IFileMDChangeListener::SizeChange:
    if (e->file->getContainerId() == 0) {
      // NOTE: This is an ugly hack. The file object has not reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      QueueForUpdate(e->location, e->sizeChange);
    } else {
      QueueForUpdate(e->file->getContainerId(), e->sizeChange);
    }

    break;

  default:
    break;
  }
}

//------------------------------------------------------------------------------
// Add tree
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AddTree(IContainerMD* obj, int64_t dsize)
{
  QueueForUpdate(obj->getId(), dsize);
}

//-------------------------------------------------------------------------------
// Remove tree
//-------------------------------------------------------------------------------
void
QuarkContainerAccounting::RemoveTree(IContainerMD* obj, int64_t dsize)
{
  QueueForUpdate(obj->getId(), -dsize);
}

//------------------------------------------------------------------------------
// Queue file info for update
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueForUpdate(IContainerMD::id_t id, int64_t dsize)
{
  if(id) {
    // The condition to stop the queueing thread is that the id = 0. The minimum container id is 1 and
    // corresponds to "/"
    // We therefore prevent users to queue the container id = 0 for update (which should not happen!)
    mIdSizeToUpdateQueue.emplace(id,dsize);
  }
}

//------------------------------------------------------------------------------
// Propagate updates in the hierarchical structure. Method ran by an
// asynchronous thread.
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AssistedPropagateUpdates(ThreadAssistant& assistant)
noexcept
{
  PropagateUpdates(&assistant);
}

//------------------------------------------------------------------------------
// Submits container and size changes in order to propagate them to the hierarchical structure later on.
// Method ran by an asynchronous thread.
//------------------------------------------------------------------------------
void QuarkContainerAccounting::AssistedQueueForUpdate(ThreadAssistant& assistant) noexcept
{
  AsyncQueueForUpdate(&assistant);
}

//------------------------------------------------------------------------------
// Propagate updates in the hierarchical structure
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::PropagateUpdates(ThreadAssistant* assistant)
{
  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    {
      // Update the indexes to have the async thread working on the batch to
      // commit and the incoming updates to go to the batch to update
      std::lock_guard<std::mutex> scope_lock(mMutexBatch);
      std::swap(mAccumulateIndx, mCommitIndx);
    }

    auto& batch = mBatch[mCommitIndx];

    if(!batch.mMap.empty()){
      for (auto const& elem : batch.mMap) {
        try {
          auto contLock = mContainerMDSvc->getContainerMDWriteLocked(elem.first);
          auto cont = contLock->getUnderlyingPtr();
          cont->updateTreeSize(elem.second);
          mContainerMDSvc->updateStore(cont.get());
        } catch (const MDException& e) {
          // TODO: (esindril) error message using default logging
          continue;
        }
      }
    }

    batch.mMap.clear();

    if (mUpdateIntervalSec) {
      if (assistant) {
        assistant->wait_for(std::chrono::seconds(mUpdateIntervalSec));
      } else {
        std::this_thread::sleep_for(std::chrono::seconds(mUpdateIntervalSec));
      }
    } else {
      break;
    }
  }
}

//------------------------------------------------------------------------------
// For each containerId and its associated size modified, this function
// will go up the tree from container to '/' and submit the entire tree for
// modification in the PropagateUpdate thread
//------------------------------------------------------------------------------
void QuarkContainerAccounting::AsyncQueueForUpdate(ThreadAssistant* assistant)
{
  std::pair<eos::IContainerMD::id_t,int64_t> contIdSize;
  std::vector<IContainerMD::id_t> idsToUpdate;

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    uint16_t deepness = 0;

    mIdSizeToUpdateQueue.wait_pop(contIdSize);
    if(!contIdSize.first) {
      // Container ID = 0 (see ~QuarkContainerAccounting()), we
      // stop this thread
      break;
    }

    IContainerMD::id_t id = contIdSize.first;
    int64_t dsize = contIdSize.second;
    // Go up the tree and give the ids to update to the batch that will
    // be taken by the PropagateUpdate thread
    while ((id > 1) && (deepness < 255)) {
      try {
        idsToUpdate.push_back(id);
        auto contLock = mContainerMDSvc->getContainerMDReadLocked(id);
        auto cont = contLock->getUnderlyingPtr();
        id = cont->getParentId();
        ++deepness;
      } catch (const MDException& e) {
        // TODO (esindril): error message using default logging
        break;
      }
    }

    std::lock_guard<std::mutex> scope_lock(mMutexBatch);
    auto& batch = mBatch[mAccumulateIndx];

    for (auto idToUpdate : idsToUpdate) {
      auto it_map = batch.mMap.find(idToUpdate);

      if (it_map != batch.mMap.end()) {
        it_map->second += dsize;
      } else {
        batch.mMap.emplace(idToUpdate, dsize);
      }
    }

    idsToUpdate.clear();
  }
}

EOSNSNAMESPACE_END
