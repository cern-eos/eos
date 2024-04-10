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
QuarkContainerAccounting::QuarkContainerAccounting(IContainerMDSvc* svc,
    eos::common::RWMutex* ns_mutex, int32_t update_interval)
  : mAccumulateIndx(0), mCommitIndx(1), mShutdown(false),
    mUpdateIntervalSec(update_interval), mContainerMDSvc(svc),
    gNsRwMutex(ns_mutex)
{
  mBatch.resize(2);

  // If update interval is 0 then we disable async updates
  if (mUpdateIntervalSec) {
    mThread.reset(&QuarkContainerAccounting::AssistedPropagateUpdates, this);
  }
}

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
QuarkContainerAccounting::~QuarkContainerAccounting()
{
  mShutdown = true;

  if (mUpdateIntervalSec) {
    mThread.join();
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
  uint16_t deepness = 0;
  std::shared_ptr<IContainerMD> cont;
  std::vector<IContainerMD::id_t> idsToUpdate;
  while ((id > 1) && (deepness < 255)) {
    try {
      cont = mContainerMDSvc->getContainerMD(id);
    } catch (const MDException& e) {
      // TODO (esindril): error message using default logging
      break;
    }
    idsToUpdate.push_back(id);
    id = cont->getParentId();
    ++deepness;
  }

  std::lock_guard<std::mutex> scope_lock(mMutexBatch);
  auto& batch = mBatch[mAccumulateIndx];

  for(auto idToUpdate: idsToUpdate) {
    auto it_map = batch.mMap.find(idToUpdate);

    if (it_map != batch.mMap.end()) {
      it_map->second += dsize;
    } else {
      batch.mMap.emplace(idToUpdate, dsize);
    }
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
// Propagate updates in the hierarchical structure
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::PropagateUpdates(ThreadAssistant* assistant)
{
  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    if (mShutdown) {
      break;
    }

    {
      // Update the indexes to have the async thread working on the batch to
      // commit and the incoming updates to go to the batch to update
      std::lock_guard<std::mutex> scope_lock(mMutexBatch);
      std::swap(mAccumulateIndx, mCommitIndx);
    }

    auto& batch = mBatch[mCommitIndx];

    if(!batch.mMap.empty()){
      std::shared_ptr<IContainerMD> cont;

      for (auto const& elem : batch.mMap) {
        try {
          cont = mContainerMDSvc->getContainerMD(elem.first);
          eos::MDLocking::ContainerWriteLock locker(cont);
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

EOSNSNAMESPACE_END
