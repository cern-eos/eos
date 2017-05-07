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
ContainerAccounting::ContainerAccounting(IContainerMDSvc* svc,
    eos::common::RWMutex* ns_mutex,
    int32_t update_interval)
  : mAccumulateIndx(0), mCommitIndx(1), mShutdown(false),
    mUpdateIntervalSec(update_interval), mContainerMDSvc(svc),
    gNsRwMutex(ns_mutex)
{
  mBatch.resize(2);
  mThread = std::thread(&ContainerAccounting::PropagateUpdates, this);
}

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
ContainerAccounting::~ContainerAccounting()
{
  mShutdown = true;
  mThread.join();
}

//----------------------------------------------------------------------------
// Notifications about changes in the main view
//----------------------------------------------------------------------------
void
ContainerAccounting::fileMDChanged(IFileMDChangeListener::Event* e)
{
  switch (e->action) {
  // We are only interested in SizeChange events
  case IFileMDChangeListener::SizeChange:
    if (e->file->getContainerId() == 0) {
      // NOTE: This is an ugly hack. The file object has not reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      QueueForUpdate(e->location, e->sizeChange, OpType::FILE);
    } else {
      QueueForUpdate(e->file->getContainerId(), e->sizeChange, OpType::FILE);
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
ContainerAccounting::AddTree(IContainerMD* obj, int64_t dsize)
{
  QueueForUpdate(obj->getId(), dsize, OpType::TREE);
}

//-------------------------------------------------------------------------------
// Remove tree
//-------------------------------------------------------------------------------
void
ContainerAccounting::RemoveTree(IContainerMD* obj, int64_t dsize)
{
  QueueForUpdate(obj->getId(), dsize, OpType::TREE);
}


//------------------------------------------------------------------------------
// Queue file info for update
//------------------------------------------------------------------------------
void
ContainerAccounting::QueueForUpdate(eos::IContainerMD::id_t id, int64_t dsize,
                                    OpType op)
{
  uint16_t deepness = 0;
  std::lock_guard<std::mutex> scope_lock(mMutexBatch);
  auto& batch = mBatch[mAccumulateIndx];

  // If this is a tree operation and we already have an update for the root of
  // of the mv then we need to update the current tree size taking into
  // account the previous add/remove operation.
  if (op == OpType::TREE) {
    auto it_map = batch.mMap.find(id);

    if (it_map != batch.mMap.end()) {
      dsize += it_map->second;
    }
  }

  std::shared_ptr<IContainerMD> cont;

  while ((id > 1) && (deepness < 255)) {
    try {
      cont = mContainerMDSvc->getContainerMD(id);
    } catch (eos::MDException& e) {
      // TODO (esindril): error message using default logging
      break;
    }

    auto it_map = batch.mMap.find(id);

    if (it_map != batch.mMap.end()) {
      it_map->second += dsize;
    } else {
      batch.mMap.emplace(id, dsize);
    }

    id = cont->getParentId();
    deepness++;
  }
}

//------------------------------------------------------------------------------
// Propagate updates in the hierarchical structure. Method ran by the
// asynchronous thread.
//------------------------------------------------------------------------------
void
ContainerAccounting::PropagateUpdates()
{
  while (true) {
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
    {
      // Need to lock the namespace
      eos::common::RWMutexWriteLock wr_lock(*gNsRwMutex);
      std::shared_ptr<eos::IContainerMD> cont;

      for (auto const& elem : batch.mMap) {
        try {
          cont = mContainerMDSvc->getContainerMD(elem.first);
          cont->addTreeSize(elem.second);
          mContainerMDSvc->updateStore(cont.get());
        } catch (eos::MDException& e) {
          // TODO: (esindril) error message using default logging
          continue;
        }
      }
    }
    batch.mMap.clear();
    std::this_thread::sleep_for(std::chrono::seconds(mUpdateIntervalSec));
  }
}

EOSNSNAMESPACE_END
