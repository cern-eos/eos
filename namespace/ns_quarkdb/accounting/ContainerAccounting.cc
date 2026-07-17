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
  mIdTreeInfosToUpdateQueue.emplace(0,TreeInfos{0,0,0});
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
    // e->file can be nullptr here
    if ((e->file && e->file->getContainerId() == 0) || !e->file) {
      // NOTE: This is an ugly hack. The file object has not reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      QueueForUpdate(e->location, e->treeChange);
    } else {
      QueueForUpdate(e->file->getContainerId(), e->treeChange);
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
QuarkContainerAccounting::AddTree(IContainerMD* obj, TreeInfos treeAccounting)
{
  QueueForUpdate(obj->getId(), treeAccounting);
}

//-------------------------------------------------------------------------------
// Remove tree
//-------------------------------------------------------------------------------
void
QuarkContainerAccounting::RemoveTree(IContainerMD* obj, TreeInfos treeAccounting)
{
  QueueForUpdate(obj->getId(), -treeAccounting);
}

//------------------------------------------------------------------------------
// Queue file info for update
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueForUpdate(IContainerMD::id_t id, TreeInfos treeAccounting)
{
  if (id && (id != sFlushBarrierId)) {
    // The condition to stop the queueing thread is that the id = 0. The minimum container
    // id is 1 and corresponds to "/" We therefore prevent users to queue the container id
    // = 0 for update (which should not happen!) The id = sFlushBarrierId is reserved as
    // the Flush() barrier sentinel.
    mIdTreeInfosToUpdateQueue.emplace(id,treeAccounting);
  }
}

//------------------------------------------------------------------------------
// Synchronously apply all tree info updates queued so far
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::Flush()
{
  // Only one flusher at a time: the barrier state below is a single slot
  std::lock_guard<std::mutex> drain_lock(mDrainMutex);

  if (mUpdateIntervalSec == 0) {
    // Async updates are disabled, no thread consumes the queue: drain it in
    // the calling thread
    std::pair<IContainerMD::id_t, TreeInfos> contIdTreeInfos;

    while (mIdTreeInfosToUpdateQueue.try_pop(contIdTreeInfos)) {
      if (contIdTreeInfos.first && (contIdTreeInfos.first != sFlushBarrierId)) {
        AccumulateUpdate(contIdTreeInfos.first, contIdTreeInfos.second);
      }
    }
  } else if (!WaitForBarrier()) {
    // The queueing thread is gone (shutting down): whatever is still in the
    // queue will never be accumulated, so those deltas are lost. Persisting
    // the batch on top of that would write values missing them, which nothing
    // would ever repair. Drop the batch instead, as the destructor does.
    return false;
  }

  PropagateUpdatesOnce();
  return true;
}

//------------------------------------------------------------------------------
// Push a barrier sentinel and wait for the queueing thread to acknowledge it
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::WaitForBarrier()
{
  std::unique_lock<std::mutex> barrier_lock(mBarrierMutex);

  if (mQueueThreadStopped) {
    return false;
  }

  // The queue is a strict FIFO with the AsyncQueueForUpdate thread as only
  // consumer: once the barrier is acknowledged, everything enqueued before it
  // has been accumulated into the batch
  mBarrierReached = false;
  mIdTreeInfosToUpdateQueue.emplace(sFlushBarrierId, TreeInfos{0, 0, 0});
  mBarrierCv.wait(barrier_lock,
                  [this]() { return mBarrierReached || mQueueThreadStopped; });
  return mBarrierReached;
}

//------------------------------------------------------------------------------
// Mark the queueing thread as stopped and release a Flush() waiting for a
// barrier that will never be acknowledged
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::MarkQueueThreadStopped()
{
  {
    std::lock_guard<std::mutex> barrier_lock(mBarrierMutex);
    mQueueThreadStopped = true;
  }
  mBarrierCv.notify_all();
}

//------------------------------------------------------------------------------
// Start recording the ids of the containers under recompute tree info deltas
// get applied to
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::StartRecordingUpdatedContIds(ContIdSet cont_ids_under_recompute)
{
  // Taking mFlushMutex guarantees no propagation cycle is mid-apply while the
  // containers under recompute change: cycles starting after this returns see
  // the new set, so every delta they apply to one of them is recorded
  std::lock_guard<std::mutex> flush_lock(mFlushMutex);
  mContIdsUnderRecompute = std::move(cont_ids_under_recompute);
  // Move-assigning an empty set releases the buckets, unlike clear()
  mUpdatedContIds = {};
}

//------------------------------------------------------------------------------
// Stop recording updated container ids and drop the recorded set
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::StopRecordingUpdatedContIds()
{
  std::lock_guard<std::mutex> flush_lock(mFlushMutex);
  // An empty set of containers under recompute means recording is off
  mContIdsUnderRecompute = {};
  mUpdatedContIds = {};
}

//------------------------------------------------------------------------------
// Return the set of container ids deltas were applied to since recording
// started (or since the previous call) and clear it
//------------------------------------------------------------------------------
QuarkContainerAccounting::ContIdSet
QuarkContainerAccounting::TakeUpdatedContIds()
{
  // Blocks until any propagation cycle in flight is done, so the returned set
  // never cuts one in half. Only the recompute calls this, once per pass.
  std::lock_guard<std::mutex> flush_lock(mFlushMutex);
  // Moves the set out (bucket storage is stolen, no element copy) and leaves
  // the member empty
  return std::exchange(mUpdatedContIds, {});
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
    PropagateUpdatesOnce();

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
// Perform one propagation cycle: swap the batches and apply the committed
// deltas to the containers
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::PropagateUpdatesOnce()
{
  std::lock_guard<std::mutex> flush_lock(mFlushMutex);
  {
    // Update the indexes to have the async thread working on the batch to
    // commit and the incoming updates to go to the batch to update
    std::lock_guard<std::mutex> scope_lock(mMutexBatch);
    std::swap(mAccumulateIndx, mCommitIndx);
  }

  auto& batch = mBatch[mCommitIndx];

  // mContIdsUnderRecompute and mUpdatedContIds are both guarded by mFlushMutex,
  // held for the whole cycle, so the recording cannot be reconfigured mid-apply
  const bool recording = !mContIdsUnderRecompute.empty();

  for (auto const& elem : batch.mMap) {
    try {
      auto cont = mContainerMDSvc->getContainerMD(elem.first);
      eos::MDLocking::ContainerWriteLock contLock(cont.get());
      cont->updateTreeSize(elem.second.dsize);
      cont->updateTreeFiles(elem.second.dtreefiles);
      cont->updateTreeContainers(elem.second.dtreecontainers);
      mContainerMDSvc->updateStore(cont.get());
    } catch (const MDException& e) {
      // TODO: (esindril) error message using default logging
      continue;
    }

    // This batch holds deltas originating from the whole namespace, while only
    // a container the recompute rewrites with an absolute value can be
    // corrupted by one: the others are dropped here rather than collected and
    // discarded by the caller. Recorded only once durably applied: an id must
    // not be visible to TakeUpdatedContIds() before its delta is reflected in
    // the namespace.
    if (recording && mContIdsUnderRecompute.count(elem.first)) {
      mUpdatedContIds.insert(elem.first);
    }
  }

  batch.mMap.clear();
}

//------------------------------------------------------------------------------
// For each containerId and its associated size modified, this function
// will go up the tree from container to '/' and submit the entire tree for
// modification in the PropagateUpdate thread
//------------------------------------------------------------------------------
void QuarkContainerAccounting::AsyncQueueForUpdate(ThreadAssistant* assistant)
{
  std::pair<eos::IContainerMD::id_t,TreeInfos> contIdTreeInfos;

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    mIdTreeInfosToUpdateQueue.wait_pop(contIdTreeInfos);
    if(!contIdTreeInfos.first) {
      // Container ID = 0 (see ~QuarkContainerAccounting()), we
      // stop this thread
      break;
    }

    if (contIdTreeInfos.first == sFlushBarrierId) {
      // Flush() barrier: everything enqueued before it has been accumulated
      {
        std::lock_guard<std::mutex> barrier_lock(mBarrierMutex);
        mBarrierReached = true;
      }
      mBarrierCv.notify_all();
      continue;
    }

    AccumulateUpdate(contIdTreeInfos.first, contIdTreeInfos.second);
  }

  // No consumer left on the queue from now on, so a barrier would never be
  // acknowledged: unblock any Flush() waiting for one
  MarkQueueThreadStopped();
}

//------------------------------------------------------------------------------
// Walk up the parent chain of the given container and accumulate the tree
// info delta into the batch for the container and all its ancestors
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AccumulateUpdate(IContainerMD::id_t id,
                                           const TreeInfos& treeInfos)
{
  std::vector<IContainerMD::id_t> idsToUpdate;
  uint16_t deepness = 0;

  // Go up the tree and give the ids to update to the batch that will
  // be taken by the PropagateUpdate thread
  while ((id > 1) && (deepness < 255)) {
    try {
      idsToUpdate.push_back(id);
      auto cont = mContainerMDSvc->getContainerMD(id);
      // One operation, no need to lock the container
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
      it_map->second += treeInfos;
    } else {
      batch.mMap.emplace(idToUpdate, treeInfos);
    }
  }
}

EOSNSNAMESPACE_END
