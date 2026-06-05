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

using AccountingQueuedUpdate = container_accounting::QueuedUpdate;
using TreeSizeRecomputeContext = container_accounting::TreeSizeRecomputeContext;

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
  // Stop the AsyncQueueUpdate thread by queuing the AccountingQueuedUpdate type "Stop"
  mIdTreeInfosToUpdateQueue.emplace(AccountingQueuedUpdate::Type::Stop);
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
  if (id) {
    // Container id 0 is invalid for tree-accounting updates.
    mIdTreeInfosToUpdateQueue.emplace(id, treeAccounting);
  }
}

//------------------------------------------------------------------------------
// Flush queued tree-size accounting updates
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::FlushTreeSizeUpdates(bool is_admin_recompute)
{
  if (mUpdateIntervalSec) {
    auto barrier = std::make_shared<std::promise<void>>();
    auto future = barrier->get_future();
    mIdTreeInfosToUpdateQueue.emplace(barrier);
    // Wait until the queueing thread catches up to this point in the queue.
    future.wait();
  } else {
    AccountingQueuedUpdate update;

    while (mIdTreeInfosToUpdateQueue.try_pop(update)) {
      if (update.mType == AccountingQueuedUpdate::Type::Update) {
        QueueAncestorsForUpdate(update.mId, update.mTreeInfos);
      }
    }
  }

  PropagateUpdatesOnce(is_admin_recompute);
}

//------------------------------------------------------------------------------
// Start a best-effort tree-size recompute window
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::StartTreeSizeRecompute(std::vector<IContainerMD::id_t> ids)
{
  {
    std::lock_guard<std::mutex> lock(mMutexRecompute);

    if (mActiveRecompute) {
      return false;
    }
  }

  auto context = std::make_shared<TreeSizeRecomputeContext>(std::move(ids));
  std::lock_guard<std::mutex> lock(mMutexRecompute);

  if (mActiveRecompute) {
    return false;
  }

  mActiveRecompute = context;
  return true;
}

//------------------------------------------------------------------------------
// Reset dirty marker for a recompute window
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::ResetTreeSizeRecomputeDirty()
{
  std::lock_guard<std::mutex> lock(mMutexRecompute);

  if (mActiveRecompute) {
    mActiveRecompute->mDirty = false;
  }
}

//------------------------------------------------------------------------------
// Check whether accounting touched the recompute window
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::IsTreeSizeRecomputeDirty() const
{
  std::lock_guard<std::mutex> lock(mMutexRecompute);
  return mActiveRecompute && mActiveRecompute->mDirty;
}

//------------------------------------------------------------------------------
// Atomically finish a recompute window if it is still clean
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::TryFinishTreeSizeRecompute()
{
  std::lock_guard<std::mutex> lock(mMutexRecompute);

  if (!mActiveRecompute) {
    return true;
  }

  if (mActiveRecompute->mDirty) {
    return false;
  }

  mActiveRecompute.reset();
  return true;
}

//------------------------------------------------------------------------------
// Abort a recompute window
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AbortTreeSizeRecompute()
{
  std::lock_guard<std::mutex> lock(mMutexRecompute);

  mActiveRecompute.reset();
}

//------------------------------------------------------------------------------
// Get active recompute context
//------------------------------------------------------------------------------
std::shared_ptr<const TreeSizeRecomputeContext>
QuarkContainerAccounting::GetRecomputeContext() const
{
  std::lock_guard<std::mutex> lock(mMutexRecompute);
  return mActiveRecompute;
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
// Queue one delta for all ancestors of the given container
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueAncestorsForUpdate(IContainerMD::id_t id,
                                                  TreeInfos treeInfos)
{
  uint16_t deepness = 0;
  std::vector<IContainerMD::id_t> idsToUpdate;

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

//------------------------------------------------------------------------------
// Merge source update batch into destination update batch
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::MergeBatch(const UpdateT& src, UpdateT& dst)
{
  for (const auto& elem : src.mMap) {
    auto it_map = dst.mMap.find(elem.first);

    if (it_map != dst.mMap.end()) {
      it_map->second += elem.second;
    } else {
      dst.mMap.emplace(elem.first, elem.second);
    }
  }
}

//------------------------------------------------------------------------------
// Mark recompute context dirty if the update is covered by admin recompute
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::MarkDirtyIfInAdminRecomputeContext(IContainerMD::id_t id)
{
  std::lock_guard<std::mutex> lock(mMutexRecompute);

  if (!mActiveRecompute || !mActiveRecompute->Contains(id)) {
    return false;
  }

  mActiveRecompute->mDirty = true;
  return true;
}

//------------------------------------------------------------------------------
// Propagate one accumulated batch
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::PropagateUpdatesOnce(bool is_admin_recompute)
{
  std::lock_guard<std::mutex> propagate_lock(mMutexPropagate);
  auto admin_recompute_context = is_admin_recompute ? GetRecomputeContext() : nullptr;

  {
    // Update the indexes to have the async thread working on the batch to
    // commit and the incoming updates to go to the batch to update
    std::lock_guard<std::mutex> scope_lock(mMutexBatch);
    std::swap(mAccumulateIndx, mCommitIndx);
  }

  auto& batch = mBatch[mCommitIndx];
  UpdateT deferred;

  if (!batch.mMap.empty()) {
    for (auto const& elem : batch.mMap) {
      if (admin_recompute_context) {
        // This admin flush only handles updates that overlap the recomputed
        // subtree. Defer unrelated entries by moving them back to the
        // accumulation batch for a later normal propagation pass.
        if (!admin_recompute_context->Contains(elem.first)) {
          deferred.mMap.emplace(elem.first, elem.second);
          continue;
        }

        // The recompute command wrote absolute tree counters for this subtree,
        // so do not replay an additive delta on top. Mark dirty instead so the
        // admin command retries or reports concurrent modifications.
        MarkDirtyIfInAdminRecomputeContext(elem.first);
        continue;
      }

      try {
        // Normal propagation may race with an active admin recompute. If this
        // update overlaps the recomputed subtree, mark the recompute dirty and
        // skip the additive delta so it cannot corrupt freshly written absolute
        // counters.
        if (MarkDirtyIfInAdminRecomputeContext(elem.first)) {
          continue;
        }

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
    }
  }

  batch.mMap.clear();

  if (!deferred.mMap.empty()) {
    std::lock_guard<std::mutex> scope_lock(mMutexBatch);
    MergeBatch(deferred, mBatch[mAccumulateIndx]);
  }
}

//------------------------------------------------------------------------------
// For each containerId and its associated size modified, this function
// will go up the tree from container to '/' and submit the entire tree for
// modification in the PropagateUpdate thread
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AsyncQueueForUpdate(ThreadAssistant* assistant)
{
  AccountingQueuedUpdate update;

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    mIdTreeInfosToUpdateQueue.wait_pop(update);

    if (update.mType == AccountingQueuedUpdate::Type::Stop) {
      // see ~QuarkContainerAccounting(), we
      // stop this thread
      break;
    }

    if (update.mType == AccountingQueuedUpdate::Type::Barrier) {
      // Wake up FlushTreeSizeUpdates(). By the time this marker is reached, every
      // older queued update has already been folded into the accumulation batch.
      update.mBarrier->set_value();
      continue;
    }

    QueueAncestorsForUpdate(update.mId, update.mTreeInfos);
  }
}

EOSNSNAMESPACE_END
