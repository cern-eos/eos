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
#include "common/Logging.hh"
#include "namespace/MDLocking.hh"
#include "namespace/interface/ContainerIterators.hh"
#include <algorithm>
#include <chrono>
#include <iostream>

EOSNSNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
QuarkContainerAccounting::QuarkContainerAccounting(IContainerMDSvc* cont_svc,
                                                   IFileMDSvc* file_svc,
                                                   int32_t update_interval)
    : mAccumulateIndx(0)
    , mCommitIndx(1)
    , mUpdateIntervalSec(update_interval)
    , mContainerMDSvc(cont_svc)
    , mFileMDSvc(file_svc)
{
  mBatch.resize(2);

  // If update interval is 0 then we disable async updates
  if (mUpdateIntervalSec) {
    mThread.reset(&QuarkContainerAccounting::AssistedPropagateUpdates, this);
    mQueueForUpdateThread.reset(&QuarkContainerAccounting::AssistedQueueForUpdate, this);
    mDirtyRecomputeThread.reset(&QuarkContainerAccounting::AssistedRecomputeDirty, this);
  }
}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
QuarkContainerAccounting::~QuarkContainerAccounting()
{
  // Stop the AsyncQueueUpdate thread by queuing containerID = 0
  mIdTreeInfosToUpdateQueue.emplace(0, TreeInfos{0, 0, 0});
  if (mUpdateIntervalSec) {
    mThread.join();
    mQueueForUpdateThread.join();
  }

  // Request termination before notifying, so the CV-bound thread wakes and
  // immediately sees the flag (notifying first would let it go back to sleep).
  mDirtyRecomputeThread.stop();
  {
    std::lock_guard<std::mutex> lk(mDirtyRecomputeMutex);
    mDirtyRecomputeCV.notify_all();
  }
  mDirtyRecomputeThread.join();
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
    // The condition to stop the queueing thread is that the id = 0. The minimum container id is 1 and
    // corresponds to "/"
    // We therefore prevent users to queue the container id = 0 for update (which should not happen!)
    mIdTreeInfosToUpdateQueue.emplace(id, treeAccounting);
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

    if (!batch.mMap.empty()) {
      // Drop fenced entries' deltas, but record their ids in the dirty
      // journal. Done under a single write lock so the partition and the
      // journal insert can't race the fence state.
      {
        eos::common::RWMutexWriteLock fenceLock(mFenceMutex);

        if (!mFencedIds.empty()) {
          for (auto it = batch.mMap.begin(); it != batch.mMap.end();) {
            if (mFencedIds.count(it->first)) {
              mDirtyJournal.insert(it->first);
              it = batch.mMap.erase(it);
            } else {
              ++it;
            }
          }
        }
      }

      // Apply unfenced entries normally
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
      }
    }

    batch.mMap.clear();

    // Signal drain waiters that a propagation cycle completed
    mPropagationCycleCount.fetch_add(1);
    {
      std::lock_guard<std::mutex> lk(mDrainCompleteMutex);
      mDrainCompleteCV.notify_all();
    }

    if (mUpdateIntervalSec) {
      if (mDrainRequested.load()) {
        // Skip sleep — drain is waiting for fast cycle completion
      } else if (assistant) {
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
  std::pair<eos::IContainerMD::id_t, TreeInfos> contIdTreeInfos;
  std::vector<IContainerMD::id_t> idsToUpdate;

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    uint16_t deepness = 0;

    mIdTreeInfosToUpdateQueue.wait_pop(contIdTreeInfos);
    if (!contIdTreeInfos.first) {
      // Container ID = 0 (see ~QuarkContainerAccounting()), we
      // stop this thread
      break;
    }

    IContainerMD::id_t id = contIdTreeInfos.first;
    // Go up the tree and give the ids to update to the batch that will
    // be taken by the PropagateUpdate thread
    while ((id > 1) && (deepness < kMaxAncestorWalkDepth)) {
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
        it_map->second += contIdTreeInfos.second;
      } else {
        batch.mMap.emplace(idToUpdate, contIdTreeInfos.second);
      }
    }

    idsToUpdate.clear();
  }
}

//------------------------------------------------------------------------------
// Run a recompute_tree_size BFS over bfsIds with the fence/drain protocol
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::RecomputeSubtreeWithFencing(
    const std::unordered_set<IContainerMD::id_t>& bfsIds,
    const std::function<void()>& runBfs)
{
  // Serialise recomputes: two concurrent ones would clobber each other's
  // fence state. The claim is released by unfence().
  bool expected = false;

  if (!mRecomputeInFlight.compare_exchange_strong(expected, true)) {
    return false;
  }

  fenceContainers(bfsIds);

  try {
    // Drop in-flight deltas that predate the absolute values runBfs is
    // about to set.
    drainFencedDeltas();
    discardJournal();

    runBfs();

    // Capture deltas that arrived during runBfs, install them as the
    // narrowed fence, and hand to the async recompute thread.
    drainFencedDeltas();
    auto dirtyIds = collectAndFenceDirty();

    if (!dirtyIds.empty()) {
      scheduleRecompute(std::move(dirtyIds));
    } else {
      unfence();
    }
  } catch (...) {
    // Don't leak the in-flight claim or fence on a runBfs exception.
    unfence();
    throw;
  }

  return true;
}

//------------------------------------------------------------------------------
// Fence a set of container IDs
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::fenceContainers(
    const std::unordered_set<IContainerMD::id_t>& ids)
{
  eos::common::RWMutexWriteLock lock(mFenceMutex);
  mFencedIds = ids;
  mDirtyJournal.clear();
}

//------------------------------------------------------------------------------
// Drain all in-flight deltas for fenced containers
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::drainFencedDeltas()
{
  // Wait for the concurrent queue to drain (all items picked up by
  // AsyncQueueForUpdate)
  while (!mIdTreeInfosToUpdateQueue.empty()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Request immediate propagation cycles (skip the sleep interval)
  mDrainRequested.store(true);

  // Wait for two full propagation cycles to ensure:
  // 1) Items that were in the accumulate batch are swapped to commit and processed
  // 2) Items that AsyncQueueForUpdate put into the new accumulate batch after
  //    the first swap are also processed
  uint64_t target = mPropagationCycleCount.load() + 2;
  {
    std::unique_lock<std::mutex> lk(mDrainCompleteMutex);
    mDrainCompleteCV.wait(
        lk, [this, target]() { return mPropagationCycleCount.load() >= target; });
  }

  mDrainRequested.store(false);
}

//------------------------------------------------------------------------------
// Discard all deltas in the recompute journal
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::discardJournal()
{
  eos::common::RWMutexWriteLock lock(mFenceMutex);
  mDirtyJournal.clear();
}

//------------------------------------------------------------------------------
// Atomically collect dirty IDs and install them as the new fence
//------------------------------------------------------------------------------
std::unordered_set<IContainerMD::id_t>
QuarkContainerAccounting::collectAndFenceDirty()
{
  eos::common::RWMutexWriteLock lock(mFenceMutex);
  std::unordered_set<IContainerMD::id_t> result;
  result.swap(mDirtyJournal);
  mFencedIds = result;
  return result;
}

//------------------------------------------------------------------------------
// Clear fenced set and discard journal, resuming normal propagation
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::unfence()
{
  {
    eos::common::RWMutexWriteLock lock(mFenceMutex);
    mFencedIds.clear();
    mDirtyJournal.clear();
  }
  // Release the in-flight claim regardless of which path led here (sync
  // no-dirty branch, async tail, or exception cleanup).
  mRecomputeInFlight.store(false);
}

//------------------------------------------------------------------------------
// Schedule async recompute of dirty containers
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::scheduleRecompute(std::unordered_set<IContainerMD::id_t>&& ids)
{
  if (ids.empty()) {
    return;
  }

  std::lock_guard<std::mutex> lk(mDirtyRecomputeMutex);
  mPendingDirtyRecompute.merge(std::move(ids));
  mDirtyRecomputeCV.notify_one();
}

//------------------------------------------------------------------------------
// Async dirty recompute thread
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AssistedRecomputeDirty(ThreadAssistant& assistant) noexcept
{
  while (!assistant.terminationRequested()) {
    std::unordered_set<IContainerMD::id_t> batch;
    {
      std::unique_lock<std::mutex> lk(mDirtyRecomputeMutex);
      mDirtyRecomputeCV.wait(lk, [this, &assistant]() {
        return !mPendingDirtyRecompute.empty() || assistant.terminationRequested();
      });

      if (assistant.terminationRequested()) {
        break;
      }

      std::swap(batch, mPendingDirtyRecompute);
    }

    // Shrink-based convergence: keep iterating while the dirty set is
    // making progress; bail after kMaxStallIterations consecutive
    // non-shrinking passes to bound work under sustained write load.
    static constexpr int kMaxStallIterations = 5;
    int stalls = 0;
    size_t prevSize = batch.size();

    while (!batch.empty() && stalls < kMaxStallIterations) {

      if (assistant.terminationRequested()) {
        break;
      }

      recomputeBatchLeafToRoot(batch, assistant);
      drainFencedDeltas();
      batch = collectAndFenceDirty();

      if (batch.size() < prevSize) {
        stalls = 0;
      } else {
        ++stalls;
      }

      prevSize = batch.size();
    }

    // One extra reconciliation pass before unfencing when the loop bailed
    // with a non-empty batch.
    if (!batch.empty()) {
      recomputeBatchLeafToRoot(batch, assistant);
    }

    unfence();
  }
}

//------------------------------------------------------------------------------
// Recompute every container in `batch` in leaf-to-root order
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::recomputeBatchLeafToRoot(
    const std::unordered_set<IContainerMD::id_t>& batch, ThreadAssistant& assistant)
{
  // Pair each dirty id with its depth from the root.
  std::vector<std::pair<uint16_t, IContainerMD::id_t>> depthSorted;
  depthSorted.reserve(batch.size());

  for (const auto& id : batch) {
    uint16_t depth = 0;
    IContainerMD::id_t cur = id;

    while (cur > 1 && depth < kMaxAncestorWalkDepth) {
      try {
        auto cont = mContainerMDSvc->getContainerMD(cur);
        cur = cont->getParentId();
        ++depth;
      } catch (const MDException& e) {
        break;
      }
    }

    depthSorted.emplace_back(depth, id);
  }

  // Deepest first so a parent's recompute reads its children's already
  // corrected tree values.
  std::sort(depthSorted.begin(), depthSorted.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });

  for (const auto& [depth, id] : depthSorted) {
    if (assistant.terminationRequested()) {
      break;
    }

    recomputeContainer(id, batch);
  }
}

//------------------------------------------------------------------------------
// Recompute a single container's tree values from current namespace state
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::recomputeContainer(
    IContainerMD::id_t id, const std::unordered_set<IContainerMD::id_t>& dirtySet)
{
  try {
    auto cont = mContainerMDSvc->getContainerMD(id);
    uint64_t treeSize = 0;
    uint64_t treeFiles = 0;
    uint64_t treeContainers = 0;

    for (auto fit = FileMapIterator(cont); fit.valid(); fit.next()) {
      try {
        auto fmd = mFileMDSvc->getFileMD(fit.value());
        treeSize += fmd->getSize();
        treeFiles += 1;
      } catch (const MDException& e) {
        continue;
      }
    }

    for (auto cit = ContainerMapIterator(cont); cit.valid(); cit.next()) {
      try {
        auto child = mContainerMDSvc->getContainerMD(cit.value());
        eos::MDLocking::ContainerReadLock rdLock(child.get());
        treeSize += child->getTreeSize();
        treeFiles += child->getTreeFiles();
        treeContainers += child->getTreeContainers() + 1;
      } catch (const MDException& e) {
        continue;
      }
    }

    int64_t deltaSize;
    int64_t deltaFiles;
    int64_t deltaContainers;
    IContainerMD::id_t parentId;
    {
      eos::MDLocking::ContainerWriteLock wrLock(cont.get());
      deltaSize =
          static_cast<int64_t>(treeSize) - static_cast<int64_t>(cont->getTreeSize());
      deltaFiles =
          static_cast<int64_t>(treeFiles) - static_cast<int64_t>(cont->getTreeFiles());
      deltaContainers = static_cast<int64_t>(treeContainers) -
                        static_cast<int64_t>(cont->getTreeContainers());
      cont->setTreeSize(treeSize);
      cont->setTreeFiles(treeFiles);
      cont->setTreeContainers(treeContainers);
      mContainerMDSvc->updateStore(cont.get());
      parentId = cont->getParentId();
    }

    // Skip propagation for dirty parents; they'll recompute themselves and
    // read our just-corrected value directly.
    if (parentId > 0 && !dirtySet.count(parentId) &&
        (deltaSize != 0 || deltaFiles != 0 || deltaContainers != 0)) {
      QueueForUpdate(parentId, {deltaSize, deltaFiles, deltaContainers});
    }
  } catch (const MDException& e) {
    return;
  }
}

EOSNSNAMESPACE_END
