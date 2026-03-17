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

//------------------------------------------------------------------------------
// Thread-local DeltaSource for tagging events from the current thread.
// Rename operations set this to kRename via DeltaSourceScope so that
// addContainer/removeContainer/addFile/removeFile events are tagged correctly.
//------------------------------------------------------------------------------
static thread_local DeltaSource sThreadDeltaSource = DeltaSource::kFileChange;

DeltaSourceScope::DeltaSourceScope(DeltaSource source)
    : mPrevious(sThreadDeltaSource)
{
  sThreadDeltaSource = source;
}

DeltaSourceScope::~DeltaSourceScope() { sThreadDeltaSource = mPrevious; }

DeltaSource
getThreadLocalDeltaSource()
{
  return sThreadDeltaSource;
}

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
// Destructor
//----------------------------------------------------------------------------
QuarkContainerAccounting::~QuarkContainerAccounting()
{
  // Stop the AsyncQueueUpdate thread by queuing containerID = 0
  mIdTreeInfosToUpdateQueue.emplace(0, TreeInfos{0, 0, 0}, DeltaSource::kFileChange);
  if (mUpdateIntervalSec) {
    mThread.join();
    mQueueForUpdateThread.join();
  }

  // Stop the dirty recompute thread
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
  case IFileMDChangeListener::SizeChange: {
    // The DeltaSource is determined by the thread-local set via DeltaSourceScope.
    // During rename operations, this is kRename; otherwise kFileChange (default).
    DeltaSource source = getThreadLocalDeltaSource();

    // e->file can be nullptr here
    if ((e->file && e->file->getContainerId() == 0) || !e->file) {
      // NOTE: This is an ugly hack. The file object has not reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      QueueForUpdate(e->location, e->treeChange, source);
    } else {
      QueueForUpdate(e->file->getContainerId(), e->treeChange, source);
    }

    break;
  }

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
  QueueForUpdate(obj->getId(), treeAccounting, DeltaSource::kRename);
}

//-------------------------------------------------------------------------------
// Remove tree
//-------------------------------------------------------------------------------
void
QuarkContainerAccounting::RemoveTree(IContainerMD* obj, TreeInfos treeAccounting)
{
  QueueForUpdate(obj->getId(), -treeAccounting, DeltaSource::kRename);
}

//------------------------------------------------------------------------------
// Queue file info for update
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueForUpdate(IContainerMD::id_t id, TreeInfos treeAccounting,
                                         DeltaSource source)
{
  if(id) {
    // The condition to stop the queueing thread is that the id = 0. The minimum container id is 1 and
    // corresponds to "/"
    // We therefore prevent users to queue the container id = 0 for update (which should not happen!)
    mIdTreeInfosToUpdateQueue.emplace(id, treeAccounting, source);
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
      // Separate fenced entries from unfenced ones
      std::unordered_map<IContainerMD::id_t, TaggedTreeInfos> fencedEntries;
      {
        eos::common::RWMutexReadLock fenceLock(mFenceMutex);

        if (!mFencedIds.empty()) {
          for (auto it = batch.mMap.begin(); it != batch.mMap.end();) {
            if (mFencedIds.count(it->first)) {
              fencedEntries[it->first] = it->second;
              it = batch.mMap.erase(it);
            } else {
              ++it;
            }
          }
        }
      }

      // Apply unfenced entries normally (combine file-change and tree-move deltas)
      for (auto const& elem : batch.mMap) {
        try {
          auto cont = mContainerMDSvc->getContainerMD(elem.first);
          eos::MDLocking::ContainerWriteLock contLock(cont.get());
          TreeInfos total;
          total += elem.second.fileChanges;
          total += elem.second.renameDeltas;
          cont->updateTreeSize(total.dsize);
          cont->updateTreeFiles(total.dtreefiles);
          cont->updateTreeContainers(total.dtreecontainers);
          mContainerMDSvc->updateStore(cont.get());
        } catch (const MDException& e) {
          // TODO: (esindril) error message using default logging
          continue;
        }
      }

      // Merge fenced entries into the recompute journal (preserving tags)
      if (!fencedEntries.empty()) {
        eos::common::RWMutexWriteLock fenceLock(mFenceMutex);

        for (auto& [id, tagged] : fencedEntries) {
          auto it = mRecomputeJournal.find(id);

          if (it != mRecomputeJournal.end()) {
            it->second.fileChanges += tagged.fileChanges;
            it->second.renameDeltas += tagged.renameDeltas;
            it->second.hadFileChangeActivity |= tagged.hadFileChangeActivity;
          } else {
            mRecomputeJournal.emplace(id, tagged);
          }
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
  QueueEntry entry;
  std::vector<IContainerMD::id_t> idsToUpdate;

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    uint16_t deepness = 0;

    mIdTreeInfosToUpdateQueue.wait_pop(entry);
    if (!entry.id) {
      // Container ID = 0 (see ~QuarkContainerAccounting()), we
      // stop this thread
      break;
    }

    IContainerMD::id_t id = entry.id;
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
        if (entry.source == DeltaSource::kFileChange) {
          it_map->second.fileChanges += entry.delta;
          it_map->second.hadFileChangeActivity = true;
        } else {
          it_map->second.renameDeltas += entry.delta;
        }
      } else {
        TaggedTreeInfos tagged;

        if (entry.source == DeltaSource::kFileChange) {
          tagged.fileChanges = entry.delta;
          tagged.hadFileChangeActivity = true;
        } else {
          tagged.renameDeltas = entry.delta;
        }

        batch.mMap.emplace(idToUpdate, tagged);
      }
    }

    idsToUpdate.clear();
  }
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
  mRecomputeJournal.clear();
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
  mRecomputeJournal.clear();
}

//------------------------------------------------------------------------------
// Collect file-change deltas from journal and clear journal
//------------------------------------------------------------------------------
std::unordered_map<IContainerMD::id_t, TreeInfos>
QuarkContainerAccounting::collectFileChangeDeltas()
{
  eos::common::RWMutexWriteLock lock(mFenceMutex);
  std::unordered_map<IContainerMD::id_t, TreeInfos> result;

  for (const auto& [id, tagged] : mRecomputeJournal) {
    const auto& fc = tagged.fileChanges;

    if (fc.dsize != 0 || fc.dtreefiles != 0 || fc.dtreecontainers != 0) {
      result[id] = fc;
    }
  }

  mRecomputeJournal.clear();
  return result;
}

//------------------------------------------------------------------------------
// Collect container IDs with file-change activity, clear journal
//------------------------------------------------------------------------------
std::unordered_set<IContainerMD::id_t>
QuarkContainerAccounting::collectDirtyContainerIds()
{
  eos::common::RWMutexWriteLock lock(mFenceMutex);
  std::unordered_set<IContainerMD::id_t> result;

  // Any container that has a journal entry (file-change OR rename) needs
  // recompute: rename deltas during BFS represent ancestor corrections that
  // are otherwise discarded with the journal, leaving stale tree counts.
  for (const auto& [id, tagged] : mRecomputeJournal) {
    result.insert(id);
  }

  mRecomputeJournal.clear();
  return result;
}

//------------------------------------------------------------------------------
// Clear fenced set and discard journal, resuming normal propagation
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::unfence()
{
  eos::common::RWMutexWriteLock lock(mFenceMutex);
  mFencedIds.clear();
  mRecomputeJournal.clear();
}

//------------------------------------------------------------------------------
// Set the file metadata service pointer
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::setFileMDSvc(IFileMDSvc* svc)
{
  mFileMDSvc = svc;
  mDirtyRecomputeThread.reset(&QuarkContainerAccounting::AssistedRecomputeDirty, this);
}

//------------------------------------------------------------------------------
// Schedule async recompute of dirty containers
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::scheduleRecompute(std::unordered_set<IContainerMD::id_t>&& ids)
{
  if (ids.empty() || !mFileMDSvc) {
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

    // Iterate: recompute dirty containers, then check for new dirty
    // containers from concurrent operations during the recompute. The dirty
    // containers remain fenced, so concurrent deltas are diverted to the
    // journal. Converges because each iteration processes a smaller set.
    static constexpr int kMaxDirtyRecomputeIterations = 3;

    for (int iter = 0; iter < kMaxDirtyRecomputeIterations && !batch.empty(); ++iter) {

      if (assistant.terminationRequested()) {
        break;
      }

      // Determine depth of each dirty container (walk up to root)
      std::vector<std::pair<uint16_t, IContainerMD::id_t>> depthSorted;
      depthSorted.reserve(batch.size());

      for (const auto& id : batch) {
        uint16_t depth = 0;
        IContainerMD::id_t cur = id;

        while (cur > 1 && depth < 255) {
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

      // Sort by depth descending (deepest first = leaf-to-root)
      std::sort(depthSorted.begin(), depthSorted.end(),
                [](const auto& a, const auto& b) { return a.first > b.first; });

      // Recompute each dirty container in leaf-to-root order
      for (const auto& [depth, id] : depthSorted) {
        if (assistant.terminationRequested()) {
          break;
        }

        recomputeContainer(id, batch);
      }

      // Flush any deltas from concurrent operations during the recompute
      // into the journal, then check for new dirty containers
      drainFencedDeltas();
      batch = collectDirtyContainerIds();

      if (!batch.empty()) {
        // Narrow the fence to the new (smaller) dirty set
        fenceContainers(batch);
      }
    }

    // Final pass: if the iteration cap was hit with a non-empty batch, the
    // last collectDirtyContainerIds returned containers that were never
    // recomputed. Process them once more before unfencing so we don't leave
    // stale tree values behind.
    if (!batch.empty()) {

      std::vector<std::pair<uint16_t, IContainerMD::id_t>> depthSorted;
      depthSorted.reserve(batch.size());

      for (const auto& id : batch) {
        uint16_t depth = 0;
        IContainerMD::id_t cur = id;

        while (cur > 1 && depth < 255) {
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

      std::sort(depthSorted.begin(), depthSorted.end(),
                [](const auto& a, const auto& b) { return a.first > b.first; });

      for (const auto& [depth, id] : depthSorted) {
        if (assistant.terminationRequested()) {
          break;
        }

        recomputeContainer(id, batch);
      }
    }

    unfence();
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

    // Sum file sizes
    for (auto fit = FileMapIterator(cont); fit.valid(); fit.next()) {
      try {
        auto fmd = mFileMDSvc->getFileMD(fit.value());
        treeSize += fmd->getSize();
        treeFiles += 1;
      } catch (const MDException& e) {
        continue;
      }
    }

    // Sum child container tree values
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

    // Compute delta between new and old values, set absolute values
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

    // Propagate delta to ancestors via the normal accounting pipeline,
    // but only if the parent is NOT in the dirty set (dirty parents will
    // be recomputed themselves and read corrected child values directly)
    if (parentId > 0 && !dirtySet.count(parentId) &&
        (deltaSize != 0 || deltaFiles != 0 || deltaContainers != 0)) {
      QueueForUpdate(parentId, {deltaSize, deltaFiles, deltaContainers},
                     DeltaSource::kFileChange);
    }
  } catch (const MDException& e) {
    return;
  }
}

EOSNSNAMESPACE_END
