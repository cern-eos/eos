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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingSequencer.hh"
#include <chrono>
#include <iostream>

EOSNSNAMESPACE_BEGIN

namespace {

constexpr auto kTreeSizeFenceWaitTimeout = std::chrono::seconds(1);

void
AddTreeInfos(TreeInfos& target, const TreeInfos& source)
{
  target += source;
}

void
CountIncludedSubtreeUpdate(TreeSizeAccountingFenceStats& stats,
                           const TreeSizeAccountingEvent& event)
{
  if (event.type == TreeSizeAccountingEventType::SubtreeAttach) {
    ++stats.includedSubtreeAttachUpdates;
  } else if (event.type == TreeSizeAccountingEventType::SubtreeDetach) {
    ++stats.includedSubtreeDetachUpdates;
  }
}

} // namespace

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
  mIdTreeInfosToUpdateQueue.emplace();
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
    CaptureTreeSizeJournalEntry(e->treeChange, e->treeSizeAccountingEvent);

    // e->file can be nullptr here
    if ((e->file && e->file->getContainerId() == 0) || !e->file) {
      // NOTE: This is an ugly hack. The file object has not reference to the
      // container id, therefore we hijack the "location" member of the Event
      // class to pass in the container id.
      QueueForUpdate(e->location, e->treeChange, e->treeSizeAccountingEvent);
    } else {
      QueueForUpdate(e->file->getContainerId(), e->treeChange,
                     e->treeSizeAccountingEvent);
    }

    break;

  default:
    break;
  }
}

//------------------------------------------------------------------------------
// Start a raw tree-size journal capture session
//------------------------------------------------------------------------------
std::unique_ptr<TreeSizeJournalCaptureScope>
QuarkContainerAccounting::StartTreeSizeJournalCapture()
{
  return mTreeSizeJournalCapture.StartCapture();
}

//------------------------------------------------------------------------------
// Add tree
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::AddTree(IContainerMD* obj, uint64_t subtreeRootId,
                                  TreeInfos treeAccounting)
{
  const std::optional<TreeSizeAccountingEvent> accounting_event =
      ReserveTreeSizeAccountingEvent(TreeSizeAccountingEventType::SubtreeAttach,
                                     obj->getId(), subtreeRootId);
  CaptureTreeSizeJournalEntry(treeAccounting, accounting_event);
  QueueForUpdate(obj->getId(), treeAccounting, accounting_event);
}

//-------------------------------------------------------------------------------
// Remove tree
//-------------------------------------------------------------------------------
void
QuarkContainerAccounting::RemoveTree(IContainerMD* obj, uint64_t subtreeRootId,
                                     TreeInfos treeAccounting)
{
  const auto tree_change = -treeAccounting;
  const std::optional<TreeSizeAccountingEvent> accounting_event =
      ReserveTreeSizeAccountingEvent(TreeSizeAccountingEventType::SubtreeDetach,
                                     obj->getId(), subtreeRootId);
  CaptureTreeSizeJournalEntry(tree_change, accounting_event);
  QueueForUpdate(obj->getId(), tree_change, accounting_event);
}

//------------------------------------------------------------------------------
// Queue file info for update
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueForUpdate(IContainerMD::id_t id, TreeInfos treeAccounting)
{
  QueueForUpdate(id, treeAccounting, std::nullopt);
}

//------------------------------------------------------------------------------
// Queue file info for update with optional sequence metadata
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueForUpdate(
    IContainerMD::id_t id, TreeInfos treeAccounting,
    std::optional<TreeSizeAccountingEvent> accounting_event)
{
  if (!id) {
    return;
  }

  TreeSizeAccountingUpdate update;
  update.id = id;
  update.treeInfos = treeAccounting;

  if (accounting_event.has_value()) {
    update.hasAccountingMetadata = true;
    update.accountingEvent = *accounting_event;
  }

  bool queued_for_fence = false;

  {
    std::lock_guard<std::mutex> fence_lock(mMutexFence);

    if (mFence.active) {
      mFence.directUpdates.push_back(std::move(update));
      queued_for_fence = true;
    }
  }

  if (queued_for_fence) {
    mFenceCv.notify_all();
    return;
  }

  // The condition to stop the queueing thread is that the id = 0. The minimum
  // container id is 1 and corresponds to "/". We therefore prevent users to
  // queue the container id = 0 for update.
  mIdTreeInfosToUpdateQueue.emplace(std::move(update));
}

//------------------------------------------------------------------------------
// Capture one tree-size journal entry when recompute journal capture is active
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::CaptureTreeSizeJournalEntry(
    const TreeInfos& treeInfos,
    const std::optional<TreeSizeAccountingEvent>& accounting_event)
{
  if (!mTreeSizeJournalCapture.IsActive()) {
    return;
  }

  TreeSizeJournalEntry entry;
  entry.hasAccountingMetadata = accounting_event.has_value();

  if (entry.hasAccountingMetadata) {
    entry.accountingEvent = *accounting_event;
  }

  entry.treeChange = treeInfos;
  mTreeSizeJournalCapture.Capture(entry);
}

//------------------------------------------------------------------------------
// Build target container updates for all ancestors affected by one direct update
//------------------------------------------------------------------------------
std::vector<QuarkContainerAccounting::TreeSizeAccountingUpdate>
QuarkContainerAccounting::BuildPropagationUpdates(
    const TreeSizeAccountingUpdate& update) const
{
  std::vector<TreeSizeAccountingUpdate> updates;
  uint16_t deepness = 0;
  IContainerMD::id_t id = update.id;

  while ((id > 1) && (deepness < 255)) {
    TreeSizeAccountingUpdate target = update;
    target.id = id;
    updates.push_back(target);

    if (!mContainerMDSvc) {
      break;
    }

    try {
      auto cont = mContainerMDSvc->getContainerMD(id);
      id = cont->getParentId();
      ++deepness;
    } catch (const MDException&) {
      break;
    }
  }

  return updates;
}

//------------------------------------------------------------------------------
// Queue one target update into the normal live accounting batch
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueBatchUpdateLocked(const TreeSizeAccountingUpdate& update)
{
  std::lock_guard<std::mutex> scope_lock(mMutexBatch);
  mBatch[mAccumulateIndx].mUpdates.push_back(update);
}

//------------------------------------------------------------------------------
// Route one target update through the active fence or normal live accounting
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::RouteFenceOrBatchUpdateLocked(
    const TreeSizeAccountingUpdate& update)
{
  if (mFence.active && (mFence.request.coveredContainerIds.count(update.id) != 0)) {
    if (!update.hasAccountingMetadata) {
      ++mFence.stats.unsequencedCoveredUpdates;
      ++mFence.stats.replayAfterPublishUpdates;
      mFence.replayAfterPublishUpdates.push_back(update);
      return;
    }

    if (update.accountingEvent.sequence <= mFence.request.validatedThroughSequence) {
      ++mFence.stats.includedInPublishUpdates;
      CountIncludedSubtreeUpdate(mFence.stats, update.accountingEvent);
      mFence.includedInPublishUpdates.push_back(update);
    } else {
      ++mFence.stats.replayAfterPublishUpdates;
      mFence.replayAfterPublishUpdates.push_back(update);
    }

    return;
  }

  if (mFence.active) {
    ++mFence.stats.passedThroughUpdates;
  }

  QueueBatchUpdateLocked(update);
}

//------------------------------------------------------------------------------
// Route several target updates through the active fence or normal live accounting
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::RouteFenceOrBatchUpdatesLocked(
    const std::vector<TreeSizeAccountingUpdate>& updates)
{
  for (const auto& update : updates) {
    RouteFenceOrBatchUpdateLocked(update);
  }
}

//------------------------------------------------------------------------------
// Drain direct updates still waiting for ancestor expansion
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::DrainRawQueueLocked(std::unique_lock<std::mutex>& fence_lock)
{
  TreeSizeAccountingUpdate update;

  while (mIdTreeInfosToUpdateQueue.try_pop(update)) {
    if (!update.id) {
      continue;
    }

    ++mFence.stats.drainedRawQueueUpdates;

    fence_lock.unlock();
    const auto updates = BuildPropagationUpdates(update);
    fence_lock.lock();

    RouteFenceOrBatchUpdatesLocked(updates);
  }
}

//------------------------------------------------------------------------------
// Drain direct updates queued while the fence was already active
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::DrainFenceDirectUpdatesLocked(
    std::unique_lock<std::mutex>& fence_lock)
{
  std::vector<TreeSizeAccountingUpdate> direct_updates;
  direct_updates.swap(mFence.directUpdates);

  for (const auto& update : direct_updates) {
    if (!update.id) {
      continue;
    }

    ++mFence.stats.drainedRawQueueUpdates;

    fence_lock.unlock();
    const auto updates = BuildPropagationUpdates(update);
    fence_lock.lock();

    RouteFenceOrBatchUpdatesLocked(updates);
  }

  return !direct_updates.empty();
}

//------------------------------------------------------------------------------
// Drain already expanded but not yet propagated target updates
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::DrainBatchUpdatesLocked()
{
  std::vector<TreeSizeAccountingUpdate> updates;

  {
    std::lock_guard<std::mutex> scope_lock(mMutexBatch);

    for (auto& batch : mBatch) {
      mFence.stats.drainedBatchUpdates += batch.mUpdates.size();
      updates.insert(updates.end(), batch.mUpdates.begin(), batch.mUpdates.end());
      batch.mUpdates.clear();
    }
  }

  RouteFenceOrBatchUpdatesLocked(updates);
}

//------------------------------------------------------------------------------
// Replay target updates into normal live accounting
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::ReplayFenceUpdatesLocked(
    const std::vector<TreeSizeAccountingUpdate>& updates)
{
  std::lock_guard<std::mutex> scope_lock(mMutexBatch);
  auto& batch = mBatch[mAccumulateIndx].mUpdates;
  batch.insert(batch.end(), updates.begin(), updates.end());
}

//------------------------------------------------------------------------------
// Register updates that have left the batch and are being propagated
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::RegisterInFlightUpdatesLocked(
    const std::vector<TreeSizeAccountingUpdate>& updates)
{
  mInFlightUpdates.insert(mInFlightUpdates.end(), updates.begin(), updates.end());
}

//------------------------------------------------------------------------------
// Register a direct update while it is being expanded to target ancestors
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::RegisterRawUpdateInFlight()
{
  std::lock_guard<std::mutex> fence_lock(mMutexFence);
  ++mRawUpdatesInFlight;
}

//------------------------------------------------------------------------------
// Unregister a direct update after ancestor expansion has been routed
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::UnregisterRawUpdateInFlight()
{
  {
    std::lock_guard<std::mutex> fence_lock(mMutexFence);

    if (mRawUpdatesInFlight) {
      --mRawUpdatesInFlight;
    }
  }

  mFenceCv.notify_all();
}

//------------------------------------------------------------------------------
// Unregister propagated updates once the propagation pass has finished
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::UnregisterInFlightUpdates(
    const std::vector<TreeSizeAccountingUpdate>& updates)
{
  {
    std::lock_guard<std::mutex> fence_lock(mMutexFence);

    for (const auto& update : updates) {
      for (auto it = mInFlightUpdates.begin(); it != mInFlightUpdates.end(); ++it) {
        if (it->id == update.id) {
          mInFlightUpdates.erase(it);
          break;
        }
      }
    }
  }

  mFenceCv.notify_all();
}

//------------------------------------------------------------------------------
// Classify covered in-flight updates against the validated publish sequence
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::ClassifyInFlightUpdatesLocked()
{
  for (const auto& update : mInFlightUpdates) {
    if (mFence.request.coveredContainerIds.count(update.id) == 0) {
      continue;
    }

    if (!update.hasAccountingMetadata) {
      ++mFence.stats.unsequencedCoveredUpdates;
      ++mFence.stats.replayAfterPublishUpdates;
      mFence.inFlightReplayAfterPublishUpdates.push_back(update);
      continue;
    }

    if (update.accountingEvent.sequence <= mFence.request.validatedThroughSequence) {
      ++mFence.stats.includedInPublishUpdates;
      CountIncludedSubtreeUpdate(mFence.stats, update.accountingEvent);
    } else {
      ++mFence.stats.replayAfterPublishUpdates;
      mFence.inFlightReplayAfterPublishUpdates.push_back(update);
    }
  }
}

//------------------------------------------------------------------------------
// Count in-flight updates that target containers covered by the active fence
//------------------------------------------------------------------------------
uint64_t
QuarkContainerAccounting::CountCoveredInFlightUpdatesLocked() const
{
  uint64_t count = 0;

  for (const auto& update : mInFlightUpdates) {
    if (mFence.request.coveredContainerIds.count(update.id) != 0) {
      ++count;
    }
  }

  return count;
}

//------------------------------------------------------------------------------
// Wait briefly until the fence has seen queued and in-progress live updates
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::WaitForFenceDrainLocked(
    std::unique_lock<std::mutex>& fence_lock)
{
  const auto deadline = std::chrono::steady_clock::now() + kTreeSizeFenceWaitTimeout;

  while (mFence.active) {
    const auto in_flight = CountCoveredInFlightUpdatesLocked();
    mFence.stats.inFlightCoveredUpdates = in_flight;

    if (!in_flight && !mRawUpdatesInFlight && mFence.directUpdates.empty()) {
      return true;
    }

    if (!mFence.directUpdates.empty()) {
      DrainFenceDirectUpdatesLocked(fence_lock);
      continue;
    }

    if (mFenceCv.wait_until(fence_lock, deadline) != std::cv_status::timeout) {
      continue;
    }

    mFence.stats.inFlightCoveredUpdates = CountCoveredInFlightUpdatesLocked();

    if (mFence.stats.inFlightCoveredUpdates || mRawUpdatesInFlight ||
        !mFence.directUpdates.empty()) {
      mFence.stats.inFlightWaitTimeout = true;
      return false;
    }

    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Acquire a short publish fence for covered tree-size accounting updates
//------------------------------------------------------------------------------
TreeSizeAccountingFenceStats
QuarkContainerAccounting::AcquireTreeSizeAccountingFence(
    const TreeSizeAccountingFenceRequest& request)
{
  std::unique_lock<std::mutex> fence_lock(mMutexFence);

  if (mFence.active) {
    return TreeSizeAccountingFenceStats{};
  }

  mFence = TreeSizeAccountingFenceState{};
  mFence.active = true;
  mFence.request = request;
  mFence.stats.acquired = true;
  mFence.stats.coveredContainerIds = request.coveredContainerIds.size();

  DrainBatchUpdatesLocked();
  DrainRawQueueLocked(fence_lock);
  DrainFenceDirectUpdatesLocked(fence_lock);
  ClassifyInFlightUpdatesLocked();

  if (!WaitForFenceDrainLocked(fence_lock)) {
    auto stats = mFence.stats;
    stats.acquired = false;
    ReplayFenceUpdatesLocked(mFence.includedInPublishUpdates);
    ReplayFenceUpdatesLocked(mFence.replayAfterPublishUpdates);
    mFence = TreeSizeAccountingFenceState{};
    return stats;
  }

  return mFence.stats;
}

//------------------------------------------------------------------------------
// Release the active publish fence
//------------------------------------------------------------------------------
TreeSizeAccountingFenceStats
QuarkContainerAccounting::ReleaseTreeSizeAccountingFence(
    TreeSizeAccountingFenceReleaseMode mode)
{
  std::unique_lock<std::mutex> fence_lock(mMutexFence);

  if (!mFence.active) {
    return TreeSizeAccountingFenceStats{};
  }

  DrainRawQueueLocked(fence_lock);
  DrainFenceDirectUpdatesLocked(fence_lock);
  WaitForFenceDrainLocked(fence_lock);

  const auto stats = mFence.stats;

  if (mode == TreeSizeAccountingFenceReleaseMode::AbortBeforePublish) {
    ReplayFenceUpdatesLocked(mFence.includedInPublishUpdates);
    ReplayFenceUpdatesLocked(mFence.replayAfterPublishUpdates);
  } else {
    ReplayFenceUpdatesLocked(mFence.replayAfterPublishUpdates);
    ReplayFenceUpdatesLocked(mFence.inFlightReplayAfterPublishUpdates);
  }

  mFence = TreeSizeAccountingFenceState{};
  fence_lock.unlock();
  mFenceCv.notify_all();
  return stats;
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
    std::vector<TreeSizeAccountingUpdate> updates;

    {
      // Update the indexes to have the async thread working on the batch to
      // commit and the incoming updates to go to the batch to update
      std::lock_guard<std::mutex> fence_lock(mMutexFence);
      std::lock_guard<std::mutex> scope_lock(mMutexBatch);
      std::swap(mAccumulateIndx, mCommitIndx);
      updates.swap(mBatch[mCommitIndx].mUpdates);
      RegisterInFlightUpdatesLocked(updates);
    }

    std::unordered_map<IContainerMD::id_t, TreeInfos> aggregated_updates;

    for (const auto& update : updates) {
      auto it_map = aggregated_updates.find(update.id);

      if (it_map != aggregated_updates.end()) {
        AddTreeInfos(it_map->second, update.treeInfos);
      } else {
        aggregated_updates.emplace(update.id, update.treeInfos);
      }
    }

    if (!aggregated_updates.empty()) {
      for (auto const& elem : aggregated_updates) {
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

    UnregisterInFlightUpdates(updates);

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
  TreeSizeAccountingUpdate update;

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    mIdTreeInfosToUpdateQueue.wait_pop(update);

    if (!update.id) {
      // Container ID = 0 (see ~QuarkContainerAccounting()), we
      // stop this thread
      break;
    }

    RegisterRawUpdateInFlight();
    const auto updates = BuildPropagationUpdates(update);

    {
      std::lock_guard<std::mutex> fence_lock(mMutexFence);
      RouteFenceOrBatchUpdatesLocked(updates);
    }

    UnregisterRawUpdateInFlight();
  }
}

EOSNSNAMESPACE_END
