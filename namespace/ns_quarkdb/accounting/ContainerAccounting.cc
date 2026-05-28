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
#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>

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
  {
    std::lock_guard<std::mutex> scope_lock(mMutexEventQueue);
    mAccountingQueue.emplace(EventType::Stop, NextSequence(), 0, 0, TreeInfos{0, 0, 0});
  }

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
    if (e->containerId) {
      QueueForUpdate(e->containerId, e->treeChange);
    } else if (e->file) {
      QueueForUpdate(e->file->getContainerId(), e->treeChange);
    } else {
      QueueForUpdate(e->location, e->treeChange);
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
QuarkContainerAccounting::AddTree(IContainerMD::id_t id, TreeInfos treeAccounting)
{
  QueueForUpdate(id, treeAccounting);
}

void
QuarkContainerAccounting::AddTree(IContainerMD* obj, TreeInfos treeAccounting)
{
  AddTree(obj->getId(), treeAccounting);
}

//-------------------------------------------------------------------------------
// Remove tree
//-------------------------------------------------------------------------------
void
QuarkContainerAccounting::RemoveTree(IContainerMD::id_t id, TreeInfos treeAccounting)
{
  QueueForUpdate(id, -treeAccounting);
}

void
QuarkContainerAccounting::RemoveTree(IContainerMD* obj, TreeInfos treeAccounting)
{
  RemoveTree(obj->getId(), treeAccounting);
}

//------------------------------------------------------------------------------
// Move tree
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::MoveTree(IContainerMD::id_t oldParentId,
                                   IContainerMD::id_t newParentId,
                                   IContainerMD::id_t movedId, TreeInfos treeAccounting)
{
  {
    std::lock_guard<std::mutex> scope_lock(mMutexEventQueue);
    const auto sequence = mSequence.fetch_add(2) + 1;
    const auto deltaSequence = ++mDeltaSequence;
    // A directory move is one logical tree change: remove the moved subtree
    // from the old parent and add it to the new parent. Recording the move at
    // this sequence lets pending deltas below the moved directory walk the
    // parent chain that was valid when they were queued, instead of racing with
    // the current topology.
    //
    // Lock order: mMutexEventQueue may be taken before mMutexParentHistory (in
    // RecordMove). Never take mMutexEventQueue while holding mMutexParentHistory.
    RecordMove(sequence, movedId, oldParentId, newParentId);
    mAccountingQueue.emplace(EventType::Delta, sequence, deltaSequence, oldParentId,
                             -treeAccounting);
    mAccountingQueue.emplace(EventType::Delta, sequence + 1, deltaSequence, newParentId,
                             treeAccounting);
  }
}

void
QuarkContainerAccounting::MoveTree(IContainerMD* oldParent, IContainerMD* newParent,
                                   IContainerMD* moved, TreeInfos treeAccounting)
{
  MoveTree(oldParent->getId(), newParent->getId(), moved->getId(), treeAccounting);
}

//------------------------------------------------------------------------------
// Get current accounting sequence
//------------------------------------------------------------------------------
uint64_t
QuarkContainerAccounting::GetAccountingSequence() const
{
  return mDeltaSequence.load();
}

//------------------------------------------------------------------------------
// Set tree if accounting unchanged
//------------------------------------------------------------------------------
bool
QuarkContainerAccounting::SetTreeIfAccountingUnchanged(IContainerMD::id_t id,
                                                       TreeInfos treeAccounting,
                                                       uint64_t accountingSequence)
{
  std::lock_guard<std::mutex> scope_lock(mMutexEventQueue);

  if (mDeltaSequence.load() != accountingSequence) {
    return false;
  }

  mAccountingQueue.emplace(EventType::Reset, NextSequence(), accountingSequence, id,
                           treeAccounting);
  return true;
}

bool
QuarkContainerAccounting::SetTreeIfAccountingUnchanged(IContainerMD* obj,
                                                       TreeInfos treeAccounting,
                                                       uint64_t accountingSequence)
{
  return SetTreeIfAccountingUnchanged(obj->getId(), treeAccounting, accountingSequence);
}

//------------------------------------------------------------------------------
// Reserve accounting delta
//------------------------------------------------------------------------------
IFileMDChangeListener::ReservedAccountingDelta
QuarkContainerAccounting::ReserveAccountingDelta(IContainerMD::id_t containerId,
                                                 TreeInfos treeInfos)
{
  if (((treeInfos.dsize == 0) && (treeInfos.dtreefiles == 0) &&
       (treeInfos.dtreecontainers == 0))) {
    return {};
  }

  return {true, NextSequence(), ++mDeltaSequence, containerId, treeInfos};
}

//------------------------------------------------------------------------------
// Publish accounting delta
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::PublishAccountingDelta(
    const IFileMDChangeListener::ReservedAccountingDelta& delta)
{
  if (!delta.valid) {
    return;
  }

  mAccountingQueue.emplace(EventType::Delta, delta.sequence, delta.deltaSequence,
                           delta.containerId, delta.treeChange);
}

//------------------------------------------------------------------------------
// Queue file info for update
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::QueueForUpdate(IContainerMD::id_t id, TreeInfos treeAccounting)
{
  PublishAccountingDelta(ReserveAccountingDelta(id, treeAccounting));
}

//------------------------------------------------------------------------------
// Get the next accounting sequence
//------------------------------------------------------------------------------
QuarkContainerAccounting::AccountingSequence
QuarkContainerAccounting::NextSequence()
{
  return mSequence.fetch_add(1) + 1;
}

//------------------------------------------------------------------------------
// Record a container parent change
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::RecordMove(AccountingSequence sequence,
                                     IContainerMD::id_t movedId,
                                     IContainerMD::id_t oldParentId,
                                     IContainerMD::id_t newParentId)
{
  if (oldParentId == newParentId) {
    return;
  }

  std::lock_guard<std::mutex> scope_lock(mMutexParentHistory);
  mParentHistory[movedId].push_back({sequence, oldParentId, newParentId});
}

//------------------------------------------------------------------------------
// Return the parent id a container had at the given accounting sequence
//------------------------------------------------------------------------------
IContainerMD::id_t
QuarkContainerAccounting::ParentAt(IContainerMD::id_t id, AccountingSequence sequence)
{
  {
    std::lock_guard<std::mutex> scope_lock(mMutexParentHistory);
    auto it = mParentHistory.find(id);

    if (it != mParentHistory.end() && !it->second.empty()) {
      const auto& history = it->second;

      for (auto move_it = history.rbegin(); move_it != history.rend(); ++move_it) {
        if (sequence >= move_it->mSequence) {
          return move_it->mNewParentId;
        }
      }

      return history.front().mOldParentId;
    }
  }

  auto cont = mContainerMDSvc->getContainerMD(id);
  return cont->getParentId();
}

//------------------------------------------------------------------------------
// Move pending delta map to ordered commit operations
//------------------------------------------------------------------------------
void
QuarkContainerAccounting::FlushDeltasToOperations(UpdateBatch& batch)
{
  if (batch.mMap.empty()) {
    return;
  }

  batch.mOperations.emplace_back(batch.mDeltaSequence, std::move(batch.mMap));
  batch.mMap.clear();
  batch.mDeltaSequence = 0;
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
    FlushDeltasToOperations(batch);

    if (!batch.mOperations.empty()) {
      for (auto const& operation : batch.mOperations) {
        if (operation.mType == CommitOperationType::Reset) {
          if ((mDeltaSequence.load() != operation.mDeltaSequence) ||
              (mAppliedDeltaSequence.load() < operation.mDeltaSequence)) {
            continue;
          }

          try {
            auto cont = mContainerMDSvc->getContainerMD(operation.mId);
            eos::MDLocking::ContainerWriteLock contLock(cont.get());

            cont->setTreeSize(static_cast<uint64_t>(operation.mTreeInfos.dsize));
            cont->setTreeFiles(static_cast<uint64_t>(operation.mTreeInfos.dtreefiles));
            cont->setTreeContainers(
                static_cast<uint64_t>(operation.mTreeInfos.dtreecontainers));
            mContainerMDSvc->updateStore(cont.get());
          } catch (const MDException& e) {
            // TODO: (esindril) error message using default logging
            continue;
          }

          continue;
        }

        for (auto const& elem : operation.mDeltas) {
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

        mAppliedDeltaSequence.store(
            std::max(mAppliedDeltaSequence.load(), operation.mDeltaSequence));
      }
    }

    batch.mOperations.clear();

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
  AccountingEvent event;
  std::map<AccountingSequence, AccountingEvent> pendingEvents;
  std::vector<IContainerMD::id_t> idsToUpdate;
  AccountingSequence nextSequence = 1;

  auto processEvent = [this, &idsToUpdate](const AccountingEvent& event) {
    if (event.mType == EventType::Stop) {
      return true;
    }

    if (event.mType == EventType::Reset) {
      std::lock_guard<std::mutex> scope_lock(mMutexBatch);
      auto& batch = mBatch[mAccumulateIndx];
      FlushDeltasToOperations(batch);
      batch.mOperations.emplace_back(event.mDeltaSequence, event.mId, event.mTreeInfos);
      return false;
    }

    IContainerMD::id_t id = event.mId;
    uint16_t deepness = 0;

    // Go up the tree and give the ids to update to the batch that will
    // be taken by the PropagateUpdate thread
    while ((id > 1) && (deepness < 255)) {
      try {
        if (std::find(idsToUpdate.begin(), idsToUpdate.end(), id) != idsToUpdate.end()) {
          break;
        }

        idsToUpdate.push_back(id);
        id = ParentAt(id, event.mSequence);
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
        it_map->second += event.mTreeInfos;
      } else {
        batch.mMap.emplace(idToUpdate, event.mTreeInfos);
      }
    }

    batch.mDeltaSequence = std::max(batch.mDeltaSequence, event.mDeltaSequence);

    idsToUpdate.clear();
    return false;
  };

  while ((assistant && !assistant->terminationRequested()) || (!assistant)) {
    mAccountingQueue.wait_pop(event);

    if (event.mSequence < nextSequence) {
      continue;
    }

    pendingEvents.emplace(event.mSequence, event);

    while (true) {
      auto it = pendingEvents.find(nextSequence);

      if (it == pendingEvents.end()) {
        break;
      }

      const bool stop = processEvent(it->second);
      pendingEvents.erase(it);
      ++nextSequence;

      if (stop) {
        return;
      }
    }
  }
}

EOSNSNAMESPACE_END
