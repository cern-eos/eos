//------------------------------------------------------------------------------
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//! @brief Container subtree accounting
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
#include "common/AssistedThread.hh"
#include "common/ConcurrentQueue.hh"
#include "common/RWMutex.hh"
#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Container subtree accounting listener
//------------------------------------------------------------------------------
class QuarkContainerAccounting : public IFileMDChangeListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param cont_svc container metadata service
  //! @param file_svc file metadata service (needed by the async dirty-recompute
  //!        thread to read individual file sizes during a recompute)
  //! @param update_interval interval in seconds when updates are propagated
  //----------------------------------------------------------------------------
  QuarkContainerAccounting(IContainerMDSvc* cont_svc, IFileMDSvc* file_svc,
                           int32_t update_interval = 5);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~QuarkContainerAccounting();

  //----------------------------------------------------------------------------
  //! Delete copy/move constructor and assignment operators
  //----------------------------------------------------------------------------
  QuarkContainerAccounting(const QuarkContainerAccounting& other) = delete;
  QuarkContainerAccounting& operator=(const QuarkContainerAccounting& other) =
    delete;
  QuarkContainerAccounting(QuarkContainerAccounting&& other) = delete;
  QuarkContainerAccounting& operator=(QuarkContainerAccounting&& other) = delete;

  //----------------------------------------------------------------------------
  //! Notify me about the changes in the main view
  //----------------------------------------------------------------------------
  virtual void fileMDChanged(IFileMDChangeListener::Event* e);

  //----------------------------------------------------------------------------
  //! Notify me about files when recovering from changelog
  //----------------------------------------------------------------------------
  virtual void
  fileMDRead(IFileMD* obj) {}

  //----------------------------------------------------------------------------
  //! Recheck the current file object and make any modifications necessary so
  //! that the information is consistent in the back-end KV store.
  //!
  //! @param file file object to be checked
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool
  fileMDCheck(IFileMD* file)
  {
    return true;
  }

  //----------------------------------------------------------------------------
  //! Add tree
  //!
  //! @param obj container where the tree information should be added
  //! @param treeAccounting tree accounting information to be updated
  //----------------------------------------------------------------------------
  void AddTree(IContainerMD* obj, TreeInfos treeAccounting);

  //----------------------------------------------------------------------------
  //! Remove tree
  //!
  //! @param obj container where the tree should be removed from
  //! @param dsize size of the subtree to be removed
  //----------------------------------------------------------------------------
  void RemoveTree(IContainerMD* obj, TreeInfos treeAccounting);

  //----------------------------------------------------------------------------
  //! Queue info for update
  //!
  //! @param pid container id
  //! @param dsize size change
  //----------------------------------------------------------------------------
  void QueueForUpdate(IContainerMD::id_t pid, TreeInfos treeInfos);

  //----------------------------------------------------------------------------
  //! Propagate updates in the hierarchical structure
  //!
  //! @param assistant thread doing the propagation or null by default if the
  //!        update should be done in the calling thread.
  //----------------------------------------------------------------------------
  void PropagateUpdates(ThreadAssistant* assistant = nullptr);

  //----------------------------------------------------------------------------
  //! Queue container ids and size to update the tree size
  //!
  //! @param assistant thread doing the queueing or null by default if the
  //!        update should be done in the calling thread.
  //----------------------------------------------------------------------------
  void AsyncQueueForUpdate(ThreadAssistant * assistant = nullptr);

  //----------------------------------------------------------------------------
  //! Run an absolute recompute over `bfsIds` while protecting it from
  //! concurrent file/rename traffic (EOS-6577). The caller supplies the
  //! leaves-to-root walk as `runBfs`; this method wraps fence / drain /
  //! dirty-set capture around it so callers cannot leave the system
  //! half-fenced on early return.
  //!
  //! @param bfsIds containers covered by the recompute
  //! @param runBfs caller-provided leaves-to-root recompute body
  //!
  //! @return true if the recompute ran, false if it was refused because a
  //!         previous recompute (sync phase or async dirty-recompute tail)
  //!         is still in flight
  //----------------------------------------------------------------------------
  bool RecomputeSubtreeWithFencing(const std::unordered_set<IContainerMD::id_t>& bfsIds,
                                   const std::function<void()>& runBfs);

  //----------------------------------------------------------------------------
  //! Whether a recompute is currently running. Caller-side hint that lets
  //! expensive setup work (e.g. the BFS subtree walk) be skipped early;
  //! `RecomputeSubtreeWithFencing` itself still does the authoritative
  //! atomic check.
  //----------------------------------------------------------------------------
  bool
  isRecomputeInFlight() const
  {
    return mRecomputeInFlight.load();
  }

  //----------------------------------------------------------------------------
  //! @name Fencing primitives
  //!
  //! Implementation details of `RecomputeSubtreeWithFencing` and the async
  //! dirty-recompute thread. Exposed only so unit tests can drive the state
  //! machine step-by-step; **production callers must go through
  //! `RecomputeSubtreeWithFencing`**.
  //! @{
  //----------------------------------------------------------------------------

  //! Divert deltas for `ids` from the namespace into the dirty journal until
  //! `unfence()`.
  //!
  //! @param ids container IDs to fence
  void fenceContainers(const std::unordered_set<IContainerMD::id_t>& ids);

  //! Block until every in-flight delta has reached either the namespace
  //! (unfenced ids) or the dirty journal (fenced ids).
  void drainFencedDeltas();

  //! Drop everything currently held in the dirty journal.
  void discardJournal();

  //! Under one write-lock: swap the dirty journal out, install it as the
  //! new fence, and return it. The atomicity closes the lost-update race
  //! where a separate read + fence-reset would let a concurrent
  //! `PropagateUpdates` cycle drop a delta into the just-emptied journal.
  //!
  //! @return container IDs that were in the dirty journal
  std::unordered_set<IContainerMD::id_t> collectAndFenceDirty();

  //! Sole release point for the recompute protocol: clears fence, journal,
  //! and the `mRecomputeInFlight` claim.
  void unfence();
  //! @}

  //----------------------------------------------------------------------------
  //! Hand a dirty set to the async recompute thread. Each container is
  //! re-read from the namespace and set to its true absolute values; the
  //! resulting delta is propagated to non-dirty ancestors via the normal
  //! pipeline.
  //!
  //! @param ids dirty container IDs to recompute (moved from)
  //----------------------------------------------------------------------------
  void scheduleRecompute(std::unordered_set<IContainerMD::id_t>&& ids);

private:

  //----------------------------------------------------------------------------
  //! Propagate updates in the hierarchical structure. Method ran by the
  //! asynchronous thread.
  //!
  //! @param assistant thread doing the propagation
  //----------------------------------------------------------------------------
  void AssistedPropagateUpdates(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Queue containerIds and size to update in thein the hierarchical structure. Method ran by the
  //! asynchronous thread.
  //!
  //! @param assistant thread doing the propagation
  //----------------------------------------------------------------------------
  void AssistedQueueForUpdate(ThreadAssistant& assistant) noexcept;

  //! Safety ceiling on the ancestor walk in the queue and recompute paths;
  //! same value as the original hard-coded limit, just shared.
  static constexpr uint16_t kMaxAncestorWalkDepth = 255;

  //----------------------------------------------------------------------------
  //! Async recompute thread: picks up dirty container IDs and recomputes
  //! their tree values from current namespace state.
  //!
  //! @param assistant thread doing the recompute
  //----------------------------------------------------------------------------
  void AssistedRecomputeDirty(ThreadAssistant& assistant) noexcept;

  //----------------------------------------------------------------------------
  //! Recompute a single container's tree values from current namespace state.
  //! Reads all files and child containers, computes correct absolute values.
  //! If the parent is not in the dirty set, queues a delta correction for the
  //! parent via the normal accounting pipeline.
  //!
  //! @param id container ID to recompute
  //! @param dirtySet the full set of dirty container IDs (to avoid
  //!        propagating deltas to parents that will be recomputed themselves)
  //----------------------------------------------------------------------------
  void recomputeContainer(IContainerMD::id_t id,
                          const std::unordered_set<IContainerMD::id_t>& dirtySet);

  //----------------------------------------------------------------------------
  //! Recompute every container in `batch` in leaf-to-root order so each
  //! parent reads its children's already-corrected tree values. Honours
  //! `assistant.terminationRequested()` between containers.
  //!
  //! @param batch container IDs to recompute
  //! @param assistant thread doing the recompute
  //----------------------------------------------------------------------------
  void recomputeBatchLeafToRoot(const std::unordered_set<IContainerMD::id_t>& batch,
                                ThreadAssistant& assistant);

  //! Update structure containing the nodes that need an update. We try to
  //! optimise the number of updates to the backend by computing the final
  //! size deltas from a number of individual updates.
  struct UpdateT {
    std::unordered_map<IContainerMD::id_t, TreeInfos> mMap; ///< Map updates
  };

  //! Vector of two elements containing the batch which is currently being
  //! accumulated and the batch which is being committed to the namespace by
  //! the asynchronous thread
  std::vector<UpdateT> mBatch;
  std::mutex mMutexBatch; ///< Mutex protecting access to the updates batch
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index o the batch committing updates
  AssistedThread mThread; ///< Thread updating the namespace
  AssistedThread mQueueForUpdateThread; ///< Thread update queueing thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< container MD service
  eos::common::ConcurrentQueue<std::pair<IContainerMD::id_t, TreeInfos>>
      mIdTreeInfosToUpdateQueue; ///< Queue containing containerIds and their
                                 ///< corresponding infos to update

  //! @name Recompute fencing (prevents race between recompute_tree_size and rename)
  //! @{
  eos::common::RWMutex mFenceMutex; ///< Protects mFencedIds and mDirtyJournal
  std::unordered_set<IContainerMD::id_t> mFencedIds; ///< Containers being recomputed
  std::unordered_set<IContainerMD::id_t>
      mDirtyJournal; ///< IDs of fenced containers that received deltas; need recompute
  std::atomic<bool> mDrainRequested{false};        ///< Skip sleep in PropagateUpdates
  std::atomic<uint64_t> mPropagationCycleCount{0}; ///< Drain synchronization counter
  std::mutex mDrainCompleteMutex;                  ///< Protects mDrainCompleteCV
  std::condition_variable mDrainCompleteCV; ///< Notified after each propagation cycle
  std::atomic<bool> mRecomputeInFlight{
      false}; ///< Serializes RecomputeSubtreeWithFencing; cleared by unfence()
  //! @}

  //! @name Async dirty container recompute
  //! @{
  IFileMDSvc* mFileMDSvc;                    ///< File MD service
  AssistedThread mDirtyRecomputeThread;      ///< Thread recomputing dirty containers
  std::mutex mDirtyRecomputeMutex;           ///< Protects mPendingDirtyRecompute
  std::condition_variable mDirtyRecomputeCV; ///< Signals new dirty IDs available
  std::unordered_set<IContainerMD::id_t> mPendingDirtyRecompute; ///< Dirty container IDs
  //! @}
};

EOSNSNAMESPACE_END
