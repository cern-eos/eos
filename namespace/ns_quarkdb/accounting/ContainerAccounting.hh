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
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Source of a tree size delta, used to distinguish file changes from directory
//! moves so that the recompute fencing can selectively re-apply deltas
//------------------------------------------------------------------------------
enum class DeltaSource : uint8_t {
  kFileChange = 0, ///< File creation, deletion, or size change
  kRename = 1      ///< Directory/file move (rename operations)
};

//------------------------------------------------------------------------------
//! RAII scope guard that sets the thread-local DeltaSource for all accounting
//! events generated on the current thread. Use this in rename/move operations
//! so that addContainer/removeContainer/addFile/removeFile events are tagged
//! as kRename instead of the default kFileChange.
//!
//! Example:
//!   {
//!     DeltaSourceScope moveScope(DeltaSource::kRename);
//!     dir->removeContainer("child");
//!     newdir->addContainer(child.get());
//!   } // automatically resets to kFileChange
//------------------------------------------------------------------------------
class DeltaSourceScope {
public:
  explicit DeltaSourceScope(DeltaSource source);
  ~DeltaSourceScope();
  DeltaSourceScope(const DeltaSourceScope&) = delete;
  DeltaSourceScope& operator=(const DeltaSourceScope&) = delete;

private:
  DeltaSource mPrevious;
};

//------------------------------------------------------------------------------
//! Get the thread-local DeltaSource that fileMDChanged should use for tagging
//! events on the current thread. Defaults to kFileChange.
//------------------------------------------------------------------------------
DeltaSource getThreadLocalDeltaSource();

//------------------------------------------------------------------------------
//! Tree size deltas tagged by source. Allows the fencing journal to track
//! file-change and tree-move deltas separately.
//------------------------------------------------------------------------------
struct TaggedTreeInfos {
  TreeInfos fileChanges; ///< Accumulated file-change deltas
  TreeInfos renameDeltas; ///< Accumulated rename deltas
  bool hadFileChangeActivity =
      false; ///< True if any kFileChange event touched this entry
};

//------------------------------------------------------------------------------
//! Container subtree accounting listener
//------------------------------------------------------------------------------
class QuarkContainerAccounting : public IFileMDChangeListener
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param svc container metadata service
  //! @param update_interval interval in seconds when updates are propagated
  //----------------------------------------------------------------------------
  QuarkContainerAccounting(IContainerMDSvc* svc,
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
  void QueueForUpdate(IContainerMD::id_t pid, TreeInfos treeInfos,
                      DeltaSource source = DeltaSource::kFileChange);

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
  //! Fence a set of container IDs. While fenced, PropagateUpdates will divert
  //! deltas for these containers into a journal instead of applying them to the
  //! namespace. This is used by recompute_tree_size to prevent stale deltas
  //! from corrupting the recomputed absolute values.
  //!
  //! @param ids set of container IDs to fence
  //----------------------------------------------------------------------------
  void fenceContainers(const std::unordered_set<IContainerMD::id_t>& ids);

  //----------------------------------------------------------------------------
  //! Drain all in-flight deltas: wait for the concurrent queue to empty and
  //! for PropagateUpdates to complete two full cycles. After this returns, all
  //! pre-existing deltas for fenced containers are in the journal.
  //----------------------------------------------------------------------------
  void drainFencedDeltas();

  //----------------------------------------------------------------------------
  //! Discard all deltas currently held in the recompute journal.
  //----------------------------------------------------------------------------
  void discardJournal();

  //----------------------------------------------------------------------------
  //! Collect file-change deltas from the journal and clear the journal.
  //! Tree-move deltas (from AddTree/RemoveTree) are discarded. The returned
  //! deltas can be re-applied after unfencing to preserve file changes that
  //! occurred during the BFS recompute.
  //!
  //! @return map of container ID to file-change TreeInfos deltas
  //----------------------------------------------------------------------------
  std::unordered_map<IContainerMD::id_t, TreeInfos> collectFileChangeDeltas();

  //----------------------------------------------------------------------------
  //! Collect the set of container IDs that had file-change deltas in the
  //! journal, then clear the journal. Rename deltas are ignored.
  //! Unlike collectFileChangeDeltas(), this returns only the container IDs
  //! (not the delta values), allowing the caller to recompute those containers
  //! from current state rather than re-applying net deltas.
  //!
  //! @return set of container IDs that had file-change activity during fencing
  //----------------------------------------------------------------------------
  std::unordered_set<IContainerMD::id_t> collectDirtyContainerIds();

  //----------------------------------------------------------------------------
  //! Clear the fenced set and discard the journal, resuming normal delta
  //! propagation for all containers.
  //----------------------------------------------------------------------------
  void unfence();

  //----------------------------------------------------------------------------
  //! Set the file metadata service pointer, needed for async recompute of
  //! dirty containers (iterating files to compute tree size).
  //!
  //! @param svc file metadata service
  //----------------------------------------------------------------------------
  void setFileMDSvc(IFileMDSvc* svc);

  //----------------------------------------------------------------------------
  //! Schedule an asynchronous recompute of a set of dirty containers.
  //! Each container is re-read from current namespace state (files + children)
  //! and its tree values are set to the correct absolute values. The delta
  //! between old and new values is propagated to ancestors via the normal
  //! accounting pipeline. This provides eventual consistency for containers
  //! whose tree values may have been captured in an intermediate state by BFS.
  //!
  //! @param ids set of container IDs to recompute
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

  //! Update structure containing the nodes that need an update. We try to
  //! optimise the number of updates to the backend by computing the final
  //! size deltas from a number of individual updates.
  struct UpdateT {
    std::unordered_map<IContainerMD::id_t, TaggedTreeInfos> mMap; ///< Map updates
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
  //! Entry in the update queue, carrying the delta source tag
  struct QueueEntry {
    IContainerMD::id_t id = 0;
    TreeInfos delta;
    DeltaSource source = DeltaSource::kFileChange;
    QueueEntry() = default;
    QueueEntry(IContainerMD::id_t i, TreeInfos d, DeltaSource s)
        : id(i)
        , delta(d)
        , source(s)
    {
    }
  };
  eos::common::ConcurrentQueue<QueueEntry>
      mIdTreeInfosToUpdateQueue; ///< Queue containing containerIds and their
                                 ///< corresponding infos to update

  //! @name Recompute fencing (prevents race between recompute_tree_size and rename)
  //! @{
  eos::common::RWMutex mFenceMutex; ///< Protects mFencedIds and mRecomputeJournal
  std::unordered_set<IContainerMD::id_t> mFencedIds; ///< Containers being recomputed
  std::unordered_map<IContainerMD::id_t, TaggedTreeInfos>
      mRecomputeJournal;                           ///< Diverted deltas (tagged by source)
  std::atomic<bool> mDrainRequested{false};        ///< Skip sleep in PropagateUpdates
  std::atomic<uint64_t> mPropagationCycleCount{0}; ///< Drain synchronization counter
  std::mutex mDrainCompleteMutex;                  ///< Protects mDrainCompleteCV
  std::condition_variable mDrainCompleteCV; ///< Notified after each propagation cycle
  //! @}

  //! @name Async dirty container recompute
  //! @{
  IFileMDSvc* mFileMDSvc{nullptr};           ///< File MD service (set via setFileMDSvc)
  AssistedThread mDirtyRecomputeThread;      ///< Thread recomputing dirty containers
  std::mutex mDirtyRecomputeMutex;           ///< Protects mPendingDirtyRecompute
  std::condition_variable mDirtyRecomputeCV; ///< Signals new dirty IDs available
  std::unordered_set<IContainerMD::id_t> mPendingDirtyRecompute; ///< Dirty container IDs
  //! @}
};

EOSNSNAMESPACE_END
