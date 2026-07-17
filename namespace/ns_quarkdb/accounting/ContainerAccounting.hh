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
#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include <condition_variable>
#include <limits>
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
  //! Set of container ids
  using ContIdSet = std::unordered_set<IContainerMD::id_t>;

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
  //! Synchronously apply all tree info updates queued so far: wait until
  //! everything already in the queue has been accumulated into the batch
  //! (FIFO barrier) and then propagate the accumulated deltas to the
  //! containers. When this returns true, every update queued before the call
  //! is reflected in the namespace.
  //!
  //! @note Must not be called while holding an MD lock on a container that
  //!       may have pending updates, otherwise it can deadlock against the
  //!       propagation applying those updates.
  //!
  //! @return true if the barrier was acknowledged, i.e. the full guarantee
  //!         above holds. False if the queueing thread has already exited
  //!         (shutting down): nothing is propagated in that case, since the
  //!         deltas still sitting in the queue can no longer be consumed and
  //!         persisting the batch without them would write values that no
  //!         later update would repair. Callers must give up, not carry on.
  //----------------------------------------------------------------------------
  bool Flush();

  //----------------------------------------------------------------------------
  //! Start recording the ids of the containers tree info deltas get applied
  //! to (see PropagateUpdatesOnce). Clears any previously recorded ids and
  //! replaces the containers under recompute. Serialized with the propagation
  //! cycles: every delta applied after this method returns is guaranteed to be
  //! recorded. Used by the tree size recompute to detect the containers it
  //! raced with.
  //!
  //! @param cont_ids_under_recompute ids of the containers the caller is
  //!        recomputing. Only deltas applied to those are recorded: a
  //!        propagation cycle applies deltas originating from the whole
  //!        namespace, while only a container whose absolute value the caller
  //!        overwrites can be corrupted by a racing delta. Recording the rest
  //!        would size the recorded set by the instance-wide activity during
  //!        the recompute instead of by the recompute's own work. An empty set
  //!        therefore records nothing.
  //----------------------------------------------------------------------------
  void StartRecordingUpdatedContIds(ContIdSet cont_ids_under_recompute);

  //----------------------------------------------------------------------------
  //! Stop recording updated container ids and drop both the recorded set and
  //! the containers under recompute
  //----------------------------------------------------------------------------
  void StopRecordingUpdatedContIds();

  //----------------------------------------------------------------------------
  //! Return the set of container ids deltas were applied to since recording
  //! started (or since the previous call) and clear it. Always a subset of the
  //! containers under recompute passed to StartRecordingUpdatedContIds(), so
  //! the caller does not have to intersect it back with them. Empty when not
  //! recording.
  //----------------------------------------------------------------------------
  ContIdSet TakeUpdatedContIds();

  //----------------------------------------------------------------------------
  //! RAII scope starting/stopping the recording of updated container ids
  //----------------------------------------------------------------------------
  class UpdatedContIdsRecordingScope {
  public:
    UpdatedContIdsRecordingScope(QuarkContainerAccounting& acc,
                                 ContIdSet cont_ids_under_recompute)
        : mAcc(acc)
    {
      mAcc.StartRecordingUpdatedContIds(std::move(cont_ids_under_recompute));
    }

    //--------------------------------------------------------------------------
    //! Restart the recording with a new set of containers under recompute
    //--------------------------------------------------------------------------
    void
    Restart(ContIdSet cont_ids_under_recompute)
    {
      mAcc.StartRecordingUpdatedContIds(std::move(cont_ids_under_recompute));
    }

    ~UpdatedContIdsRecordingScope() { mAcc.StopRecordingUpdatedContIds(); }

    UpdatedContIdsRecordingScope(const UpdatedContIdsRecordingScope&) = delete;
    UpdatedContIdsRecordingScope& operator=(const UpdatedContIdsRecordingScope&) = delete;

  private:
    QuarkContainerAccounting& mAcc;
  };

private:
  //! Queue sentinel used by Flush() as a FIFO barrier: once popped by the
  //! queueing thread, everything enqueued before it has been accumulated.
  //! Note: id 0 is already used as the stop-thread sentinel.
  static constexpr IContainerMD::id_t sFlushBarrierId =
      std::numeric_limits<IContainerMD::id_t>::max();

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
  //! Walk up the parent chain of the given container and accumulate the
  //! tree info delta into the batch for the container and all its ancestors
  //!
  //! @param id container id where the change originated
  //! @param treeInfos tree info delta to accumulate
  //----------------------------------------------------------------------------
  void AccumulateUpdate(IContainerMD::id_t id, const TreeInfos& treeInfos);

  //----------------------------------------------------------------------------
  //! Perform one propagation cycle: swap the accumulate/commit batches and
  //! apply the committed deltas to the containers. Serialized with mFlushMutex
  //! so the periodic thread and Flush() never apply concurrently; outside of
  //! this method the commit batch is always empty, hence a single call
  //! applies everything accumulated so far.
  //----------------------------------------------------------------------------
  void PropagateUpdatesOnce();

  //----------------------------------------------------------------------------
  //! Push a barrier sentinel onto the update queue and wait until the queueing
  //! thread acknowledges it, i.e. until everything enqueued before the barrier
  //! has been accumulated into the batch. Called by Flush(), which holds
  //! mDrainMutex, so at most one barrier is ever in flight.
  //!
  //! @return true if the barrier was acknowledged, false if the queueing
  //!         thread has exited and no consumer can acknowledge it
  //----------------------------------------------------------------------------
  bool WaitForBarrier();

  //----------------------------------------------------------------------------
  //! Mark the queueing thread as stopped and release any Flush() blocked on a
  //! barrier that will now never be acknowledged
  //----------------------------------------------------------------------------
  void MarkQueueThreadStopped();

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
  std::mutex mFlushMutex; ///< Serializes batch propagation (thread vs Flush)
  std::mutex mDrainMutex; ///< Serializes concurrent Flush() callers
  std::mutex mBarrierMutex;           ///< Protects the two barrier flags below
  std::condition_variable mBarrierCv; ///< Signals a barrier acknowledgement
  //! Set by the queueing thread when it pops the sentinel of the Flush() in
  //! progress. A single slot is enough: Flush() callers hold mDrainMutex.
  bool mBarrierReached{false};
  //! Set when the queueing thread exits. No consumer is left to acknowledge a
  //! barrier, so a Flush() waiting for one (or arriving later) must not block.
  bool mQueueThreadStopped{false};
  //! Ids of the containers the recompute is currently rewriting with absolute
  //! values. Deltas applied to any other container cannot corrupt it and are
  //! not recorded. Empty means recording is off. Guarded by mFlushMutex, which
  //! a propagation cycle holds throughout, so it stays constant for a cycle.
  ContIdSet mContIdsUnderRecompute;
  //! Ids of the containers under recompute deltas were applied to while
  //! recording, i.e. the ones the recompute raced with. Subset of
  //! mContIdsUnderRecompute. Guarded by mFlushMutex: written by a propagation
  //! cycle, taken by the recompute.
  ContIdSet mUpdatedContIds;
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index o the batch committing updates
  AssistedThread mThread; ///< Thread updating the namespace
  AssistedThread mQueueForUpdateThread; ///< Thread update queueing thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< container MD service
  eos::common::ConcurrentQueue<std::pair<IContainerMD::id_t, TreeInfos>>  mIdTreeInfosToUpdateQueue; ///< Queue containing containerIds and their corresponding infos to update
};

EOSNSNAMESPACE_END
