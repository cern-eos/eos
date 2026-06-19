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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingService.hh"
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Container subtree accounting listener
//------------------------------------------------------------------------------
class QuarkContainerAccounting : public IFileMDChangeListener,
                                 public ITreeSizeAccountingService {
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
  //! @param subtreeRootId root id of the subtree producing this delta
  //! @param treeAccounting tree accounting information to be updated
  //----------------------------------------------------------------------------
  void AddTree(IContainerMD* obj, uint64_t subtreeRootId, TreeInfos treeAccounting);

  //----------------------------------------------------------------------------
  //! Remove tree
  //!
  //! @param obj container where the tree should be removed from
  //! @param subtreeRootId root id of the subtree producing this delta
  //! @param treeAccounting tree accounting information to be updated
  //----------------------------------------------------------------------------
  void RemoveTree(IContainerMD* obj, uint64_t subtreeRootId, TreeInfos treeAccounting);

  //----------------------------------------------------------------------------
  //! Queue info for update
  //!
  //! @param pid container id
  //! @param dsize size change
  //----------------------------------------------------------------------------
  void QueueForUpdate(IContainerMD::id_t pid, TreeInfos treeInfos);

  //----------------------------------------------------------------------------
  //! Start a raw tree-size journal capture session
  //----------------------------------------------------------------------------
  std::unique_ptr<TreeSizeJournalCaptureScope> StartTreeSizeJournalCapture() override;

  //----------------------------------------------------------------------------
  //! Acquire a short publish fence for covered tree-size accounting updates
  //----------------------------------------------------------------------------
  TreeSizeAccountingFenceStats
  AcquireTreeSizeAccountingFence(const TreeSizeAccountingFenceRequest& request) override;

  //----------------------------------------------------------------------------
  //! Release the active publish fence
  //----------------------------------------------------------------------------
  TreeSizeAccountingFenceStats
  ReleaseTreeSizeAccountingFence(TreeSizeAccountingFenceReleaseMode mode) override;

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

private:
  struct TreeSizeAccountingUpdate {
    IContainerMD::id_t id = 0;
    TreeInfos treeInfos;
    bool hasAccountingMetadata = false;
    TreeSizeAccountingEvent accountingEvent;
  };

  struct TreeSizeAccountingFenceState {
    bool active = false;
    TreeSizeAccountingFenceRequest request;
    TreeSizeAccountingFenceStats stats;
    std::vector<TreeSizeAccountingUpdate> directUpdates;
    std::vector<TreeSizeAccountingUpdate> includedInPublishUpdates;
    std::vector<TreeSizeAccountingUpdate> replayAfterPublishUpdates;
    std::vector<TreeSizeAccountingUpdate> inFlightReplayAfterPublishUpdates;
  };

  void QueueForUpdate(IContainerMD::id_t pid, TreeInfos treeInfos,
                      std::optional<TreeSizeAccountingEvent> accounting_event);
  void CaptureTreeSizeJournalEntry(
      const TreeInfos& treeInfos,
      const std::optional<TreeSizeAccountingEvent>& accounting_event);
  std::vector<TreeSizeAccountingUpdate>
  BuildPropagationUpdates(const TreeSizeAccountingUpdate& update) const;
  void QueueBatchUpdateLocked(const TreeSizeAccountingUpdate& update);
  void RouteFenceOrBatchUpdateLocked(const TreeSizeAccountingUpdate& update);
  void
  RouteFenceOrBatchUpdatesLocked(const std::vector<TreeSizeAccountingUpdate>& updates);
  void DrainRawQueueLocked(std::unique_lock<std::mutex>& fence_lock);
  bool DrainFenceDirectUpdatesLocked(std::unique_lock<std::mutex>& fence_lock);
  void DrainBatchUpdatesLocked();
  void ReplayFenceUpdatesLocked(const std::vector<TreeSizeAccountingUpdate>& updates);
  void RegisterRawUpdateInFlight();
  void UnregisterRawUpdateInFlight();
  void
  RegisterInFlightUpdatesLocked(const std::vector<TreeSizeAccountingUpdate>& updates);
  void UnregisterInFlightUpdates(const std::vector<TreeSizeAccountingUpdate>& updates);
  void ClassifyInFlightUpdatesLocked();
  uint64_t CountCoveredInFlightUpdatesLocked() const;
  bool WaitForFenceDrainLocked(std::unique_lock<std::mutex>& fence_lock);

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

  //! Update structure containing the nodes that need an update. We try to
  //! optimise the number of updates to the backend by computing the final
  //! size deltas from a number of individual updates.
  struct UpdateT {
    std::vector<TreeSizeAccountingUpdate> mUpdates; ///< Pending target updates
  };

  //! Vector of two elements containing the batch which is currently being
  //! accumulated and the batch which is being committed to the namespace by
  //! the asynchronous thread
  std::vector<UpdateT> mBatch;
  std::mutex mMutexBatch; ///< Mutex protecting access to the updates batch
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index o the batch committing updates
  std::mutex mMutexFence;              ///< Mutex protecting publish fence state
  std::condition_variable mFenceCv;    ///< Notifies fence state changes
  TreeSizeAccountingFenceState mFence; ///< Active publish fence state
  uint64_t mRawUpdatesInFlight = 0;    ///< Direct updates being expanded
  std::vector<TreeSizeAccountingUpdate> mInFlightUpdates; ///< Updates being propagated
  AssistedThread mThread; ///< Thread updating the namespace
  AssistedThread mQueueForUpdateThread; ///< Thread update queueing thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< container MD service
  TreeSizeJournalCaptureController
      mTreeSizeJournalCapture; ///< Optional journal capture controller
  eos::common::ConcurrentQueue<TreeSizeAccountingUpdate>
      mIdTreeInfosToUpdateQueue; ///< Queue containing direct container updates
};

EOSNSNAMESPACE_END
