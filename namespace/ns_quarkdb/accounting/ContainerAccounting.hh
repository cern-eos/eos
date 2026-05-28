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
#include "namespace/ns_quarkdb/accounting/ContainerAccountingTypes.hh"
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
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
  //! @param id container id where the tree information should be added
  //! @param treeAccounting tree accounting information to be updated
  //----------------------------------------------------------------------------
  void AddTree(IContainerMD::id_t id, TreeInfos treeAccounting) override;
  void AddTree(IContainerMD* obj, TreeInfos treeAccounting) override;

  //----------------------------------------------------------------------------
  //! Remove tree
  //!
  //! @param id container id where the tree should be removed from
  //! @param treeAccounting tree accounting information to be removed
  //----------------------------------------------------------------------------
  void RemoveTree(IContainerMD::id_t id, TreeInfos treeAccounting) override;
  void RemoveTree(IContainerMD* obj, TreeInfos treeAccounting) override;

  //----------------------------------------------------------------------------
  //! Move tree accounting from one parent to another
  //!
  //! @param oldParent old parent container
  //! @param newParent new parent container
  //! @param moved moved container
  //! @param treeAccounting tree accounting information to be moved
  //----------------------------------------------------------------------------
  void MoveTree(IContainerMD::id_t oldParentId, IContainerMD::id_t newParentId,
                IContainerMD::id_t movedId, TreeInfos treeAccounting) override;
  void MoveTree(IContainerMD* oldParent, IContainerMD* newParent, IContainerMD* moved,
                TreeInfos treeAccounting) override;

  //----------------------------------------------------------------------------
  //! Get current accounting sequence
  //----------------------------------------------------------------------------
  uint64_t GetAccountingSequence() const override;

  //----------------------------------------------------------------------------
  //! Set absolute tree accounting if accounting stream is unchanged
  //!
  //! @param obj container where the tree information should be reset
  //! @param treeAccounting absolute tree accounting values
  //! @param accountingSequence expected accounting sequence
  //!
  //! @return true if the reset was handled
  //----------------------------------------------------------------------------
  bool SetTreeIfAccountingUnchanged(IContainerMD::id_t id, TreeInfos treeAccounting,
                                    uint64_t accountingSequence) override;
  bool SetTreeIfAccountingUnchanged(IContainerMD* obj, TreeInfos treeAccounting,
                                    uint64_t accountingSequence) override;

  //----------------------------------------------------------------------------
  //! Reserve an accounting delta sequence before a metadata change becomes visible
  //----------------------------------------------------------------------------
  IFileMDChangeListener::ReservedAccountingDelta
  ReserveAccountingDelta(IContainerMD::id_t containerId, TreeInfos treeInfos) override;

  //----------------------------------------------------------------------------
  //! Publish a previously reserved accounting delta
  //----------------------------------------------------------------------------
  void PublishAccountingDelta(
      const IFileMDChangeListener::ReservedAccountingDelta& delta) override;

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

  using AccountingSequence = container_accounting::AccountingSequence;
  using AccountingEvent = container_accounting::AccountingEvent;
  using CommitOperation = container_accounting::CommitOperation;
  using CommitOperationType = container_accounting::CommitOperationType;
  using EventType = container_accounting::EventType;
  using ParentMove = container_accounting::ParentMove;
  using UpdateBatch = container_accounting::UpdateBatch;

  //----------------------------------------------------------------------------
  //! Get the next accounting sequence number
  //----------------------------------------------------------------------------
  AccountingSequence NextSequence();

  //----------------------------------------------------------------------------
  //! Record a container parent change
  //----------------------------------------------------------------------------
  void RecordMove(AccountingSequence sequence, IContainerMD::id_t movedId,
                  IContainerMD::id_t oldParentId, IContainerMD::id_t newParentId);

  //----------------------------------------------------------------------------
  //! Return the parent id a container had at the given accounting sequence
  //----------------------------------------------------------------------------
  IContainerMD::id_t ParentAt(IContainerMD::id_t id, AccountingSequence sequence);

  //----------------------------------------------------------------------------
  //! Move pending delta map to ordered commit operations
  //----------------------------------------------------------------------------
  void FlushDeltasToOperations(UpdateBatch& batch);

  //! Vector of two elements containing the batch which is currently being
  //! accumulated and the batch which is being committed to the namespace by
  //! the asynchronous thread
  std::vector<UpdateBatch> mBatch;
  std::mutex mMutexBatch; ///< Mutex protecting access to the updates batch
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index o the batch committing updates
  AssistedThread mThread; ///< Thread updating the namespace
  AssistedThread mQueueForUpdateThread; ///< Thread update queueing thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< container MD service

  std::atomic<AccountingSequence> mSequence{0};      ///< Order of all accounting events
  std::atomic<AccountingSequence> mDeltaSequence{0}; ///< Latest queued accounting delta
  std::atomic<AccountingSequence> mAppliedDeltaSequence{
      0};                         ///< Latest applied accounting delta
  std::mutex mMutexEventQueue;    ///< Mutex protecting multi-event queue operations
  std::mutex mMutexParentHistory; ///< Mutex protecting parent history
  std::unordered_map<IContainerMD::id_t, std::vector<ParentMove>>
      mParentHistory; ///< Parent changes indexed by moved container id
  eos::common::ConcurrentQueue<AccountingEvent>
      mAccountingQueue; ///< Queue containing accounting events to update
};

EOSNSNAMESPACE_END
