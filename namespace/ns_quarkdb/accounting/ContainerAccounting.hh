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
#include <algorithm>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

namespace container_accounting {

//! Accounting queue item
struct QueuedUpdate {
  enum class Type { Update, Barrier, Stop };

  QueuedUpdate() = default;
  QueuedUpdate(IContainerMD::id_t id, TreeInfos treeInfos)
      : mType(Type::Update)
      , mId(id)
      , mTreeInfos(treeInfos)
  {
  }
  explicit QueuedUpdate(Type type)
      : mType(type)
  {
  }
  explicit QueuedUpdate(std::shared_ptr<std::promise<void>> barrier)
      : mType(Type::Barrier)
      , mBarrier(std::move(barrier))
  {
  }

  Type mType = Type::Update;  ///< Queue item kind: update, flush barrier or stop
  IContainerMD::id_t mId = 0; ///< Container id whose ancestors need accounting
  TreeInfos mTreeInfos;       ///< Tree accounting delta associated with mId
  std::shared_ptr<std::promise<void>> mBarrier; ///< Flush synchronization marker
};

//! Active tree-size recompute coverage
struct TreeSizeRecomputeContext {
  explicit TreeSizeRecomputeContext(std::vector<IContainerMD::id_t> ids)
      : mIds(std::move(ids))
  {
    std::sort(mIds.begin(), mIds.end());
    mIds.erase(std::unique(mIds.begin(), mIds.end()), mIds.end());
  }

  bool
  Contains(IContainerMD::id_t id) const
  {
    return std::binary_search(mIds.begin(), mIds.end(), id);
  }

  void
  AddSkippedDelta(IContainerMD::id_t id, TreeInfos treeInfos)
  {
    auto it = mSkippedDeltas.find(id);

    if (it != mSkippedDeltas.end()) {
      it->second += treeInfos;
    } else {
      mSkippedDeltas.emplace(id, treeInfos);
    }
  }

  std::vector<IContainerMD::id_t> mIds; ///< Sorted recompute-covered container ids
  //! Covered deltas skipped during current recompute attempt
  std::unordered_map<IContainerMD::id_t, TreeInfos> mSkippedDeltas;
  bool mDirty = false; ///< True once accounting touched the recompute coverage
};

} // namespace container_accounting

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
  //! Flush queued tree-size accounting updates
  //----------------------------------------------------------------------------
  void FlushTreeSizeUpdates(bool is_admin_recompute = false) override;

  //----------------------------------------------------------------------------
  //! Start a best-effort tree-size recompute window
  //!
  //! Only one recompute context can be active. One can use epoch numbers
  //! instead of the boolean if there is a need to have different
  //! recompute contexts.
  //----------------------------------------------------------------------------
  bool StartTreeSizeRecompute(std::vector<IContainerMD::id_t> ids) override;

  //----------------------------------------------------------------------------
  //! Reset dirty marker for a recompute window
  //----------------------------------------------------------------------------
  void ResetTreeSizeRecomputeDirty() override;

  //----------------------------------------------------------------------------
  //! Check whether accounting touched the recompute window
  //----------------------------------------------------------------------------
  bool IsTreeSizeRecomputeDirty() const override;

  //----------------------------------------------------------------------------
  //! Atomically finish a recompute window if it is still clean
  //----------------------------------------------------------------------------
  bool TryFinishTreeSizeRecompute() override;

  //----------------------------------------------------------------------------
  //! Abort a recompute window
  //----------------------------------------------------------------------------
  void AbortTreeSizeRecompute(bool requeue_skipped_deltas = false) override;

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

  //----------------------------------------------------------------------------
  //! Queue one delta for all ancestors of the given container
  //----------------------------------------------------------------------------
  void QueueAncestorsForUpdate(IContainerMD::id_t id, TreeInfos treeInfos);

  //----------------------------------------------------------------------------
  //! Propagate one accumulated batch
  //----------------------------------------------------------------------------
  void PropagateUpdatesOnce(bool is_admin_recompute = false);

  //----------------------------------------------------------------------------
  //! Mark recompute context dirty if the update is covered by admin recompute
  //----------------------------------------------------------------------------
  bool MarkDirtyIfInAdminRecomputeContext(IContainerMD::id_t id, TreeInfos treeInfos);

  //! Update structure containing the nodes that need an update. We try to
  //! optimise the number of updates to the backend by computing the final
  //! size deltas from a number of individual updates.
  struct UpdateT {
    std::unordered_map<IContainerMD::id_t, TreeInfos> mMap; ///< Map updates
  };

  //----------------------------------------------------------------------------
  //! Merge source update batch into destination update batch
  //----------------------------------------------------------------------------
  void MergeBatch(const UpdateT& src, UpdateT& dst);

  //----------------------------------------------------------------------------
  //! Merge one accounting delta into destination update batch
  //----------------------------------------------------------------------------
  void MergeDelta(IContainerMD::id_t id, TreeInfos treeInfos, UpdateT& dst);

  //----------------------------------------------------------------------------
  //! Merge skipped recompute deltas into destination update batch
  //----------------------------------------------------------------------------
  void MergeDeltas(const std::unordered_map<IContainerMD::id_t, TreeInfos>& src,
                   UpdateT& dst);

  //----------------------------------------------------------------------------
  //! Get active recompute context
  //----------------------------------------------------------------------------
  std::shared_ptr<const container_accounting::TreeSizeRecomputeContext>
  GetRecomputeContext() const;

  //! Vector of two elements containing the batch which is currently being
  //! accumulated and the batch which is being committed to the namespace by
  //! the asynchronous thread
  std::vector<UpdateT> mBatch;
  std::mutex mMutexBatch; ///< Mutex protecting access to the updates batch
  //! Serialize propagation passes. This is intentionally separate from
  //! mMutexBatch so queueing can keep accumulating new deltas while a
  //! propagation pass performs container lookups, locking and store updates.
  std::mutex mMutexPropagate;
  uint8_t mAccumulateIndx; ///< Index of the batch accumulating updates
  uint8_t mCommitIndx; ///< Index o the batch committing updates
  AssistedThread mThread; ///< Thread updating the namespace
  AssistedThread mQueueForUpdateThread; ///< Thread update queueing thread
  uint32_t mUpdateIntervalSec; ///< Interval in seconds when updates are pushed
  IContainerMDSvc* mContainerMDSvc; ///< container MD service
  eos::common::ConcurrentQueue<container_accounting::QueuedUpdate>
      mIdTreeInfosToUpdateQueue;      ///< Queue containing containerIds and their
                                      ///< corresponding infos to update
  mutable std::mutex mMutexRecompute; ///< Mutex protecting recompute context
  std::shared_ptr<container_accounting::TreeSizeRecomputeContext>
      mActiveRecompute;                         ///< Active recompute context
};

EOSNSNAMESPACE_END
