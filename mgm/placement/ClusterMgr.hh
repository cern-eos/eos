//------------------------------------------------------------------------------
//! @file ClusterMgr.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "common/concurrency/AtomicUniquePtr.h"
#include "common/concurrency/RCULite.hh"
#include "mgm/placement/ClusterDataTypes.hh"
#include "mgm/placement/SelectionStrategy.hh"
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace eos::mgm::placement {

//! RCUMutexT is compatible with the std::shared_mutex api and can therefore be
//! used with std::shared_lock and std::unique_lock. It may be swapped for
//! anything else conforming to that api.
using RCUMutexT = eos::common::RCUMutexT<>;

//------------------------------------------------------------------------------
//! Struct FillLimitsConfig - the configured fill thresholds of one space, the
//! authoritative plain-value copy of what "space config" set. Distinct from
//! the live atomic FillLimits a snapshot carries: this is what the ClusterMgr
//! stamps onto every snapshot it commits, the snapshot's own copy is what
//! placement actually reads.
//------------------------------------------------------------------------------
struct FillLimitsConfig {
  uint8_t cap = kDefaultFillCapPercent;   ///< Placement stops here
  uint8_t warn = kDefaultFillWarnPercent; ///< Weight decay starts here
};

//------------------------------------------------------------------------------
//! Class ClusterMgr - holds the topology snapshot of one space. The snapshot is
//! kept in an atomic unique pointer guarded by an RCU mutex: readers are
//! wait-free, a full topology change swaps in a new snapshot, and per disk
//! status or weight changes mutate the atomics of the current one in place.
//------------------------------------------------------------------------------
class ClusterMgr {
public:
  //----------------------------------------------------------------------------
  //! Struct ClusterDataPtr - RCU read guarded handle to a topology snapshot.
  //! The snapshot stays alive for as long as the handle does.
  //----------------------------------------------------------------------------
  struct ClusterDataPtr {
    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param data_ snapshot to guard
    //! @param rcu_domain_ RCU domain to take the read lock on
    //--------------------------------------------------------------------------
    ClusterDataPtr(ClusterData* data_, RCUMutexT& rcu_domain_)
        : mRlock(rcu_domain_)
        , mData(data_)
    {}

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~ClusterDataPtr() = default;

    //--------------------------------------------------------------------------
    //! Get a reference to the guarded snapshot
    //!
    //! @return snapshot reference
    //--------------------------------------------------------------------------
    const ClusterData&
    operator()() const
    {
      return *mData;
    }

    //--------------------------------------------------------------------------
    //! Get a pointer to the guarded snapshot
    //!
    //! @return snapshot pointer
    //--------------------------------------------------------------------------
    ClusterData*
    operator->() const
    {
      return mData;
    }

    //--------------------------------------------------------------------------
    //! Check if the handle guards a snapshot
    //!
    //! @return true if a snapshot is held, otherwise false
    //--------------------------------------------------------------------------
    operator bool() const { return mData != nullptr; }

  private:
    eos::common::RCUReadLock<RCUMutexT> mRlock; ///< RCU read lock
    ClusterData* mData;                         ///< Guarded snapshot
  };

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ClusterMgr() = default;

  //----------------------------------------------------------------------------
  //! Class SnapshotBuilder - accumulates a topology snapshot and commits it to
  //! its ClusterMgr. Nested in ClusterMgr because the two are one transaction:
  //! the handler is the private draft, the manager the live snapshot it swaps
  //! in. Being a member it reaches ClusterMgr's private AddClusterData directly,
  //! and its constructors are private so the only way to open a build is through
  //! the GetSnapshotBuilder factories below.
  //!
  //! A build is finalized exactly once, either by Commit() (publish the draft)
  //! or Abandon() (discard it, keeping the manager's current snapshot). The
  //! destructor commits any build not yet finalized, so a scope-exit build still
  //! publishes; callers that must not publish a half-built draft on failure call
  //! Abandon() explicitly.
  //----------------------------------------------------------------------------
  class SnapshotBuilder {
  public:
    //--------------------------------------------------------------------------
    //! Destructor, commits the accumulated topology unless it was already
    //! finalized by an explicit Commit() or Abandon()
    //--------------------------------------------------------------------------
    ~SnapshotBuilder() { Commit(); }

    //--------------------------------------------------------------------------
    //! Publish the accumulated topology as the manager's live snapshot. A no-op
    //! if the build was already finalized, so it is safe to call explicitly and
    //! then let the destructor run.
    //--------------------------------------------------------------------------
    void
    Commit()
    {
      if (mFinalized) {
        return;
      }

      mFinalized = true;
      AggregateBucketWeights();
      mClusterMgr.AddClusterData(std::move(mData));
    }

    //--------------------------------------------------------------------------
    //! Discard the accumulated topology without publishing, leaving the
    //! manager's current snapshot in place. Use on a failed incremental update
    //! so a degraded draft never goes live. Finalizes the build; the destructor
    //! will not publish afterwards.
    //--------------------------------------------------------------------------
    void
    Abandon()
    {
      mFinalized = true;
    }

    //--------------------------------------------------------------------------
    //! Add a bucket to the hierarchy
    //!
    //! @param bucket_type type of the bucket, see BucketType
    //! @param bucket_id identifier of the bucket, must not be positive
    //! @param parent_bucket_id identifier of the parent bucket, 0 for the root
    //!
    //! @return true if successful, otherwise false
    //!
    //! @note the parent must already exist, its level is what the new bucket's
    //!       level is derived from
    //! @note refused if the parent already holds children of another kind, see
    //!       Bucket::RecordChildType
    //--------------------------------------------------------------------------
    bool AddBucket(uint8_t bucket_type, ItemIdT bucket_id, ItemIdT parent_bucket_id = 0);

    //--------------------------------------------------------------------------
    //! Get the bucket of the scheduling group with the given index, creating it
    //! below the root if this is the first disk to reach that group.
    //!
    //! The identifier is allocated like any other, below everything already in
    //! use, so a group added to a topology that already holds geo buckets
    //! cannot find its slot taken - which is what a group being registered
    //! into a live snapshot used to run into.
    //!
    //! @param group_index scheduling group index, as the FsView numbers them
    //!
    //! @return identifier of the group bucket, 0 if it could not be created
    //--------------------------------------------------------------------------
    ItemIdT GetOrAddGroup(unsigned int group_index);

    //--------------------------------------------------------------------------
    //! Get the bucket of the scheduling group with the given index
    //!
    //! @param group_index scheduling group index
    //!
    //! @return identifier of the group bucket, 0 if there is none
    //--------------------------------------------------------------------------
    ItemIdT GetGroupBucket(unsigned int group_index) const;

    //--------------------------------------------------------------------------
    //! Get the bucket naming one geotag atom below the given parent, creating it
    //! if this is the first disk to reach that part of the hierarchy
    //!
    //! @param parent_bucket_id identifier of the parent bucket
    //! @param atom geotag atom, one "::" separated component of a geotag
    //! @param bucket_type type to give the bucket if it has to be created
    //!
    //! @return identifier of the bucket, 0 if it could not be created
    //--------------------------------------------------------------------------
    ItemIdT GetOrAddGeoBucket(ItemIdT parent_bucket_id, std::string_view atom,
                              uint8_t bucket_type);

    //--------------------------------------------------------------------------
    //! Add a disk to the given bucket
    //!
    //! @param d disk to add
    //! @param bucket_id identifier of the bucket holding the disk
    //!
    //! @return true if successful, otherwise false
    //!
    //! @note refused if the bucket already holds sub-buckets, see
    //!       Bucket::RecordChildType
    //--------------------------------------------------------------------------
    bool AddDisk(Disk d, ItemIdT bucket_id);

    //--------------------------------------------------------------------------
    //! Add a disk whose id continues the disks array, appending instead of
    //! resizing. Disks are stored at index fsid - 1.
    //!
    //! @param d disk to add
    //! @param bucket_id identifier of the bucket holding the disk
    //!
    //! @return true if successful, otherwise false
    //!
    //! @note refused if the bucket already holds sub-buckets, see
    //!       Bucket::RecordChildType
    //--------------------------------------------------------------------------
    bool AddDiskSequential(Disk d, ItemIdT bucket_id);

    //--------------------------------------------------------------------------
    //! Get the type of the bucket registered under the given identifier
    //!
    //! @param bucket_id identifier to look up, 0 names the root
    //!
    //! @return bucket type, or -1 if no bucket is registered there
    //--------------------------------------------------------------------------
    int GetBucketTypeOf(ItemIdT bucket_id) const;

    //--------------------------------------------------------------------------
    //! Remove a disk from the accumulated topology, leaving the hole shape the
    //! arrays use for an unassigned fsid. The bucket it hung from keeps its
    //! place even if emptied - zero weight keeps it out of the way, and the
    //! next full rebuild prunes it.
    //!
    //! @param fsid file system identifier
    //!
    //! @return true if the disk existed, otherwise false
    //--------------------------------------------------------------------------
    bool RemoveDisk(fsid_t fsid);

  private:
    friend class ClusterMgr;

    //--------------------------------------------------------------------------
    //! Constructor building an empty topology
    //!
    //! @param mgr cluster manager to commit to
    //! @param max_buckets number of buckets to pre-allocate
    //--------------------------------------------------------------------------
    SnapshotBuilder(ClusterMgr& mgr, size_t max_buckets = 256)
        : mClusterMgr(mgr)
    {
      mData.buckets.resize(max_buckets);
    }

    //--------------------------------------------------------------------------
    //! Constructor building on top of an existing topology
    //!
    //! @param mgr cluster manager to commit to
    //! @param data topology to start from
    //--------------------------------------------------------------------------
    SnapshotBuilder(ClusterMgr& mgr, ClusterData&& data)
        : mClusterMgr(mgr)
        , mData(std::move(data))
    {
      // The allocator hands out identifiers below every one already in use, so
      // seeded with existing data its low water mark has to be recovered from
      // it - otherwise a new geo bucket would be given an identifier the
      // existing hierarchy already owns
      for (const auto& bucket : mData.buckets) {
        mLowestBucketId = std::min(mLowestBucketId, bucket.id);
      }
    }

    //--------------------------------------------------------------------------
    //! Reserve an identifier for a bucket the caller does not name itself, ie. a
    //! geo bucket. Identifiers are handed out below every one already in use, so
    //! they never collide with the scheduling groups, which own a fixed range.
    //!
    //! @return unused bucket identifier, always negative
    //--------------------------------------------------------------------------
    ItemIdT AllocBucketId();

    //--------------------------------------------------------------------------
    //! Recompute Bucket::total_weight for the whole hierarchy, bottom up, so
    //! that an interior bucket carries the raw capacity weight of everything
    //! below it and not only of the disks attached directly to it
    //!
    //! @note total_weight is the raw capacity-weight sum, consistent with how
    //!       AddDisk/RemoveDisk maintain it incrementally. Fill decay is applied
    //!       live at disk level and is intentionally not baked in here.
    //! @note called once when the snapshot is committed; AddDisk only maintains
    //!       the weight of a disk's immediate parent
    //--------------------------------------------------------------------------
    void AggregateBucketWeights();

    //--------------------------------------------------------------------------
    //! Sum the weight of one bucket subtree and store it on the bucket
    //!
    //! @param bucket_id bucket to weigh
    //! @param visited marks buckets already weighed, guards against a cycle
    //!
    //! @return total weight of the subtree
    //--------------------------------------------------------------------------
    uint64_t WeighSubtree(ItemIdT bucket_id, std::vector<bool>& visited);

    //--------------------------------------------------------------------------
    //! Get the scheduling group a bucket sits below, walking up the parent
    //! chain, bounded like every parent walk
    //!
    //! @param bucket_id bucket to start from
    //!
    //! @return group identifier, 0 if the bucket is itself a group or no group
    //!         is found above it
    //--------------------------------------------------------------------------
    ItemIdT FindOwningGroup(ItemIdT bucket_id) const;

    //--------------------------------------------------------------------------
    //! Register a disk in the flat leaf view of the group owning its bucket,
    //! creating the view on first use, see Bucket::flat_view. A disk hanging
    //! from its group directly needs no view, the group already is one.
    //!
    //! @param bucket_id bucket the disk was attached to
    //! @param disk_id file system identifier of the disk
    //! @param weight weight of the disk
    //--------------------------------------------------------------------------
    void TrackDiskInFlatView(ItemIdT bucket_id, ItemIdT disk_id, uint8_t weight);

    //--------------------------------------------------------------------------
    //! Remove a disk from the flat leaf view of the group owning its bucket,
    //! the counterpart of TrackDiskInFlatView on the RemoveDisk path
    //!
    //! @param bucket_id bucket the disk was attached to
    //! @param disk_id file system identifier of the disk
    //! @param weight weight of the disk
    //--------------------------------------------------------------------------
    void UntrackDiskFromFlatView(ItemIdT bucket_id, ItemIdT disk_id, uint8_t weight);

    ClusterMgr& mClusterMgr; ///< Manager the topology is committed to
    ClusterData mData;       ///< Topology being accumulated
    //! Lowest bucket identifier handed out so far, the allocator counts down
    //! from here
    ItemIdT mLowestBucketId{0};
    //! Whether the build has been resolved (published or discarded); guards
    //! Commit against a double publish and stops the destructor re-committing
    bool mFinalized{false};
  };

  //----------------------------------------------------------------------------
  //! Get a handler building a brand new topology snapshot
  //!
  //! @param max_buckets number of buckets to pre-allocate
  //!
  //! @return storage handler committing on destruction
  //----------------------------------------------------------------------------
  SnapshotBuilder GetSnapshotBuilder(size_t max_buckets = 256);

  //----------------------------------------------------------------------------
  //! Get a handler seeded with a copy of the current topology snapshot
  //!
  //! @return storage handler committing on destruction
  //----------------------------------------------------------------------------
  SnapshotBuilder GetSnapshotBuilderWithData();

  //----------------------------------------------------------------------------
  //! Get the epoch of the current snapshot, bumped on every change
  //!
  //! @return current epoch
  //----------------------------------------------------------------------------
  EpochIdT
  GetCurrentEpoch() const
  {
    return mCurrentEpoch;
  }

  //----------------------------------------------------------------------------
  //! Get an RCU read guarded handle to the current snapshot
  //!
  //! @return snapshot handle
  //----------------------------------------------------------------------------
  ClusterDataPtr GetClusterData();

  //----------------------------------------------------------------------------
  //! Update the configuration status of a disk in place
  //!
  //! @param disk_id file system identifier
  //! @param status new configuration status
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskStatus(fsid_t disk_id, ConfigStatus status);

  //----------------------------------------------------------------------------
  //! Update the active status of a disk in place
  //!
  //! @param disk_id file system identifier
  //! @param status new active status
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskStatus(fsid_t disk_id, ActiveStatus status);

  //----------------------------------------------------------------------------
  //! Update the weight of a disk in place
  //!
  //! @param disk_id file system identifier
  //! @param weight new weight
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskWeight(fsid_t disk_id, uint8_t weight);

  //----------------------------------------------------------------------------
  //! Update the fill level of a disk in place
  //!
  //! @param disk_id file system identifier
  //! @param percent_used fill level in percent
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskPercentUsed(fsid_t disk_id, uint8_t percent_used);

  //----------------------------------------------------------------------------
  //! Update the free space of a disk in place from an FST publish, reconciling
  //! the bookings made since the last one, see ClusterData::SetDiskFreeSpace
  //!
  //! @param disk_id file system identifier
  //! @param free_bytes free space in bytes
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskFreeSpace(fsid_t disk_id, uint64_t free_bytes);

  //----------------------------------------------------------------------------
  //! Set the configured fill thresholds of this space. This ClusterMgr is the
  //! authoritative owner: the values are remembered and stamped onto every
  //! snapshot committed from now on, and applied to the current snapshot right
  //! away. Validation is the caller's job, see FsScheduler::SetFillLimits.
  //!
  //! @param cap fill level in percent at which a disk stops taking replicas
  //! @param warn fill level in percent at which the weight starts to decay
  //----------------------------------------------------------------------------
  void SetConfiguredFillLimits(uint8_t cap, uint8_t warn);

  //----------------------------------------------------------------------------
  //! Get the configured fill thresholds of this space
  //!
  //! @return configured fill thresholds
  //----------------------------------------------------------------------------
  FillLimitsConfig GetConfiguredFillLimits() const;

  //----------------------------------------------------------------------------
  //! Disable a geotag branch for the given operations. The rule is remembered
  //! (stamped onto every future snapshot) and resolved onto the current one.
  //! Validation is the caller's job, see FsScheduler::AddDisabledBranch.
  //!
  //! @param canonical canonicalized geotag of the branch
  //! @param op_mask operations to disable, see kDisabledPlct / kDisabledAccess
  //!
  //! @return true (the rule set always accepts an add)
  //----------------------------------------------------------------------------
  bool AddDisabledBranch(const std::string& canonical, uint8_t op_mask);

  //----------------------------------------------------------------------------
  //! Re-enable a previously disabled geotag branch for the given operations
  //!
  //! @param canonical canonicalized geotag of the branch
  //! @param op_mask operations to re-enable
  //!
  //! @return true if a matching rule existed, otherwise false
  //----------------------------------------------------------------------------
  bool RmDisabledBranch(const std::string& canonical, uint8_t op_mask);

  //----------------------------------------------------------------------------
  //! Get the configured disabled branch rules of this space
  //!
  //! @return canonical geotag to operations mask, empty if none
  //----------------------------------------------------------------------------
  DisabledBranchesT GetDisabledBranches() const;

  //----------------------------------------------------------------------------
  //! Set the configured placement strategy override of this space. This
  //! ClusterMgr is the authoritative owner. Unlike the fill limits and disabled
  //! branches the strategy does not shape the topology snapshot and is therefore
  //! not stamped onto it, only consulted at schedule time; it is read on the hot
  //! scheduling path, so it lives in a lock-free atomic rather than under
  //! mConfigMutex.
  //!
  //! @param strategy placement strategy to use for this space
  //----------------------------------------------------------------------------
  void SetConfiguredStrategy(PlacementStrategyT strategy);

  //----------------------------------------------------------------------------
  //! Get the configured placement strategy override of this space
  //!
  //! @return the override, or nullopt if the space has none and the caller
  //!         should fall back to the global default
  //----------------------------------------------------------------------------
  std::optional<PlacementStrategyT> GetConfiguredStrategy() const;

  //----------------------------------------------------------------------------
  //! Get a human readable dump of the current snapshot
  //!
  //! @param type what to dump, one of "bucket", "disk" or "all"
  //!
  //! @return string representation of the topology
  //----------------------------------------------------------------------------
  std::string GetState(std::string_view type);

  //----------------------------------------------------------------------------
  //! Get the aggregate health picture of the current snapshot, epoch included
  //!
  //! @return state summary, empty if there is no snapshot yet
  //----------------------------------------------------------------------------
  ClusterStateSummary GetStateSummary();

  //----------------------------------------------------------------------------
  //! Get the writable placement capacity of the current snapshot, see
  //! ClusterData::GetWritableFreeGiB. The figure is cached for the given TTL
  //! so that a hot caller (the FUSE create path asks on every file) does not
  //! pay the disk pass each time; an epoch bump - a rebuild or an incremental
  //! topology change - invalidates the cache immediately, so the TTL only
  //! bounds the staleness against the in-place updates (FST publishes,
  //! bookings, status flips), which is also all the accuracy the figure has
  //! ever offered. A TTL of zero bypasses the cache.
  //!
  //! @param ttl how long a computed figure may be served
  //!
  //! @return writable free space in GiB
  //----------------------------------------------------------------------------
  uint64_t GetWritableFreeGiB(std::chrono::milliseconds ttl);

private:
  //----------------------------------------------------------------------------
  //! Swap in a new topology snapshot and drop the old one. Private: the only
  //! caller is the nested SnapshotBuilder committing in its destructor.
  //!
  //! @param data new snapshot
  //----------------------------------------------------------------------------
  void AddClusterData(ClusterData&& data);

  //----------------------------------------------------------------------------
  //! Re-resolve the configured disabled branch rules onto the current
  //! snapshot. The caller must hold mConfigMutex.
  //----------------------------------------------------------------------------
  void ReapplyDisabledBranchesLocked();

  //! Current topology snapshot
  eos::common::atomic_unique_ptr<ClusterData> mClusterData;
  std::atomic<EpochIdT> mCurrentEpoch{0};   ///< Epoch of the current snapshot
  RCUMutexT mClusterMgrRcu;                 ///< RCU domain guarding the snapshot
  //! Cached writable capacity in GiB, see GetWritableFreeGiB. The three cells
  //! are not updated as one unit; a torn refresh can at worst serve a figure
  //! one refresh stale for one TTL, which the coarse consumers tolerate.
  std::atomic<uint64_t> mCapCacheGiB{0};
  //! steady_clock time the figure was computed, in ns since epoch. The TTL is
  //! applied against this at read time, see GetWritableFreeGiB.
  std::atomic<int64_t> mCapCacheTime{0};
  std::atomic<EpochIdT> mCapCacheEpoch{0}; ///< Epoch the figure was computed at
  //! Guards the authoritative configuration below and serializes it against
  //! the snapshot commit in AddClusterData, so a rebuild always stamps a
  //! consistent view of the config onto the new snapshot. Held only on cold
  //! config and rebuild paths, never on the scheduling hot path.
  mutable std::mutex mConfigMutex;
  FillLimitsConfig mConfiguredFillLimits; ///< Authoritative fill thresholds
  DisabledBranchesT mDisabledBranches;    ///< Authoritative disabled branches
  //! Authoritative placement strategy override, or unset to fall back to the
  //! global default. A scalar read on the hot scheduling path, so it lives in a
  //! lock-free atomic rather than under mConfigMutex above; it does not shape
  //! the snapshot, so AddClusterData does not stamp it. Encoded as int16_t so
  //! that -1 means "no override", PlacementStrategyT having no unset value.
  std::atomic<int16_t> mConfiguredStrategy{-1};
};

} // namespace eos::mgm::placement
