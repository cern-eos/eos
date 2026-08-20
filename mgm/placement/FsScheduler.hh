//------------------------------------------------------------------------------
//! @file FsScheduler.hh
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
#include "mgm/placement/ClusterBuilder.hh"
#include "mgm/placement/ClusterMgr.hh"
#include "mgm/placement/FlatScheduler.hh"
#include <mutex>
#include <optional>

namespace eos::mgm::placement {

//! Map of space name to the cluster manager holding its topology. Shared
//! rather than unique pointers so that an incremental update can publish a
//! copy of the map with one entry changed while readers still walk the old
//! one, see FsScheduler::InsertFs.
using ClusterMgrMapT = std::map<std::string, std::shared_ptr<ClusterMgr>>;

//------------------------------------------------------------------------------
//! Get the free space of a file system a booking may actually claim: the raw
//! statfs figure minus the headroom the FST keeps back. The one definition of
//! this arithmetic, shared by the topology builder and the FST publish
//! listener so the two can never drift apart.
//!
//! @param fs file system to read
//!
//! @return usable free space in bytes
//------------------------------------------------------------------------------
uint64_t GetUsableFreeBytes(eos::common::FileSystem& fs);

//------------------------------------------------------------------------------
//! Describe one file system for the topology builder. The one definition of
//! this projection, shared by the bulk space description and the incremental
//! registration hooks so the two can never drift apart.
//!
//! @param fs file system to read
//! @param group_index index of its scheduling group
//!
//! @return description of the file system
//------------------------------------------------------------------------------
FsDescription DescribeFs(eos::common::FileSystem& fs, unsigned int group_index);

//------------------------------------------------------------------------------
//! Struct ClusterMgrHandler - interface building the per space topology. It
//! decouples FsScheduler from the FsView, so that tests can inject a topology
//! without an MGM behind it.
//------------------------------------------------------------------------------
struct ClusterMgrHandler {
  //----------------------------------------------------------------------------
  //! Build the cluster manager of every known space. A space already present
  //! in the existing map is rebuilt on top of its current ClusterMgr so that
  //! the authoritative per-space configuration the manager owns (fill limits,
  //! disabled branches) survives the rebuild - there is no external
  //! re-stamping. A space gone from the topology is dropped by omission.
  //!
  //! @param existing the currently live per-space cluster managers, empty on
  //!        the first build
  //!
  //! @return map of space name to cluster manager
  //----------------------------------------------------------------------------
  virtual ClusterMgrMapT MakeClusterMgr(const ClusterMgrMapT& existing) = 0;

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~ClusterMgrHandler() = default;
};

//------------------------------------------------------------------------------
//! Struct EosClusterMgrHandler - builds the topology out of the MGM FsView
//------------------------------------------------------------------------------
struct EosClusterMgrHandler : public ClusterMgrHandler {
  //----------------------------------------------------------------------------
  //! Build the cluster manager of every known space
  //!
  //! @return map of space name to cluster manager
  //----------------------------------------------------------------------------
  ClusterMgrMapT MakeClusterMgr(const ClusterMgrMapT& existing) override;
};

//------------------------------------------------------------------------------
//! Class FsScheduler - MGM facing facade of the flat mScheduler. Owns the per
//! space topology snapshots and the per space strategy configuration, takes the
//! RCU read locks and forwards the requests to the engine.
//------------------------------------------------------------------------------
class FsScheduler {
public:
  using ClusterMgrMapPtrT = eos::common::atomic_unique_ptr<ClusterMgrMapT>;

  //! The configured fill thresholds of one space. The authoritative copy now
  //! lives on the space's ClusterMgr (see ClusterMgr::SetConfiguredFillLimits);
  //! this alias is what GetFillLimits returns to callers.
  using SpaceFillLimits = FillLimitsConfig;

  //! Disabled branch rules per space, see DisabledBranchesT. Used only as the
  //! return type of GetAllDisabledBranches; the authoritative rules live on
  //! each space's ClusterMgr.
  using SpaceDisabledMapT = std::map<std::string, DisabledBranchesT>;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_buckets number of buckets to size the engine for initially. Not
  //!        a ceiling: the topology is built long after this and keeps growing,
  //!        so the engine grows its per bucket state off every snapshot it is
  //!        handed, see FlatScheduler::EnsureCapacity.
  //! @param _handler handler building the per space topology
  //----------------------------------------------------------------------------
  FsScheduler(size_t max_buckets, std::unique_ptr<ClusterMgrHandler>&& _handler)
      : mDefaultPlctStrategy(placement::PlacementStrategyT::kGeoTreeLegacy)
      , mScheduler(std::make_unique<FlatScheduler>(max_buckets))
      , mClusterHandler(std::move(_handler))
  {}

  //----------------------------------------------------------------------------
  //! Default constructor, builds the topology out of the MGM FsView
  //----------------------------------------------------------------------------
  FsScheduler()
      : FsScheduler(1024, std::make_unique<EosClusterMgrHandler>())
  {
  }

  //----------------------------------------------------------------------------
  //! Select the disks holding the replicas of a new file
  //!
  //! @param spaceName name of the space to place in
  //! @param n_replicas number of replicas to place
  //!
  //! @return placement result, convertible to false if placement failed
  //----------------------------------------------------------------------------
  PlacementResult Schedule(const std::string& spaceName, uint8_t n_replicas);

  //----------------------------------------------------------------------------
  //! Select the disks holding the replicas of a new file
  //!
  //! @param spaceName name of the space to place in
  //! @param args placement arguments
  //!
  //! @return placement result, convertible to false if placement failed
  //----------------------------------------------------------------------------
  PlacementResult Schedule(const std::string& spaceName, PlacementArgs args);

  //----------------------------------------------------------------------------
  //! Select which of the existing replicas of a file should be accessed
  //!
  //! @param spaceName name of the space holding the file
  //! @param args access arguments, updated with the selected index
  //!
  //! @return 0 if successful, otherwise an errno value
  //----------------------------------------------------------------------------
  int Access(const std::string& spaceName, AccessArgs& args);

  //----------------------------------------------------------------------------
  //! Rebuild the topology snapshot of every space out of the current FsView
  //----------------------------------------------------------------------------
  void UpdateClusterData();

  //----------------------------------------------------------------------------
  //! Restore the persisted per space scheduler configuration out of the FsView
  //! space config: placement strategy, fill thresholds and disabled branches.
  //! Called once at boot, after the first UpdateClusterData, and the mirror
  //! image of what SpaceCmd / SchedCmd persist when the operator changes any
  //! of these at runtime.
  //!
  //! @note takes the FsView view mutex, so the caller must not hold it
  //----------------------------------------------------------------------------
  void LoadConfig();

  //----------------------------------------------------------------------------
  //! Insert one file system into its space's topology snapshot, the
  //! incremental counterpart of UpdateClusterData used by the FsView
  //! registration hooks. A space the scheduler has not seen yet is created
  //! with the file system as its first disk; a file system already present
  //! is replaced, so a re-registration cannot duplicate it. No-op until the
  //! first full build - the boot time registrations, which arrive one by
  //! one, are absorbed by that build instead of each copying a snapshot.
  //!
  //! @param spacename name of the space
  //! @param desc description of the file system
  //!
  //! @return true if the snapshot was updated, otherwise false
  //----------------------------------------------------------------------------
  bool InsertFs(const std::string& spacename, const FsDescription& desc);

  //----------------------------------------------------------------------------
  //! Remove one file system from its space's topology snapshot, the
  //! incremental counterpart of UpdateClusterData used by the FsView
  //! registration hooks. Emptied buckets and an emptied space are left in
  //! place - zero weight keeps them out of the way, and the next full
  //! rebuild prunes them.
  //!
  //! @param spacename name of the space
  //! @param fsid file system identifier
  //!
  //! @return true if the file system was removed, otherwise false
  //----------------------------------------------------------------------------
  bool RemoveFs(const std::string& spacename, fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Update the operations a disk accepts, in place
  //!
  //! @param spaceName name of the space holding the disk
  //! @param disk_id file system identifier
  //! @param ops new set of operations
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskOps(const std::string& spaceName, fsid_t disk_id, FsOpMask ops);

  //----------------------------------------------------------------------------
  //! Update the active status of a disk in place
  //!
  //! @param spaceName name of the space holding the disk
  //! @param disk_id file system identifier
  //! @param status new active status
  //! @param bstatus current boot status
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskStatus(const std::string& spaceName, fsid_t disk_id, ActiveStatus status,
                     eos::common::BootStatus bstatus);

  //----------------------------------------------------------------------------
  //! Update the weight of a disk in place
  //!
  //! @param spaceName name of the space holding the disk
  //! @param disk_id file system identifier
  //! @param weight new weight
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskWeight(const std::string& spaceName, fsid_t disk_id, uint8_t weight);

  //----------------------------------------------------------------------------
  //! Update the fill level of a disk in place
  //!
  //! @param spaceName name of the space holding the disk
  //! @param disk_id file system identifier
  //! @param percent_used fill level in percent
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskPercentUsed(const std::string& spaceName, fsid_t disk_id,
                          uint8_t percent_used);

  //----------------------------------------------------------------------------
  //! Update the free space of a disk in place from an FST publish, reconciling
  //! the bookings made since the last one, see ClusterData::SetDiskFreeSpace
  //!
  //! @param spaceName name of the space holding the disk
  //! @param disk_id file system identifier
  //! @param free_bytes free space in bytes, see GetUsableFreeBytes
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDiskFreeSpace(const std::string& spaceName, fsid_t disk_id,
                        uint64_t free_bytes);

  //----------------------------------------------------------------------------
  //! Check if the mScheduler holds a usable topology
  //!
  //! @return true if running, otherwise false
  //----------------------------------------------------------------------------
  bool IsRunning() const;

  //----------------------------------------------------------------------------
  //! Set the global default placement strategy
  //!
  //! @param strategy_sv string representation of the strategy
  //----------------------------------------------------------------------------
  void SetPlacementStrategy(std::string_view strategy_sv);

  //----------------------------------------------------------------------------
  //! Set the placement strategy of one space
  //!
  //! @param spacename name of the space
  //! @param strategy_sv string representation of the strategy
  //----------------------------------------------------------------------------
  void SetPlacementStrategy(const std::string& spacename, std::string_view strategy_sv);

  //----------------------------------------------------------------------------
  //! Get the global default placement strategy
  //!
  //! @return placement strategy type
  //----------------------------------------------------------------------------
  PlacementStrategyT GetPlacementStrategy();

  //----------------------------------------------------------------------------
  //! Get the placement strategy of one space, falling back to the global
  //! default if the space has no override
  //!
  //! @param spacename name of the space
  //!
  //! @return placement strategy type
  //----------------------------------------------------------------------------
  PlacementStrategyT GetPlacementStrategy(const std::string& spacename);

  //----------------------------------------------------------------------------
  //! Set both fill thresholds of one space, the one entry point that
  //! validates the pair. The values are remembered and stamped onto the
  //! space's topology snapshot, current and future ones alike.
  //!
  //! @param spacename name of the space
  //! @param cap fill level in percent at which a disk stops taking replicas
  //! @param warn fill level in percent at which the weight starts to decay
  //!
  //! @return true if the pair is valid (warn < cap <= 100), otherwise false
  //----------------------------------------------------------------------------
  bool SetFillLimits(const std::string& spacename, uint8_t cap, uint8_t warn);

  //----------------------------------------------------------------------------
  //! Set the fill cap of one space, keeping its configured warning level
  //!
  //! @param spacename name of the space
  //! @param cap fill level in percent at which a disk stops taking replicas
  //!
  //! @return true if the resulting pair is valid, otherwise false
  //----------------------------------------------------------------------------
  bool SetFillRatioLimit(const std::string& spacename, uint8_t cap);

  //----------------------------------------------------------------------------
  //! Set the fill warning level of one space, keeping its configured cap
  //!
  //! @param spacename name of the space
  //! @param warn fill level in percent at which the weight starts to decay
  //!
  //! @return true if the resulting pair is valid, otherwise false
  //----------------------------------------------------------------------------
  bool SetFillRatioWarn(const std::string& spacename, uint8_t warn);

  //----------------------------------------------------------------------------
  //! Get the configured fill thresholds of one space, the defaults if the
  //! space has no override
  //!
  //! @param spacename name of the space
  //!
  //! @return fill thresholds
  //----------------------------------------------------------------------------
  SpaceFillLimits GetFillLimits(const std::string& spacename);

  //----------------------------------------------------------------------------
  //! Disable a geotag branch of one space for the given operations, the flat
  //! equivalent of the geotree addDisabledBranch. The rule is remembered and
  //! resolved onto the space's topology snapshot, current and future ones
  //! alike; a geotag matching no bucket yet is accepted and takes effect once
  //! the topology grows the branch.
  //!
  //! @param spacename name of the space
  //! @param geotag geotag of the branch, canonicalized before use
  //! @param op_mask operations to disable, see kDisabledPlct / kDisabledAccess
  //!
  //! @return true if the rule is valid, otherwise false
  //----------------------------------------------------------------------------
  bool AddDisabledBranch(const std::string& spacename, const std::string& geotag,
                         uint8_t op_mask);

  //----------------------------------------------------------------------------
  //! Re-enable a previously disabled geotag branch for the given operations
  //!
  //! @param spacename name of the space
  //! @param geotag geotag of the branch, canonicalized before use
  //! @param op_mask operations to re-enable
  //!
  //! @return true if a matching rule existed, otherwise false
  //----------------------------------------------------------------------------
  bool RmDisabledBranch(const std::string& spacename, const std::string& geotag,
                        uint8_t op_mask);

  //----------------------------------------------------------------------------
  //! Get the disabled branch rules of one space
  //!
  //! @param spacename name of the space
  //!
  //! @return canonical geotag to operations mask, empty if none
  //----------------------------------------------------------------------------
  DisabledBranchesT GetDisabledBranches(const std::string& spacename);

  //----------------------------------------------------------------------------
  //! Get the disabled branch rules of every space that has any
  //!
  //! @return space name to rules
  //----------------------------------------------------------------------------
  SpaceDisabledMapT GetAllDisabledBranches();

  //----------------------------------------------------------------------------
  //! Get a human readable dump of the topology of one space
  //!
  //! @param spacename name of the space
  //! @param type_sv what to dump, one of "bucket", "disk" or "all"
  //!
  //! @return string representation of the topology
  //----------------------------------------------------------------------------
  std::string GetState(const std::string& spacename, std::string_view type_sv);

  //----------------------------------------------------------------------------
  //! Get the names of every space holding a topology snapshot
  //!
  //! @return space names
  //----------------------------------------------------------------------------
  std::vector<std::string> GetSpaces();

  //----------------------------------------------------------------------------
  //! Get a human readable state summary of one space: strategy, fill
  //! thresholds and the aggregate health picture of its topology snapshot -
  //! the printInfo equivalent of the geotree engine
  //!
  //! @param spacename name of the space
  //!
  //! @return string representation of the state
  //----------------------------------------------------------------------------
  std::string GetSpaceState(const std::string& spacename);

  //----------------------------------------------------------------------------
  //! Get the writable placement capacity of one space in bytes, the flat
  //! equivalent of GeoTreeEngine::placementSpace: the free space on the disks
  //! a placement can actually use, bookings already discounted, see
  //! ClusterData::GetWritableFreeGiB. Served from a per space TTL cache so
  //! that hot callers (the FUSE create path asks on every file) do not pay
  //! the disk pass each time, see ClusterMgr::GetWritableFreeGiB for the
  //! staleness bounds.
  //!
  //! @param spacename name of the space
  //!
  //! @return capacity in bytes; nullopt when the scheduler is not running,
  //!         the space is unknown or it resolves to the legacy geotree
  //!         engine - the caller falls back to that engine then, and 0 stays
  //!         unambiguous as "the space is full"
  //----------------------------------------------------------------------------
  std::optional<uint64_t> GetPlacementCapacity(const std::string& spacename);

  //----------------------------------------------------------------------------
  //! Set how long a computed placement capacity may be served from the cache.
  //! Meant for tests, which need a deterministic figure; production stays at
  //! kCapacityCacheTTL.
  //!
  //! @param ttl time to live, zero bypasses the cache
  //----------------------------------------------------------------------------
  void
  SetCapacityCacheTTL(std::chrono::milliseconds ttl)
  {
    mCapacityCacheTTLMs.store(ttl.count(), std::memory_order_release);
  }

  //! Default TTL of the cached placement capacity. The figure only moves
  //! meaningfully on the FST publish cadence, which is far slower than this.
  static constexpr std::chrono::milliseconds kCapacityCacheTTL{30000};

private:
  //----------------------------------------------------------------------------
  //! Get the cluster manager of one space
  //!
  //! @param spaceName name of the space
  //!
  //! @return cluster manager or nullptr if the space is unknown
  //----------------------------------------------------------------------------
  ClusterMgr* GetClusterMgr(const std::string& spaceName);

  //----------------------------------------------------------------------------
  //! Apply a change to a space's manager, binding a fresh empty manager to the
  //! space first if it has none yet, so configuration always has a manager to
  //! land on. The frequent case - the manager already exists - runs @c apply
  //! under an RCU read lock only, with no writer serialization: it mutates the
  //! manager, never the map. Only when the space has no manager yet is the RCU
  //! writer lock taken to create and publish it; @c apply then runs on the new
  //! manager under that same lock, so a concurrent rebuild cannot drop the
  //! not-yet-configured manager between its creation and its configuration.
  //! Because @c apply runs while the map is held alive (read or write side),
  //! the manager it sees stays valid for the whole call.
  //!
  //! @param spacename name of the space
  //! @param apply callable invoked as apply(ClusterMgr&)
  //----------------------------------------------------------------------------
  template <typename ApplyFn>
  void
  ConfigureSpace(const std::string& spacename, ApplyFn&& apply)
  {
    {
      // Frequent path: the space already has a manager. A read lock keeps it
      // alive across the update; the mutation lands on the manager alone.
      eos::common::RCUReadLock rlock(mClusterRcuMutex);

      if (ClusterMgr* mgr = GetClusterMgr(spacename)) {
        apply(*mgr);
        return;
      }
    }
    // Rare path: bind a manager to the space. The RCU writer lock is the sole
    // serializer of the map publishers now, so hold it across the whole
    // read-modify-swap; the old map is reclaimed only after readers drain.
    ClusterMgrMapT* old_map = nullptr;
    {
      std::unique_lock<RCUMutexT> wlock(mClusterRcuMutex);
      ClusterMgr* mgr = GetClusterMgr(spacename);

      if (mgr) {
        // Another writer created it between our read and write lock: just
        // mutate it, the map already carries it.
        apply(*mgr);
      } else {
        ClusterMgrMapT next;

        if (mClusterMgrMap) {
          next = *mClusterMgrMap;
        }

        auto new_mgr = std::make_shared<ClusterMgr>();
        mgr = new_mgr.get();
        next.insert_or_assign(spacename, std::move(new_mgr));
        apply(*mgr);
        old_map = mClusterMgrMap.reset(new ClusterMgrMapT(std::move(next)));
      }
    } // wlock: rcu_synchronize() then release, so old_map has no readers left
    delete old_map;
  }

  //----------------------------------------------------------------------------
  //! Commit one file system into a manager's topology snapshot, the snapshot
  //! side of InsertFs. Mutates the manager's own RCU-protected snapshot, not
  //! the manager map, so it is safe under either side of mClusterRcuMutex.
  //!
  //! @param mgr manager to update
  //! @param desc description of the file system
  //!
  //! @return true if the snapshot was updated, otherwise false
  //----------------------------------------------------------------------------
  bool PopulateFsSnapshot(ClusterMgr& mgr, const FsDescription& desc);

  //----------------------------------------------------------------------------
  //! Whether a manager carries any non-default configuration worth keeping
  //! alive across a rebuild that did not rebuild its space (a configured space
  //! with no filesystem yet, or one configured then emptied).
  //!
  //! @param mgr manager to inspect
  //!
  //! @return true if the manager holds non-default configuration
  //----------------------------------------------------------------------------
  static bool HasConfiguration(ClusterMgr& mgr);

  std::atomic<bool> mIsRunning{false};                  ///< Mark if a topology is loaded
  //! TTL of the cached placement capacity in ms, see SetCapacityCacheTTL
  std::atomic<int64_t> mCapacityCacheTTLMs{kCapacityCacheTTL.count()};
  std::atomic<PlacementStrategyT> mDefaultPlctStrategy; ///< Global default
  std::unique_ptr<FlatScheduler> mScheduler;            ///< Placement engine
  std::unique_ptr<ClusterMgrHandler> mClusterHandler;   ///< Topology builder
  ClusterMgrMapPtrT mClusterMgrMap;                     ///< Per space topology snapshots
  //! Guards the cluster-manager map. Its RCU domain carries an exclusive writer
  //! lock, so it doubles as the sole serializer of the map publishers - the
  //! config setters' eager bind (ConfigureSpace), the rebuild (UpdateClusterData)
  //! and the incremental InsertFs. Each holds the writer lock across its whole
  //! read-modify-swap, which is why they do not use ScopedRCUWrite (it would
  //! re-lock the non-recursive writer lock): without that a second writer would
  //! copy the same old map and drop the first one's change. Readers of the map
  //! (Schedule/Access and the getters) take only the RCU read side and never
  //! serialize against these cold writers. The per-space fill limits, disabled
  //! branches and strategy override are not stored here: each ClusterMgr owns
  //! and serializes its own, see ClusterMgr::SetConfiguredFillLimits.
  RCUMutexT mClusterRcuMutex;
};

} // namespace eos::mgm::placement
