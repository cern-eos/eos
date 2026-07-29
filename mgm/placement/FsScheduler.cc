//------------------------------------------------------------------------------
//! @file FsScheduler.cc
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

#include "mgm/placement/FsScheduler.hh"
#include "common/Logging.hh"
#include "common/utils/ContainerUtils.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/placement/ClusterBuilder.hh"

namespace eos::mgm::placement
{

static constexpr int MAX_GROUPS_TO_TRY = 10;

//------------------------------------------------------------------------------
// Get the free space of a file system a booking may actually claim
//------------------------------------------------------------------------------
uint64_t
GetUsableFreeBytes(eos::common::FileSystem& fs)
{
  // freebytes is the raw statfs figure, the headroom the FST keeps back is
  // not taken off it. A booking can only claim what is left after that,
  // which is the same arithmetic the geotree engine does.
  const long long free_bytes = fs.GetLongLong("stat.statfs.freebytes");
  const long long headroom =
      eos::common::StringConversion::GetSizeFromString(fs.GetString("headroom"));
  return (free_bytes > headroom) ? static_cast<uint64_t>(free_bytes - headroom) : 0;
}

//------------------------------------------------------------------------------
// Describe one file system for the topology builder
//------------------------------------------------------------------------------
FsDescription
DescribeFs(eos::common::FileSystem& fs, unsigned int group_index)
{
  FsDescription desc;
  desc.fsid = fs.GetId();
  desc.group_index = group_index;
  desc.geotag = fs.GetString("stat.geotag");
  desc.capacity = fs.GetLongLong("stat.statfs.capacity");
  desc.free_bytes = GetUsableFreeBytes(fs);
  // filled is supposed to be between 0 & 100
  desc.percent_used = static_cast<uint8_t>(fs.GetDouble("stat.statfs.filled"));
  desc.config_status = fs.GetConfigStatus();
  desc.active_status = GetActiveStatus(fs.GetActiveStatus(), fs.GetStatus());
  return desc;
}

namespace {

//----------------------------------------------------------------------------
//! Describe every file system of one space's scheduling groups, so that the
//! topology builder can work without knowing about the FsView
//!
//! @param groups scheduling groups of a single space
//!
//! @return description of every file system of those groups
//!
//! @note the caller must hold FsView::gFsView.ViewMutex
//----------------------------------------------------------------------------
std::vector<FsDescription>
DescribeGroups(const std::set<FsGroup*>& groups)
{
  std::vector<FsDescription> fs_list;

  for (auto group_iter : groups) {
    const auto group_index = group_iter->GetIndex();

    for (auto it_fs = group_iter->begin(); it_fs != group_iter->end(); ++it_fs) {
      auto fs = FsView::gFsView.mIdView.lookupByID(*it_fs);

      if (!fs) {
        eos_static_warning("msg=\"Skipping unknown filesystem\" fsid=%u "
                           "group_index=%u",
                           *it_fs, group_index);
        continue;
      }

      fs_list.push_back(DescribeFs(*fs, group_index));
    }
  }

  return fs_list;
}

//----------------------------------------------------------------------------
//! Populate a ClusterMgr from one space's scheduling groups
//!
//! @param cluster_mgr manager to fill
//! @param groups scheduling groups of a single space
//!
//! @note the caller must hold FsView::gFsView.ViewMutex
//----------------------------------------------------------------------------
void
FillClusterMgr(ClusterMgr& cluster_mgr, const std::set<FsGroup*>& groups)
{
  BuildClusterData(cluster_mgr, DescribeGroups(groups));
}

} // anonymous namespace

//------------------------------------------------------------------------------
// Build the cluster manager of every known space
//------------------------------------------------------------------------------
ClusterMgrMapT
EosClusterMgrHandler::MakeClusterMgr(const ClusterMgrMapT& existing)
{
  ClusterMgrMapT space_cluster_map;
  eos::common::RWMutexReadLock rd_lock(FsView::gFsView.ViewMutex);

  for (const auto& space_group_kv : FsView::gFsView.mSpaceGroupView) {
    eos_static_info("msg=\"Creating FsScheduler space\" space=\"%s\" "
                    "total_groups=%llu",
                    space_group_kv.first.c_str(), space_group_kv.second.size());
    // Rebuild on top of the space's current manager if it has one, so the
    // configuration that manager owns (fill limits, disabled branches) rides
    // through the rebuild untouched. A space seen for the first time gets a
    // fresh manager with default configuration.
    std::shared_ptr<ClusterMgr> mgr;

    if (auto it = existing.find(space_group_kv.first); it != existing.end()) {
      mgr = it->second;
    } else {
      mgr = std::make_shared<ClusterMgr>();
    }

    FillClusterMgr(*mgr, space_group_kv.second);
    space_cluster_map.insert_or_assign(space_group_kv.first, std::move(mgr));
  }

  // A space gone from the topology is dropped from this map by omission.
  // UpdateClusterData still carries its manager forward if it holds
  // configuration, so nothing is lost while the space has no filesystem.
  return space_cluster_map;
}

//------------------------------------------------------------------------------
// Get the cluster manager of one space
//------------------------------------------------------------------------------
ClusterMgr*
FsScheduler::GetClusterMgr(const std::string& spaceName)
{
  if (!mClusterMgrMap) {
    return nullptr;
  }

  if (auto kv = mClusterMgrMap->find(spaceName); kv != mClusterMgrMap->end()) {
    return kv->second.get();
  }

  return nullptr;
}

//------------------------------------------------------------------------------
// Whether a manager carries any non-default configuration
//------------------------------------------------------------------------------
bool
FsScheduler::HasConfiguration(ClusterMgr& mgr)
{
  const auto limits = mgr.GetConfiguredFillLimits();
  return (limits.cap != kDefaultFillCapPercent) ||
         (limits.warn != kDefaultFillWarnPercent) || !mgr.GetDisabledBranches().empty() ||
         mgr.GetConfiguredStrategy().has_value();
}

//------------------------------------------------------------------------------
// Commit one file system into a manager's topology snapshot
//------------------------------------------------------------------------------
bool
FsScheduler::PopulateFsSnapshot(ClusterMgr& mgr, const FsDescription& desc)
{
  if (mgr.GetClusterData()) {
    // Existing snapshot: copy it, apply the one change, commit. A
    // re-registration must not leave the disk listed under two buckets, so the
    // draft first removes any existing entry for the fsid, then re-adds it.
    // Commit only on success: a failed add abandons the draft, leaving the
    // manager's current snapshot untouched rather than publishing one with the
    // disk missing - the next refresh repairs any staleness either way.
    auto builder = mgr.GetSnapshotBuilderWithData();
    builder.RemoveDisk(desc.fsid);

    if (!AddFsToCluster(builder, desc)) {
      builder.Abandon();
      return false;
    }

    builder.Commit();
    return true;
  }

  // Freshly bound manager with no snapshot yet (a brand new space, or one
  // configured ahead of its first filesystem): build it from the single
  // description, which also sizes the bucket array. AddClusterData stamps any
  // configuration the manager already carries.
  BuildClusterData(mgr, {desc});
  return true;
}

//------------------------------------------------------------------------------
// Rebuild the topology snapshot of every space out of the current FsView
//------------------------------------------------------------------------------
void
FsScheduler::UpdateClusterData()
{
  if (!mClusterHandler) {
    eos_static_crit("%s", "msg=\"Cluster handler is not yet initialized!\"");
    //Throw an exception? There is no api to set a cluster handler currently!
    return;
  }

  // Rebuild every space on top of its existing manager. Each manager owns and
  // carries its own configuration, so a space already known keeps its fill
  // limits and disabled branches with no re-stamping here - the choreography
  // this method used to run is gone.
  ClusterMgrMapT existing;
  {
    eos::common::RCUReadLock rlock(mClusterRcuMutex);

    if (mClusterMgrMap) {
      existing = *mClusterMgrMap;
    }
  }
  auto cluster_map = mClusterHandler->MakeClusterMgr(existing);
  ClusterMgrMapT* old_map = nullptr;
  {
    // The RCU writer lock is the sole serializer of the map publishers (config
    // setters, InsertFs, this rebuild), held across the whole read-modify-swap.
    // MakeClusterMgr above already took the FsView lock and released it, so the
    // writer lock is never held while acquiring FsView - the order the boot
    // restore relies on. Read the live map here under the lock rather than trust
    // the pre-build snapshot: a config setter may have bound a manager to a new
    // space while MakeClusterMgr was running.
    std::unique_lock<RCUMutexT> wlock(mClusterRcuMutex);

    // A manager this build did not rebuild is kept iff it carries configuration:
    // a space configured but with no filesystem yet (its manager is bound and
    // holds the config), or one configured then emptied. An emptied space with
    // only default config is pruned as before.
    if (mClusterMgrMap) {
      for (const auto& [space, mgr] : *mClusterMgrMap) {
        if (cluster_map.count(space) || !HasConfiguration(*mgr)) {
          continue;
        }

        cluster_map.insert_or_assign(space, mgr);
      }
    }

    old_map = mClusterMgrMap.reset(new ClusterMgrMapT(std::move(cluster_map)));
  } // wlock: rcu_synchronize() then release, so old_map has no readers left
  delete old_map;
  mIsRunning.store(true, std::memory_order_release);
}

//------------------------------------------------------------------------------
// Insert one file system into its space's topology snapshot
//------------------------------------------------------------------------------
bool
FsScheduler::InsertFs(const std::string& spacename, const FsDescription& desc)
{
  if (spacename.empty() || (desc.fsid == 0)) {
    return false;
  }

  // Until the first full build the scheduler is not running; the boot time
  // registrations, which arrive one by one under the FsView lock, are
  // absorbed by that build instead of each copying a snapshot
  if (!IsRunning()) {
    return false;
  }

  // Bind a manager to the space if it has none yet and commit the file system
  // into its snapshot, as one operation. The frequent case - the space already
  // exists - runs under an RCU read lock with no writer serialization; only a
  // brand new space takes the writer lock. Either way ConfigureSpace keeps the
  // manager alive across the populate, so a concurrent rebuild cannot free it
  // mid-commit. The FsView write lock the Register hook holds is above
  // mClusterRcuMutex, matching the boot restore order.
  bool ok = false;
  ConfigureSpace(spacename, [&](ClusterMgr& mgr) { ok = PopulateFsSnapshot(mgr, desc); });

  if (ok) {
    eos_static_info("msg=\"Inserted file system into scheduler snapshot\" "
                    "space=%s fsid=%u geotag=\"%s\"",
                    spacename.c_str(), desc.fsid, desc.geotag.c_str());
  } else {
    eos_static_err("msg=\"Failed inserting file system into scheduler "
                   "snapshot\" space=%s fsid=%u",
                   spacename.c_str(), desc.fsid);
  }

  return ok;
}

//------------------------------------------------------------------------------
// Remove one file system from its space's topology snapshot
//------------------------------------------------------------------------------
bool
FsScheduler::RemoveFs(const std::string& spacename, fsid_t fsid)
{
  if (spacename.empty() || (fsid == 0) || !IsRunning()) {
    return false;
  }

  std::shared_ptr<ClusterMgr> cluster_mgr;
  {
    eos::common::RCUReadLock rlock(mClusterRcuMutex);

    if (mClusterMgrMap) {
      if (auto kv = mClusterMgrMap->find(spacename); kv != mClusterMgrMap->end()) {
        cluster_mgr = kv->second;
      }
    }
  }

  if (!cluster_mgr) {
    return false;
  }

  {
    // Make sure the disk is actually there before copying the whole snapshot
    auto cluster_data = cluster_mgr->GetClusterData();

    if (!cluster_data || (fsid > cluster_data->disks.size()) ||
        (cluster_data->disks[fsid - 1].id == 0)) {
      return false;
    }
  }

  bool ok = false;
  {
    // Publish the pruned snapshot only if the disk was actually there; a no-op
    // removal abandons the draft so it does not republish an identical snapshot.
    auto builder = cluster_mgr->GetSnapshotBuilderWithData();
    ok = builder.RemoveDisk(fsid);

    if (ok) {
      builder.Commit();
    } else {
      builder.Abandon();
    }
  }

  if (ok) {
    eos_static_info("msg=\"Removed file system from scheduler snapshot\" "
                    "space=%s fsid=%u",
                    spacename.c_str(), fsid);
  }

  return ok;
}

//------------------------------------------------------------------------------
// Select the disks holding the replicas of a new file
//------------------------------------------------------------------------------
PlacementResult
FsScheduler::Schedule(const std::string& spaceName, PlacementArgs args)
{
  if (!IsValidPlacementStrategy(args.strategy)) {
    args.strategy = GetPlacementStrategy(spaceName);
    eos_static_info("msg=\"Overriding scheduling strategy to space default\": %s",
                    StrategyToStr(args.strategy).c_str());
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    eos_static_crit("msg=\"scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return {};
  }

  PlacementResult result;
  auto cluster_data_ptr = cluster_mgr->GetClusterData();

  if (!cluster_data_ptr) {
    eos_static_crit("msg=\"scheduler holds no committed snapshot for space=%s\"",
                    spaceName.c_str());
    return {};
  }

  for (int i = 0; i < MAX_GROUPS_TO_TRY; i++) {
    // Vary the salt so that the deterministic strategies actually explore a
    // different part of the hierarchy on every attempt
    args.salt = i;
    result = mScheduler->Schedule(cluster_data_ptr(), args);

    if (result.IsValidPlacement(args.n_replicas)) {
      return result;
    } else {
      eos_static_debug("msg=\"scheduler failed to place replicas\" "
                       "requested=%d placed=%d err=\"%s\"",
                       result.n_replicas, result.n_filled, result.ErrorString().c_str());
    }

    // A fresh salt only changes which part of the hierarchy the deterministic
    // strategies explore, so it can only turn an ENOSPC (ran out of room on this
    // pass) into a success. A rejected argument (EINVAL) or an inconsistent
    // snapshot (ERANGE) is deterministic in the salt - retrying just repeats the
    // exact same work MAX_GROUPS_TO_TRY times, so bail out now.
    if (result.ret_code != ENOSPC) {
      break;
    }
  }

  return result;
}

//------------------------------------------------------------------------------
// Select the disks holding the replicas of a new file
//------------------------------------------------------------------------------
PlacementResult
FsScheduler::Schedule(const std::string& spaceName, uint8_t n_replicas)
{
  return Schedule(spaceName, PlacementArgs(n_replicas, ConfigStatus::kRW,
                                           GetPlacementStrategy(spaceName)));
}

//------------------------------------------------------------------------------
// Select which of the existing replicas of a file should be accessed
//------------------------------------------------------------------------------
int
FsScheduler::Access(const std::string& spaceName, AccessArgs& args)
{
  if (!IsValidPlacementStrategy(args.strategy)) {
    args.strategy = GetPlacementStrategy(spaceName);
    eos_static_info("msg=\"Overriding access strategy to space default\": %s",
                    StrategyToStr(args.strategy).c_str());
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return EINVAL;
  }

  auto cluster_data_ptr = cluster_mgr->GetClusterData();

  if (!cluster_data_ptr) {
    eos_static_crit("msg=\"scheduler holds no committed snapshot for space=%s\"",
                    spaceName.c_str());
    return EINVAL;
  }

  return mScheduler->Access(cluster_data_ptr(), args);
}

bool
FsScheduler::SetDiskStatus(const std::string& spaceName, fsid_t disk_id,
                           ConfigStatus status)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return false;
  }

  return cluster_mgr->SetDiskStatus(disk_id, status);
}

bool
FsScheduler::SetDiskStatus(const std::string& spaceName, fsid_t disk_id,
                           ActiveStatus status, eos::common::BootStatus bstatus)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return false;
  }

  auto _status = GetActiveStatus(status, bstatus);
  return cluster_mgr->SetDiskStatus(disk_id, _status);
}

//------------------------------------------------------------------------------
// Update the weight of a disk in place
//------------------------------------------------------------------------------
bool
FsScheduler::SetDiskWeight(const std::string& spaceName, fsid_t disk_id, uint8_t weight)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for\" space=%s",
                    spaceName.c_str());
    return false;
  }

  return cluster_mgr->SetDiskWeight(disk_id, weight);
}

//------------------------------------------------------------------------------
// Update the fill level of a disk in place
//------------------------------------------------------------------------------
bool
FsScheduler::SetDiskPercentUsed(const std::string& spaceName, fsid_t disk_id,
                                uint8_t percent_used)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    return false;
  }

  return cluster_mgr->SetDiskPercentUsed(disk_id, percent_used);
}

//------------------------------------------------------------------------------
// Update the free space of a disk in place from an FST publish
//------------------------------------------------------------------------------
bool
FsScheduler::SetDiskFreeSpace(const std::string& spaceName, fsid_t disk_id,
                              uint64_t free_bytes)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spaceName);

  if (!cluster_mgr) {
    return false;
  }

  return cluster_mgr->SetDiskFreeSpace(disk_id, free_bytes);
}

//------------------------------------------------------------------------------
// Set the global default placement strategy
//------------------------------------------------------------------------------
void
FsScheduler::SetPlacementStrategy(std::string_view strategy_sv)
{
  mDefaultPlctStrategy.store(StrategyFromStr(strategy_sv), std::memory_order_release);
}

//------------------------------------------------------------------------------
// Get the global default placement strategy
//------------------------------------------------------------------------------
PlacementStrategyT
FsScheduler::GetPlacementStrategy()
{
  return mDefaultPlctStrategy.load(std::memory_order_acquire);
}

//------------------------------------------------------------------------------
// Set the placement strategy of one space
//------------------------------------------------------------------------------
void
FsScheduler::SetPlacementStrategy(const std::string& spacename,
                                  std::string_view strategy_sv)
{
  // Store the override on the space's manager - the one place the configuration
  // lives - binding a fresh manager to the space first if it has none yet.
  const PlacementStrategyT strategy = StrategyFromStr(strategy_sv);
  ConfigureSpace(spacename,
                 [strategy](ClusterMgr& mgr) { mgr.SetConfiguredStrategy(strategy); });
  eos_static_info("msg=\"Configured default mScheduler type for\" space=%s, strategy=%s",
                  spacename.c_str(), strategy_sv.data());
}

//------------------------------------------------------------------------------
// Get the placement strategy of one space
//------------------------------------------------------------------------------
PlacementStrategyT
FsScheduler::GetPlacementStrategy(const std::string& spacename)
{
  {
    eos::common::RCUReadLock rlock(mClusterRcuMutex);

    if (auto* cluster_mgr = GetClusterMgr(spacename)) {
      if (auto strategy = cluster_mgr->GetConfiguredStrategy()) {
        return *strategy;
      }
    }
  }

  return GetPlacementStrategy();
}

//------------------------------------------------------------------------------
// Set both fill thresholds of one space, the one entry point validating the
// pair
//------------------------------------------------------------------------------
bool
FsScheduler::SetFillLimits(const std::string& spacename, uint8_t cap, uint8_t warn)
{
  if (spacename.empty() || (cap > 100) || (warn >= cap)) {
    return false;
  }

  // Store and apply the thresholds on the space's manager - the one place the
  // configuration lives - binding a fresh manager to the space first if it has
  // none yet.
  ConfigureSpace(spacename, [cap, warn](ClusterMgr& mgr) {
    mgr.SetConfiguredFillLimits(cap, warn);
  });
  eos_static_info("msg=\"Configured fill thresholds for\" space=%s, "
                  "fill_cap=%u fill_warn=%u",
                  spacename.c_str(), (unsigned)cap, (unsigned)warn);
  return true;
}

//------------------------------------------------------------------------------
// Set the fill cap of one space, keeping its configured warning level
//------------------------------------------------------------------------------
bool
FsScheduler::SetFillRatioLimit(const std::string& spacename, uint8_t cap)
{
  return SetFillLimits(spacename, cap, GetFillLimits(spacename).warn);
}

//------------------------------------------------------------------------------
// Set the fill warning level of one space, keeping its configured cap
//------------------------------------------------------------------------------
bool
FsScheduler::SetFillRatioWarn(const std::string& spacename, uint8_t warn)
{
  return SetFillLimits(spacename, GetFillLimits(spacename).cap, warn);
}

//------------------------------------------------------------------------------
// Get the configured fill thresholds of one space
//------------------------------------------------------------------------------
FsScheduler::SpaceFillLimits
FsScheduler::GetFillLimits(const std::string& spacename)
{
  eos::common::RCUReadLock rlock(mClusterRcuMutex);

  if (auto* cluster_mgr = GetClusterMgr(spacename)) {
    return cluster_mgr->GetConfiguredFillLimits();
  }

  return {};
}

//------------------------------------------------------------------------------
// Disable a geotag branch of one space for the given operations
//------------------------------------------------------------------------------
bool
FsScheduler::AddDisabledBranch(const std::string& spacename, const std::string& geotag,
                               uint8_t op_mask)
{
  const std::string canonical = NormalizeGeoTag(geotag);
  op_mask &= kDisabledAll;

  if (spacename.empty() || canonical.empty() || (op_mask == 0)) {
    return false;
  }

  // Let the space's manager own and resolve the rule in one place, binding a
  // fresh manager to the space first if it has none yet.
  ConfigureSpace(spacename, [&canonical, op_mask](ClusterMgr& mgr) {
    mgr.AddDisabledBranch(canonical, op_mask);
  });
  eos_static_info("msg=\"Disabled scheduler branch\" space=%s geotag=\"%s\" ops=%s",
                  spacename.c_str(), canonical.c_str(),
                  DisabledOpsToStr(op_mask).c_str());
  return true;
}

//------------------------------------------------------------------------------
// Re-enable a previously disabled geotag branch for the given operations
//------------------------------------------------------------------------------
bool
FsScheduler::RmDisabledBranch(const std::string& spacename, const std::string& geotag,
                              uint8_t op_mask)
{
  const std::string canonical = NormalizeGeoTag(geotag);
  op_mask &= kDisabledAll;

  if (spacename.empty() || canonical.empty() || (op_mask == 0)) {
    return false;
  }

  // A rule can only exist on an existing manager; there is nothing to bind here.
  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spacename);

  if ((cluster_mgr == nullptr) || !cluster_mgr->RmDisabledBranch(canonical, op_mask)) {
    return false;
  }

  eos_static_info("msg=\"Re-enabled scheduler branch\" space=%s geotag=\"%s\" ops=%s",
                  spacename.c_str(), canonical.c_str(),
                  DisabledOpsToStr(op_mask).c_str());
  return true;
}

//------------------------------------------------------------------------------
// Get the disabled branch rules of one space
//------------------------------------------------------------------------------
DisabledBranchesT
FsScheduler::GetDisabledBranches(const std::string& spacename)
{
  eos::common::RCUReadLock rlock(mClusterRcuMutex);

  if (auto* cluster_mgr = GetClusterMgr(spacename)) {
    return cluster_mgr->GetDisabledBranches();
  }

  return {};
}

//------------------------------------------------------------------------------
// Get the disabled branch rules of every space that has any
//------------------------------------------------------------------------------
FsScheduler::SpaceDisabledMapT
FsScheduler::GetAllDisabledBranches()
{
  SpaceDisabledMapT all;
  // Every configured space has a bound manager that owns its rules; collect the
  // non-empty ones.
  eos::common::RCUReadLock rlock(mClusterRcuMutex);

  if (mClusterMgrMap) {
    for (const auto& [space, mgr] : *mClusterMgrMap) {
      if (auto rules = mgr->GetDisabledBranches(); !rules.empty()) {
        all.insert_or_assign(space, std::move(rules));
      }
    }
  }

  return all;
}

//------------------------------------------------------------------------------
// Get a human readable dump of the topology of one space
//------------------------------------------------------------------------------
std::string
FsScheduler::GetState(const std::string& spacename, std::string_view type_sv)
{
  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spacename);

  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for\" space=%s",
                    spacename.c_str());
    return {};
  }

  return cluster_mgr->GetState(type_sv);
}

//------------------------------------------------------------------------------
// Get the names of every space holding a topology snapshot
//------------------------------------------------------------------------------
std::vector<std::string>
FsScheduler::GetSpaces()
{
  std::vector<std::string> spaces;
  eos::common::RCUReadLock rlock(mClusterRcuMutex);

  if (mClusterMgrMap) {
    spaces.reserve(mClusterMgrMap->size());

    for (const auto& kv : *mClusterMgrMap) {
      spaces.push_back(kv.first);
    }
  }

  return spaces;
}

//------------------------------------------------------------------------------
// Get a human readable state summary of one space
//------------------------------------------------------------------------------
std::string
FsScheduler::GetSpaceState(const std::string& spacename)
{
  const auto strategy = GetPlacementStrategy(spacename);
  const auto default_strategy = GetPlacementStrategy();
  const auto limits = GetFillLimits(spacename);
  std::stringstream ss;
  ss << "space: " << spacename << "\n"
     << "  running   : " << (IsRunning() ? "yes" : "no") << "\n"
     << "  strategy  : " << StrategyToStr(strategy);

  if (strategy != default_strategy) {
    ss << " (space override, global default: " << StrategyToStr(default_strategy) << ")";
  } else {
    ss << " (global default)";
  }

  ss << "\n"
     << "  fill      : warn=" << static_cast<int>(limits.warn)
     << "% cap=" << static_cast<int>(limits.cap) << "%";

  if ((limits.warn == kDefaultFillWarnPercent) &&
      (limits.cap == kDefaultFillCapPercent)) {
    ss << " (defaults)";
  } else {
    ss << " (defaults warn=" << static_cast<int>(kDefaultFillWarnPercent)
       << "% cap=" << static_cast<int>(kDefaultFillCapPercent) << "%)";
  }

  ss << "\n  disabled  : ";
  const auto disabled = GetDisabledBranches(spacename);

  if (disabled.empty()) {
    ss << "none";
  } else {
    bool first = true;

    for (const auto& [geotag, op_mask] : disabled) {
      if (!first) {
        ss << ", ";
      }

      ss << geotag << ":" << DisabledOpsToStr(op_mask);
      first = false;
    }
  }

  ss << "\n";
  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spacename);

  if (!cluster_mgr) {
    ss << "  topology  : none\n";
    return ss.str();
  }

  const auto summary = cluster_mgr->GetStateSummary();
  ss << "  epoch     : " << summary.epoch << "\n"
     << "  topology  : " << summary.n_groups << " groups, " << summary.n_disks
     << " disks\n"
     << "  status    : " << summary.n_online << " online / " << summary.n_offline
     << " offline; " << summary.n_rw << " rw, " << summary.n_ro << " ro, "
     << summary.n_drain << " drain, " << summary.n_other << " other\n"
     << "  weight    : capacity=" << summary.capacity_weight
     << " effective=" << summary.effective_weight << "\n"
     << "  space     : free=" << summary.free_gib << " GiB booked=" << summary.booked_gib
     << " GiB writable=" << summary.writable_free_gib << " GiB\n";
  return ss.str();
}

//------------------------------------------------------------------------------
// Get the writable placement capacity of one space in bytes
//------------------------------------------------------------------------------
std::optional<uint64_t>
FsScheduler::GetPlacementCapacity(const std::string& spacename)
{
  if (!IsRunning()) {
    return std::nullopt;
  }

  // A space routed to the legacy engine gets its figure from that engine,
  // same contract as the placement bridge in Scheduler.cc
  if (GetPlacementStrategy(spacename) == PlacementStrategyT::kGeoTreeLegacy) {
    return std::nullopt;
  }

  eos::common::RCUReadLock rlock(mClusterRcuMutex);
  auto* cluster_mgr = GetClusterMgr(spacename);

  if (!cluster_mgr) {
    return std::nullopt;
  }

  const std::chrono::milliseconds ttl{
      mCapacityCacheTTLMs.load(std::memory_order_acquire)};
  return cluster_mgr->GetWritableFreeGiB(ttl) * kFreeSpaceUnit;
}

//------------------------------------------------------------------------------
// Check if the mScheduler holds a usable topology
//------------------------------------------------------------------------------
bool
FsScheduler::IsRunning() const
{
  return mIsRunning.load(std::memory_order_acquire);
}

}// eos::mgm::placement
