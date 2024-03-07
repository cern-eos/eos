#include "common/utils/ContainerUtils.hh"
#include "common/Logging.hh"
#include "mgm/FsView.hh"
#include "mgm/placement/FsScheduler.hh"

namespace eos::mgm::placement {

static constexpr int MAX_GROUPS_TO_TRY=10;

std::map<std::string, std::unique_ptr<ClusterMgr>>
EosClusterMgrHandler::make_cluster_mgr()
{
  std::map<std::string, std::unique_ptr<ClusterMgr>> space_cluster_map;
  {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    for (auto space_group_kv : FsView::gFsView.mSpaceGroupView) {
      space_cluster_map.insert_or_assign(space_group_kv.first,
                                         std::make_unique<ClusterMgr>());

      auto total_groups = space_group_kv.second.size();
      eos_static_info("msg=\"Creating FSScheduler with \" total_groups=%llu",
                      total_groups);
      auto storage_handler =
          space_cluster_map[space_group_kv.first]->getStorageHandler(
              common::next_power2(total_groups + 10));
      bool status =
          storage_handler.addBucket(get_bucket_type(StdBucketType::ROOT), 0);
      if (!status) {
        eos_static_crit("msg=\"Failed to add root bucket!\"");
      }

      for (auto group_iter : space_group_kv.second) {
        auto group_index = group_iter->GetIndex();
        item_id_t group_id = GroupIDtoBucketID(group_index);
        eos_static_info("msg=\"Adding group \" Group ID=%d, Internal bucket ID=%d",
                        group_index, group_id);
        bool status = storage_handler.addBucket(
            get_bucket_type(StdBucketType::GROUP), group_id, 0);
        if (!status) {
          eos_static_crit("msg=\"Failed to add group bucket!\" group_id=%d",
                          group_id);
        }
        for (auto it_fs = group_iter->begin(); it_fs != group_iter->end();
             ++it_fs) {
          auto fs = FsView::gFsView.mIdView.lookupByID(*it_fs);
          auto capacity = fs->GetLongLong("stat.statfs.capacity");
          uint8_t used = static_cast<uint8_t>(
              fs->GetDouble("stat.statfs.filled")); // filled is supposed to be between 0 & 100
          uint8_t weight = 1;
          if (capacity > (1LL << 40)) {
            weight = capacity / (1LL << 40);
          }
          auto active_status = getActiveStatus(fs->GetActiveStatus(),
                                               fs->GetStatus());
          auto add_status = storage_handler.addDisk(Disk(fs->GetId(), fs->GetConfigStatus(),
                                                         active_status, weight, used),
                                                group_id);
          eos_static_info("msg=\"Adding disk at \" ID=%d group_id=%d status=%d",
                          fs->GetId(), group_id, add_status);
        }
      }
    }
  }

  return space_cluster_map;
}

std::unique_ptr<ClusterMgr>
EosClusterMgrHandler::make_cluster_mgr(const std::string& spaceName)
{

  auto cluster_mgr = std::make_unique<ClusterMgr>();
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);

  if (auto space_group_kv = FsView::gFsView.mSpaceGroupView.find(spaceName);
      space_group_kv != FsView::gFsView.mSpaceGroupView.end()) {

    auto total_groups = space_group_kv->second.size();
    auto storage_handler =
        cluster_mgr->getStorageHandler(common::next_power2(total_groups + 1));
    storage_handler.addBucket(get_bucket_type(StdBucketType::ROOT), 0);

    for (auto group_iter : space_group_kv->second) {
      auto group_index = group_iter->GetIndex();
      item_id_t group_id = GroupIDtoBucketID(group_index);
      eos_static_info("msg=\"Adding group \" Group ID=%d, Internal bucket ID=%d",
                      group_index, group_id);
      bool status = storage_handler.addBucket(get_bucket_type(StdBucketType::GROUP),
                                group_id);
      if (!status) {
        eos_static_crit("msg=\"Failed to add group bucket!\" group_id=%d",
                        group_id);
      }

      for (auto it_fs = group_iter->begin(); it_fs != group_iter->end();
           ++it_fs) {
        auto fs = FsView::gFsView.mIdView.lookupByID(*it_fs);
        auto capacity = fs->GetLongLong("stat.statfs.capacity");
        uint8_t used = static_cast<uint8_t>(fs->GetDouble(
            "stat.statfs.filled")); // filled is supposed to be between 0 & 100
        uint8_t weight = 1;
        if (capacity > (1LL << 40)) {
          weight = capacity / (1LL << 40);
        }
        auto active_status = getActiveStatus(fs->GetActiveStatus(),
                                             fs->GetStatus());
        storage_handler.addDisk(Disk(fs->GetId(), fs->GetConfigStatus(),
                                     active_status, weight, used),
                                group_id);
      }
    }
  }

  return cluster_mgr;
}

ClusterMgr*
FSScheduler::get_cluster_mgr(const std::string& spaceName)
{
  if (!cluster_mgr_map) {
    return nullptr;
  }
  if (auto kv = cluster_mgr_map->find(spaceName);
      kv != cluster_mgr_map->end()) {
    return kv->second.get();
  }
  return nullptr;
}

void
FSScheduler::updateClusterData()
{
  if (!cluster_handler) {
    eos_static_crit("msg=\"Cluster handler is not yet initialized!\"");
    //Throw an exception? There is no api to set a cluster handler currently!
    return;
  }
  auto cluster_map = cluster_handler->make_cluster_mgr();
  eos::common::ScopedRCUWrite(cluster_rcu_mutex, cluster_mgr_map,
                              new ClusterMapT(std::move(cluster_map)));
  mIsRunning.store(true, std::memory_order_release);
}

PlacementResult
FSScheduler::schedule(const string& spaceName,
                      PlacementArguments args)
{
  if (!is_valid_placement_strategy(args.strategy)) {
    args.strategy = getPlacementStrategy(spaceName);
    eos_static_info("msg=\"Overriding scheduling strategy to space default\": %s",
                    strategy_to_str(args.strategy).c_str());
  }

  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto cluster_mgr = get_cluster_mgr(spaceName);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return {};
  }

  PlacementResult result;
  auto cluster_data_ptr = cluster_mgr->getClusterData();
  for (int i=0; i < MAX_GROUPS_TO_TRY; i++) {
    result = scheduler->schedule(cluster_data_ptr(), args);
    if (result.is_valid_placement(args.n_replicas)) {
      return result;
    } else {
      eos_static_debug("msg=\"Scheduler failed to place %d replicas\" err=%s",
                       result.n_replicas, result.error_string());
    }
  }
  return result;
}

PlacementResult
FSScheduler::schedule(const std::string& spaceName, uint8_t n_replicas)
{
  return schedule(spaceName, PlacementArguments(n_replicas, ConfigStatus::kRW,
                                                getPlacementStrategy(spaceName)));
}

bool
FSScheduler::setDiskStatus(const std::string& spaceName, fsid_t disk_id,
                           ConfigStatus status)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto* cluster_mgr = get_cluster_mgr(spaceName);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return false;
  }

  return cluster_mgr->setDiskStatus(disk_id, status);
}

bool
FSScheduler::setDiskStatus(const std::string& spaceName, fsid_t disk_id,
                           ActiveStatus status,
                           eos::common::BootStatus bstatus)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto* cluster_mgr = get_cluster_mgr(spaceName);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return false;
  }
  auto _status = getActiveStatus(status, bstatus);
  return cluster_mgr->setDiskStatus(disk_id, _status);
}

bool
FSScheduler::setDiskWeight(const std::string& spaceName, fsid_t disk_id,
                           uint8_t weight)
{
  if (spaceName.empty() || disk_id == 0) {
    return false;
  }

  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto* cluster_mgr = get_cluster_mgr(spaceName);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for\" space=%s",
                    spaceName.c_str());
    return false;
  }
  return cluster_mgr->setDiskWeight(disk_id, weight);
}

void
FSScheduler::setPlacementStrategy(std::string_view strategy_sv)
{
  placement_strategy.store(strategy_from_str(strategy_sv),
                           std::memory_order_release);
}

PlacementStrategyT
FSScheduler::getPlacementStrategy()
{
  return placement_strategy.load(std::memory_order_acquire);
}
void
FSScheduler::setPlacementStrategy(const string& spacename,
                                  std::string_view strategy_sv)
{
  std::map<std::string, PlacementStrategyT> strategy_map;
  if (space_strategy_map && !space_strategy_map->empty())
  {
    eos::common::RCUReadLock rlock(cluster_rcu_mutex);
    strategy_map.insert(space_strategy_map->begin(),
                        space_strategy_map->end());
  }
  strategy_map.insert_or_assign(spacename, strategy_from_str(strategy_sv));

  eos::common::ScopedRCUWrite(cluster_rcu_mutex, space_strategy_map,
                              new SpaceStrategyMapT(std::move(strategy_map)));
  eos_static_info("msg=\"Configured default scheduler type for\" space=%s, strategy=%s",
                  spacename.c_str(), strategy_sv.data());
}

PlacementStrategyT
FSScheduler::getPlacementStrategy(const string& spacename)
{
  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  if (space_strategy_map && !space_strategy_map->empty())
  {
        if (auto kv = space_strategy_map->find(spacename);
            kv != space_strategy_map->end()) {
          return kv->second;
        }
  }
  return getPlacementStrategy();
}

std::string
FSScheduler::getStateStr(const std::string& spacename, std::string_view type_sv)
{

  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto* cluster_mgr = get_cluster_mgr(spacename);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for\" space=%s",
                    spacename.c_str());
    return {};
  }
  return cluster_mgr->getStateStr(type_sv);
}

bool
FSScheduler::isRunning() const
{
  return mIsRunning.load(std::memory_order_acquire);
}

}// eos::mgm::placement
