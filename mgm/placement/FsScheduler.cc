#include "common/utils/ContainerUtils.hh"
#include "common/Logging.hh"
#include "mgm/FsView.hh"
#include "mgm/placement/FsScheduler.hh"

namespace eos::mgm::placement {

constexpr int kBaseGroupOffset = -10;

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
        item_id_t group_id = kBaseGroupOffset - group_iter->GetIndex();
        eos_static_info("msg=\"Adding group at \" ID=%d", group_id);
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
          storage_handler.addDisk(Disk(fs->GetId(), fs->GetConfigStatus(),
                                       fs->GetActiveStatus(), weight, used),
                                  group_id);
          eos_static_info("msg=\"Adding disk at \" ID=%d group_id=%d",
                          fs->GetId(), group_id);
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
      item_id_t group_id = kBaseGroupOffset - group_iter->GetIndex();
      storage_handler.addBucket(get_bucket_type(StdBucketType::GROUP),
                                group_id);
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
        storage_handler.addDisk(Disk(fs->GetId(), fs->GetConfigStatus(),
                                     fs->GetActiveStatus(), weight, used),
                                group_id);
      }
    }
  }

  return cluster_mgr;
}

ClusterMgr*
FSScheduler::get_cluster_mgr(const std::string& spaceName)
{
  if (auto kv = cluster_mgr_map->find(spaceName);
      kv != cluster_mgr_map->end()) {
    return kv->second.get();
  }
  return nullptr;
}

void
FSScheduler::updateClusterData()
{
  auto cluster_map = cluster_handler->make_cluster_mgr();
  eos::common::ScopedRCUWrite(cluster_rcu_mutex, cluster_mgr_map,
                              new ClusterMapT(std::move(cluster_map)));
}

PlacementResult
FSScheduler::schedule(const string& spaceName,
                      FlatScheduler::PlacementArguments args)
{
  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto cluster_mgr = get_cluster_mgr(spaceName);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return {};
  }

  auto cluster_data_ptr = cluster_mgr->getClusterData();
  return scheduler->schedule(cluster_data_ptr(), args);
}

PlacementResult
FSScheduler::schedule(const std::string& spaceName, uint8_t n_replicas)
{
  return schedule(spaceName, FlatScheduler::PlacementArguments(n_replicas));
}

bool
FSScheduler::setDiskStatus(const string& spaceName, fsid_t disk_id,
                           ConfigStatus status)
{
  eos::common::RCUReadLock rlock(cluster_rcu_mutex);
  auto* cluster_mgr = get_cluster_mgr(spaceName);
  if (!cluster_mgr) {
    eos_static_crit("msg=\"Scheduler is not yet initialized for space=%s\"",
                    spaceName.c_str());
    return false;
  }

  return cluster_mgr->setDiskStatus(disk_id, status);
}

}// eos::mgm::placement
