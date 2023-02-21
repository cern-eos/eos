#include "common/utils/ContainerUtils.hh"
#include "common/Logging.hh"
#include "mgm/FsView.hh"
#include "mgm/placement/FsScheduler.hh"

namespace eos::mgm::placement {

constexpr int kBaseGroupOffset = -10;

void FSScheduler::updateClusterData(const std::string& spaceName)
{
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  auto space_group_kv = FsView::gFsView.mSpaceGroupView.find(spaceName);

  if (space_group_kv == FsView::gFsView.mSpaceGroupView.end()) {
    return;
  }
  std::map <int, int> group_index_map;
  auto total_groups = space_group_kv->second.size();
  auto storage_handler = cluster_mgr->getStorageHandler(common::next_power2(total_groups + 1));
  storage_handler.addBucket(get_bucket_type(StdBucketType::ROOT), 0);

  for (auto group_iter : space_group_kv->second) {
    item_id_t group_id = kBaseGroupOffset - group_iter->GetIndex();
    storage_handler.addBucket(get_bucket_type(StdBucketType::GROUP),
                              group_id);
    for (auto it_fs = group_iter->begin(); it_fs != group_iter->end(); ++it_fs) {
      auto fs = FsView::gFsView.mIdView.lookupByID(*it_fs);
        auto capacity = fs->GetLongLong("stat.statfs.capacity");
        uint8_t used = static_cast<uint8_t>(fs->GetDouble("stat.statfs.filled")); // filled is supposed to be between 0 & 100
        uint8_t weight = 1;
        if (capacity >  (1ULL << 40)) {
          weight = capacity / (1ULL << 40);
        }
        storage_handler.addDisk(Disk(fs->GetId(), fs->GetConfigStatus(), fs->GetActiveStatus(), weight, used),
                                group_id);
    }
  }

}

PlacementResult FSScheduler::schedule(uint8_t n_replicas)
{
  if (!initialized) {
    eos_static_crit("msg=\"Scheduler is not yet initialized\"");
    return {};
  }
  auto cluster_data_ptr = cluster_mgr->getClusterData();
  return scheduler->schedule(cluster_data_ptr(),
                             {n_replicas});
}

} // eos::mgm::placement
