#include "mgm/placement/Scheduler.hh"
#include "common/utils/ContainerUtils.hh"
#include <queue>

namespace eos::mgm::placement {

constexpr uint8_t MAX_PLACEMENT_ATTEMPTS = 20;

std::unique_ptr<RRSeeder>
makeRRSeeder(PlacementStrategyT strategy, size_t max_buckets)
{
  if (strategy == PlacementStrategyT::kThreadLocalRoundRobin) {
    return std::make_unique<ThreadLocalRRSeeder>(max_buckets);
  }
  return std::make_unique<GlobalRRSeeder>(max_buckets);

}

PlacementResult
RoundRobinPlacement::chooseItems(const ClusterData& cluster_data, Args args)
{
  PlacementResult result(args.n_replicas);
  if (args.n_replicas == 0) {
    result.err_msg = "Zero replicas requested";
    return result;
  }
  int32_t bucket_index = -args.bucket_id;
  auto bucket_sz = cluster_data.buckets.size();

  if (bucket_sz < args.n_replicas ||
      (bucket_index > 0 && (size_t)bucket_index > bucket_sz) ||
      bucket_sz > mSeed->getNumSeeds()) {
    if (bucket_sz < args.n_replicas) {
      result.err_msg = "More replicas than bucket size!";
    } else if (bucket_sz > mSeed->getNumSeeds()) {
      result.err_msg = "More buckets than random seeds!";
    } else {
      result.err_msg = "Bucket index out of range!";
    }
    result.ret_code = ERANGE;
    return result;
  }

  const auto& bucket = cluster_data.buckets.at(bucket_index);

  if (bucket.items.empty()) {
    result.err_msg = "Bucket " + std::to_string(bucket.id) + "is empty!";
    result.ret_code = ENOENT;
    return result;
  }

  auto rr_seed = mSeed->get(bucket_index, args.n_replicas);
  int items_added = 0;
  for (int i = 0;
       (items_added < args.n_replicas) && (i < MAX_PLACEMENT_ATTEMPTS); i++) {

    auto id = eos::common::pickIndexRR(bucket.items, rr_seed + i);
    item_id_t item_id = id;
    if (id > 0) {
      // we are dealing with a disk! check if it is usable
      if ((size_t)id > cluster_data.disks.size()) {
        result.err_msg = "Disk ID unknown!";
        result.ret_code = ERANGE;
        return result;
      }

      const auto& disk = cluster_data.disks.at(id - 1);
      auto disk_status = disk.config_status.load(std::memory_order_relaxed);
      if (disk_status < args.status) {
        continue;
        // TODO: We could potentially reseed the RR index in case of failure, should be fairly simple to do here! Uncommenting should work? rr_seed = GetRRSeed(n_replicas - items_added);
      }
      item_id = disk.id;
    }
    result.ids[items_added++] = item_id;
  }

  if (items_added != args.n_replicas) {
    result.err_msg = "Could not find enough items to place replicas";
    result.ret_code = ENOSPC;
    return result;
  }

  result.ret_code = 0;
  return result;
}

PlacementResult
FlatScheduler::schedule(const ClusterData& cluster_data,
                        PlacementArguments args)
{

  PlacementResult result;
  if (args.n_replicas == 0) {
    result.err_msg = "Zero replicas requested";
    return result;
  } else if (isValidBucketId(args.bucket_id, cluster_data)) {
    result.err_msg = "Bucket id out of range";
    return result;
  }

  if (args.default_placement) {
    return scheduleDefault(cluster_data, args);
  }

  // classical BFS
  std::queue<item_id_t> item_queue;
  item_queue.push(args.bucket_id);
  int result_index = 0;

  while (!item_queue.empty()) {
    item_id_t bucket_id = item_queue.front();
    item_queue.pop();
    if (!isValidBucketId(bucket_id, cluster_data)) {
      result.err_msg = "Invalid bucket id";
      return result;
    }

    auto bucket = cluster_data.buckets.at(-bucket_id);
    auto items_to_place = args.rules.at(bucket.bucket_type);
    if (items_to_place == -1) {
      items_to_place = args.n_replicas;
    }
    PlacementStrategy::Args plct_args{bucket_id, items_to_place,
          args.status};


    auto result = mPlacementStrategy->chooseItems(cluster_data, plct_args);
    if (!result) {
      return result;
    } else {
      for (int i=0; i < result.n_replicas; ++i) {
        auto _id = result.ids[i];
        if (_id < 0) {
          item_queue.push(_id);
        } else {
          result.ids[result_index++] = _id;
        }
      }
    }
  }
  return result;
}

PlacementResult
FlatScheduler::scheduleDefault(const ClusterData& cluster_data,
                               FlatScheduler::PlacementArguments args)
{
  do {
    const auto& bucket = cluster_data.buckets.at(-args.bucket_id);
    uint8_t n_replicas = 1;
    if (bucket.bucket_type == static_cast<uint8_t>(StdBucketType::GROUP)) {
      n_replicas = args.n_replicas;
    }

    PlacementStrategy::Args plct_args{args.bucket_id, n_replicas, args.status};
    auto result = mPlacementStrategy->chooseItems(cluster_data, plct_args);

    if (!result || result.ids.empty()) {
      return result;
    }

    if (result.n_replicas == args.n_replicas) {
      return result;
    }

    args.bucket_id = result.ids.front();
  } while(args.bucket_id < 0);

  return {};
}


} // namespace eos::mgm::placement
