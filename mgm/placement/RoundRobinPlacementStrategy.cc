#include "mgm/placement/RoundRobinPlacementStrategy.hh"
#include "common/utils/ContainerUtils.hh"

namespace eos::mgm::placement {

std::unique_ptr<RRSeeder>
makeRRSeeder(PlacementStrategyT strategy, size_t max_buckets)
{
  if (strategy == PlacementStrategyT::kThreadLocalRoundRobin) {
    return std::make_unique<ThreadLocalRRSeeder>(max_buckets);
  }
  return std::make_unique<GlobalRRSeeder>(max_buckets);
}


PlacementResult
RoundRobinPlacement::placeFiles(const ClusterData& cluster_data, Args args)
{

  PlacementResult result(args.n_replicas);
  if (!validateArgs(cluster_data, args, result)) {
    return result;
  }

  int32_t bucket_index = -args.bucket_id;
  auto bucket_sz = cluster_data.buckets.size();

  if (bucket_sz > mSeed->getNumSeeds()) {
      result.err_msg = "More buckets than random seeds! seeds=" +
                       std::to_string(mSeed->getNumSeeds()) +
                       " buckets=" + std::to_string(bucket_sz);
    result.ret_code = ERANGE;
    return result;
  }

  const auto& bucket = cluster_data.buckets.at(bucket_index);
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


} // namespace eos::mgm::placement
