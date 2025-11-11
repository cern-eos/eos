#include "mgm/placement/RoundRobinPlacementStrategy.hh"
#include "common/utils/ContainerUtils.hh"

namespace eos::mgm::placement {

std::unique_ptr<RRSeeder>
makeRRSeeder(PlacementStrategyT strategy, size_t max_buckets)
{
  if (strategy == PlacementStrategyT::kThreadLocalRoundRobin) {
    return std::make_unique<ThreadLocalRRSeeder>(max_buckets);
  } else if (strategy == PlacementStrategyT::kRandom) {
    return std::make_unique<RandomSeeder>(max_buckets);
  } else if (strategy == PlacementStrategyT::kFidRandom) {
    return std::make_unique<FidSeeder>(max_buckets);
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

  const auto& bucket = cluster_data.buckets[bucket_index];
  auto rr_seed = mSeed->get(bucket_index, args.n_replicas, args.fid);
  int items_added = 0;
  for (int i = 0;
       (items_added < args.n_replicas) && (i < MAX_PLACEMENT_ATTEMPTS); i++) {

    auto id = eos::common::pickIndexRR(bucket.items, rr_seed + i);

    // While it is highly unlikely that we'll get a duplicate with RR placement,
    // random seed gen can still generate the same seed twice.
    if (result.contains(id)) {
      continue;
    }

    item_id_t item_id = id;
    if (id > 0) {
      // we are dealing with a disk! check if it is usable
      if ((size_t)id > cluster_data.disks.size()) {
        result.err_msg = "Disk ID unknown!";
        result.ret_code = ERANGE;
        return result;
      }

      if (!PlacementStrategy::validDiskPlct(item_id, cluster_data, args)) {
        continue;
      }
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


int
RoundRobinPlacement::access(const ClusterData &cluster_data, AccessArguments args)
{
  return EINVAL;
}



} // namespace eos::mgm::placement
