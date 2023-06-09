#include "mgm/placement/FlatScheduler.hh"
#include "mgm/placement/RoundRobinPlacementStrategy.hh"
#include "mgm/placement/WeightedPlacementStrategy.hh"
#include "mgm/placement/WeightedRoundRobinStrategy.hh"
#include <queue>

namespace eos::mgm::placement {

std::unique_ptr<PlacementStrategy>
makePlacementStrategy(PlacementStrategyT type, size_t max_buckets)
{
  switch (type) {
  case PlacementStrategyT::kRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kThreadLocalRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kRandom: [[fallthrough]];
  case PlacementStrategyT::kFidRandom:
    return std::make_unique<RoundRobinPlacement>(type, max_buckets);
  case PlacementStrategyT::kWeightedRandom:
    return std::make_unique<WeightedRandomPlacement>(type, max_buckets);
  case PlacementStrategyT::kWeightedRoundRobin:
    return std::make_unique<WeightedRoundRobinPlacement>(type, max_buckets);
  default:
    return nullptr;
  }

}

FlatScheduler::FlatScheduler(size_t max_buckets)
{
  for (size_t i = 0; i < TOTAL_PLACEMENT_STRATEGIES; i++) {
    mPlacementStrategy[i] = makePlacementStrategy(
        static_cast<PlacementStrategyT>(i), max_buckets);
  }
}

FlatScheduler::FlatScheduler(PlacementStrategyT strategy, size_t max_buckets)
: mDefaultStrategy(strategy)
{
  mPlacementStrategy[static_cast<int>(strategy)] =
      makePlacementStrategy(strategy, max_buckets);
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

  if (! is_valid_placement_strategy(args.strategy)) {
    args.strategy = mDefaultStrategy;
  }

  // classical BFS
  std::queue<item_id_t> item_queue;
  item_queue.push(args.bucket_id);
  int result_index = 0;
  uint8_t n_final_replicas = args.n_replicas;

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
      items_to_place = n_final_replicas;
    }

    args.bucket_id = bucket_id;
    args.n_replicas = items_to_place;

    auto result = mPlacementStrategy[strategy_index(args.strategy)]->placeFiles(cluster_data, args);
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
                               PlacementArguments args)
{
  uint8_t n_final_replicas = args.n_replicas;
  if (!is_valid_placement_strategy(args.strategy) ||
      mPlacementStrategy[strategy_index(args.strategy)] == nullptr) {
    PlacementResult result;
    result.err_msg = "Not a valid PlacementStrategy";
    result.ret_code = EINVAL;
    return result;
  }

  do {
    const auto& bucket = cluster_data.buckets.at(-args.bucket_id);
    uint8_t n_replicas = 1;
    if (bucket.bucket_type == static_cast<uint8_t>(StdBucketType::GROUP)) {
      n_replicas = n_final_replicas;
    }

    args.n_replicas = n_replicas;
    //PlacementStrategy::Args plct_args{args.bucket_id, n_replicas, args.status};
    auto result = mPlacementStrategy[strategy_index(args.strategy)]->placeFiles(cluster_data, args);
    if (!result || result.ids.empty()) {
      return result;
    }

    if (result.is_valid_placement(n_replicas)) {
      return result;
    }

    args.bucket_id = result.ids.front();
  } while(args.bucket_id < 0);

  return {};
}


} // namespace eos::mgm::placement
