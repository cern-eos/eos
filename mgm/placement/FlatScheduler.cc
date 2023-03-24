#include "mgm/placement/FlatScheduler.hh"

#include <queue>

namespace eos::mgm::placement {


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

  if (is_valid_placement_strategy(mDefaultStrategy)) {
    args.strategy = mDefaultStrategy;
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


    auto result = mPlacementStrategy[strategy_index(args.strategy)]->placeFiles(cluster_data, plct_args);
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
    auto result = mPlacementStrategy[strategy_index(args.strategy)]->placeFiles(cluster_data, plct_args);

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
