#include "mgm/placement/WeightedRoundRobinStrategy.hh"
#include "common/utils/ContainerUtils.hh"
#include "common/Logging.hh"
#include <mutex>

namespace eos::mgm::placement {


struct WeightedRoundRobinPlacement::Impl {
  PlacementResult placeFiles(const ClusterData& data,
                             Args args);

  void fill_weights(const ClusterData& data)
  {
    total_wt = 0;
    std::for_each(data.buckets.begin(),
                  data.buckets.end(),
                  [this](const Bucket& bucket) {
                    mItemWeights[bucket.id] = bucket.total_weight;
                    total_wt += bucket.total_weight;
                  });
    total_disk_wt = 0;
    std::for_each(data.disks.begin(),
                  data.disks.end(),
                  [this](const Disk& disk) {
                    auto disk_wt = disk.weight.load(std::memory_order_acquire);
                    mItemWeights[disk.id] = disk_wt;
                    total_disk_wt += disk_wt;
                  });
  }

  std::map<item_id_t, int> mItemWeights;
  std::atomic<epoch_id_t> mCurrentEpoch;
  std::map<item_id_t, int> mBucketIndex;
  std::atomic<int64_t> total_wt {0};
  std::atomic<int64_t> total_disk_wt{0};
  std::mutex wt_mtx;
};



PlacementResult
WeightedRoundRobinPlacement::Impl::placeFiles(const ClusterData& cluster_data,
                                              Args args)
{
  std::scoped_lock lock(wt_mtx);
  //NOTE: when 2 requests reach at the same point when near 0, we'll end up
  //granting all of them... in spite of near 0 weights.. this is fine as the
  //weighting is still an approximate means and there is no need for exactness,
  //the next cycle should refresh the weights correctly

  if (total_wt < (args.n_replicas)) {
    eos_static_info("%s","msg=\"Refilling weights\"");
    fill_weights(cluster_data);
  }

  PlacementResult result(args.n_replicas);

  auto bucket_index_kv = mBucketIndex[args.bucket_id]++;
  int32_t bucket_index = -args.bucket_id;
  const auto& bucket = cluster_data.buckets[bucket_index];
  int items_added = 0;
  for (int i = 0;
       (items_added < args.n_replicas) && (i < MAX_PLACEMENT_ATTEMPTS); i++) {
    item_id_t item_id = eos::common::pickIndexRR(bucket.items, bucket_index_kv++);

    if (result.contains(item_id)) {
      continue;
    }

    if (item_id > 0) {
      if ((mItemWeights[args.bucket_id] < args.n_replicas)
          || mItemWeights[args.bucket_id] == mItemWeights[item_id]) {
        fill_weights(cluster_data);
      }

      if (--mItemWeights[item_id] < 0) {
        eos_static_debug("msg=\"Skipping scheduling 0 wt item at\" item_id=%d total_wt=%llu",
                         item_id, total_wt.load(std::memory_order_relaxed));
        continue;
      }

      if (std::find(args.excludefs.begin(),
                    args.excludefs.end(), item_id) != args.excludefs.end()) {
        continue;
      }

      if ((size_t)item_id > cluster_data.disks.size()) {
        result.err_msg = "Disk ID unknown!";
        result.ret_code = ERANGE;
        return result;
      }

      const auto& disk = cluster_data.disks[item_id - 1];
      if (disk.active_status.load(std::memory_order_acquire) !=
          eos::common::ActiveStatus::kOnline) {
        continue;
      }

      auto disk_status = disk.config_status.load(std::memory_order_relaxed);
      if (disk_status < args.status) {
        continue;
      }


      item_id = disk.id;
      --total_wt;
      --mItemWeights[args.bucket_id];


    } else {
      // We're dealing with a bucket, make sure we've enough wt left!
      if (mItemWeights[item_id] < args.n_replicas) {
        continue;
      }
    }
    result.ids[items_added++] = item_id;
  }
  if (items_added != args.n_replicas) {
    result.err_msg = "Failed to place files!";
    result.ret_code = ENOSPC;
    return result;
  }
  result.ret_code = 0;
  return result;
}

WeightedRoundRobinPlacement::WeightedRoundRobinPlacement(PlacementStrategyT strategy,
                                                         size_t max_buckets):
  mImpl(std::make_unique<Impl>())
{}

PlacementResult WeightedRoundRobinPlacement::placeFiles(const ClusterData& data,
                                                        Args args)
{
  PlacementResult result(args.n_replicas);
  if (!validateArgs(data, args, result)) {
    return result;
  }

  return mImpl->placeFiles(data, std::move(args));
}

int WeightedRoundRobinPlacement::access(const ClusterData &data, AccessArguments args)
{
  return EINVAL;
}

WeightedRoundRobinPlacement::~WeightedRoundRobinPlacement() = default;
} // eos::mgm::placement
