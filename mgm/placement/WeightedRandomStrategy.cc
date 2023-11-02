#include "mgm/placement/WeightedRandomStrategy.hh"
#include "common/Logging.hh"
#include <random>
#include <shared_mutex>

namespace eos::mgm::placement {

struct WeightedRandomPlacement::Impl {
  PlacementResult placeFiles(const ClusterData& data,
                             Args args);

  void populateWeights(const ClusterData& data);
  std::shared_mutex mtx;
  std::discrete_distribution<> mBucketWeights;
  std::map<item_id_t, std::discrete_distribution<>> mDiskWeights;
};

void WeightedRandomPlacement::Impl::populateWeights(const ClusterData& data)
{
  std::vector<int> weights(data.buckets.size());
  std::vector<int> item_weights;

  // TODO optimize single element lists! no need to use a random distrib!
  for (const auto& bucket : data.buckets) {
    weights.at(-bucket.id) = bucket.total_weight;
    for (const auto& item_id : bucket.items) {
      if (item_id > 0) {
        item_weights.push_back(data.disks.at(item_id - 1).weight);
      } else {
        item_weights.push_back(data.buckets.at(-item_id).total_weight);
      }
    }

    mDiskWeights.emplace(bucket.id, std::discrete_distribution<>(item_weights.begin(),
                                                                 item_weights.end()));

    item_weights.clear();
  }
  mBucketWeights = std::discrete_distribution<>(weights.begin(), weights.end());
}

PlacementResult WeightedRandomPlacement::Impl::placeFiles(const ClusterData& data,
                                                          Args args)
{
  PlacementResult result(args.n_replicas);
  static thread_local std::random_device rd;
  static thread_local std::mt19937 gen(rd());
  std::shared_lock rlock(mtx);

  // This is only called at initialization
  if (mBucketWeights.max() == 0) {
    rlock.unlock();
    std::unique_lock wlock(mtx);
    if (mBucketWeights.max() == 0) {
      try {
        populateWeights(data);
      } catch (std::exception& e) {
        eos_static_crit("msg=\"exception while populating weights\" ec=%d emsg=\"%s\"",
                        EINVAL, e.what());
        result.err_msg = e.what();
        result.ret_code = EINVAL;
        return result;
      }
    }
  }

  int32_t bucket_index = -args.bucket_id;
  int items_added = 0;

  for (int i = 0; items_added < args.n_replicas && i < MAX_PLACEMENT_ATTEMPTS;
       i++) {
    auto item_index = mDiskWeights[args.bucket_id](gen);
    eos_static_debug("Got item_index=%d item_id=%d",
                     item_index, data.buckets[bucket_index].items[item_index]);
    //result.ids[i] = data.buckets[bucket_index].items[item_index];
    item_id_t item_id = data.buckets[bucket_index].items[item_index];

    if (result.contains(item_id)) {
      eos_static_info("msg=\"Skipping duplicate result\" item_id=%d", item_id);
      continue;
    }

    if (item_id > 0) {
      if ((size_t)item_id > data.disks.size()) {
        result.err_msg = "Disk ID out of range";
        result.ret_code = ERANGE;
        return result;
      }

      const auto& disk = data.disks[item_id - 1];
      if (disk.config_status.load(std::memory_order_relaxed) < args.status) {
        continue;
      }

      if (std::find(args.excludefs.begin(), args.excludefs.end(), item_id)
          != args.excludefs.end()) {
        continue;
      }
    }
    result.ids[items_added++] = item_id;
  }
  result.ret_code = 0;
  return result;
}

WeightedRandomPlacement::WeightedRandomPlacement(PlacementStrategyT strategy,
                                                 size_t max_buckets) :
    mImpl(std::make_unique<Impl>())
{
}

PlacementResult WeightedRandomPlacement::placeFiles(const ClusterData& data,
                                                    Args args)
{
  PlacementResult result(args.n_replicas);
  if (!validateArgs(data, args, result)){
    return result;
  }
  return mImpl->placeFiles(data, std::move(args));
}

WeightedRandomPlacement::~WeightedRandomPlacement() = default;

} // namespace eos::mgm::placement
