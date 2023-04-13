#include "common/Logging.hh"
#include "mgm/placement/WeightedPlacementStrategy.hh"
#include <random>
#include <shared_mutex>

namespace eos::mgm::placement {

struct WeightedRandomPlacement::Impl {
  PlacementResult placeFiles(const ClusterData& data,
                             Args args);

  void populateWeights(const ClusterData& data);
  Impl(): rd(), gen(rd()) {}
  std::shared_mutex mtx;
  std::discrete_distribution<> mBucketWeights;
  std::map<item_id_t, std::discrete_distribution<>> mDiskWeights;
  std::random_device rd;
  std::mt19937 gen;
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

  for (int i = 0; i < args.n_replicas; i++) {
    auto item_index = mDiskWeights[args.bucket_id](gen);
    eos_static_debug("Got item_index=%d item_id=%d",
                     item_index, data.buckets[bucket_index].items[item_index]);
    result.ids[i] = data.buckets[bucket_index].items[item_index];
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