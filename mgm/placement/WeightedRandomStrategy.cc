#include "mgm/placement/WeightedRandomStrategy.hh"
#include "common/Logging.hh"
#include <random>
#include <shared_mutex>

namespace eos::mgm::placement
{

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

    mDiskWeights.emplace(bucket.id,
                         std::discrete_distribution<>(item_weights.begin(),
                             item_weights.end()));
    item_weights.clear();
  }

  mBucketWeights = std::discrete_distribution<>(weights.begin(), weights.end());
}

PlacementResult WeightedRandomPlacement::Impl::placeFiles(
  const ClusterData& data,
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
    item_id_t item_id = data.buckets[bucket_index].items[item_index];
    eos_static_debug("Got item_index=%d item_id=%d",
                     item_index, item_id);

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

      if (!PlacementStrategy::validDiskPlct(item_id, data, args.excludefs, args.status)) {
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

  if (!validateArgs(data, args, result)) {
    return result;
  }

  return mImpl->placeFiles(data, std::move(args));
}

int WeightedRandomPlacement::access(const ClusterData &data, AccessArguments args)
{

  //TODO move all of the common validation to base class!
  if (args.selectedfs.empty()) {
    return ENOENT;
  }

  uint64_t best_score = 0;
  size_t best_index = std::numeric_limits<size_t>::max();

  for (const auto& fsid: args.selectedfs) {
    if ((size_t)fsid > data.disks.size()) {
      eos_static_info("msg=\"FlatScheduler Access - Skipping invalid fsid\" fsid=%u", fsid);
      continue;
    }
    const auto& disk = data.disks[fsid - 1];

    if (!validDiskPlct(fsid, data, args.unavailfs ? *args.unavailfs : std::vector<uint32_t>{},
                        eos::common::ConfigStatus::kRO)) {
      continue;
    }

    auto h = hashFid(args.inode, fsid);
    auto wt = disk.weight.load(std::memory_order_relaxed);
    uint64_t score = h/wt;
    if (best_index == std::numeric_limits<size_t>::max() ||
        score < best_score) {
      best_score = score;
      best_index = fsid;
    }

  }

  if (best_index <= args.selectedfs.size()) {
    args.selectedIndex = best_index;
    return 0;
  }

  return ENOENT;
}

WeightedRandomPlacement::~WeightedRandomPlacement() = default;

} // namespace eos::mgm::placement
