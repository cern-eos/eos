#include "mgm/groupbalancer/FreeSpaceBalancerEngine.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "common/Logging.hh"

namespace eos::mgm::group_balancer
{

void FreeSpaceBalancerEngine::configure(const engine_conf_t& conf)
{
  using namespace std::string_view_literals;
  std::string err;
  std::scoped_lock lock(mtx);
  mMinDeviation = extract_percent_value(conf, "min_threshold"sv, 2, &err);

  if (!err.empty()) {
    eos_static_err("msg=\"failed to set min_deviation\" err=%s", err.c_str());
  }

  mMaxDeviation = extract_percent_value(conf, "max_threshold"sv, 2, &err);

  if (!err.empty()) {
    eos_static_err("msg=\"failed to set max_deviation\" err=%s", err.c_str());
  }

  mBlocklistedGroups = extract_commalist_value(conf, "blocklisted_groups"sv);
}

void FreeSpaceBalancerEngine::recalculate()
{
  uint64_t total_size{0};
  uint64_t total_used{0};
  uint16_t count{0};
  std::scoped_lock lock(mtx);
  std::for_each(data.mGroupSizes.begin(), data.mGroupSizes.end(),
  [&](const auto & kv) {
    if (mBlocklistedGroups.find(kv.first) == mBlocklistedGroups.end()) {
      const auto& group_info = kv.second;

      if (group_info.on()) {
        total_size += group_info.capacity();
        total_used += group_info.usedBytes();
        ++count;
      }
    }
  });
  mTotalFreeSpace = total_size - total_used;

  if (count > 0) {
    mGroupFreeSpace = mTotalFreeSpace / count; // integer division, half of a byte
    // makes no sense, round down is fine
  }
}

uint64_t FreeSpaceBalancerEngine::getGroupFreeSpace() const
{
  return mGroupFreeSpace;
}

uint64_t FreeSpaceBalancerEngine::getFreeSpaceULimit() const
{
  return static_cast<uint64_t>(mGroupFreeSpace * (1 + mMaxDeviation));
}

uint64_t FreeSpaceBalancerEngine::getFreeSpaceLLimit() const
{
  return static_cast<uint64_t>(mGroupFreeSpace * (1 - mMinDeviation));
}

void FreeSpaceBalancerEngine::updateGroup(const std::string& group_name)
{
  std::scoped_lock lock(mtx);
  // clear threshold is a set erase. should always work!
  clear_threshold(group_name);

  if (mBlocklistedGroups.find(group_name) != mBlocklistedGroups.end()) {
    return;
  }

  auto kv = data.mGroupSizes.find(group_name);

  if (kv == data.mGroupSizes.end()) {
    return;
  }

  uint64_t group_free_bytes = kv->second.capacity() - kv->second.usedBytes();
  uint64_t upper_limit = getFreeSpaceULimit();
  uint64_t lower_limit = getFreeSpaceLLimit();

  if (group_free_bytes > upper_limit) {
    data.mGroupsUnderThreshold.emplace(group_name);
  }

  if (group_free_bytes < lower_limit) {
    data.mGroupsOverThreshold.emplace(group_name);
  }
}

std::string FreeSpaceBalancerEngine::get_status_str(bool detail,
    bool monitoring) const
{
  std::stringstream oss;
  std::scoped_lock lock(mtx);

  std::scoped_lock lock(mtx);
  if (!monitoring) {
    oss << "Engine configured: FreeSpace\n";
    oss << "Min Threshold   : " << mMinDeviation << "\n";
    oss << "Max Threshold   : " << mMaxDeviation << "\n";
    oss << "Total Freespace : " << mTotalFreeSpace << "\n";
    oss << "Group Freespace : " << mGroupFreeSpace << "\n";
  }

  oss << BalancerEngine::get_status_str(detail, monitoring);

  if (!mBlocklistedGroups.empty()) {
    oss << "Blocklisted groups: \n";

    for (const auto& group : mBlocklistedGroups) {
      oss << group << "\n";
    }
  }

  return oss.str();
}

}
