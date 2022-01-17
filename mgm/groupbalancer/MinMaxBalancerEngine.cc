#include "mgm/groupbalancer/MinMaxBalancerEngine.hh"
#include "common/Logging.hh"

namespace eos::mgm::group_balancer {
using namespace std::string_view_literals;

void MinMaxBalancerEngine::configure(const engine_conf_t& conf)
{
  mMinThreshold = extract_percent_value(conf,"min_threshold"sv);
  mMaxThreshold = extract_percent_value(conf,"max_threshold"sv);
}

void MinMaxBalancerEngine::updateGroup(const std::string& group_name)
{
  auto kv = data.mGroupSizes.find(group_name);
  if (kv == data.mGroupSizes.end()) {
    return;
  }

  clear_threshold(group_name);

  if (kv->second.filled() > mMaxThreshold) {
    data.mGroupsOverThreshold.emplace(group_name);
  } else if (kv->second.filled() < mMinThreshold) {
    data.mGroupsUnderThreshold.emplace(group_name);
  }
}

} // namespace eos::mgm::group_balancer
