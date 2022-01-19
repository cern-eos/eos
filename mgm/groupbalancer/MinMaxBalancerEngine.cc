#include "mgm/groupbalancer/MinMaxBalancerEngine.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "common/Logging.hh"
#include <sstream>

namespace eos::mgm::group_balancer {
using namespace std::string_view_literals;

void MinMaxBalancerEngine::configure(const engine_conf_t& conf)
{
  std::string err;
  mMinThreshold = extract_percent_value(conf,"min_threshold"sv, 60, &err);
  if (!err.empty()) {
    eos_static_err("msg=Failed to set min_threshold, err=%s", err.c_str());
  }
  mMaxThreshold = extract_percent_value(conf,"max_threshold"sv, 90, &err);
  if (!err.empty()) {
    eos_static_err("msg=Failed to set max_threshold, err=%s", err.c_str());
  }

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

std::string MinMaxBalancerEngine::get_status_str(bool detail, bool monitoring) const
{
  std::stringstream oss;
  if (!monitoring) {
    oss << "Engine configured: MinMax\n";
    oss << "Min Threshold    : " << mMinThreshold << "\n";
    oss << "Max Threshold    : " << mMaxThreshold << "\n";
  }
  oss << BalancerEngine::get_status_str(detail, monitoring);
  return oss.str();
}

} // namespace eos::mgm::group_balancer
