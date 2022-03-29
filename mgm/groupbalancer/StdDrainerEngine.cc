#include "mgm/groupbalancer/StdDrainerEngine.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"

namespace eos::mgm::group_balancer {

void
StdDrainerEngine::configure(const engine_conf_t& conf)
{
  using namespace std::string_view_literals;
  std::string err;
  mThreshold = extract_percent_value(conf, "threshold", 0.01, &err);
}

void StdDrainerEngine::recalculate()
{
  mAvgUsedSize = calculateAvg(data.mGroupSizes);
}
void
StdDrainerEngine::updateGroup(const std::string& group_name)
{
  // Groups Under threshold are targets and Over threshold are source groups
  auto kv = data.mGroupSizes.find(group_name);
  if (kv == data.mGroupSizes.end()) {
    return;
  }

  const GroupSizeInfo& groupSizeInfo = kv->second;

  if (groupSizeInfo.draining()) {
    data.mGroupsOverThreshold.emplace(group_name);
    data.mGroupsUnderThreshold.erase(group_name);
  } else if (groupSizeInfo.on()) {
    double diffWithAvg = groupSizeInfo.filled() - mAvgUsedSize;
    if (mThreshold == 0 ||
        (std::abs(diffWithAvg) > mThreshold && diffWithAvg < 0)) {
      data.mGroupsUnderThreshold.emplace(group_name);
    }
  }
}

} // namespace eos::mgm::group_balancer
