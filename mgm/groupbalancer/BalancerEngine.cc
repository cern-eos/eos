#include "BalancerEngine.hh"
#include "common/Logging.hh"

namespace eos::mgm::group_balancer {

void BalancerEngine::populateGroupsInfo(group_size_map&& info)
{
  clear();
  mGroupSizes = std::move(info);
  recalculate();
  updateGroups();
}

void BalancerEngine::clear_thresholds()
{
  mGroupsOverThreshold.clear();
  mGroupsUnderThreshold.clear();
}

void BalancerEngine::clear()
{
  mGroupSizes.clear();
  clear_thresholds();
}



void BalancerEngine::updateGroups()
{
  clear_thresholds();
  if (!mGroupSizes.size()) {
    return;
  }

  for (const auto& kv: mGroupSizes) {
    updateGroup(kv.first);
  }
}

int BalancerEngine::record_transfer(std::string_view source_group,
                                    std::string_view target_group,
                                    uint64_t filesize)
{
  auto source_grp = mGroupSizes.find(source_group);
  auto target_grp = mGroupSizes.find(target_group);

  if (source_grp == mGroupSizes.end() || target_grp == mGroupSizes.end()) {
    eos_static_err("Invalid source/target groups given!");
    return ENOENT;
  }

  source_grp->second.swapFile(&target_grp->second, filesize);
  return 0;
}

} // eos::mgm::GroupBalancer
