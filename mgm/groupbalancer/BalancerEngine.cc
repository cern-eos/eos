#include "BalancerEngine.hh"
#include "common/Logging.hh"

namespace eos::mgm::group_balancer {

void BalancerEngine::populateGroupsInfo(group_size_map&& info)
{
  clear();
  data.mGroupSizes = std::move(info);
  recalculate();
  updateGroups();
}

void BalancerEngine::clear_thresholds()
{
  data.mGroupsOverThreshold.clear();
  data.mGroupsUnderThreshold.clear();
}

void BalancerEngine::clear()
{
  data.mGroupSizes.clear();
  clear_thresholds();
}



void BalancerEngine::updateGroups()
{
  clear_thresholds();
  if (!data.mGroupSizes.size()) {
    return;
  }

  for (const auto& kv: data.mGroupSizes) {
    updateGroup(kv.first);
  }
}

int BalancerEngine::record_transfer(std::string_view source_group,
                                    std::string_view target_group,
                                    uint64_t filesize)
{
  auto source_grp = data.mGroupSizes.find(source_group);
  auto target_grp = data.mGroupSizes.find(target_group);

  if (source_grp == data.mGroupSizes.end() || target_grp == data.mGroupSizes.end()) {
    eos_static_err("Invalid source/target groups given!");
    return ENOENT;
  }

  source_grp->second.swapFile(&target_grp->second, filesize);
  return 0;
}

template <typename C>
static std::string pprint(const C& c, uint8_t items_per_line)
{
  std::stringstream ss;
  ss << "[\n";
  uint8_t ctr = 0;
  for (const auto& it: c) {
    ss << it << ", ";
    if (++ctr%items_per_line == 0) {
      ss << "\n";
    }
  }
  ss << "]\n";
  return ss.str();
}

std::string BalancerEngine::get_status_str(bool detail) const
{
  std::stringstream oss;
  oss << "Total Group Size: " << data.mGroupSizes.size() << "\n"
      << "Total Groups Over Threshold: " << data.mGroupsOverThreshold.size() << "\n"
      << "Total Groups Under Threshold: " << data.mGroupsUnderThreshold.size() << "\n";

  if (detail) {
    oss << "Groups Over Threshold:" << pprint(data.mGroupsOverThreshold,10) << "\n";
    oss << "Groups Under Threshold: " << pprint(data.mGroupsUnderThreshold,10) << "\n";
  }

  return oss.str();
}
} // eos::mgm::GroupBalancer
