#include "BalancerEngine.hh"
#include "common/Logging.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "common/table_formatter/TableFormatterBase.hh"

namespace eos::mgm::group_balancer {

void BalancerEngine::populateGroupsInfo(group_size_map&& info)
{
  clear();
  data.mGroupSizes = std::move(info);
  recalculate();
  updateGroups();
}

void BalancerEngine::clear_threshold(const std::string& group_name)
{
  data.mGroupsOverThreshold.erase(group_name);
  data.mGroupsUnderThreshold.erase(group_name);
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

groups_picked_t
BalancerEngine::pickGroupsforTransfer()
{
  if (data.mGroupsUnderThreshold.size() == 0 || data.mGroupsOverThreshold.size() == 0) {
    if (data.mGroupsOverThreshold.size() == 0) {
      eos_static_debug("No groups over the average!");
    }

    if (data.mGroupsUnderThreshold.size() == 0) {
      eos_static_debug("No groups under the average!");
    }

    recalculate();
    return {};
  }


  auto over_it = data.mGroupsOverThreshold.begin();
  auto under_it = data.mGroupsUnderThreshold.begin();
  int rndIndex = getRandom(data.mGroupsOverThreshold.size() - 1);
  std::advance(over_it, rndIndex);
  rndIndex = getRandom(data.mGroupsUnderThreshold.size() - 1);
  std::advance(under_it, rndIndex);
  return {*over_it, *under_it};
}

std::string BalancerEngine::generate_table(const threshold_group_set& groups) const
{
  TableFormatterBase table_threshold_groups(true);
  std::string format_s = "-s";
  std::string format_l = "+l";
  std::string format_f = "f";

  table_threshold_groups.SetHeader({
      {"Group",10,"-s"},
      {"UsedBytes",10,"+l"},
      {"Capacity",10,"+l"},
      {"Filled",10,"f"}
    });


  TableData table_data;
  for (const auto& grp: groups) {
    const auto kv = data.mGroupSizes.find(grp);
    if (kv == data.mGroupSizes.end()) {
      continue;
    }
    TableRow row;
    row.emplace_back(grp, "-s");
    // FIXME force a double conversion as the current table cell ultimately will
    // use a double when using + anyway. a TODO is have the table formatter
    // itself understand uint64_t type and avoid a double conversion as units
    // can be done without
    row.emplace_back((double)kv->second.usedBytes(), format_l);
    row.emplace_back((double)kv->second.capacity(), format_l);
    row.emplace_back(kv->second.filled(), format_f);
    table_data.emplace_back(std::move(row));
  }
  table_threshold_groups.AddRows(table_data);
  return table_threshold_groups.GenerateTable();
}


std::string BalancerEngine::get_status_str(bool detail, bool monitoring) const
{
  std::stringstream oss;

  if (monitoring) {
    oss << "groupbalancer.groups_over_threshold=" << data.mGroupsOverThreshold.size()
        << " groupbalancer.groups_under_threshold=" << data.mGroupsUnderThreshold.size();
    return oss.str();
  }


  oss << "Total Group Size: " << data.mGroupSizes.size() << "\n"
      << "Total Groups Over Threshold: " << data.mGroupsOverThreshold.size() << "\n"
      << "Total Groups Under Threshold: " << data.mGroupsUnderThreshold.size() << "\n";

 if (detail) {
   oss << "Groups Over Threshold\n";
   oss << generate_table(data.mGroupsOverThreshold) << "\n";
   oss << "Groups Under Threshold\n";
   oss << generate_table(data.mGroupsUnderThreshold) << "\n";
 }

  return oss.str();
}
} // eos::mgm::GroupBalancer
