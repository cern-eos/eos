//------------------------------------------------------------------------------
// File: StdDevBalancerEngine.cc
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/groupbalancer/StdDevBalancerEngine.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "common/Logging.hh"

namespace eos::mgm::group_balancer {

void StdDevBalancerEngine::configure(const engine_conf_t& conf)
{
  using namespace std::string_view_literals;
  mThreshold = extract_percent_value(conf, "threshold"sv);
}

void StdDevBalancerEngine::recalculate()
{
  mAvgUsedSize = calculateAvg(data.mGroupSizes);
}


void StdDevBalancerEngine::updateGroup(const std::string& group_name)
{
  auto kv = data.mGroupSizes.find(group_name);
  if (kv == data.mGroupSizes.end()) {
    return;
  }

  const GroupSize& groupSize = kv->second;
  double diffWithAvg = ((double) groupSize.filled()
                        - ((double) mAvgUsedSize));
  // set erase only erases if found, so this is safe without key checking
  data.mGroupsOverThreshold.erase(group_name);
  data.mGroupsUnderThreshold.erase(group_name);
  eos_static_debug("diff=%.02f threshold=%.02f", diffWithAvg, mThreshold);

  // Group is mThreshold over or under the average used size
  if (abs(diffWithAvg) > mThreshold) {
    if (diffWithAvg > 0) {
      data.mGroupsOverThreshold.emplace(group_name);
    } else {
      data.mGroupsUnderThreshold.emplace(group_name);
    }
  }
}

std::string StdDevBalancerEngine::get_status_str(bool detail, bool monitoring) const
{
  std::stringstream oss;
  if (!monitoring) {
    oss << "Engine configured: Std\n";
    oss << "Deviation Threshold    : " << mThreshold << "\n";
  }
  oss << BalancerEngine::get_status_str(detail, monitoring);
  return oss.str();

}
}
