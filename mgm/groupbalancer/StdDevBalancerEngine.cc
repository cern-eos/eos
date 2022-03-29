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

namespace eos::mgm::group_balancer
{

void StdDevBalancerEngine::configure(const engine_conf_t& conf)
{
  using namespace std::string_view_literals;
  std::string err;
  mMinDeviation = extract_percent_value(conf, "min_threshold"sv, 0.05, &err);

  if (!err.empty()) {
    eos_static_err("msg=\"failed to set min_deviation\" err=%s", err.c_str());
  }

  mMaxDeviation = extract_percent_value(conf, "max_threshold"sv, 0.05, &err);

  if (!err.empty()) {
    eos_static_err("msg=\"failed to set max_deviation\" err=%s", err.c_str());
  }
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

  const GroupSizeInfo& groupSize = kv->second;
  double diffWithAvg = groupSize.filled() - mAvgUsedSize;
  // set erase only erases if found, so this is safe without key checking
  data.mGroupsOverThreshold.erase(group_name);
  data.mGroupsUnderThreshold.erase(group_name);
  eos_static_debug("diff=%.02f", diffWithAvg);

  if (abs(diffWithAvg) > mMaxDeviation && diffWithAvg > 0) {
    data.mGroupsOverThreshold.emplace(group_name);
  }

  // Group is mThreshold over or under the average used size
  if (abs(diffWithAvg) > mMinDeviation && diffWithAvg < 0) {
    data.mGroupsUnderThreshold.emplace(group_name);
  }
}

std::string StdDevBalancerEngine::get_status_str(bool detail,
    bool monitoring) const
{
  std::stringstream oss;

  if (!monitoring) {
    oss << "Engine configured          : Std\n";
    oss << "Current Computed Average   : " << mAvgUsedSize << "\n";
    oss << "Min Deviation Threshold    : " << mMinDeviation << "\n";
    oss << "Max Deviation Threshold    : " << mMaxDeviation << "\n";
  }

  oss << BalancerEngine::get_status_str(detail, monitoring);
  return oss.str();
}

} // namespace eos::mgm::group_balancer
