//------------------------------------------------------------------------------
// File: FreeSpaceBalancerEngine.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland
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

#pragma once
#include <unordered_set>
#include "mgm/groupbalancer/BalancerEngine.hh"

namespace eos::mgm::group_balancer
{

class FreeSpaceBalancerEngine: public BalancerEngine
{
public:
  using group_set_t = std::unordered_set<std::string>;
  void recalculate() override;
  void updateGroup(const std::string& group_name) override;
  void configure(const engine_conf_t& conf) override;
  std::string get_status_str(bool detail = false,
                             bool monitoring = false) const override;
  // Currently consumed by tests, show the expected free space per group
  uint64_t getGroupFreeSpace() const;
  uint64_t getFreeSpaceULimit() const;
  uint64_t getFreeSpaceLLimit() const;
private:
  uint64_t mTotalFreeSpace; //!< Total Free space in the space
  uint64_t mGroupFreeSpace; //!< Per Group Free Space
  double mMinDeviation {0.02};     //!< Allowed percent deviation from left of GroupFreeSpace
  double mMaxDeviation {0.02};     //!< Allowed percent deviation from right of GroupFreeSpace
  //! TODO future: make this part of the base class and make this feature
  //! available for all engines
  group_set_t mBlocklistedGroups; //!< Groups that will be blocked from participation
};
}
