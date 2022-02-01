//------------------------------------------------------------------------------
//! @file FsBalancerStats.cc
//! @author Elvin Sindrilaru <esindril@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "mgm/balancer/FsBalancerStats.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Update statistics with information from the group and file systems stats
//------------------------------------------------------------------------------
void FsBalancerStats::Update(eos::mgm::FsView* fs_view, double threshold)
{
  std::set<std::string> grp_to_remove;
  std::set<std::string> grp_to_update;
  auto grp_dev = fs_view->GetUnbalancedGroups(mSpaceName, threshold);

  for (const auto& elem : grp_dev) {
    // Check if group needs to be added or updated
    auto it = mGrpToMaxDev.find(elem.first);

    if (it != mGrpToMaxDev.end()) {
      // Check against the cached value
      if (std::floor(mGrpToMaxDev[elem.first]) != std::floor(elem.second)) {
        grp_to_update.insert(elem.first);
      }
    } else {
      grp_to_update.insert(elem.first);
    }
  }

  // Check if there are groups that need to be removed
  for (const auto& elem : mGrpToMaxDev) {
    if (grp_dev.find(elem.first) == grp_dev.end()) {
      grp_to_remove.insert(elem.first);
    }
  }

  for (const auto& grp : grp_to_remove) {
    mGrpToMaxDev.erase(grp);
    mGrpToPrioritySets.erase(grp);
  }

  if (grp_to_update.empty()) {
    return;
  }

  for (const auto& grp : grp_to_update) {
    mGrpToMaxDev.emplace(grp, grp_dev[grp]);
    mGrpToPrioritySets.emplace(grp, fs_view->GetFsToBalance(grp, threshold));
  }

  return;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------


EOSMGMNAMESPACE_END
