//------------------------------------------------------------------------------
// File: GroupsInfoFetcher.hh
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


#pragma once
#include "mgm/groupbalancer/BalancerEngineTypes.hh"
#include <memory>

namespace eos::mgm::group_balancer {
// A simple interface to populate the group_size map per group. This is useful
// for DI scenarios where we can alternatively fill in the group_size structures
struct IGroupsInfoFetcher {
  virtual group_size_map fetch() = 0;
  virtual ~IGroupsInfoFetcher() = default;
};

class eosGroupsInfoFetcher final: public IGroupsInfoFetcher
{
public:
  eosGroupsInfoFetcher(const std::string& _spaceName): spaceName(_spaceName) {}

  group_size_map fetch() override;

private:
  std::string spaceName;
};


}
