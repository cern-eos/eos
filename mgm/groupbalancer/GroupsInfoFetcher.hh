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
#include <string>

namespace eos::mgm::group_balancer {
// A simple interface to populate the group_size map per group. This is useful
// for DI scenarios where we can alternatively fill in the group_size structures
struct IGroupsInfoFetcher {
  virtual group_size_map fetch() = 0;
  virtual ~IGroupsInfoFetcher() = default;
};


struct OnGroupStatusFilter {
  bool operator()(GroupStatus status)  {
    return status == GroupStatus::ON;
  }
};

// This class fetches groups info from a given space and returns a map of groupname
// GroupSizeInfo, the groups can be filtered based on a status function that takes
// any callable object which returns bool and takes a GroupStatus argument
// examples:
//     eosGroupsInfoFetcher fetcher(space); // default. filters "ON" group
//                                          // ie. only these will be selected
//     eosGroupsInfoFetcher fetcher(space, OnGroupStatusFilter{});
//     eosGroupsInfoFetcher fetcher(space, [](GroupStatus s) { return s==GroupStatus::ON; });
//
class eosGroupsInfoFetcher final: public IGroupsInfoFetcher
{
  // A base filter function that must implement a apply method with status arg
  struct base_group_status_filter {
    virtual bool apply(GroupStatus status) = 0;
    virtual ~base_group_status_filter() = default;
  };

  // We inherit from the above base, to hold any callable which implements a
  // bool operator()(GroupStatus), since all of this is private, you can
  // hold almost any object that implements a call operator taking a status
  template <typename F>
  struct group_status_filter : public base_group_status_filter {
    group_status_filter(F&& _f): f(std::forward<F>(_f)) {};
    virtual bool apply(GroupStatus status) { return f(status);}
    F f;
  };

public:

  template <typename F>
  eosGroupsInfoFetcher(const std::string& _spaceName, F&& f): spaceName(_spaceName),
                                                              status_filter_fn(std::make_unique<group_status_filter<F>>(std::forward<F>(f))) {}

  eosGroupsInfoFetcher(const std::string& _spaceName): spaceName(_spaceName),
                                                       status_filter_fn(new group_status_filter(OnGroupStatusFilter{}))
  {}

  group_size_map fetch() override;

  bool is_valid_status(GroupStatus status) { return status_filter_fn->apply(status); }
private:
  std::string spaceName;
  std::unique_ptr<base_group_status_filter> status_filter_fn;
};


}
