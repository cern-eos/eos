//------------------------------------------------------------------------------
// File: BalancerEngine.hh
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
#include <map>
#include <numeric>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <random>
#include "mgm/groupbalancer/BalancerEngineTypes.hh"

namespace eos::mgm::group_balancer {

struct IBalancerEngine
{
  // //----------------------------------------------------------------------------
  // // Fills mGroupSizes, calculates avg and classifies the data
  // //----------------------------------------------------------------------------
  // virtual void populateGroupsInfo(IGroupsInfoFetcher* f) = 0;


  //----------------------------------------------------------------------------
  //! Recalculates the sizes average from the mGroupSizes
  //----------------------------------------------------------------------------
  virtual void recalculate() = 0;

  //----------------------------------------------------------------------------
  //! clears all data structures, also used when re-filling all info
  //----------------------------------------------------------------------------
  virtual void clear() = 0;

  //----------------------------------------------------------------------------
  //! Classifies a given group in one of the 2 categories
  //----------------------------------------------------------------------------
  virtual void updateGroup(const std::string& group_name) = 0;

  //----------------------------------------------------------------------------
  //! Classifies all groups in one of the 2 categories
  //----------------------------------------------------------------------------
  virtual void updateGroups() = 0;

  //----------------------------------------------------------------------------
  //! Return randomly a pair of groups over avg & under avg that will be used
  //! for transferring
  //----------------------------------------------------------------------------
  virtual groups_picked_t pickGroupsforTransfer() = 0;

  //----------------------------------------------------------------------------
  //! Return a pair of groups over avg & under avg at given index
  //----------------------------------------------------------------------------
  virtual groups_picked_t pickGroupsforTransfer(uint64_t index) = 0;

  virtual void configure(const engine_conf_t& conf) = 0;

  virtual const group_size_map& get_group_sizes() const = 0;

  virtual std::string get_status_str(bool detail,bool monitoring) const = 0;

  virtual ~IBalancerEngine() {};
};

struct BalancerEngineData
{
  threshold_group_set mGroupsOverThreshold;
  threshold_group_set mGroupsUnderThreshold;
  group_size_map mGroupSizes;
};

// A simple base class implementing common functionalities for most BalancerEngines
// Note that this class doesn't implement the entire interface, so cannot be constructed!
class BalancerEngine: public IBalancerEngine
{
public:
  virtual void populateGroupsInfo(group_size_map&& info);
  void updateGroups() override;
  void clear() override;

  const group_size_map& get_group_sizes() const override
  {
    return data.mGroupSizes;
  }

  std::string get_status_str(bool detail=false, bool monitoring=false) const override;


  BalancerEngine() = default;
  virtual ~BalancerEngine() = default;

  // Only useful for unit-testing/ validating the status of the balancerengine
  const BalancerEngineData& get_data() const
  {
    return data;
  }

  groups_picked_t pickGroupsforTransfer() override;

  groups_picked_t pickGroupsforTransfer(uint64_t index) override;

  bool canPick() const {
    return !data.mGroupsOverThreshold.empty() &&
           !data.mGroupsUnderThreshold.empty();
  }

protected:
  BalancerEngineData data;

  void clear_threshold(const std::string& group_name);
  void clear_thresholds();
  std::string generate_table(const threshold_group_set& items) const;

};


} // namespace eos::mgm::group_balancer
