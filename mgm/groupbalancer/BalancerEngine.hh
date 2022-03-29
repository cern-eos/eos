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

namespace eos::mgm::group_balancer {

//------------------------------------------------------------------------------
//! @brief Class representing a group's size
//! It holds the capacity and the current used space of a group.
//------------------------------------------------------------------------------
class GroupSize
{
public:
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  GroupSize(uint64_t usedBytes, uint64_t capacity) : mSize(usedBytes),
                                                     mCapacity(capacity)
  {}

  //------------------------------------------------------------------------------
  //! Subtracts the given size from this group and adds it to the given toGroup
  //!
  //! @param toGroup the group where to add the size
  //! @param size the file size that should be swapped
  //------------------------------------------------------------------------------
  void swapFile(GroupSize* toGroup, uint64_t size)
  {
    toGroup->mSize += size;
    mSize -= size;
  }

  uint64_t
  usedBytes() const
  {
    return mSize;
  }

  uint64_t
  capacity() const
  {
    return mCapacity;
  }

  double
  filled() const
  {
    return (double) mSize / (double) mCapacity;
  }

private:
  uint64_t mSize;
  uint64_t mCapacity;
};



// Allow std::string_view -> std::string lookups on keys
using group_size_map = std::map<std::string,GroupSize, std::less<>>;
using threshold_group_set = std::unordered_set<std::string>;
using groups_picked_t = std::pair<std::string, std::string>;
using engine_conf_t = std::map<std::string, std::string, std::less<>>;

enum class BalancerEngineT {
  stddev,
  minmax,
  total_count
};

// A simple interface to populate the group_size map per group. This is useful
// for DI scenarios where we can alternatively fill in the group_size structures
struct IBalancerInfoFetcher {
  virtual group_size_map fetch() = 0;
};


struct IBalancerEngine
{
  // //----------------------------------------------------------------------------
  // // Fills mGroupSizes, calculates avg and classifies the data
  // //----------------------------------------------------------------------------
  // virtual void populateGroupsInfo(IBalancerInfoFetcher* f) = 0;


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
  //! Return a pair of groups over avg & under avg that will be used for
  //transferring
  //----------------------------------------------------------------------------
  virtual groups_picked_t pickGroupsforTransfer() = 0;


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

protected:
  BalancerEngineData data;

  void clear_threshold(const std::string& group_name);
  void clear_thresholds();
  std::string generate_table(const threshold_group_set& items) const;

};


} // namespace eos::mgm::group_balancer
