#pragma once
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

#include <map>
#include <numeric>
#include <cstdint>
#include <string>
#include <unordered_set>

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
using groups_picked_t = std::pair<std::string, std::string>;
using engine_conf_t = std::map<std::string, std::string, std::less<>>;

// A simple interface to populate the group_size map per group. This is useful
// for DI scenarios where we can alternatively fill in the group_size structures
struct IBalancerInfoFetcher {
  virtual group_size_map fetch() = 0;
};

inline double calculateAvg(const group_size_map& m)
{
  if (!m.size())
    return 0.0;

  return std::accumulate(m.begin(),m.end(),0ULL,
                         [](uint64_t s,const auto& kv) { return s + kv.second.filled(); })/m.size();
}

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

  // TODO: make this configurable
  virtual void configure(const engine_conf_t& conf) = 0;

  virtual int record_transfer(std::string_view source_group,
                              std::string_view target_group,
                              uint64_t filesize) = 0;

  virtual const group_size_map& get_group_sizes() const = 0;

  virtual ~IBalancerEngine() {};
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
    return mGroupSizes;
  }


  int record_transfer(std::string_view source_group,
                      std::string_view target_group,
                      uint64_t filesize) override;



  BalancerEngine() = default;
  virtual ~BalancerEngine() = default;
protected:
  std::unordered_set<std::string> mGroupsOverThreshold;
  std::unordered_set<std::string> mGroupsUnderThreshold;
  group_size_map mGroupSizes;

private:
  void clear_thresholds();

};


} // namespace eos::mgm::group_balancer
