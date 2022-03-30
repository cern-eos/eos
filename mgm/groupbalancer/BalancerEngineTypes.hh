//------------------------------------------------------------------------------
// File: BalancerEngineTypes.hh
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
#include <string_view>
#include <unordered_set>
#include <map>

// Some enums and typedefs for various types used in BalancerEngines
namespace eos::mgm::group_balancer {

// enum representing the various states a group can be in
enum class GroupStatus {
  ON,
  OFF,
  DRAIN
};

inline constexpr GroupStatus getGroupStatus(std::string_view status) {
  using namespace std::string_view_literals;
  if (status.compare("on"sv) == 0) {
    return GroupStatus::ON;
  } else if (status.compare("drain"sv) == 0) {
    return GroupStatus::DRAIN;
  }
  return GroupStatus::OFF;
}

//------------------------------------------------------------------------------
//! @brief Class representing a group's size
//! It holds the capacity and the current used space of a group.
//------------------------------------------------------------------------------
class GroupSizeInfo {
public:
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  GroupSizeInfo(uint64_t usedBytes, uint64_t capacity)
      : mStatus(GroupStatus::ON), mSize(usedBytes), mCapacity(capacity)
  {
  }

  GroupSizeInfo(GroupStatus status, uint64_t usedBytes, uint64_t capacity)
      : mStatus(status), mSize(usedBytes), mCapacity(capacity)
  {}
  //------------------------------------------------------------------------------
  //! Subtracts the given size from this group and adds it to the given toGroup
  //!
  //! @param toGroup the group where to add the size
  //! @param size the file size that should be swapped
  //------------------------------------------------------------------------------
  void swapFile(GroupSizeInfo* toGroup, uint64_t size)
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

  bool draining() const {
    return mStatus == GroupStatus::DRAIN;
  }

  bool on() const {
    return mStatus == GroupStatus::ON;
  }

private:
  GroupStatus mStatus;
  uint64_t mSize;
  uint64_t mCapacity;
};

// std::less<> basically allows for transparent compare of keys,
// allowing std::string_view -> std::string lookups on keys
// please note when doing const char*, unless explicitly converted using for eg
// operator ""sv, you'll end up creating a new overload which would be unecessary
// normally this promises find/count etc will work without allocations which is a plus
// eg. group_size_map.find("default.20"sv)  // good
//     group_size_map.find(some_str)       // good
//     group_size_map.find("default.20") // bad will use an overload of std::less<char[xx]> here
using group_size_map = std::map<std::string, GroupSizeInfo, std::less<>>;
using threshold_group_set = std::unordered_set<std::string>;
using groups_picked_t = std::pair<std::string, std::string>;
using engine_conf_t = std::map<std::string, std::string, std::less<>>;

enum class BalancerEngineT {
  stddev,
  minmax,
  total_count
};

}

