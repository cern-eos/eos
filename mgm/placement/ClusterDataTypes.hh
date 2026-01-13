// ----------------------------------------------------------------------
// File: ClusterDataTypes
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#ifndef EOS_CLUSTERDATATYPES_HH
#define EOS_CLUSTERDATATYPES_HH

#include "common/FileSystem.hh"
#include <array>
#include <unordered_map>

namespace eos::mgm::placement
{

using fsid_t = eos::common::FileSystem::fsid_t;

// We use a item_id to represent a storage element, negative numbers represent
// storage elements in the hierarchy, ie. groups/racks/room/site etc.
using item_id_t = int32_t;
using epoch_id_t = uint64_t;
using ConfigStatus = eos::common::ConfigStatus;
using ActiveStatus = eos::common::ActiveStatus;
// A struct representing a disk, this is the lowest level of the hierarchy,
// disk ids map 1:1 to fsids, however it is necessary that the last bit of fsid_t
// is not used, as we use a int32_t for the rest of the placement hierarchy.
// the struct is packed to 8 bytes, so upto 8192 disks
// can fit in a single 64kB cache, it is recommended to keep this struct aligned
inline ActiveStatus getActiveStatus(ActiveStatus status,
                                    eos::common::BootStatus bstatus)
{
  if (status == ActiveStatus::kOnline) {
    if (bstatus != eos::common::BootStatus::kBooted) {
      return ActiveStatus::kOffline;
    }
  }

  return status;
}

struct Disk {
  fsid_t id;
  mutable std::atomic<ConfigStatus> config_status {ConfigStatus::kUnknown};
  mutable std::atomic<ActiveStatus> active_status {ActiveStatus::kUndefined};

  mutable std::atomic<uint8_t> weight{0}; // we really don't need floating point precision
  mutable std::atomic<uint8_t> percent_used{0};

  Disk() : id(0) {}

  explicit Disk(fsid_t _id) : id(_id) {}

  Disk(fsid_t _id, ConfigStatus _config_status,
       ActiveStatus _active_status, uint8_t _weight, uint8_t _percent_used = 0)
    : id(_id), config_status(_config_status), active_status(_active_status),
      weight(_weight), percent_used(_percent_used)
  {}

  // TODO future: these copy constructors must only be used at construction time
  // explicit copy constructor as atomic types are not copyable
  Disk(const Disk& other)
    : Disk(other.id, other.config_status.load(std::memory_order_relaxed),
           other.active_status.load(std::memory_order_relaxed),
           other.weight.load(std::memory_order_relaxed),
           other.percent_used.load(std::memory_order_relaxed))
  {
  }

  Disk& operator=(const Disk& other)
  {
    id = other.id;
    config_status.store(other.config_status.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
    active_status.store(other.active_status.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
    weight.store(other.weight.load(std::memory_order_relaxed),
                 std::memory_order_relaxed);
    percent_used.store(other.percent_used.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
    return *this;
  }

  friend bool
  operator<(const Disk& l, const Disk& r)
  {
    return l.id < r.id;
  }

  std::string to_string() const
  {
    std::stringstream ss;
    ss << "id: " << id << "\n"
       << "ConfigStatus: "
       << common::FileSystem::GetConfigStatusAsString(config_status.load(
             std::memory_order_relaxed))
       << "\n"
       << "ActiveStatus: "
       << common::FileSystem::GetActiveStatusAsString(active_status.load(
             std::memory_order_relaxed))
       << "\n"
       << "Weight: " << static_cast<uint16_t>(weight.load(std::memory_order_relaxed))
       << "\n"
       << "UsedPercent: " << static_cast<uint16_t>(percent_used.load(
             std::memory_order_relaxed));
    return ss.str();
  }
};

static_assert(sizeof(Disk) == 8, "Disk data type not aligned to 8 bytes!");

// some common storage elements, these could be user defined in the future
enum class StdBucketType : uint8_t {
  GROUP = 0,
  RACK,
  ROOM,
  SITE,
  ROOT,
  COUNT
};

constexpr uint8_t
get_bucket_type(StdBucketType t)
{
  return static_cast<uint8_t>(t);
}

inline std::string BucketTypeToStr(StdBucketType t)
{
  switch (t) {
  case StdBucketType::GROUP:
    return "group";

  case StdBucketType::RACK:
    return "rack";

  case StdBucketType::ROOM:
    return "room";

  case StdBucketType::SITE:
    return "site";

  case StdBucketType::ROOT:
    return "root";

  default:
    return "unknown";
  }
}

// Constant to offset the group id, so group ids would be starting from this offset
// in memory they'd be stored at -group_id
constexpr int kBaseGroupOffset = -10;


// Return bucket index from group id, guaranteed to be -ve
// If we have groups > INT_MAX we're in UB land, but this is mostly not possible
inline constexpr item_id_t
GroupIDtoBucketID(unsigned int group_index)
{
  return kBaseGroupOffset - group_index;
}

inline constexpr unsigned int
BucketIDtoGroupID(item_id_t bucket_id)
{
  return kBaseGroupOffset - bucket_id;
}

struct Bucket {
  item_id_t id;
  uint32_t total_weight;
  uint8_t bucket_type;
  std::vector<item_id_t> items;
  std::string location;
  std::string full_geotag;

  Bucket() = default;

  Bucket(item_id_t _id, uint8_t type)
    : id(_id), total_weight(0), bucket_type(type)
  {
  }

  friend bool
  operator<(const Bucket& l, const Bucket& r)
  {
    return l.id < r.id;
  }

  std::string to_string() const
  {
    std::string group_str;

    if (bucket_type == get_bucket_type(StdBucketType::GROUP)) {
      group_str = "Group Index: " +
                  std::to_string(BucketIDtoGroupID(id)) + "\n";
    }

    std::stringstream ss;
    ss << "id: " << id << "\n"
       << group_str
       << "Total Weight: " << total_weight << "\n"
       << "Bucket Type: "
       << BucketTypeToStr(static_cast<StdBucketType>(bucket_type))
       << "\nItem List: ";

    for (const auto& it : items) {
      ss << it << ", ";
    }

    return ss.str();
  }
};

struct ClusterData {
  std::vector<Disk> disks;
  std::vector<Bucket> buckets;
  std::unordered_map<fsid_t, std::string> disk_tags;

  bool setDiskStatus(fsid_t id, ConfigStatus status)
  {
    if (id > disks.size()) {
      return false;
    }

    disks[id - 1].config_status.store(status, std::memory_order_release);
    return true;
  }

  bool setDiskStatus(fsid_t id, ActiveStatus status)
  {
    if (id > disks.size()) {
      return false;
    }

    disks[id - 1].active_status.store(status, std::memory_order_release);
    return true;
  }

  bool setDiskWeight(fsid_t id, uint8_t weight)
  {
    if (id > disks.size()) {
      return false;
    }

    disks[id - 1].weight.store(weight, std::memory_order_release);
    return true;
  }

  std::string getDisksAsString() const
  {
    std::string result_str;
    result_str.append("Total Disks: ");
    result_str.append(std::to_string(disks.size()));
    result_str.append("\n");

    for (const auto& d : disks) {
      result_str.append(d.to_string());
      result_str.append("\n");
    }

    return result_str;
  }

  std::string getBucketsAsString() const
  {
    std::string result_str;

    for (const auto& b : buckets) {
      if (b.id == 0 && b.bucket_type == 0) {
        continue;
      }

      result_str.append(b.to_string());
      result_str.append("\n");
    }

    return result_str;
  }

  void setDiskTag(std::string_view tag, fsid_t id)
  {
    disk_tags.insert_or_assign(id, tag);
  }
};

inline bool isValidBucketId(item_id_t id, const ClusterData& data)
{
  return id < 0 && (-id < (int)data.buckets.size());
}


} // eos::mgm::placement

#endif // EOS_CLUSTERDATATYPES_HH
