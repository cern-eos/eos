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
#include "common/table_formatter/TableFormatterBase.hh"
#include "table_formatter/TableFormatterBase.hh"
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

static constexpr size_t kMaxItemsInline = 12;
static inline std::string
FormatItemList(const std::vector<item_id_t>& items)
{
  if (items.empty()) {
    return "-";
  }

  std::string out;
  const size_t limit = std::min(items.size(), kMaxItemsInline);

  for (size_t i = 0; i < limit; ++i) {
    if (i > 0) {
      out += ", ";
    }

    out += std::to_string(items[i]);
  }

  if (items.size() > kMaxItemsInline) {
    out += " ... (";
    out += std::to_string(items.size());
    out += " total)";
  }

  return out;
}

struct ClusterData {
  std::vector<Disk> disks;
  std::vector<Bucket> buckets;
  std::vector<std::vector<uint64_t>> disk_tags;

  // Diagnostic data structures. Not used in hot path
  std::unordered_map<fsid_t, std::string> disk_tag_map;
  std::unordered_map<uint64_t, std::string> geo_hash_registry;

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

  //----------------------------------------------------------------------------
  //! Return the disk list as a formatted table.
  //!
  //! Columns: fsid | config | active | weight | used% | geotag
  //----------------------------------------------------------------------------
  std::string getDisksAsString() const
  {
    TableFormatterBase table;

    table.SetHeader({
        std::make_tuple("fsid", 9, "l"),
        std::make_tuple("config", 15, "s"),
        std::make_tuple("active", 15, "s"),
        std::make_tuple("weight", 10, "l"),
        std::make_tuple("used%", 8, "l"),
        std::make_tuple("geotag", 30, "s"),
    });

    for (const auto& d : disks) {
      auto cs = d.config_status.load(std::memory_order_relaxed);
      auto as = d.active_status.load(std::memory_order_relaxed);
      uint8_t pct = d.percent_used.load(std::memory_order_relaxed);

      std::string configStr = common::FileSystem::GetConfigStatusAsString(cs);
      std::string activeStr = common::FileSystem::GetActiveStatusAsString(as);

      TableFormatterColor configColor = NONE;
      switch (cs) {
      case ConfigStatus::kRW:
        configColor = BGREEN;
        break;
      case ConfigStatus::kRO:
        configColor = BYELLOW;
        break;
      case ConfigStatus::kDrain:
      case ConfigStatus::kDrainDead:
      case ConfigStatus::kOff:
        configColor = BRED;
        break;
      default:
        break;
      }

      TableFormatterColor activeColor = NONE;
      if (as == ActiveStatus::kOnline) {
        activeColor = BGREEN;
      } else if (as == ActiveStatus::kOffline) {
        activeColor = BRED;
      }

      // warn at >=80%, alert at >=95%
      TableFormatterColor pctColor = NONE;
      if (pct >= 95) {
        pctColor = BRED;
      } else if (pct >= 80) {
        pctColor = BYELLOW;
      }

      std::string geotag;
      if (auto it = disk_tag_map.find(d.id); it != disk_tag_map.end()) {
        geotag = it->second;
      }

      TableRow row;
      row.emplace_back(static_cast<long long int>(d.id), "l");
      row.emplace_back(configStr, "s", "", false, configColor);
      row.emplace_back(activeStr, "s", "", false, activeColor);
      row.emplace_back(
          static_cast<long long int>(d.weight.load(std::memory_order_relaxed)), "l");
      row.emplace_back(static_cast<long long int>(pct), "l", "%", false, pctColor);
      row.emplace_back(geotag, "s-");

      table.AddRows({row});
    }

    return table.GenerateTable(HEADER);
  }

  //----------------------------------------------------------------------------
  //! Return the bucket hierarchy as a formatted table.
  //!
  //! Columns: type | id | group | weight | item_count | items
  //----------------------------------------------------------------------------
  std::string getBucketsAsString() const
  {
    TableFormatterBase table;

    table.SetHeader({
        std::make_tuple("type", 10, "s"),
        std::make_tuple("id", 9, "l"),
        std::make_tuple("group", 9, "l"),
        std::make_tuple("weight", 10, "l"),
        std::make_tuple("item_count", 8, "l"),
        std::make_tuple("items", 57, "s"),
    });

    for (const auto& b : buckets) {
      if (b.id == 0 && b.bucket_type == 0) {
        continue;
      }

      auto btype = static_cast<StdBucketType>(b.bucket_type);

      long long groupIndex = -1;
      if (btype == StdBucketType::GROUP) {
        groupIndex = static_cast<long long>(BucketIDtoGroupID(b.id));
      }

      TableRow row;
      row.emplace_back(BucketTypeToStr(btype), "s");
      row.emplace_back(static_cast<long long int>(b.id), "l");

      if (groupIndex >= 0) {
        row.emplace_back(groupIndex, "l");
      } else {
        row.emplace_back(std::string("-"), "s");
      }

      row.emplace_back(static_cast<long long int>(b.total_weight), "l");
      row.emplace_back(static_cast<long long int>(b.items.size()), "l");
      row.emplace_back(FormatItemList(b.items), "s-");

      table.AddRows({row});
    }

    return table.GenerateTable(HEADER);
  }

};

inline bool isValidBucketId(item_id_t id, const ClusterData& data)
{
  return id < 0 && (-id < (int)data.buckets.size());
}


} // eos::mgm::placement

#endif // EOS_CLUSTERDATATYPES_HH
