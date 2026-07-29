//------------------------------------------------------------------------------
//! @file ClusterDataFormatter.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "mgm/placement/ClusterDataFormatter.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "mgm/placement/ClusterDataTypes.hh"
#include <sstream>

namespace eos::mgm::placement {

namespace {
//! Maximum number of items listed before the output gets elided
constexpr size_t kMaxItemsInline = 12;

//------------------------------------------------------------------------------
//! Format an item list for display, eliding it beyond kMaxItemsInline entries
//!
//! @param items items to format
//!
//! @return string representation
//------------------------------------------------------------------------------
std::string
FormatItemList(const std::vector<ItemIdT>& items)
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
} // anonymous namespace

//------------------------------------------------------------------------------
// Get a human readable description of a disk
//------------------------------------------------------------------------------
std::string
ToString(const Disk& disk)
{
  std::stringstream ss;
  ss << "id: " << disk.id << "\n"
     << "ConfigStatus: "
     << common::FileSystem::GetConfigStatusAsString(
            disk.config_status.load(std::memory_order_relaxed))
     << "\n"
     << "ActiveStatus: "
     << common::FileSystem::GetActiveStatusAsString(
            disk.active_status.load(std::memory_order_relaxed))
     << "\n"
     << "Weight: " << static_cast<uint16_t>(disk.weight.load(std::memory_order_relaxed))
     << "\n"
     << "UsedPercent: "
     << static_cast<uint16_t>(disk.percent_used.load(std::memory_order_relaxed)) << "\n"
     << "FreeGiB: " << disk.free_gib.load(std::memory_order_relaxed) << "\n"
     << "BookedGiB: " << disk.booked_gib.load(std::memory_order_relaxed);
  return ss.str();
}

//------------------------------------------------------------------------------
// Get a human readable description of a bucket
//------------------------------------------------------------------------------
std::string
ToString(const Bucket& bucket)
{
  std::string group_str;

  if (bucket.group_index != kNoGroupIndex) {
    group_str = "Group Index: " + std::to_string(bucket.group_index) + "\n";
  }

  std::stringstream ss;
  ss << "Id: " << bucket.id << "\n"
     << group_str << "Parent: " << bucket.parent << "\n"
     << "Level: " << static_cast<uint16_t>(bucket.level) << "\n"
     << "Total Weight: " << bucket.total_weight << "\n"
     << "Bucket Type: " << BucketTypeToStr(static_cast<BucketType>(bucket.bucket_type))
     << "\n"
     << "Disabled: "
     << DisabledOpsToStr(bucket.disabled_ops.load(std::memory_order_relaxed))
     << "\nItem List: ";

  for (const auto& it : bucket.items) {
    ss << it << ", ";
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Get the disk list of a snapshot as a formatted table
//------------------------------------------------------------------------------
std::string
GetDisksAsString(const ClusterData& data)
{
  TableFormatterBase table;
  // The geotag is not stored per disk, it is the path of the bucket the disk
  // hangs from
  const auto parents = data.GetDiskParents();

  table.SetHeader({
      std::make_tuple("fsid", 9, "l"),
      std::make_tuple("config", 15, "s"),
      std::make_tuple("active", 15, "s"),
      std::make_tuple("weight", 10, "l"),
      std::make_tuple("used%", 8, "l"),
      std::make_tuple("free(GiB)", 12, "l"),
      std::make_tuple("booked(GiB)", 12, "l"),
      std::make_tuple("geotag", 30, "s"),
  });

  for (const auto& d : data.disks) {
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
    if (auto it = parents.find(d.id); it != parents.end()) {
      geotag = data.GetGeoTag(it->second);
    }

    TableRow row;
    row.emplace_back(static_cast<long long int>(d.id), "l");
    row.emplace_back(configStr, "s", "", false, configColor);
    row.emplace_back(activeStr, "s", "", false, activeColor);
    row.emplace_back(static_cast<long long int>(d.weight.load(std::memory_order_relaxed)),
                     "l");
    row.emplace_back(static_cast<long long int>(pct), "l", "%", false, pctColor);
    row.emplace_back(
        static_cast<long long int>(d.free_gib.load(std::memory_order_relaxed)), "l");
    row.emplace_back(
        static_cast<long long int>(d.booked_gib.load(std::memory_order_relaxed)), "l");
    row.emplace_back(geotag, "s-");

    table.AddRows({row});
  }

  return table.GenerateTable(HEADER);
}

//------------------------------------------------------------------------------
// Get the bucket hierarchy of a snapshot as a formatted table
//------------------------------------------------------------------------------
std::string
GetBucketsAsString(const ClusterData& data)
{
  TableFormatterBase table;

  table.SetHeader({
      std::make_tuple("type", 10, "s"),
      std::make_tuple("id", 9, "l"),
      std::make_tuple("parent", 9, "l"),
      std::make_tuple("level", 7, "l"),
      std::make_tuple("group", 9, "l"),
      std::make_tuple("geotag", 24, "s"),
      std::make_tuple("disabled", 10, "s"),
      std::make_tuple("weight", 10, "l"),
      std::make_tuple("item_count", 8, "l"),
      std::make_tuple("items", 43, "s"),
  });

  for (const auto& b : data.buckets) {
    // Skip the holes in the id range, which carry the INVALID sentinel type
    if (b.bucket_type == GetBucketType(BucketType::INVALID)) {
      continue;
    }

    auto btype = static_cast<BucketType>(b.bucket_type);

    long long groupIndex = -1;
    if (b.group_index != kNoGroupIndex) {
      groupIndex = static_cast<long long>(b.group_index);
    }

    TableRow row;
    row.emplace_back(BucketTypeToStr(btype), "s");
    row.emplace_back(static_cast<long long int>(b.id), "l");
    row.emplace_back(static_cast<long long int>(b.parent), "l");
    row.emplace_back(static_cast<long long int>(b.level), "l");

    if (groupIndex >= 0) {
      row.emplace_back(groupIndex, "l");
    } else {
      row.emplace_back(std::string("-"), "s");
    }

    std::string geotag = data.GetGeoTag(b.id);
    row.emplace_back(geotag.empty() ? std::string("-") : geotag, "s-");
    const uint8_t dmask = b.disabled_ops.load(std::memory_order_relaxed) & kDisabledAll;
    row.emplace_back(dmask ? DisabledOpsToStr(dmask) : std::string("-"), "s", "", false,
                     dmask ? BRED : NONE);
    row.emplace_back(static_cast<long long int>(b.total_weight), "l");
    row.emplace_back(static_cast<long long int>(b.items.size()), "l");
    row.emplace_back(FormatItemList(b.items), "s-");

    table.AddRows({row});
  }

  return table.GenerateTable(HEADER);
}

} // namespace eos::mgm::placement
