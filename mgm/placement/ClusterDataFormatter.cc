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
#include <algorithm>
#include <optional>
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

//------------------------------------------------------------------------------
//! Color of the scheduling state of a disk: green when clients can still
//! write, yellow when they can only read, red once no client traffic is served
//------------------------------------------------------------------------------
TableFormatterColor
SchedColor(FsOpMask ops)
{
  if (common::AllowsOp(ops, kClientCreate) ||
      common::AllowsOp(ops, SchedOp{SchedActivity::kClient, SchedDirection::kUpdate})) {
    return BGREEN;
  } else if (common::AllowsOp(ops, kClientRead)) {
    return BYELLOW;
  }

  return BRED;
}

//------------------------------------------------------------------------------
//! Color of the active status of a disk
//------------------------------------------------------------------------------
TableFormatterColor
ActiveColor(ActiveStatus as)
{
  if (as == ActiveStatus::kOnline) {
    return BGREEN;
  } else if (as == ActiveStatus::kOffline) {
    return BRED;
  }

  return NONE;
}

//------------------------------------------------------------------------------
//! Color of the fill level of a disk: warn at >=80%, alert at >=95%
//------------------------------------------------------------------------------
TableFormatterColor
FillColor(uint8_t pct)
{
  if (pct >= 95) {
    return BRED;
  } else if (pct >= 80) {
    return BYELLOW;
  }

  return NONE;
}

//------------------------------------------------------------------------------
//! Glyphs of the tree cells, as TableFormatterBase draws a cell of format "t"
//------------------------------------------------------------------------------
enum TreeGlyph : unsigned {
  kGlyphBlank = 0,     ///< nothing
  kGlyphPipe = 1,      ///< "│"
  kGlyphLastArrow = 2, ///< "└─▶"
  kGlyphMidArrow = 3,  ///< "├─▶"
  kGlyphLastLine = 4,  ///< "└──", continued by the next cell
  kGlyphMidLine = 5,   ///< "├──", continued by the next cell
  kGlyphDash = 6,      ///< "───", continued by the next cell
  kGlyphDashArrow = 7  ///< "──▶"
};

//------------------------------------------------------------------------------
//! What the subtree below a bucket holds
//------------------------------------------------------------------------------
struct SubtreeStats {
  uint64_t n_disks = 0;
  uint64_t n_online = 0;
  uint64_t free_gib = 0;
  uint64_t booked_gib = 0;
};

//------------------------------------------------------------------------------
//! Render the hierarchy of a snapshot the way "geosched show tree" does: one
//! column per tree level, the group in the first, each geotag atom one column
//! further right, and the disks pointed at from the last one. A node at level
//! L draws its connector in column L - 1 and its name in column L; the columns
//! before carry a pipe for every ancestor that still has siblings to come.
//------------------------------------------------------------------------------
class TreeRenderer {
public:
  TreeRenderer(const ClusterData& data, std::string_view space_name)
      : mData(data)
      , mSpaceName(space_name)
      , mStats(data.buckets.size())
  {
  }

  std::string
  Render()
  {
    const Bucket* root = mData.GetBucket(0);

    if (!root) {
      return {};
    }

    std::vector<const Bucket*> groups;

    for (const auto id : root->items) {
      if (const Bucket* group = mData.GetBucket(id)) {
        groups.push_back(group);
        mDepth = std::max(mDepth, GeoDepth(*group, 0));
      }
    }

    std::sort(groups.begin(), groups.end(), [](const Bucket* a, const Bucket* b) {
      return (a->group_index != b->group_index) ? (a->group_index < b->group_index)
                                                : (a->id > b->id);
    });

    TableHeader header;
    header.push_back(std::make_tuple("group", 6, "-s"));

    if (mDepth > 0) {
      header.push_back(std::make_tuple("geotag", 6, "-s"));
    }

    for (size_t i = 1; i < mDepth; ++i) {
      header.push_back(std::make_tuple("lev" + std::to_string(i), 4, "-s"));
    }

    header.push_back(std::make_tuple("fsid", 6, "l"));
    header.push_back(std::make_tuple("sched", 12, "s"));
    header.push_back(std::make_tuple("active", 8, "s"));
    header.push_back(std::make_tuple("weight", 6, "l"));
    header.push_back(std::make_tuple("used%", 5, "l"));
    header.push_back(std::make_tuple("free(GiB)", 9, "l"));
    header.push_back(std::make_tuple("booked(GiB)", 11, "l"));
    header.push_back(std::make_tuple("online", 6, "s"));
    header.push_back(std::make_tuple("disabled", 8, "s"));
    mTable.SetHeader(header);

    for (const Bucket* group : groups) {
      mTable.AddSeparator();
      mMore.assign(1, false);
      AddBucketRow(*group, 0, true);
      AddChildren(*group, 1);
    }

    return mTable.GenerateTable(HEADER);
  }

private:
  //----------------------------------------------------------------------------
  //! Get the number of geotag levels below a bucket, bounded by kMaxGeoDepth
  //----------------------------------------------------------------------------
  size_t
  GeoDepth(const Bucket& bucket, size_t level) const
  {
    size_t depth = 0;

    if ((level > kMaxGeoDepth) ||
        (bucket.child_type != GetChildType(ChildType::kGeoBuckets))) {
      return depth;
    }

    for (const auto id : bucket.items) {
      if (const Bucket* child = mData.GetBucket(id)) {
        depth = std::max(depth, 1 + GeoDepth(*child, level + 1));
      }
    }

    return depth;
  }

  //----------------------------------------------------------------------------
  //! Get what the subtree below a bucket holds, computed once per bucket
  //----------------------------------------------------------------------------
  const SubtreeStats&
  Stats(const Bucket& bucket, size_t level = 0)
  {
    const size_t index = static_cast<size_t>(-bucket.id);
    auto& stats = mStats[index];

    if (stats) {
      return *stats;
    }

    stats.emplace();

    for (const auto id : bucket.items) {
      if (id > 0) {
        if (const Disk* disk = mData.GetDisk(id)) {
          ++stats->n_disks;
          stats->n_online += (disk->active_status.load(std::memory_order_relaxed) ==
                              ActiveStatus::kOnline);
          stats->free_gib += disk->free_gib.load(std::memory_order_relaxed);
          stats->booked_gib += disk->booked_gib.load(std::memory_order_relaxed);
        }
      } else if (const Bucket* child = mData.GetBucket(id);
                 child && (level <= kMaxGeoDepth)) {
        const SubtreeStats& sub = Stats(*child, level + 1);
        stats->n_disks += sub.n_disks;
        stats->n_online += sub.n_online;
        stats->free_gib += sub.free_gib;
        stats->booked_gib += sub.booked_gib;
      }
    }

    return *stats;
  }

  //----------------------------------------------------------------------------
  //! Add the rows of the children of a bucket, buckets by geotag atom and
  //! disks by fsid
  //----------------------------------------------------------------------------
  void
  AddChildren(const Bucket& bucket, size_t level)
  {
    if (level > mDepth + 1) {
      return; // malformed hierarchy, deeper than the columns
    }

    if (bucket.child_type == GetChildType(ChildType::kDisks)) {
      std::vector<const Disk*> disks;

      for (const auto id : bucket.items) {
        if (const Disk* disk = mData.GetDisk(id)) {
          disks.push_back(disk);
        }
      }

      std::sort(disks.begin(), disks.end(),
                [](const Disk* a, const Disk* b) { return a->id < b->id; });

      for (size_t i = 0; i < disks.size(); ++i) {
        AddDiskRow(*disks[i], level, i + 1 == disks.size());
      }

      return;
    }

    std::vector<const Bucket*> children;

    for (const auto id : bucket.items) {
      if (const Bucket* child = mData.GetBucket(id)) {
        children.push_back(child);
      }
    }

    std::sort(children.begin(), children.end(),
              [](const Bucket* a, const Bucket* b) { return a->geo_atom < b->geo_atom; });

    for (size_t i = 0; i < children.size(); ++i) {
      const bool last = (i + 1 == children.size());
      mMore.resize(level + 1);
      mMore[level] = !last;
      AddBucketRow(*children[i], level, last);
      AddChildren(*children[i], level + 1);
    }
  }

  //----------------------------------------------------------------------------
  //! Add the connector cells of a node at the given level, up to and including
  //! the one in column level - 1
  //----------------------------------------------------------------------------
  void
  AddConnectors(TableRow& row, size_t level, unsigned glyph) const
  {
    for (size_t col = 0; col + 1 < level; ++col) {
      const bool pipe = (col + 1 < mMore.size()) && mMore[col + 1];
      row.emplace_back(static_cast<unsigned>(pipe ? kGlyphPipe : kGlyphBlank), "t");
    }

    if (level > 0) {
      row.emplace_back(glyph, "t");
    }
  }

  //----------------------------------------------------------------------------
  //! Add the row of a group, at level 0, or of a geotag bucket
  //----------------------------------------------------------------------------
  void
  AddBucketRow(const Bucket& bucket, size_t level, bool last)
  {
    TableRow row;
    AddConnectors(row, level, last ? kGlyphLastArrow : kGlyphMidArrow);
    const FsOpMask denied = bucket.DeniedOps() & kMaskAll;
    std::string name;

    if (bucket.bucket_type == GetBucketType(BucketType::GROUP)) {
      if (bucket.group_index == kNoGroupIndex) {
        name = std::to_string(bucket.id);
      } else if (mSpaceName.empty()) {
        name = std::to_string(bucket.group_index);
      } else {
        name = std::string(mSpaceName) + "." + std::to_string(bucket.group_index);
      }
    } else {
      name = bucket.geo_atom.empty() ? std::to_string(bucket.id) : bucket.geo_atom;
    }

    row.emplace_back(name, "s", "", false, denied ? BRED : BWHITE);

    for (size_t col = level + 1; col <= mDepth; ++col) {
      row.emplace_back(std::string(), "s");
    }

    const SubtreeStats& stats = Stats(bucket);
    row.emplace_back(std::string(), "s"); // fsid
    row.emplace_back(std::string(), "s"); // sched
    row.emplace_back(std::string(), "s"); // active
    row.emplace_back(static_cast<long long int>(bucket.total_weight), "l");
    row.emplace_back(std::string(), "s"); // used%
    row.emplace_back(static_cast<long long int>(stats.free_gib), "l");
    row.emplace_back(static_cast<long long int>(stats.booked_gib), "l");
    row.emplace_back(std::to_string(stats.n_online) + "/" + std::to_string(stats.n_disks),
                     "s", "", false, (stats.n_online < stats.n_disks) ? BYELLOW : NONE);
    row.emplace_back(denied ? DeniedOpsToStr(denied) : std::string("-"), "s", "", false,
                     denied ? BRED : NONE);
    mTable.AddRows({row});
  }

  //----------------------------------------------------------------------------
  //! Add the row of a disk, its connector stretched up to the fsid column
  //----------------------------------------------------------------------------
  void
  AddDiskRow(const Disk& disk, size_t level, bool last)
  {
    TableRow row;

    if (level <= mDepth) {
      AddConnectors(row, level, last ? kGlyphLastLine : kGlyphMidLine);

      for (size_t col = level; col < mDepth; ++col) {
        row.emplace_back(static_cast<unsigned>(kGlyphDash), "t");
      }

      row.emplace_back(static_cast<unsigned>(kGlyphDashArrow), "t");
    } else {
      AddConnectors(row, level, last ? kGlyphLastArrow : kGlyphMidArrow);
    }

    const auto ops = disk.ops.load(std::memory_order_relaxed);
    const auto as = disk.active_status.load(std::memory_order_relaxed);
    const uint8_t pct = disk.percent_used.load(std::memory_order_relaxed);
    row.emplace_back(static_cast<long long int>(disk.id), "l");
    row.emplace_back(common::FormatSchedMask(ops), "s", "", false, SchedColor(ops));
    row.emplace_back(common::FileSystem::GetActiveStatusAsString(as), "s", "", false,
                     ActiveColor(as));
    row.emplace_back(
        static_cast<long long int>(disk.weight.load(std::memory_order_relaxed)), "l");
    row.emplace_back(static_cast<long long int>(pct), "l", "%", false, FillColor(pct));
    row.emplace_back(
        static_cast<long long int>(disk.free_gib.load(std::memory_order_relaxed)), "l");
    row.emplace_back(
        static_cast<long long int>(disk.booked_gib.load(std::memory_order_relaxed)), "l");
    row.emplace_back(std::string(), "s"); // online
    row.emplace_back(std::string(), "s"); // disabled
    mTable.AddRows({row});
  }

  const ClusterData& mData;
  std::string_view mSpaceName;
  TableFormatterBase mTable;
  //! Number of geotag levels, i.e. of tree columns after the group one
  size_t mDepth = 0;
  //! Whether the ancestor at each level still has siblings to come, which is
  //! what draws the pipes in front of a row
  std::vector<bool> mMore;
  //! Subtree contents per bucket, indexed like ClusterData::buckets
  std::vector<std::optional<SubtreeStats>> mStats;
};
} // anonymous namespace

//------------------------------------------------------------------------------
// Get a human readable description of a disk
//------------------------------------------------------------------------------
std::string
ToString(const Disk& disk)
{
  std::stringstream ss;
  ss << "id: " << disk.id << "\n"
     << "SchedOps: " << common::FormatSchedMask(disk.ops.load(std::memory_order_relaxed))
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
     << "Disabled: " << DeniedOpsToStr(bucket.DeniedOps()) << "\nItem List: ";

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
      std::make_tuple("sched", 24, "s"),
      std::make_tuple("active", 15, "s"),
      std::make_tuple("weight", 10, "l"),
      std::make_tuple("used%", 8, "l"),
      std::make_tuple("free(GiB)", 12, "l"),
      std::make_tuple("booked(GiB)", 12, "l"),
      std::make_tuple("geotag", 30, "s"),
  });

  for (const auto& d : data.disks) {
    // Skip the holes in the fsid range, which carry id 0
    if (d.id == 0) {
      continue;
    }

    auto ops = d.ops.load(std::memory_order_relaxed);
    auto as = d.active_status.load(std::memory_order_relaxed);
    uint8_t pct = d.percent_used.load(std::memory_order_relaxed);

    std::string configStr = common::FormatSchedMask(ops);
    std::string activeStr = common::FileSystem::GetActiveStatusAsString(as);

    const TableFormatterColor configColor = SchedColor(ops);
    const TableFormatterColor activeColor = ActiveColor(as);
    const TableFormatterColor pctColor = FillColor(pct);

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
    const FsOpMask dmask = b.DeniedOps() & kMaskAll;
    row.emplace_back(dmask ? DeniedOpsToStr(dmask) : std::string("-"), "s", "", false,
                     dmask ? BRED : NONE);
    row.emplace_back(static_cast<long long int>(b.total_weight), "l");
    row.emplace_back(static_cast<long long int>(b.items.size()), "l");
    row.emplace_back(FormatItemList(b.items), "s-");

    table.AddRows({row});
  }

  return table.GenerateTable(HEADER);
}

//------------------------------------------------------------------------------
// Get the hierarchy of a snapshot as a tree
//------------------------------------------------------------------------------
std::string
GetTreeAsString(const ClusterData& data, std::string_view space_name)
{
  return TreeRenderer(data, space_name).Render();
}

} // namespace eos::mgm::placement
