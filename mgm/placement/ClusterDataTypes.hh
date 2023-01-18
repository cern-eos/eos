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

namespace eos::mgm::placement {

using fsid_t = eos::common::FileSystem::fsid_t;

// We use a item_id to represent a storage element, negative numbers represent
// storage elements in the hierarchy, ie. groups/racks/room/site etc.
using item_id_t = int32_t;
using epoch_id_t = uint64_t;
using DiskStatus = eos::common::ConfigStatus;
// A struct representing a disk, this is the lowest level of the hierarchy,
// disk ids map 1:1 to fsids, however it is necessary that the last bit of fsid_t
// is not used, as we use a int32_t for the rest of the placement hierarchy.
// the struct is packed to 8 bytes, so upto 8192 disks
// can fit in a single 64kB cache, it is recommended to keep this struct aligned
struct Disk {
  fsid_t id;
  mutable std::atomic<DiskStatus> status {common::ConfigStatus::kUnknown};
  mutable std::atomic<uint8_t> weight{0}; // we really don't need floating point precision
  mutable std::atomic<uint8_t> percent_used{0};
  /* We've one byte left for future use - remove this line when that happens */

  Disk () : id(0) {}

  explicit Disk(fsid_t _id) : id(_id) {}

  Disk(fsid_t _id, common::ConfigStatus _status, uint8_t _weight,
       uint8_t _percent_used = 0)
      : id(_id), status(_status), weight(_weight), percent_used(_percent_used)
  {
  }

  // explicit copy constructor as atomic types are not copyable
  Disk(const Disk& other)
      : Disk(other.id, other.status.load(std::memory_order_relaxed),
             other.weight.load(std::memory_order_relaxed),
             other.percent_used.load(std::memory_order_relaxed))
  {
  }

  Disk& operator=(const Disk& other) {
    id = other.id;
    status.store(other.status.load(std::memory_order_relaxed),
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
};

static_assert(sizeof(Disk) == 8, "Disk data type not aligned to 8 bytes!");

// some common storage elements, these could be user defined in the future
enum class StdBucketType : uint8_t {
  GROUP=0,
  RACK,
  ROOM,
  SITE,
  ROOT,
  COUNT};

constexpr uint8_t
get_bucket_type(StdBucketType t)
{
  return static_cast<uint8_t>(t);
}


// Determining placement of replicas for a file
// We need to understand how many storage elements we select at each level
// of the hierarchy, for example for a 2 replica file, with 2 sites,
// we'd select 1 per site, and then going further down the hierarchy, we'd have
// to select 1 per room etc. until we reach our last abstraction at the group
// where we'd need to select as many replicas as we have left, in this case 2.
// we really don't want a tree that's more than 16 levels deep?
constexpr uint8_t MAX_PLACEMENT_HEIGHT = 16;
using selection_rules_t = std::array<uint8_t, MAX_PLACEMENT_HEIGHT>;
constexpr selection_rules_t kDefault2Replica =
    {-1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};


struct Bucket {
  item_id_t id;
  uint32_t total_weight;
  uint8_t bucket_type;
  std::vector<item_id_t> items;

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
};

struct ClusterData {
  std::vector<Disk> disks;
  std::vector<Bucket> buckets;

  void setDiskStatus(fsid_t id, DiskStatus status) {
    disks[id].status.store(status, std::memory_order_relaxed);
  }
};

inline bool isValidBucketId(item_id_t id, const ClusterData& data) {
  return id < 0 && (-id < data.buckets.size());
}

} // eos::mgm::placement

#endif // EOS_CLUSTERDATATYPES_HH
