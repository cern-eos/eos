//------------------------------------------------------------------------------
//! @file ClusterDataTypes.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "common/FileSystem.hh"
#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <xxhash.h>

namespace eos::mgm::placement
{

using fsid_t = eos::common::FileSystem::fsid_t;

//! Identifier of a storage element. Positive values are disks, negative ones
//! are buckets of the hierarchy ie. groups/racks/rooms/sites, 0 is the root.
using ItemIdT = int32_t;
using EpochIdT = uint64_t; //!< Version of a topology snapshot
using ConfigStatus = eos::common::ConfigStatus;
using ActiveStatus = eos::common::ActiveStatus;
using FsOpMask = eos::common::FsOpMask;
using SchedOp = eos::common::SchedOp;
using SchedActivity = eos::common::SchedActivity;
using SchedDirection = eos::common::SchedDirection;

//! The named operations, see common/FsOps.hh
using eos::common::kClientCreate;
using eos::common::kClientRead;
using eos::common::kClientUpdate;
using eos::common::kInternalCreate;
using eos::common::kInternalRead;
using eos::common::kInternalUpdate;

using eos::common::kMaskAll;
using eos::common::kMaskNone;
//! A disk that serves reads of both classes but takes no writes
inline constexpr FsOpMask kOpsReadOnly =
    eos::common::MaskOfDirection(SchedDirection::kRead);

//------------------------------------------------------------------------------
//! Fold the boot status into the active status, a file system that is online
//! but not booted is of no use to the scheduler
//!
//! @param status active status of the file system
//! @param bstatus boot status of the file system
//!
//! @return effective active status
//------------------------------------------------------------------------------
inline ActiveStatus
GetActiveStatus(ActiveStatus status, eos::common::BootStatus bstatus)
{
  if (status == ActiveStatus::kOnline) {
    if (bstatus != eos::common::BootStatus::kBooted) {
      return ActiveStatus::kOffline;
    }
  }

  return status;
}

//------------------------------------------------------------------------------
//! Struct Disk - lowest level of the hierarchy, disk ids map 1:1 to fsids.
//! The struct is packed to 16 bytes so that 4000 disks fit in a single
//! 64 kB cache, it is recommended to keep it aligned. The atomics allow status,
//! weight and space updates without rebuilding the topology.
//!
//! @note the top bit of fsid_t must stay unused, the rest of the placement
//!       hierarchy is addressed with an int32_t
//------------------------------------------------------------------------------
struct Disk {
  fsid_t id; ///< File system identifier
  //! Operations this file system accepts, see common/FsOps.hh. Six bits, which
  //! is why it still fits in the byte the configuration status used to take.
  mutable std::atomic<FsOpMask> ops{eos::common::kMaskNone};
  //! Active status of the file system
  mutable std::atomic<ActiveStatus> active_status {ActiveStatus::kUndefined};
  //! Relative weight, no floating point precision needed
  mutable std::atomic<uint8_t> weight{0};
  mutable std::atomic<uint8_t> percent_used{0}; ///< Fill level in percent
  //! Free space in GiB. percent_used says how attractive a disk is, this says
  //! whether a given file actually fits on it - the two are not
  //! interchangeable, a 1% margin means something very different on a 500 GB
  //! disk and on a 20 TB one. Capped rather than wrapped, see kFreeSpaceUnit.
  //! Decremented on every successful placement, so that the placements between
  //! two FST publishes see each other's bookings, see BookSpace.
  mutable std::atomic<uint32_t> free_gib{0};
  //! Space booked by placements since the last FST publish, in GiB. What has
  //! been placed but not yet written is invisible to the FST's statfs, so this
  //! is subtracted from the next published figure - and only from that one,
  //! after which the FST's own numbers are the truth again. That retires a
  //! booking whose file never got written after one publish interval instead
  //! of leaking it forever, see ClusterData::SetDiskFreeSpace.
  mutable std::atomic<uint32_t> booked_gib{0};

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Disk() : id(0) {}

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param _id file system identifier
  //----------------------------------------------------------------------------
  explicit Disk(fsid_t _id) : id(_id) {}

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param _id file system identifier
  //! @param _ops operations the file system accepts
  //! @param _active_status active status
  //! @param _weight relative weight
  //! @param _percent_used fill level in percent
  //! @param _free_gib free space in GiB
  //----------------------------------------------------------------------------
  Disk(fsid_t _id, FsOpMask _ops, ActiveStatus _active_status, uint8_t _weight,
       uint8_t _percent_used = 0, uint32_t _free_gib = 0)
      : id(_id)
      , ops(_ops)
      , active_status(_active_status)
      , weight(_weight)
      , percent_used(_percent_used)
      , free_gib(_free_gib)
  {}

  //----------------------------------------------------------------------------
  //! Copy constructor, explicit as atomic types are not copyable
  //!
  //! @param other object to copy from
  //!
  //! @note TODO future: this must only be used at construction time
  //----------------------------------------------------------------------------
  Disk(const Disk& other)
      : Disk(other.id, other.ops.load(std::memory_order_relaxed),
             other.active_status.load(std::memory_order_relaxed),
             other.weight.load(std::memory_order_relaxed),
             other.percent_used.load(std::memory_order_relaxed),
             other.free_gib.load(std::memory_order_relaxed))
  {
    booked_gib.store(other.booked_gib.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  }

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //!
  //! @param other object to copy from
  //!
  //! @return reference to the current object
  //----------------------------------------------------------------------------
  Disk& operator=(const Disk& other)
  {
    id = other.id;
    ops.store(other.ops.load(std::memory_order_relaxed), std::memory_order_relaxed);
    active_status.store(other.active_status.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
    weight.store(other.weight.load(std::memory_order_relaxed),
                 std::memory_order_relaxed);
    percent_used.store(other.percent_used.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
    free_gib.store(other.free_gib.load(std::memory_order_relaxed),
                   std::memory_order_relaxed);
    booked_gib.store(other.booked_gib.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
    return *this;
  }

  //----------------------------------------------------------------------------
  //! Check whether this disk accepts the given operation
  //!
  //! @param op operation to test
  //!
  //! @return true if the operation is allowed, otherwise false
  //----------------------------------------------------------------------------
  bool
  AllowsOp(SchedOp op) const
  {
    return eos::common::AllowsOp(ops.load(std::memory_order_acquire), op);
  }

  //----------------------------------------------------------------------------
  //! Less than operator, orders by identifier
  //!
  //! @param l left hand side
  //! @param r right hand side
  //!
  //! @return true if l sorts before r, otherwise false
  //----------------------------------------------------------------------------
  friend bool
  operator<(const Disk& l, const Disk& r)
  {
    return l.id < r.id;
  }
};

static_assert(sizeof(Disk) == 16, "Disk data type not aligned to 16 bytes!");

//! Unit the free space of a disk is recorded in. Anything smaller than this is
//! rounded down to zero, so a booking is refused on a disk with less than one
//! unit left - which is the safe direction to be wrong in.
constexpr uint64_t kFreeSpaceUnit = 1ULL << 30;

//------------------------------------------------------------------------------
//! Convert a free space in bytes to the unit stored on a disk, saturating
//! rather than wrapping
//!
//! @param free_bytes free space in bytes
//!
//! @return free space in GiB, clamped to what the field can hold
//------------------------------------------------------------------------------
inline uint32_t
FreeSpaceToGiB(uint64_t free_bytes)
{
  return static_cast<uint32_t>(std::min<uint64_t>(free_bytes / kFreeSpaceUnit,
                                                  std::numeric_limits<uint32_t>::max()));
}

//------------------------------------------------------------------------------
//! Convert a booking size in bytes to whole free space units, rounding up -
//! the free space is only known to kFreeSpaceUnit granularity, and up is the
//! safe direction to round a booking in
//!
//! @param bookingsize size to book in bytes
//!
//! @return number of units the booking occupies, clamped to what a disk's
//!         free space field can hold
//------------------------------------------------------------------------------
inline uint32_t
BookingToUnits(uint64_t bookingsize)
{
  const uint64_t units = (bookingsize + kFreeSpaceUnit - 1) / kFreeSpaceUnit;
  return static_cast<uint32_t>(
      std::min<uint64_t>(units, std::numeric_limits<uint32_t>::max()));
}

//------------------------------------------------------------------------------
//! Check whether a file of the given size still fits on a disk
//!
//! @param disk disk to check
//! @param bookingsize size to book in bytes, 0 books nothing
//!
//! @return true if the disk has room, otherwise false
//!
//! @note see BookingToUnits for the granularity of the check
//------------------------------------------------------------------------------
inline bool
HasRoomFor(const Disk& disk, uint64_t bookingsize)
{
  if (bookingsize == 0) {
    return true;
  }

  return disk.free_gib.load(std::memory_order_relaxed) >= BookingToUnits(bookingsize);
}

//------------------------------------------------------------------------------
//! Book space on a disk for a file that was just placed on it. The free space
//! is debited immediately so that concurrent placements see each other's
//! bookings rather than all fitting into the same last gigabytes, and the
//! booking is remembered so that the next FST publish, which cannot see bytes
//! not yet written, gets it subtracted, see ClusterData::SetDiskFreeSpace.
//!
//! @param disk disk to book on
//! @param bookingsize size to book in bytes, 0 books nothing
//------------------------------------------------------------------------------
inline void
BookSpace(const Disk& disk, uint64_t bookingsize)
{
  if (bookingsize == 0) {
    return;
  }

  const uint32_t units = BookingToUnits(bookingsize);
  // Both counters saturate rather than wrap, hence the CAS loops: the free
  // space clamps at zero when a raced check let one booking too many through,
  // the booked total clamps at the ceiling it cannot meaningfully exceed
  uint32_t free = disk.free_gib.load(std::memory_order_relaxed);

  while (!disk.free_gib.compare_exchange_weak(free, (free > units) ? free - units : 0,
                                              std::memory_order_relaxed)) {
  }

  uint32_t booked = disk.booked_gib.load(std::memory_order_relaxed);
  constexpr uint32_t max_booked = std::numeric_limits<uint32_t>::max();

  while (!disk.booked_gib.compare_exchange_weak(
      booked, (booked <= max_booked - units) ? booked + units : max_booked,
      std::memory_order_relaxed)) {
  }
}

//! Default fill level in percent at which a disk stops attracting new
//! replicas, the equivalent of the geotree fillratiolimit
constexpr uint8_t kDefaultFillCapPercent = 95;
//! Default fill level in percent at which a disk's weight starts to decay
constexpr uint8_t kDefaultFillWarnPercent = 80;

//------------------------------------------------------------------------------
//! Struct FillLimits - the runtime configurable fill thresholds of one
//! topology snapshot. Atomics so that a "space config" change lands on the
//! live snapshot without an epoch bump, copyable so that ClusterData keeps
//! its implicit copy semantics.
//------------------------------------------------------------------------------
struct FillLimits {
  //! Fill level in percent at which a disk stops attracting new replicas
  std::atomic<uint8_t> cap{kDefaultFillCapPercent};
  //! Fill level in percent at which a disk's weight starts to decay
  std::atomic<uint8_t> warn{kDefaultFillWarnPercent};

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FillLimits() = default;

  //----------------------------------------------------------------------------
  //! Copy constructor, explicit as atomic types are not copyable
  //!
  //! @param other object to copy from
  //----------------------------------------------------------------------------
  FillLimits(const FillLimits& other)
  {
    cap.store(other.cap.load(std::memory_order_relaxed), std::memory_order_relaxed);
    warn.store(other.warn.load(std::memory_order_relaxed), std::memory_order_relaxed);
  }

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //!
  //! @param other object to copy from
  //!
  //! @return reference to the current object
  //----------------------------------------------------------------------------
  FillLimits&
  operator=(const FillLimits& other)
  {
    cap.store(other.cap.load(std::memory_order_relaxed), std::memory_order_relaxed);
    warn.store(other.warn.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }
};

//------------------------------------------------------------------------------
//! Struct DeniedBranchesFlag - whether the snapshot carries any disabled-branch
//! rule at all. Atomic so a rule change lands on the live snapshot without an
//! epoch bump, copyable so that ClusterData keeps its implicit copy semantics -
//! the same pattern as FillLimits. Read on the scheduling hot path, see
//! FlatScheduler::PlaceInBucket.
//!
//! One flag for every operation rather than one per operation: a rule of any
//! shape costs the branch walk, which is a touch more conservative than the
//! placement-only flag it replaces and keeps the vocabulary in one place.
//------------------------------------------------------------------------------
struct DeniedBranchesFlag {
  std::atomic<uint8_t> value{0}; ///< Non-zero while any rule is active

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  DeniedBranchesFlag() = default;

  //----------------------------------------------------------------------------
  //! Copy constructor, explicit as atomic types are not copyable
  //!
  //! @param other object to copy from
  //----------------------------------------------------------------------------
  DeniedBranchesFlag(const DeniedBranchesFlag& other)
  {
    value.store(other.value.load(std::memory_order_relaxed), std::memory_order_relaxed);
  }

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //!
  //! @param other object to copy from
  //!
  //! @return reference to the current object
  //----------------------------------------------------------------------------
  DeniedBranchesFlag&
  operator=(const DeniedBranchesFlag& other)
  {
    value.store(other.value.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }
};

//------------------------------------------------------------------------------
//! Get the effective placement weight of a disk: its capacity weight scaled
//! down as the disk fills, reaching zero at the fill cap so that a nearly
//! full disk stops attracting new replicas
//!
//! @param disk disk to weigh
//! @param limits fill thresholds of the snapshot the disk belongs to
//!
//! @return effective weight, 0 if the disk should take no further replicas
//------------------------------------------------------------------------------
inline uint32_t
GetEffectiveWeight(const Disk& disk, const FillLimits& limits)
{
  const uint32_t weight = disk.weight.load(std::memory_order_relaxed);
  const uint8_t used = disk.percent_used.load(std::memory_order_relaxed);
  const uint8_t fill_cap = limits.cap.load(std::memory_order_relaxed);
  const uint8_t fill_warn = limits.warn.load(std::memory_order_relaxed);

  if ((weight == 0) || (used >= fill_cap)) {
    return 0;
  }

  if (used <= fill_warn) {
    return weight;
  }

  // Linear decay between the warning level and the cap. It never reaches zero
  // here - that is reserved for the cap - so a disk in the decay band stays
  // usable, just less attractive. The two branches above guarantee
  // fill_warn < used < fill_cap here, so the span is never zero.
  const uint32_t span = fill_cap - fill_warn;
  const uint32_t headroom = fill_cap - used;
  const uint32_t scaled = (weight * headroom) / span;
  return (scaled > 0) ? scaled : 1;
}

//------------------------------------------------------------------------------
//! Common storage elements, these could become user defined in the future
//------------------------------------------------------------------------------
enum class BucketType : uint8_t {
  GROUP = 0,
  RACK,
  ROOM,
  SITE,
  ROOT,
  NODE,
  //! Synthetic flat leaf view of a scheduling group: every disk of the group's
  //! subtree in one bucket. Never linked as anyone's child, so the normal
  //! descent cannot reach it; only Bucket::flat_view points at it.
  FLATVIEW,
  COUNT,
  //! Sentinel type of a default constructed bucket, i.e. a hole in the id
  //! range. Kept distinct from GROUP (which is 0) so that a hole is never
  //! mistaken for a real scheduling group, see ClusterData::GetBucket.
  INVALID = std::numeric_limits<uint8_t>::max()
};

//------------------------------------------------------------------------------
//! Convert a bucket type to its numeric representation
//!
//! @param t bucket type
//!
//! @return numeric bucket type
//------------------------------------------------------------------------------
constexpr uint8_t
GetBucketType(BucketType t)
{
  return static_cast<uint8_t>(t);
}

//------------------------------------------------------------------------------
//! Convert a bucket type to its string representation
//!
//! @param t bucket type
//!
//! @return string representation, "unknown" if unrecognized
//------------------------------------------------------------------------------
inline std::string
BucketTypeToStr(BucketType t)
{
  switch (t) {
  case BucketType::GROUP:
    return "group";

  case BucketType::RACK:
    return "rack";

  case BucketType::ROOM:
    return "room";

  case BucketType::SITE:
    return "site";

  case BucketType::ROOT:
    return "root";

  case BucketType::NODE:
    return "node";

  case BucketType::FLATVIEW:
    return "flatview";

  default:
    return "unknown";
  }
}

//! Maximum number of geotag levels kept below a scheduling group. A geotag with
//! more atoms than this gets truncated, the tail atoms are folded into the
//! deepest bucket.
constexpr uint8_t kMaxGeoDepth = 8;

//------------------------------------------------------------------------------
//! Get the bucket type naming the given geotag level, following the usual EOS
//! site::room::rack::node convention. Anything below the fourth level is
//! reported as a node.
//!
//! @param geo_level geotag level, starting at 1 for the first atom
//!
//! @return bucket type of that level
//------------------------------------------------------------------------------
constexpr BucketType
GeoLevelToBucketType(uint8_t geo_level)
{
  switch (geo_level) {
  case 1:
    return BucketType::SITE;

  case 2:
    return BucketType::ROOM;

  case 3:
    return BucketType::RACK;

  default:
    return BucketType::NODE;
  }
}

//------------------------------------------------------------------------------
//! Hash a single geotag atom, ie. one "::" separated component of a geotag
//!
//! @param atom geotag atom
//!
//! @return hash of the atom
//!
//! @note only used as the geotag_index key, see GeoChildKey and FindGeoChild.
//!       A 64 bit collision is caught by verifying Bucket::geo_atom on hit, so a
//!       collision degrades to a correct miss, never a wrong match.
//------------------------------------------------------------------------------
inline uint64_t
HashGeoAtom(std::string_view atom)
{
  return XXH3_64bits(atom.data(), atom.size());
}

//------------------------------------------------------------------------------
//! Split a geotag into its atoms
//!
//! @param geotag geotag, atoms separated by "::"
//!
//! @return atoms in hierarchy order, outermost first. Empty atoms, which a
//!         leading, trailing or doubled separator would produce, are dropped.
//------------------------------------------------------------------------------
inline std::vector<std::string_view>
SplitGeoTag(std::string_view geotag)
{
  std::vector<std::string_view> atoms;
  size_t start = 0;

  while (start < geotag.size()) {
    const size_t end = geotag.find("::", start);
    const size_t len =
        (end == std::string_view::npos) ? geotag.size() - start : end - start;

    if (len > 0) {
      atoms.push_back(geotag.substr(start, len));
    }

    if (end == std::string_view::npos) {
      break;
    }

    start = end + 2;
  }

  return atoms;
}

//------------------------------------------------------------------------------
//! Get the key under which a bucket is registered in ClusterData::geotag_index.
//! The same geotag atom appears below every scheduling group, so the parent has
//! to be part of the key for the lookup to be unambiguous.
//!
//! @param parent_id identifier of the parent bucket
//! @param atom_hash hash of the geotag atom, see HashGeoAtom
//!
//! @return index key
//------------------------------------------------------------------------------
inline uint64_t
GeoChildKey(ItemIdT parent_id, uint64_t atom_hash)
{
  // Mix the parent in with the golden ratio constant, the same spread as the
  // usual hash_combine
  const uint64_t parent = static_cast<uint32_t>(parent_id);
  return atom_hash ^
         (parent * 0x9E3779B97F4A7C15ULL + (atom_hash << 6) + (atom_hash >> 2));
}

//! Operations the "sched disable" aliases stand for. The rules speak the same
//! vocabulary as a filesystem's own permission mask, but hold the operations a
//! branch is closed *for*, so a set bit denies rather than allows.
inline constexpr FsOpMask kDenyPlct =
    eos::common::MaskOfDirection(SchedDirection::kCreate);
inline constexpr FsOpMask kDenyAccess =
    static_cast<FsOpMask>(eos::common::MaskOfDirection(SchedDirection::kRead) |
                          eos::common::MaskOfDirection(SchedDirection::kUpdate));
inline constexpr FsOpMask kDenyAll = kMaskAll;

//! Disabled branch rules of one space: canonical geotag to denied operations
using DisabledBranchesT = std::map<std::string, FsOpMask>;

//------------------------------------------------------------------------------
//! Convert a denied operations mask to its string representation: the alias
//! name when the mask is exactly one of them, otherwise the explicit form.
//!
//! Deliberately not FormatSchedMask - its preset names describe what a
//! filesystem allows, and reading "ro" as "read only" on a deny mask means
//! exactly the opposite of what the rule does.
//!
//! @note Display only - "sched disable ls", the topology dumps and the log
//!       lines. What gets persisted is eos::common::FormatSchedOps, the same
//!       grammar a file system's own sched.ops carries, so that the stored
//!       rule set and the stored permissions read alike; see
//!       PersistDisabledBranches in SchedCmd.cc.
//!
//! @param op_mask denied operations
//!
//! @return string representation, "none" for an empty mask
//------------------------------------------------------------------------------
inline std::string
DeniedOpsToStr(FsOpMask op_mask)
{
  switch (op_mask & kMaskAll) {
  case kDenyPlct:
    return "plct";

  case kDenyAccess:
    return "access";

  case kDenyAll:
    return "all";

  case eos::common::MaskOfActivity(SchedActivity::kClient):
    return "client";

  case eos::common::MaskOfActivity(SchedActivity::kInternal):
    return "internal";

  default:
    return eos::common::FormatSchedOps(op_mask);
  }
}

//------------------------------------------------------------------------------
//! Parse the operations a disable rule denies. Accepts the legacy aliases -
//! "plct", "access" and "all", which is what the persisted rules and the
//! existing runbooks carry - plus "client" and "internal" for a whole traffic
//! class, and otherwise the explicit "client:<letters>,internal:<letters>"
//! grammar, see eos::common::ParseSchedOps.
//!
//! @param spec specification string
//!
//! @return the denied operations, or nullopt if the spec is malformed
//------------------------------------------------------------------------------
inline std::optional<FsOpMask>
ParseDeniedSpec(std::string_view spec)
{
  if (spec == "plct") {
    return kDenyPlct;
  }

  if (spec == "access") {
    return kDenyAccess;
  }

  if (spec == "all") {
    return kDenyAll;
  }

  if (spec == "client") {
    return eos::common::MaskOfActivity(SchedActivity::kClient);
  }

  if (spec == "internal") {
    return eos::common::MaskOfActivity(SchedActivity::kInternal);
  }

  return eos::common::ParseSchedOps(spec);
}

//------------------------------------------------------------------------------
//! Bring a geotag into its canonical form: the atoms joined by "::" with the
//! degenerate separators dropped, so that a rule added as "eu::ch::" can be
//! removed as "eu::ch"
//!
//! @param geotag geotag to normalize
//!
//! @return canonical geotag, empty if no atoms survive
//------------------------------------------------------------------------------
inline std::string
NormalizeGeoTag(std::string_view geotag)
{
  std::string tag;

  for (const auto atom : SplitGeoTag(geotag)) {
    if (!tag.empty()) {
      tag += "::";
    }

    tag += atom;
  }

  return tag;
}

//! Group index carried by a bucket that is not a scheduling group, see
//! Bucket::group_index. Scheduling groups used to be addressed arithmetically,
//! at a fixed offset of their index, which meant a group added to a topology
//! that already held geo buckets found its identifier taken - the geo buckets
//! of the previous build had grown into the range. Groups are now allocated
//! from the same identifier space as every other bucket and the index is
//! resolved through ClusterData::GetGroupBucketId instead.
constexpr uint32_t kNoGroupIndex = std::numeric_limits<uint32_t>::max();

//------------------------------------------------------------------------------
//! Kind of children a bucket holds. A bucket never mixes them: the topology
//! builder attaches a disk without a geotag to a placeholder bucket rather than
//! directly to its scheduling group, so the placement descent can decide what to
//! do at a level from this one byte.
//------------------------------------------------------------------------------
enum class ChildType : uint8_t {
  kNone = 0,  ///< No child yet, the next append decides the kind
  kDisks,     ///< Disks, ie. a leaf level of the hierarchy
  kGroups,    ///< Scheduling groups, only the root holds those
  kGeoBuckets ///< Buckets of the geo hierarchy, ie. sites/rooms/racks/nodes
};

//------------------------------------------------------------------------------
//! Convert a child type to its numeric representation
//!
//! @param t child type
//!
//! @return numeric child type
//------------------------------------------------------------------------------
constexpr uint8_t
GetChildType(ChildType t)
{
  return static_cast<uint8_t>(t);
}

//------------------------------------------------------------------------------
//! Convert a child type to its string representation
//!
//! @param t child type
//!
//! @return string representation
//------------------------------------------------------------------------------
inline std::string
ChildTypeToStr(ChildType t)
{
  switch (t) {
  case ChildType::kDisks:
    return "disks";

  case ChildType::kGroups:
    return "groups";

  case ChildType::kGeoBuckets:
    return "geobuckets";

  default:
    return "none";
  }
}

//------------------------------------------------------------------------------
//! Struct Bucket - interior node of the hierarchy. Its children are either
//! positive disk ids or negative sub-bucket ids, never both, see ChildType.
//------------------------------------------------------------------------------
struct Bucket {
  ItemIdT id{0};     ///< Bucket identifier, never positive
  ItemIdT parent{0}; ///< Parent bucket, its own id for the root
  //! Flat leaf view of a scheduling group: the id of a synthetic FLATVIEW
  //! bucket holding every disk of the group's subtree, 0 when there is none -
  //! either this is not a group, or the group holds its disks directly and
  //! already is its own leaf level. Maintained by the SnapshotBuilder; what
  //! lets a placement with no geo preference skip the geo levels of the group,
  //! see FlatScheduler::PlaceInBucket.
  ItemIdT flat_view{0};
  //! Index of the scheduling group this bucket is, kNoGroupIndex for every
  //! other bucket. The identifier carries no index of its own any more, see
  //! ClusterData::GetGroupBucketId for the way in; this is the way back out, and
  //! it fits in the padding before geo_atom, hence the position.
  uint32_t group_index{kNoGroupIndex};
  //! Geotag atom naming this bucket, e.g. "rack3". Empty for the root and for
  //! the scheduling groups, which are not part of the geo hierarchy. Kept as the
  //! plain string so the geotag reassembles and matches without a side registry;
  //! buckets are few and copied only at rebuild, so the string cost is
  //! negligible.
  std::string geo_atom;
  uint32_t total_weight{0};     ///< Sum of the weights of all children
  //! Type of the bucket, see BucketType. Defaults to the INVALID sentinel so
  //! that a default constructed hole in the id range is never taken for a real
  //! GROUP (which is type 0), see ClusterData::GetBucket.
  uint8_t bucket_type{GetBucketType(BucketType::INVALID)};
  uint8_t level{0};             ///< Distance from the root, which sits at 0
  //! Operations the branch below is denied, one bit per SchedOp - the same
  //! vocabulary as a disk's own permission mask, read the other way round. Set
  //! only on the bucket a rule resolved to, not pushed down its subtree - the
  //! placement descent and the access walk up both pass through it. Atomic so
  //! a rule change lands on the live snapshot without an epoch bump.
  mutable std::atomic<FsOpMask> denied_ops{kMaskNone};
  //! Kind of the children below, see ChildType. Maintained by the topology
  //! builder, which refuses an append of the other kind, so that the descent
  //! knows what a level is without looking at a child. Fits in the padding
  //! after denied_ops, hence the position.
  uint8_t child_type{GetChildType(ChildType::kNone)};
  std::vector<ItemIdT> items; ///< Children, either disks or sub-buckets

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  Bucket() = default;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param _id bucket identifier
  //! @param type type of the bucket
  //! @param _parent parent bucket identifier
  //! @param _geo_atom geotag atom naming the bucket
  //! @param _level distance from the root
  //----------------------------------------------------------------------------
  Bucket(ItemIdT _id, uint8_t type, ItemIdT _parent = 0, std::string _geo_atom = {},
         uint8_t _level = 0)
      : id(_id)
      , parent(_parent)
      , geo_atom(std::move(_geo_atom))
      , total_weight(0)
      , bucket_type(type)
      , level(_level)
  {
  }

  //----------------------------------------------------------------------------
  //! Copy constructor, explicit as atomic types are not copyable
  //!
  //! @param other object to copy from
  //----------------------------------------------------------------------------
  Bucket(const Bucket& other)
      : id(other.id)
      , parent(other.parent)
      , flat_view(other.flat_view)
      , group_index(other.group_index)
      , geo_atom(other.geo_atom)
      , total_weight(other.total_weight)
      , bucket_type(other.bucket_type)
      , level(other.level)
      , child_type(other.child_type)
      , items(other.items)
  {
    denied_ops.store(other.denied_ops.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
  }

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //!
  //! @param other object to copy from
  //!
  //! @return reference to the current object
  //----------------------------------------------------------------------------
  Bucket&
  operator=(const Bucket& other)
  {
    id = other.id;
    parent = other.parent;
    flat_view = other.flat_view;
    group_index = other.group_index;
    geo_atom = other.geo_atom;
    total_weight = other.total_weight;
    bucket_type = other.bucket_type;
    level = other.level;
    child_type = other.child_type;
    denied_ops.store(other.denied_ops.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
    items = other.items;
    return *this;
  }

  //----------------------------------------------------------------------------
  //! Check if a rule closes the branch below for one operation
  //!
  //! @param op operation to test
  //!
  //! @return true if the operation is denied, otherwise false
  //----------------------------------------------------------------------------
  bool
  DeniesOp(SchedOp op) const
  {
    return (denied_ops.load(std::memory_order_acquire) & op.Bit()) != 0;
  }

  //----------------------------------------------------------------------------
  //! Get the operations denied below this bucket, for reporting
  //!
  //! @return denied operations mask
  //----------------------------------------------------------------------------
  FsOpMask
  DeniedOps() const
  {
    return denied_ops.load(std::memory_order_acquire);
  }

  //----------------------------------------------------------------------------
  //! Check if the bucket holds disks rather than sub-buckets. A bucket is never
  //! populated with both, the topology builder attaches a disk without a geotag
  //! to a placeholder bucket rather than directly to its scheduling group.
  //!
  //! @return true if the children are disks, otherwise false
  //----------------------------------------------------------------------------
  bool
  HoldsDisks() const
  {
    return !items.empty() && (items.front() > 0);
  }

  //----------------------------------------------------------------------------
  //! Record the kind of children this bucket holds, on the way to appending one
  //! of them. The first child decides the kind, every later one has to agree -
  //! that is what turns the never-mix invariant behind HoldsDisks into an
  //! enforced one, see the SnapshotBuilder append paths.
  //!
  //! @param type kind of the child about to be appended
  //!
  //! @return true if the child may be appended, false if the bucket already
  //!         holds children of another kind
  //----------------------------------------------------------------------------
  bool
  RecordChildType(ChildType type)
  {
    if (child_type == GetChildType(ChildType::kNone)) {
      child_type = GetChildType(type);
      return true;
    }

    return child_type == GetChildType(type);
  }

  //----------------------------------------------------------------------------
  //! Less than operator, orders by identifier
  //!
  //! @param l left hand side
  //! @param r right hand side
  //!
  //! @return true if l sorts before r, otherwise false
  //----------------------------------------------------------------------------
  friend bool
  operator<(const Bucket& l, const Bucket& r)
  {
    return l.id < r.id;
  }
};

//------------------------------------------------------------------------------
//! Struct ClusterData - immutable topology snapshot of one space. The whole
//! hierarchy is a pair of contiguous arrays, parent to child links are just
//! integers in Bucket::items, navigable by the sign of the identifier.
//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
//! Struct ClusterStateSummary - aggregate health picture of one topology
//! snapshot, the printInfo numbers. Plain values so that any admin surface
//! (the CLI now, gRPC later) can format them as it likes.
//------------------------------------------------------------------------------
struct ClusterStateSummary {
  uint32_t n_groups = 0;         ///< Scheduling groups
  uint32_t n_disks = 0;          ///< Disks, holes in the fsid range excluded
  uint32_t n_online = 0;         ///< Disks with an online active status
  uint32_t n_offline = 0;        ///< Disks with any other active status
  uint32_t n_rw = 0;             ///< Disks in a writable config status (rw, wo)
  uint32_t n_ro = 0;             ///< Disks in config status ro
  uint32_t n_drain = 0;          ///< Disks in one of the drain statuses
  uint32_t n_other = 0;          ///< Disks off, empty or unknown
  uint64_t capacity_weight = 0;  ///< Sum of the configured disk weights
  uint64_t effective_weight = 0; ///< Same sum after the fill decay
  uint64_t free_gib = 0;         ///< Total free space in GiB
  //! Free space on the disks placement can actually use, see
  //! ClusterData::GetWritableFreeGiB
  uint64_t writable_free_gib = 0;
  uint64_t booked_gib = 0;       ///< Space booked since the last FST publishes
  EpochIdT epoch = 0;            ///< Epoch of the snapshot, see ClusterMgr
};

struct ClusterData {
  std::vector<Disk> disks;     ///< Disks, the one with fsid n sits at index n - 1
  //! Bucket each disk hangs from, indexed like disks. Kept in step with disks
  //! by the SnapshotBuilder; 0 marks a hole in the fsid range. This is what lets
  //! the access path resolve a replica's geo position without walking the
  //! whole hierarchy.
  std::vector<ItemIdT> disk_parents;
  std::vector<Bucket> buckets; ///< Buckets, the one with id -n sits at index n
  //! Bucket of each scheduling group, indexed by group index, 0 where the
  //! index names no group. Groups are allocated from the same identifier space
  //! as every other bucket - which is what lets one be added to a topology
  //! that already holds geo buckets - so this is the only way from the index a
  //! request carries to the bucket it means, see GetGroupBucketId.
  std::vector<ItemIdT> group_buckets;
  //! Geo bucket per (parent, geotag atom) pair, see GeoChildKey. Lets a descent
  //! follow a client geotag one atom at a time without scanning the children.
  //! A hash collision is caught by verifying Bucket::geo_atom on hit, see
  //! FindGeoChild, so it degrades to a correct miss, never a wrong match.
  std::unordered_map<uint64_t, ItemIdT> geotag_index;
  //! Fill thresholds every disk of this snapshot is weighed against. Fresh
  //! snapshots start at the defaults; the FsScheduler re-stamps the configured
  //! values after every rebuild.
  FillLimits fill_limits;
  //! Whether any disabled-branch rule is configured at all, kept in step by
  //! ApplyDisabledBranches. What tells the flat-view fast path of the scheduler
  //! to take the full descent instead, which is where an interior denied bucket
  //! is honoured.
  DeniedBranchesFlag denied_branches;
  //! Name of the backing store a file system shares with others, per fsid.
  //! Several file systems can be configured on one backend, in which case they
  //! all publish that backend's statfs figures - so the space capacity may only
  //! count one of them, see GetWritableFreeGiB. Only the file systems that
  //! actually name a shared backend appear here, so the map stays empty on an
  //! installation without any.
  std::unordered_map<fsid_t, std::string> shared_fs;

  //----------------------------------------------------------------------------
  //! Get the bucket with the given identifier, the one validity definition
  //! shared by the scheduler and the topology builder.
  //!
  //! @param id bucket identifier, 0 is the root; a positive id names a disk
  //!
  //! @return pointer to the bucket, nullptr if the id is positive, out of
  //!         range, a hole in the id range (INVALID sentinel type) or does not
  //!         match the bucket stored at that slot
  //----------------------------------------------------------------------------
  const Bucket*
  GetBucket(ItemIdT id) const
  {
    if (id > 0) {
      return nullptr;
    }

    const size_t index = static_cast<size_t>(-id);

    if (index >= buckets.size()) {
      return nullptr;
    }

    const Bucket& bucket = buckets[index];

    // A hole carries the INVALID sentinel type, and a real bucket - the root
    // included - always stores its own id at this slot
    if ((bucket.bucket_type == GetBucketType(BucketType::INVALID)) || (bucket.id != id)) {
      return nullptr;
    }

    return &bucket;
  }

  //----------------------------------------------------------------------------
  //! Get the bucket of the scheduling group with the given index
  //!
  //! @param group_index scheduling group index, as the FsView numbers them
  //!
  //! @return bucket identifier, 0 if the index names no group of this topology
  //----------------------------------------------------------------------------
  ItemIdT
  GetGroupBucketId(int64_t group_index) const
  {
    if ((group_index < 0) || (static_cast<size_t>(group_index) >= group_buckets.size())) {
      return 0;
    }

    return group_buckets[group_index];
  }

  //----------------------------------------------------------------------------
  //! Get the disk with the given file system identifier
  //!
  //! @param id file system identifier, disks sit at index id - 1
  //!
  //! @return pointer to the disk, nullptr if the id is 0, out of range or a
  //!         hole in the fsid range
  //----------------------------------------------------------------------------
  const Disk*
  GetDisk(fsid_t id) const
  {
    if ((id == 0) || (id > disks.size())) {
      return nullptr;
    }

    const Disk& disk = disks[id - 1];
    return (disk.id == 0) ? nullptr : &disk;
  }

  //----------------------------------------------------------------------------
  //! Get the child of a bucket matching one geotag atom
  //!
  //! @param parent_id identifier of the bucket to look below
  //! @param atom geotag atom to match
  //!
  //! @return identifier of the matching bucket, 0 if there is none
  //----------------------------------------------------------------------------
  ItemIdT
  FindGeoChild(ItemIdT parent_id, std::string_view atom) const
  {
    auto it = geotag_index.find(GeoChildKey(parent_id, HashGeoAtom(atom)));

    if (it == geotag_index.end()) {
      return 0;
    }

    // Verify the atom so a 64 bit hash collision degrades to a correct miss
    // rather than resolving to the wrong bucket
    const Bucket* bucket = GetBucket(it->second);
    return (bucket && (bucket->geo_atom == atom)) ? it->second : 0;
  }

  //----------------------------------------------------------------------------
  //! Get the geotag of a bucket, reassembled by walking up to the scheduling
  //! group it belongs to
  //!
  //! @param bucket_id identifier of the bucket
  //!
  //! @return geotag, empty if the bucket is not part of the geo hierarchy
  //----------------------------------------------------------------------------
  std::string
  GetGeoTag(ItemIdT bucket_id) const
  {
    std::vector<std::string_view> atoms;
    ItemIdT id = bucket_id;

    // Bounded rather than "until the root" so that a malformed parent chain
    // cannot spin here. The bound matches the path-building loops elsewhere
    // (i < kMaxGeoDepth), which cap the geo hierarchy at kMaxGeoDepth atoms.
    for (uint8_t i = 0; (i < kMaxGeoDepth) && (id < 0); ++i) {
      if ((size_t)(-id) >= buckets.size()) {
        break;
      }

      const auto& bucket = buckets[-id];

      if (bucket.geo_atom.empty()) {
        break;
      }

      atoms.push_back(bucket.geo_atom);
      id = bucket.parent;
    }

    std::string tag;

    for (auto it = atoms.rbegin(); it != atoms.rend(); ++it) {
      if (!tag.empty()) {
        tag += "::";
      }

      tag += *it;
    }

    return tag;
  }

  //----------------------------------------------------------------------------
  //! Get the number of leading geotag atoms a disk shares with a client
  //!
  //! @param disk_id file system identifier of the disk
  //! @param client_atoms client geotag atoms, outermost first, see SplitGeoTag
  //!
  //! @return number of matching leading atoms, 0 if the disk is unknown or
  //!         sits outside the geo hierarchy
  //----------------------------------------------------------------------------
  uint8_t
  GetGeoOverlap(fsid_t disk_id, const std::vector<std::string_view>& client_atoms) const
  {
    if (client_atoms.empty() || (disk_id == 0) || (disk_id > disk_parents.size())) {
      return 0;
    }

    // Collect the geo path of the disk walking up, innermost atom first.
    // Bounded rather than "until the root" so that a malformed parent chain
    // cannot spin here. The views alias the immutable snapshot's bucket atoms.
    std::array<std::string_view, kMaxGeoDepth> path;
    uint8_t depth = 0;
    ItemIdT id = disk_parents[disk_id - 1];

    for (uint8_t i = 0; (i < kMaxGeoDepth) && (id < 0); ++i) {
      if ((size_t)(-id) >= buckets.size()) {
        break;
      }

      const auto& bucket = buckets[-id];

      if (bucket.geo_atom.empty()) {
        // reached the scheduling group above the geo levels
        break;
      }

      path[depth++] = bucket.geo_atom;
      id = bucket.parent;
    }

    // The collected path is innermost first, the client atoms outermost first
    uint8_t overlap = 0;

    while ((overlap < depth) && (overlap < client_atoms.size()) &&
           (path[depth - 1 - overlap] == client_atoms[overlap])) {
      ++overlap;
    }

    return overlap;
  }

  //----------------------------------------------------------------------------
  //! Get the bucket each disk is attached to
  //!
  //! @return map of disk identifier to bucket identifier
  //----------------------------------------------------------------------------
  std::unordered_map<ItemIdT, ItemIdT>
  GetDiskParents() const
  {
    std::unordered_map<ItemIdT, ItemIdT> parents;

    for (size_t i = 0; i < disk_parents.size(); ++i) {
      if (disk_parents[i] != 0) {
        parents.emplace(static_cast<ItemIdT>(i + 1), disk_parents[i]);
      }
    }

    return parents;
  }

  //----------------------------------------------------------------------------
  //! Update the configuration status of a disk
  //!
  //! @param id file system identifier
  //! @param status new configuration status
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool
  SetDiskOps(fsid_t id, FsOpMask ops)
  {
    const Disk* disk = GetDisk(id);

    if (disk == nullptr) {
      return false;
    }

    disk->ops.store(ops, std::memory_order_release);
    return true;
  }

  //----------------------------------------------------------------------------
  //! Update the active status of a disk
  //!
  //! @param id file system identifier
  //! @param status new active status
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool
  SetDiskStatus(fsid_t id, ActiveStatus status)
  {
    const Disk* disk = GetDisk(id);

    if (disk == nullptr) {
      return false;
    }

    disk->active_status.store(status, std::memory_order_release);
    return true;
  }

  //----------------------------------------------------------------------------
  //! Update the fill level of a disk
  //!
  //! @param id file system identifier
  //! @param percent_used fill level in percent, clamped to 100
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool
  SetDiskPercentUsed(fsid_t id, uint8_t percent_used)
  {
    const Disk* disk = GetDisk(id);

    if (disk == nullptr) {
      return false;
    }

    disk->percent_used.store(std::min<uint8_t>(percent_used, 100),
                             std::memory_order_release);
    return true;
  }

  //----------------------------------------------------------------------------
  //! Book space on a disk for a file that was just placed on it, see BookSpace
  //!
  //! @param id file system identifier
  //! @param bookingsize size to book in bytes, 0 books nothing
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool
  BookDiskSpace(fsid_t id, uint64_t bookingsize) const
  {
    const Disk* disk = GetDisk(id);

    if (disk == nullptr) {
      return false;
    }

    BookSpace(*disk, bookingsize);
    return true;
  }

  //----------------------------------------------------------------------------
  //! Update the free space of a disk from an FST publish. The published figure
  //! cannot include bytes that are booked but not yet written, so the bookings
  //! accumulated since the last publish are subtracted from it - and retired,
  //! every booking discounts exactly one publish. A booking whose write landed
  //! before the publish is subtracted once too often, which errs on the safe
  //! side for one publish interval; one whose file never got written stops
  //! haunting the disk after the same interval.
  //!
  //! @param id file system identifier
  //! @param free_bytes free space in bytes
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool
  SetDiskFreeSpace(fsid_t id, uint64_t free_bytes)
  {
    const Disk* disk = GetDisk(id);

    if (disk == nullptr) {
      return false;
    }

    const uint32_t booked = disk->booked_gib.exchange(0, std::memory_order_acq_rel);
    const uint32_t reported = FreeSpaceToGiB(free_bytes);
    disk->free_gib.store((reported > booked) ? reported - booked : 0,
                         std::memory_order_release);
    return true;
  }

  //----------------------------------------------------------------------------
  //! Get the free space placement can actually use: the sum of Disk::free_gib
  //! over every disk that is a placement candidate - online, in a writable
  //! config status and not below a branch disabled for placement. The same
  //! criteria SelectionStrategy::ValidDisk applies, so the figure agrees with
  //! what a placement descent will actually do. The flat equivalent of the
  //! geotree totalWritableSpace aggregate behind placementSpace; bookings are
  //! already discounted since BookSpace debits free_gib directly, and file
  //! systems sharing one backing store are counted once, see shared_fs.
  //!
  //! @return writable free space in GiB
  //----------------------------------------------------------------------------
  uint64_t
  GetWritableFreeGiB() const
  {
    uint64_t total = 0;
    // File systems sitting on one shared backend all report that backend's
    // free space, so only the first candidate among them contributes it -
    // otherwise the capacity is multiplied by the number of file systems
    // sharing the store. Stays empty where no file system declares one, and
    // holds views into shared_fs, which this pass does not touch.
    std::set<std::string_view> seen_shared_fs;

    for (const auto& disk : disks) {
      if (disk.id == 0) {
        continue; // hole in the fsid range
      }

      if ((disk.active_status.load(std::memory_order_relaxed) != ActiveStatus::kOnline) ||
          !disk.AllowsOp(kClientCreate)) {
        continue;
      }

      if (IsBranchDenied(disk.id, kClientCreate)) {
        continue;
      }

      if (!shared_fs.empty()) {
        auto it = shared_fs.find(disk.id);

        if ((it != shared_fs.end()) && !seen_shared_fs.insert(it->second).second) {
          continue;
        }
      }

      total += disk.free_gib.load(std::memory_order_relaxed);
    }

    return total;
  }

  //----------------------------------------------------------------------------
  //! Get the aggregate health picture of the snapshot in one pass over the
  //! disks. The epoch is not known at this level, the ClusterMgr fills it in.
  //!
  //! @return state summary
  //----------------------------------------------------------------------------
  ClusterStateSummary
  GetStateSummary() const
  {
    ClusterStateSummary summary;

    for (const auto& bucket : buckets) {
      // Holes carry the INVALID sentinel type, so a GROUP type is always a real
      // scheduling group
      if (bucket.bucket_type == GetBucketType(BucketType::GROUP)) {
        ++summary.n_groups;
      }
    }

    for (const auto& disk : disks) {
      if (disk.id == 0) {
        continue; // hole in the fsid range
      }

      ++summary.n_disks;

      if (disk.active_status.load(std::memory_order_relaxed) == ActiveStatus::kOnline) {
        ++summary.n_online;
      } else {
        ++summary.n_offline;
      }

      // Bucketed the same way the legacy status projection reads the mask, so
      // the columns keep meaning what they always did
      const FsOpMask mask = disk.ops.load(std::memory_order_relaxed);
      const bool client_write =
          eos::common::AllowsOp(mask, kClientCreate) ||
          eos::common::AllowsOp(mask,
                                SchedOp{SchedActivity::kClient, SchedDirection::kUpdate});

      if (client_write) {
        ++summary.n_rw;
      } else if (eos::common::AllowsOp(mask, kClientRead)) {
        ++summary.n_ro;
      } else if ((mask & eos::common::MaskOfActivity(SchedActivity::kInternal)) != 0) {
        // No client traffic but internal traffic still flows - what used to be
        // one of the drain statuses
        ++summary.n_drain;
      } else {
        ++summary.n_other;
      }

      summary.capacity_weight += disk.weight.load(std::memory_order_relaxed);
      summary.effective_weight += GetEffectiveWeight(disk, fill_limits);
      summary.free_gib += disk.free_gib.load(std::memory_order_relaxed);
      summary.booked_gib += disk.booked_gib.load(std::memory_order_relaxed);
    }

    summary.writable_free_gib = GetWritableFreeGiB();
    return summary;
  }

  //----------------------------------------------------------------------------
  //! Update the fill thresholds of the snapshot. Validation is the caller's
  //! job, see FsScheduler::SetFillLimits - here the values are only stored.
  //!
  //! @param cap fill level in percent at which a disk stops taking replicas
  //! @param warn fill level in percent at which the weight starts to decay
  //----------------------------------------------------------------------------
  void
  SetFillLimits(uint8_t cap, uint8_t warn)
  {
    fill_limits.cap.store(cap, std::memory_order_release);
    fill_limits.warn.store(warn, std::memory_order_release);
  }

  //----------------------------------------------------------------------------
  //! Re-resolve the disabled branch rules onto the buckets. Every mask is
  //! cleared first, so the rules passed in are always the complete set - one
  //! code path serves add, remove and the restamp after a rebuild alike. The
  //! geo hierarchy repeats below every scheduling group, so a rule flags its
  //! branch under each of them. A geotag matching no bucket is silently
  //! skipped: the rule may simply predate the part of the topology it names.
  //!
  //! @param rules canonical geotag to denied operations, see DisabledBranchesT
  //----------------------------------------------------------------------------
  void
  ApplyDisabledBranches(const DisabledBranchesT& rules)
  {
    bool any_rule = false;

    for (const auto& [geotag, op_mask] : rules) {
      any_rule = any_rule || ((op_mask & kMaskAll) != 0);
    }

    // Raised before any mask is touched and lowered only after the clearing
    // pass, so a placement running concurrently with the restamp errs towards
    // the full descent, never towards a shortcut past a mask in flux
    if (any_rule) {
      denied_branches.value.store(1, std::memory_order_release);
    }

    for (const auto& bucket : buckets) {
      bucket.denied_ops.store(kMaskNone, std::memory_order_release);
    }

    for (const auto& [geotag, op_mask] : rules) {
      const auto atoms = SplitGeoTag(geotag);

      if (atoms.empty()) {
        continue;
      }

      for (const auto& bucket : buckets) {
        // Holes carry the INVALID sentinel type, so only real scheduling
        // groups pass this filter
        if (bucket.bucket_type != GetBucketType(BucketType::GROUP)) {
          continue;
        }

        ItemIdT id = bucket.id;

        for (const auto atom : atoms) {
          id = FindGeoChild(id, atom);

          if (id == 0) {
            break;
          }
        }

        if (id != 0) {
          buckets[-id].denied_ops.fetch_or(op_mask, std::memory_order_release);
        }
      }
    }

    if (!any_rule) {
      denied_branches.value.store(0, std::memory_order_release);
    }
  }

  //----------------------------------------------------------------------------
  //! Check whether any disabled-branch rule is configured, see
  //! ApplyDisabledBranches
  //!
  //! @return true if a rule is active, otherwise false
  //----------------------------------------------------------------------------
  bool
  HasDeniedBranches() const
  {
    return denied_branches.value.load(std::memory_order_acquire) != 0;
  }

  //----------------------------------------------------------------------------
  //! Check whether a disk sits below a branch denied the given operation. Only
  //! the bucket a rule resolved to carries the mask, so the walk visits every
  //! geo ancestor of the disk.
  //!
  //! @param disk_id file system identifier of the disk
  //! @param op operation to test
  //!
  //! @return true if any geo ancestor denies the operation
  //----------------------------------------------------------------------------
  bool
  IsBranchDenied(fsid_t disk_id, SchedOp op) const
  {
    if ((disk_id == 0) || (disk_id > disk_parents.size())) {
      return false;
    }

    // Bounded rather than "until the root" so that a malformed parent chain
    // cannot spin here
    ItemIdT id = disk_parents[disk_id - 1];

    for (uint8_t i = 0; (i < kMaxGeoDepth) && (id < 0); ++i) {
      if ((size_t)(-id) >= buckets.size()) {
        break;
      }

      const auto& bucket = buckets[-id];

      if (bucket.geo_atom.empty()) {
        // reached the scheduling group above the geo levels
        break;
      }

      if (bucket.DeniesOp(op)) {
        return true;
      }

      id = bucket.parent;
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Update the weight of a disk
  //!
  //! @param id file system identifier
  //! @param weight new weight
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool
  SetDiskWeight(fsid_t id, uint8_t weight)
  {
    const Disk* disk = GetDisk(id);

    if (disk == nullptr) {
      return false;
    }

    disk->weight.store(weight, std::memory_order_release);
    return true;
  }

};

} // eos::mgm::placement
