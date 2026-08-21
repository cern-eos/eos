//------------------------------------------------------------------------------
//! @file SelectionStrategy.hh
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

#include "mgm/placement/ClusterDataTypes.hh"
#include "mgm/placement/RRSeed.hh"
#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <xxhash.h>

namespace eos::mgm::placement
{

//------------------------------------------------------------------------------
//! Struct PlacementResult - outcome of a placement request. Holds the
//! selected items, which are disk ids once the descent reached the leaves and
//! sub-bucket ids while it is still descending.
//------------------------------------------------------------------------------
struct PlacementResult {
  std::array<ItemIdT, 32> ids{0};     ///< Selected items
  int ret_code;                       ///< 0 if successful, otherwise an errno value
  int n_replicas;                     ///< Number of replicas that were requested
  //! Number of entries of ids that actually hold a selected item. Distinct
  //! from n_replicas: on a partial placement it is the smaller of the two, and
  //! it is what bounds every scan of ids.
  int n_filled;
  std::optional<std::string> err_msg; ///< Error description if any

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  PlacementResult()
      : ret_code(-1)
      , n_replicas(0)
      , n_filled(0)
  {
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param n_rep number of replicas requested
  //----------------------------------------------------------------------------
  PlacementResult(int n_rep)
      : ret_code(-1)
      , n_replicas(n_rep)
      , n_filled(0)
  {
  }

  //----------------------------------------------------------------------------
  //! Check if the placement request succeeded
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  operator bool() const
  {
    return ret_code == 0;
  }

  //----------------------------------------------------------------------------
  //! Check if the result holds the requested number of replicas and all of
  //! them are disks rather than buckets
  //!
  //! @param _n_replicas expected number of replicas
  //!
  //! @return true if the placement is complete, otherwise false
  //----------------------------------------------------------------------------
  bool
  IsValidPlacement(uint8_t _n_replicas) const
  {
    return (_n_replicas == n_filled) &&
           (std::all_of(ids.cbegin(), ids.cbegin() + n_filled,
                        [](ItemIdT id) { return id > 0; }));
  }

  //----------------------------------------------------------------------------
  //! Stream the selected items
  //!
  //! @param os output stream
  //! @param r result to stream
  //!
  //! @return output stream
  //----------------------------------------------------------------------------
  friend std::ostream& operator<< (std::ostream& os, const PlacementResult r)
  {
    for (int i = 0; i < r.n_filled; ++i) {
      os << r.ids[i] << " ";
    }

    return os;
  }

  //----------------------------------------------------------------------------
  //! Get a string representation of the selected items
  //!
  //! @return space separated list of the selected items
  //----------------------------------------------------------------------------
  std::string
  ResultString() const
  {
    std::stringstream ss;
    ss << *this;
    return ss.str();
  }

  //----------------------------------------------------------------------------
  //! Get the error description
  //!
  //! @return error description or an empty string if there was none
  //----------------------------------------------------------------------------
  std::string
  ErrorString() const
  {
    return err_msg.value_or("");
  }

  //----------------------------------------------------------------------------
  //! Check if the given item was already selected
  //!
  //! @param item item identifier
  //!
  //! @return true if present, otherwise false
  //----------------------------------------------------------------------------
  bool
  Contains(ItemIdT item) const
  {
    return std::find(ids.cbegin(), ids.cbegin() + n_filled, item) !=
           ids.cbegin() + n_filled;
  }

  //----------------------------------------------------------------------------
  //! Append a selected item, keeping n_filled in step so that Contains() and
  //! every other scan of ids stay bounded by what is actually set
  //!
  //! @param item item identifier
  //!
  //! @return true if appended, false if there was no room left
  //----------------------------------------------------------------------------
  bool
  Add(ItemIdT item)
  {
    if ((size_t)n_filled >= ids.size()) {
      return false;
    }

    ids[n_filled++] = item;
    return true;
  }
};

//------------------------------------------------------------------------------
//! How far apart in the topology the replicas of a file should be spread.
//! Mirrors Scheduler::tPlctPolicy, redeclared here so that mgm/placement stays
//! independent of mgm/scheduler.
//------------------------------------------------------------------------------
enum class PlacementPolicyT : uint8_t {
  kScattered = 0, //!< spread as widely as the topology allows
  kHybrid,        //!< collocate some replicas, spread the rest
  kGathered       //!< collocate all replicas
};

//------------------------------------------------------------------------------
//! Engine that schedules a space. The flat scheduler and the legacy geotree
//! engine coexist and which one serves a space is per space configuration.
//! This used to be smuggled into PlacementStrategyT as an extra value, which
//! forced a hole in the strategy array and made "is this a strategy" and
//! "which engine" the same question.
//------------------------------------------------------------------------------
enum class SchedEngineT : uint8_t {
  kGeoTree = 0, //!< legacy geotree engine, the default
  kFlat         //!< flat scheduler
};

//------------------------------------------------------------------------------
//! Placement strategies of the flat scheduler. Every value here is a real
//! strategy with an implementation behind it, so MakeSelectionStrategy never
//! returns nullptr for one.
//------------------------------------------------------------------------------
enum class PlacementStrategyT : uint8_t {
  kRoundRobin = 0,
  kThreadLocalRoundRobin,
  kRandom,
  kFidRandom,
  kWeightedRandom,
  kWeightedRoundRobin,
  kGeoScheduler,
  Count
};

//------------------------------------------------------------------------------
//! Struct SchedConfig - a resolved scheduler.type value: which engine serves
//! the space and, when that engine is the flat scheduler, which strategy it
//! picks with. The strategy field carries no meaning under kGeoTree.
//------------------------------------------------------------------------------
struct SchedConfig {
  SchedEngineT engine = SchedEngineT::kGeoTree;
  PlacementStrategyT strategy = PlacementStrategyT::kGeoScheduler;

  //----------------------------------------------------------------------------
  //! Whether the flat scheduler serves this space
  //----------------------------------------------------------------------------
  constexpr bool
  IsFlat() const
  {
    return engine == SchedEngineT::kFlat;
  }

  //----------------------------------------------------------------------------
  //! Equality. Two geotree configurations are equal whatever strategy field
  //! they happen to carry, since it means nothing there.
  //----------------------------------------------------------------------------
  constexpr bool
  operator==(const SchedConfig& other) const
  {
    if (engine != other.engine) {
      return false;
    }

    return !IsFlat() || (strategy == other.strategy);
  }

  constexpr bool
  operator!=(const SchedConfig& other) const
  {
    return !(*this == other);
  }
};

//! The legacy engine, which is what an unconfigured space runs
inline constexpr SchedConfig kGeoTreeSchedConfig{};

//! Total number of placement strategies
constexpr size_t TOTAL_PLACEMENT_STRATEGIES = static_cast<size_t>
    (PlacementStrategyT::Count);
//! Maximum number of items a strategy inspects before giving up
constexpr uint8_t MAX_PLACEMENT_ATTEMPTS = 100;
//! Maximum depth of the bucket hierarchy a placement descent may traverse
constexpr uint8_t MAX_PLACEMENT_DEPTH = 16;

//------------------------------------------------------------------------------
//! Check if the given strategy is a usable one
//!
//! @param strategy placement strategy type
//!
//! @return true if valid, otherwise false
//------------------------------------------------------------------------------
inline constexpr bool
IsValidPlacementStrategy(PlacementStrategyT strategy)
{
  return strategy != PlacementStrategyT::Count;
}

//------------------------------------------------------------------------------
//! Get the array index of the given strategy
//!
//! @param strategy placement strategy type
//!
//! @return array index
//------------------------------------------------------------------------------
inline constexpr size_t
StrategyIndex(PlacementStrategyT strategy)
{
  return static_cast<size_t>(strategy);
}

//------------------------------------------------------------------------------
//! Engine prefix of a flat scheduler strategy in a scheduler.type value. The
//! engine, not the strategy, is what a reader has to get right first: "geo" and
//! "geotree" used to differ by four characters and select different engines.
//------------------------------------------------------------------------------
inline constexpr std::string_view kFlatEnginePrefix = "flat:";

//------------------------------------------------------------------------------
//! Canonical name of the legacy geotree engine. It has no strategies, so it
//! names the engine on its own and never carries the prefix.
//------------------------------------------------------------------------------
inline constexpr std::string_view kGeoTreeEngineName = "geotree";

//------------------------------------------------------------------------------
//! Accepted spellings of the flat scheduler strategies, without the engine
//! prefix. The first entry of a strategy is its canonical name, which is what
//! StrategyToStr renders and what gets persisted; the rest are aliases.
//------------------------------------------------------------------------------
inline constexpr std::array<std::pair<std::string_view, PlacementStrategyT>, 13>
    kFlatStrategyNames{
        {{"geo", PlacementStrategyT::kGeoScheduler},
         {"geoscheduler", PlacementStrategyT::kGeoScheduler},
         {"roundrobin", PlacementStrategyT::kRoundRobin},
         {"rr", PlacementStrategyT::kRoundRobin},
         {"threadlocalroundrobin", PlacementStrategyT::kThreadLocalRoundRobin},
         {"threadlocalrr", PlacementStrategyT::kThreadLocalRoundRobin},
         {"tlrr", PlacementStrategyT::kThreadLocalRoundRobin},
         {"random", PlacementStrategyT::kRandom},
         {"fidrandom", PlacementStrategyT::kFidRandom},
         {"fid", PlacementStrategyT::kFidRandom},
         {"weightedrandom", PlacementStrategyT::kWeightedRandom},
         {"weightedroundrobin", PlacementStrategyT::kWeightedRoundRobin},
         {"weightedrr", PlacementStrategyT::kWeightedRoundRobin}}};

//------------------------------------------------------------------------------
//! Names of the legacy engine. "geotree" is canonical, "legacy" is the older
//! spelling and still accepted.
//------------------------------------------------------------------------------
inline constexpr std::array<std::string_view, 2> kGeoTreeEngineNames{
    {"geotree", "legacy"}};

//------------------------------------------------------------------------------
//! Look a flat strategy name up in the name table
//!
//! @param name name to look for, without the engine prefix
//!
//! @return the strategy, or nullopt if no spelling matches
//------------------------------------------------------------------------------
constexpr std::optional<PlacementStrategyT>
LookupFlatStrategyName(std::string_view name)
{
  for (const auto& [spelling, strategy] : kFlatStrategyNames) {
    if (spelling == name) {
      return strategy;
    }
  }

  return std::nullopt;
}

//------------------------------------------------------------------------------
//! Canonical name of a flat scheduler strategy, without the engine prefix
//!
//! @param strategy placement strategy type
//!
//! @return canonical spelling, "unknown" if the strategy has no name
//------------------------------------------------------------------------------
inline std::string
StrategyToStr(PlacementStrategyT strategy)
{
  for (const auto& [spelling, known] : kFlatStrategyNames) {
    if (known == strategy) {
      // First entry of a strategy is its canonical name
      return std::string(spelling);
    }
  }

  return "unknown";
}

//------------------------------------------------------------------------------
//! Render a resolved scheduler.type value in its canonical form: the engine
//! name on its own for geotree, engine prefix plus strategy for the flat
//! scheduler.
//!
//! @param config resolved configuration
//!
//! @return canonical string representation
//------------------------------------------------------------------------------
inline std::string
SchedConfigToStr(SchedConfig config)
{
  if (!config.IsFlat()) {
    return std::string(kGeoTreeEngineName);
  }

  return std::string(kFlatEnginePrefix) + StrategyToStr(config.strategy);
}

//------------------------------------------------------------------------------
//! Parse a scheduler.type value. Strict: a value it cannot name is an error
//! rather than a silent fall back to the legacy engine, so that a typo can be
//! refused at the point it is configured.
//!
//! Accepts the canonical forms - "geotree" for the legacy engine, "flat:<name>"
//! for a flat scheduler strategy - and, for backwards compatibility, every
//! spelling that was valid before the engine prefix existed.
//!
//! @param config_sv string representation
//!
//! @return the resolved configuration, or nullopt if the value names none
//------------------------------------------------------------------------------
constexpr std::optional<SchedConfig>
ParseSchedConfig(std::string_view config_sv)
{
  if (config_sv.empty()) {
    return std::nullopt;
  }

  for (const auto& name : kGeoTreeEngineNames) {
    if (config_sv == name) {
      return kGeoTreeSchedConfig;
    }
  }

  // Only a flat strategy may carry the prefix: "flat:geotree" is a
  // contradiction, and honouring it would defeat putting the engine in the name
  const bool prefixed =
      (config_sv.substr(0, kFlatEnginePrefix.size()) == kFlatEnginePrefix);
  const std::string_view name =
      prefixed ? config_sv.substr(kFlatEnginePrefix.size()) : config_sv;

  if (const auto strategy = LookupFlatStrategyName(name)) {
    return SchedConfig{SchedEngineT::kFlat, *strategy};
  }

  return std::nullopt;
}

//------------------------------------------------------------------------------
//! Check whether a scheduler.type value is one of the spellings that predate
//! the engine prefix, so that a caller can say so once rather than silently
//! rewriting what the operator typed. Only a missing prefix or a non-canonical
//! engine name counts; an alias that carries the prefix, "flat:weightedrr" say,
//! is a supported spelling.
//!
//! @param config_sv string representation
//!
//! @return true if the value resolves but is not written the current way
//------------------------------------------------------------------------------
constexpr bool
IsDeprecatedSchedConfigSpelling(std::string_view config_sv)
{
  const auto config = ParseSchedConfig(config_sv);

  if (!config.has_value()) {
    return false;
  }

  if (!config->IsFlat()) {
    return config_sv != kGeoTreeEngineName;
  }

  return config_sv.substr(0, kFlatEnginePrefix.size()) != kFlatEnginePrefix;
}

//------------------------------------------------------------------------------
//! Render the accepted scheduler.type values, canonical spellings only
//!
//! @return comma separated list, the legacy engine first
//------------------------------------------------------------------------------
inline std::string
KnownSchedConfigNames()
{
  std::string out(kGeoTreeEngineName);
  PlacementStrategyT last = PlacementStrategyT::Count;

  for (const auto& [spelling, strategy] : kFlatStrategyNames) {
    if (strategy == last) {
      continue; // an alias of the strategy already listed
    }

    last = strategy;
    out += ", ";
    out += kFlatEnginePrefix;
    out += spelling;
  }

  return out;
}

//------------------------------------------------------------------------------
//! Build the error text for a scheduler.type value naming no configuration
//!
//! @param config_sv the rejected value
//!
//! @return error message listing what would have been accepted
//------------------------------------------------------------------------------
inline std::string
UnknownSchedConfigError(std::string_view config_sv)
{
  return "error: <" + std::string(config_sv) +
         "> is not a scheduler type - accepted values are " + KnownSchedConfigNames() +
         "\n";
}

//------------------------------------------------------------------------------
//! Parse a scheduler.type value, falling back to the legacy engine when it
//! names none.
//!
//! For the read paths that have no way to report a bad value: the boot restore,
//! which must not abort over one unparseable space, and the per request
//! override. Anything that can refuse the value calls ParseSchedConfig instead.
//!
//! @param config_sv string representation
//!
//! @return the resolved configuration, geotree if unrecognized
//------------------------------------------------------------------------------
constexpr SchedConfig
SchedConfigFromStr(std::string_view config_sv)
{
  // A space with no scheduler.type configured reaches this as an empty string,
  // so this fall back is what decides which engine an untouched installation
  // runs: until the flat scheduler has soaked, that has to stay geotree.
  return ParseSchedConfig(config_sv).value_or(kGeoTreeSchedConfig);
}

//------------------------------------------------------------------------------
//! Struct PlacementArgs - inputs of a placement request
//------------------------------------------------------------------------------
struct PlacementArgs {
  ItemIdT bucket_id = 0;   ///< Bucket to start the descent from, 0 is the root
  uint8_t n_replicas;      ///< Number of replicas to place
  //! Operation this placement represents - which class of traffic wants the
  //! new replica. A disk is a candidate only if it accepts exactly this.
  SchedOp op = kClientCreate;
  uint64_t fid = 0; ///< File identifier, used by the deterministic strategies
  //! Salt mixed into the deterministic strategies, varied by the caller across
  //! retries so that a repeated request does not reproduce the same answer
  uint64_t salt = 0;
  //! Strategy to use, the scheduler default is taken if this is not valid
  PlacementStrategyT strategy = PlacementStrategyT::Count;
  std::vector<uint32_t> excludefs; ///< File systems not to place on
  int64_t forced_group_index = -1; ///< Scheduling group to force, -1 if none
  //! How widely to spread the replicas. Carried for the geo-aware strategy;
  //! the non-geo strategies ignore it.
  PlacementPolicyT plctpolicy = PlacementPolicyT::kScattered;
  //! Geolocation of the client, used as the entry point of a geo-aware
  //! descent. Empty when the client has no geotag.
  std::string_view geolocation;
  //! Number of replicas to keep together in the branch the client's geolocation
  //! points at, the rest are spread over the sibling branches. 0 disables the
  //! preference, which is what an empty geolocation amounts to. Derived by the
  //! caller from plctpolicy and the layout, see Scheduler::GetCollocatedReplicas.
  uint8_t ncollocatedfs = 0;
  //! Size the file is expected to reach, in bytes. A disk without room for it
  //! is not a candidate however attractive its weight makes it look. 0 books
  //! nothing and skips the check.
  uint64_t bookingsize = 0;

  //----------------------------------------------------------------------------
  //! Constructor descending from an explicit bucket
  //!
  //! @param _bucket_id bucket to start from, 0 is the root
  //! @param _n_replicas number of replicas to place
  //! @param _op operation the placement represents
  //! @param _fid file identifier
  //----------------------------------------------------------------------------
  PlacementArgs(ItemIdT _bucket_id, uint8_t _n_replicas, SchedOp _op = kClientCreate,
                uint64_t _fid = 0)
      : bucket_id(_bucket_id)
      , n_replicas(_n_replicas)
      , op(_op)
      , fid(_fid)
  {
  }

  //----------------------------------------------------------------------------
  //! Constructor descending from the root bucket
  //!
  //! @param _n_replicas number of replicas to place
  //! @param _op operation the placement represents
  //! @param _strategy strategy to use
  //----------------------------------------------------------------------------
  PlacementArgs(uint8_t _n_replicas, SchedOp _op = kClientCreate,
                PlacementStrategyT _strategy = PlacementStrategyT::Count)
      : n_replicas(_n_replicas)
      , op(_op)
      , strategy(_strategy)
  {
  }
};

//------------------------------------------------------------------------------
//! Struct AccessArgs - inputs of an access request. The selected replica
//! is reported back through selectedIndex, which is an index into selectedfs.
//------------------------------------------------------------------------------
struct AccessArgs {
  size_t& selectedIndex; ///< Out: index into selectedfs of the chosen replica
  ino64_t inode{0};      ///< Inode of the file being accessed
  PlacementStrategyT strategy{PlacementStrategyT::Count}; ///< Strategy to use
  std::string_view geolocation;                           ///< Geolocation of the client
  std::vector<uint32_t>* unavailfs{nullptr};              ///< Unavailable file systems
  const std::vector<uint32_t>& selectedfs;         ///< File systems holding the file
  const std::vector<uint32_t>* excludefs{nullptr}; ///< Excluded file systems
  //! File system the client must be sent to, 0 if none is forced
  uint32_t forcedfsid{0};
  //! Operation this access represents: which class of traffic, reading or
  //! updating. A replica's disk has to accept exactly this - sending an update
  //! to a disk that only serves reads is how a client ends up failing at the
  //! FST after the MGM told it everything was fine.
  SchedOp op{kClientRead};
  //! Number of replicas that have to be reachable for the request to be served.
  //! For a plain or replica read this is 1, but a RAIN layout needs the minimum
  //! number of stripes its redundancy can reconstruct from, and the access
  //! fails rather than returning a single stripe the driver cannot use.
  uint8_t n_replicas{1};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param _selectedIndex out parameter receiving the selected index
  //! @param _inode inode of the file being accessed
  //! @param _strategy strategy to use
  //! @param _geolocation geolocation of the client
  //! @param _unavailfs unavailable file systems
  //! @param _selectedfs file systems holding the file
  //! @param _excludefs excluded file systems
  //----------------------------------------------------------------------------
  AccessArgs(size_t& _selectedIndex, ino64_t _inode, PlacementStrategyT _strategy,
             std::string_view _geolocation, std::vector<uint32_t>* _unavailfs,
             const std::vector<uint32_t>& _selectedfs,
             const std::vector<uint32_t>* _excludefs = nullptr)
      : selectedIndex(_selectedIndex)
      , inode(_inode)
      , strategy(_strategy)
      , geolocation(_geolocation)
      , unavailfs(_unavailfs)
      , selectedfs(_selectedfs)
      , excludefs(_excludefs)
  {
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param _selectedIndex out parameter receiving the selected index
  //! @param _strategy strategy to use
  //! @param _selectedfs file systems holding the file
  //----------------------------------------------------------------------------
  AccessArgs(size_t& _selectedIndex, PlacementStrategyT _strategy,
             const std::vector<uint32_t>& _selectedfs)
      : selectedIndex(_selectedIndex)
      , strategy(_strategy)
      , selectedfs(_selectedfs)
  {
  }
};

//------------------------------------------------------------------------------
//! Struct SelectionStrategy - interface every selection strategy implements to
//! place new replicas, together with the validation helpers shared by all of
//! them. Access to existing replicas does not vary per placement strategy - a
//! reachable replica is picked uniformly at random - so it lives as a free
//! function in FlatScheduler.cc rather than on this interface.
//------------------------------------------------------------------------------
struct SelectionStrategy {
  //----------------------------------------------------------------------------
  //! Select the disks holding the replicas of a new file
  //!
  //! @param cluster_data topology snapshot to place on
  //! @param args placement arguments
  //!
  //! @return placement result, convertible to false if placement failed
  //----------------------------------------------------------------------------
  virtual PlacementResult Placement(const ClusterData& cluster_data,
                                    const PlacementArgs& args) const = 0;

  //----------------------------------------------------------------------------
  //! Grow whatever per bucket state the strategy keeps so that it can serve a
  //! topology of the given size. The scheduler calls this off the snapshot it
  //! is about to descend, so a hierarchy that gained buckets since the engine
  //! was built is served rather than reported as out of range. A strategy that
  //! derives everything from the snapshot in front of it keeps the default.
  //!
  //! @param n_buckets number of buckets in the topology
  //----------------------------------------------------------------------------
  virtual void
  EnsureCapacity(size_t /*n_buckets*/)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~SelectionStrategy() = default;

  //----------------------------------------------------------------------------
  //! Check that a placement request can be served by the given bucket
  //!
  //! @param cluster_data topology snapshot to place on
  //! @param args placement arguments
  //! @param result result populated with the error if validation fails
  //!
  //! @return true if the arguments are usable, otherwise false
  //----------------------------------------------------------------------------
  bool
  ValidateArgs(const ClusterData& cluster_data, const PlacementArgs& args,
               PlacementResult& result) const
  {
    if (args.n_replicas == 0) {
      result.ret_code = EINVAL;
      result.err_msg = "Zero replicas requested";
      return false;
    }

    // GetBucket is the one validity definition: it rejects positive ids, out of
    // range ids and holes, and resolves the root for id 0.
    const Bucket* bucket = cluster_data.GetBucket(args.bucket_id);

    if (bucket == nullptr) {
      result.err_msg = "Bucket ID " + std::to_string(args.bucket_id) + " is invalid!";
      result.ret_code = ERANGE;
      return false;
    }

    if (bucket->items.size() < args.n_replicas) {
      result.err_msg =
          "Bucket " + std::to_string(bucket->id) + " does not contain enough elements!";
      result.ret_code = ENOENT;
      return false;
    }

    return true;
  }

  //----------------------------------------------------------------------------
  //! Check whether a disk may receive or serve a replica: the one predicate
  //! every selection point shares, disabled branches included.
  //!
  //! The branch rule has to be answered from the disk itself and not only on
  //! the way down: the descent refuses a denied bucket as it enters, but a
  //! scheduling group served from its flat leaf view never visits the interior
  //! buckets at all, and the access path walks no buckets whatsoever.
  //! IsBranchDenied resolves it from the disk's real geo ancestry, which the
  //! flat view leaves untouched - it only ever holds ids, it never becomes a
  //! parent. Since the rules now speak the same vocabulary as the disk's own
  //! mask, both questions are the same question and are asked here once.
  //!
  //! @param disk_id disk to check
  //! @param cluster_data topology snapshot holding the disk
  //! @param excludefs file systems not to consider
  //! @param op operation the disk has to accept
  //! @param bookingsize size the disk must still have room for, 0 to skip the
  //!        check - which is what the access path wants, an existing replica is
  //!        readable however full its disk is
  //!
  //! @return true if the disk is a candidate, otherwise false
  //!
  //! @note bounds-checked, an id outside the disks array is simply not a
  //!       candidate, so callers that treat an out-of-range id as a hard error
  //!       must test for it themselves first
  //----------------------------------------------------------------------------
  static bool
  ValidDisk(ItemIdT disk_id, const ClusterData& cluster_data,
            const std::vector<uint32_t>& excludefs, SchedOp op, uint64_t bookingsize = 0)
  {
    if (disk_id <= 0 || (size_t)disk_id > cluster_data.disks.size()) {
      return false;
    }

    if (std::find(excludefs.begin(),
                  excludefs.end(),
                  disk_id) != excludefs.end()) {
      return false;
    }

    // No rule configured is the steady state of most installations, and it
    // costs one relaxed load to establish - the ancestry walk is only ever
    // paid while a rule is actually live
    if (cluster_data.HasDeniedBranches() && cluster_data.IsBranchDenied(disk_id, op)) {
      return false;
    }

    const auto& disk = cluster_data.disks[disk_id - 1];
    auto disk_active_status = disk.active_status.load(std::memory_order_acquire);
    return disk_active_status == eos::common::ActiveStatus::kOnline &&
           disk.AllowsOp(op) && HasRoomFor(disk, bookingsize);
  }

  //----------------------------------------------------------------------------
  //! Check whether a replica may serve an access request. The checks every
  //! access implementation must agree on, in one place so that the
  //! implementations cannot drift apart on which constraints they honour.
  //!
  //! @param fsid file system holding the replica, 0 marks corrupt metadata
  //! @param cluster_data topology snapshot holding the disk
  //! @param args access arguments; a forced file system narrows the selection
  //!        down to itself but still has to pass every other check, and the
  //!        exclusion list is honoured as well
  //! @param unavailfs unavailable file systems, passed separately from the
  //!        args because FlatScheduler::Access swaps in a locally widened list
  //!
  //! @return true if the replica is a candidate, otherwise false
  //----------------------------------------------------------------------------
  static bool
  IsAccessCandidate(uint32_t fsid, const ClusterData& cluster_data,
                    const AccessArgs& args, const std::vector<uint32_t>& unavailfs)
  {
    if (fsid == 0) {
      return false;
    }

    if (args.forcedfsid && (fsid != args.forcedfsid)) {
      return false;
    }

    if (args.excludefs && (std::find(args.excludefs->begin(), args.excludefs->end(),
                                     fsid) != args.excludefs->end())) {
      return false;
    }

    // bounds, unavailability, online, the disk's own permissions and the
    // disabled branch rules are all checked here
    return ValidDisk(fsid, cluster_data, unavailfs, args.op);
  }
};

//------------------------------------------------------------------------------
//! Hash a file and file system identifier pair
//!
//! @param fid file identifier
//! @param fsid file system identifier
//! @param salt optional salt
//!
//! @return hash value
//!
//! @note uses XXH3 for its distribution and performance, with little-endian
//!       encoding so that the result is stable across platforms
//------------------------------------------------------------------------------
static inline uint64_t
HashFid(uint64_t fid, uint64_t fsid, uint64_t salt = 0)
{
  uint64_t buf[3] = {
    htole64(fid),
    htole64(fsid),
    htole64(salt)
  };
  return XXH3_64bits(buf, sizeof(buf));
}

} // namespace eos::mgm::placement
