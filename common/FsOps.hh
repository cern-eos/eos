//------------------------------------------------------------------------------
// File: FsOps.hh
// Author: Elvin Sindrilaru - CERN
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

#ifndef EOS_COMMON_FS_OPS_HH
#define EOS_COMMON_FS_OPS_HH

#include "common/Namespace.hh"
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

EOSCOMMONNAMESPACE_BEGIN

//! Opaque declaration, the definition lives in common/FileSystem.hh. Declared
//! rather than included so that this header stays free of the shared-hash
//! machinery and FileSystem.hh can depend on it without a cycle.
enum class ConfigStatus : int8_t;

//------------------------------------------------------------------------------
//! Which class of traffic a scheduling request belongs to. Deliberately just
//! two: the per-subsystem distinction (drain vs balance vs conversion vs fsck)
//! buys nothing a filesystem's configuration needs to act on.
//------------------------------------------------------------------------------
enum class SchedActivity : uint8_t {
  kClient = 0, ///< user traffic: xrootd, FUSE, http, gRPC
  kInternal    ///< drain, balance, conversion, fsck
};

//------------------------------------------------------------------------------
//! What a scheduling request wants to do with a replica. Maps 1:1 onto how the
//! MGM already routes: Access(isRW=false), Access(isRW=true) and Placement.
//!
//! kUpdate and kCreate are separate because "stop this filesystem growing but
//! keep serving and updating what is already there" is a real state - it is
//! what a placement-disabled geotag branch has always meant - and a single
//! write bit cannot express it.
//------------------------------------------------------------------------------
enum class SchedDirection : uint8_t {
  kRead = 0, ///< serve an existing replica
  kUpdate,   ///< write to an existing replica
  kCreate    ///< place a new replica here
};

//! Number of directions, the stride of the mask layout
static constexpr uint8_t kNumSchedDirections = 3;
//! Number of distinct operations, i.e. bits in an FsOpMask
static constexpr uint8_t kNumSchedOps = 6;

//------------------------------------------------------------------------------
//! One scheduling operation: which class of traffic, doing what
//------------------------------------------------------------------------------
struct SchedOp {
  SchedActivity activity;
  SchedDirection direction;

  //----------------------------------------------------------------------------
  //! Position of this operation in an FsOpMask, in [0, kNumSchedOps)
  //----------------------------------------------------------------------------
  constexpr uint8_t
  Index() const
  {
    return static_cast<uint8_t>(static_cast<uint8_t>(activity) * kNumSchedDirections +
                                static_cast<uint8_t>(direction));
  }

  //----------------------------------------------------------------------------
  //! This operation as a single set bit
  //----------------------------------------------------------------------------
  constexpr uint8_t
  Bit() const
  {
    return static_cast<uint8_t>(1u << Index());
  }

  constexpr bool
  operator==(const SchedOp& other) const
  {
    return (activity == other.activity) && (direction == other.direction);
  }
};

//------------------------------------------------------------------------------
//! Set of operations a filesystem accepts, one bit per SchedOp. Six bits used,
//! two spare - which is what lets it sit in the byte ConfigStatus used to
//! occupy in the scheduler's per-disk snapshot without growing it.
//------------------------------------------------------------------------------
using FsOpMask = uint8_t;

//------------------------------------------------------------------------------
//! The six operations, named. What a call site that asks about one specific
//! operation uses instead of assembling it from its parts.
//------------------------------------------------------------------------------
//! Serving an existing replica to a client, what a plain read asks for
inline constexpr SchedOp kClientRead{SchedActivity::kClient, SchedDirection::kRead};
//! Writing to an existing replica for a client
inline constexpr SchedOp kClientUpdate{SchedActivity::kClient, SchedDirection::kUpdate};
//! Placing a new replica for a client, the default a plain placement asks for
inline constexpr SchedOp kClientCreate{SchedActivity::kClient, SchedDirection::kCreate};
//! Reading an existing replica for an internal engine, the drain source pick
inline constexpr SchedOp kInternalRead{SchedActivity::kInternal, SchedDirection::kRead};
//! Writing to an existing replica for an internal engine
inline constexpr SchedOp kInternalUpdate{SchedActivity::kInternal,
                                         SchedDirection::kUpdate};
//! Placing a new replica for an internal engine - drain, balance, conversion
inline constexpr SchedOp kInternalCreate{SchedActivity::kInternal,
                                         SchedDirection::kCreate};

//! Opaque key a trusted caller uses to declare itself internal traffic, and the
//! only value it accepts. Anything else, including its absence, is client.
static constexpr std::string_view kSchedClassKey = "eos.schedclass";
static constexpr std::string_view kSchedClassInternal = "internal";

//------------------------------------------------------------------------------
//! Mask helpers
//------------------------------------------------------------------------------
constexpr FsOpMask
MaskOf(SchedOp op)
{
  return op.Bit();
}

//! All operations of one traffic class
constexpr FsOpMask
MaskOfActivity(SchedActivity activity)
{
  return static_cast<FsOpMask>((SchedOp{activity, SchedDirection::kRead}).Bit() |
                               (SchedOp{activity, SchedDirection::kUpdate}).Bit() |
                               (SchedOp{activity, SchedDirection::kCreate}).Bit());
}

//! One direction across both traffic classes
constexpr FsOpMask
MaskOfDirection(SchedDirection direction)
{
  return static_cast<FsOpMask>((SchedOp{SchedActivity::kClient, direction}).Bit() |
                               (SchedOp{SchedActivity::kInternal, direction}).Bit());
}

static constexpr FsOpMask kMaskNone = 0;
//! Everything one traffic class may ask for
static constexpr FsOpMask kMaskClientAll = MaskOfActivity(SchedActivity::kClient);
static constexpr FsOpMask kMaskInternalAll = MaskOfActivity(SchedActivity::kInternal);
//! One direction, both traffic classes
static constexpr FsOpMask kMaskAllReads = MaskOfDirection(SchedDirection::kRead);
static constexpr FsOpMask kMaskAllUpdates = MaskOfDirection(SchedDirection::kUpdate);
static constexpr FsOpMask kMaskAllCreates = MaskOfDirection(SchedDirection::kCreate);
static constexpr FsOpMask kMaskAll =
    static_cast<FsOpMask>(kMaskClientAll | kMaskInternalAll);

//----------------------------------------------------------------------------
//! Check whether a mask allows one operation
//----------------------------------------------------------------------------
constexpr bool
AllowsOp(FsOpMask mask, SchedOp op)
{
  return (mask & op.Bit()) != 0;
}

//----------------------------------------------------------------------------
//! Check whether a mask allows any operation of the given direction, i.e.
//! regardless of which class of traffic asks for it
//----------------------------------------------------------------------------
constexpr bool
AllowsAnyOfDirection(FsOpMask mask, SchedDirection direction)
{
  return (mask & MaskOfDirection(direction)) != 0;
}

//----------------------------------------------------------------------------
//! Check whether a mask allows any read at all. A filesystem that allows none
//! can serve nothing, which is what makes a replica on it count as offline.
//----------------------------------------------------------------------------
constexpr bool
AllowsAnyRead(FsOpMask mask)
{
  return AllowsAnyOfDirection(mask, SchedDirection::kRead);
}

//----------------------------------------------------------------------------
//! Check whether a mask allows any write, i.e. an update or a new replica
//----------------------------------------------------------------------------
constexpr bool
AllowsAnyWrite(FsOpMask mask)
{
  return AllowsAnyOfDirection(mask, SchedDirection::kUpdate) ||
         AllowsAnyOfDirection(mask, SchedDirection::kCreate);
}

//----------------------------------------------------------------------------
//! Check whether a mask allows any operation of the given traffic class
//----------------------------------------------------------------------------
constexpr bool
AllowsAnyOfActivity(FsOpMask mask, SchedActivity activity)
{
  return (mask & MaskOfActivity(activity)) != 0;
}

//----------------------------------------------------------------------------
//! Check whether a mask still admits client traffic of any kind. What a call
//! site asking "is this file system still in service for users" wants.
//----------------------------------------------------------------------------
constexpr bool
AllowsAnyClient(FsOpMask mask)
{
  return AllowsAnyOfActivity(mask, SchedActivity::kClient);
}

//----------------------------------------------------------------------------
//! Check whether a mask still admits internal traffic of any kind
//----------------------------------------------------------------------------
constexpr bool
AllowsAnyInternal(FsOpMask mask)
{
  return AllowsAnyOfActivity(mask, SchedActivity::kInternal);
}

//----------------------------------------------------------------------------
//! Parse the explicit form of an operation set: a comma separated list of
//! per-class terms ("client:r,internal:ruc"), where the direction letters are
//! 'r' read, 'u' update, 'c' create and 'w' as a convenience alias for "uc".
//! A class that is not mentioned contributes no bits.
//!
//! Kept apart from ParseSchedSpec because the preset names only make sense for
//! a filesystem's allowed set: a disabled-branch rule holds the operations it
//! *denies*, where reading "ro" as "read only" is exactly backwards.
//!
//! @param spec specification string
//!
//! @return the mask, or nullopt if the spec is malformed
//----------------------------------------------------------------------------
std::optional<FsOpMask> ParseSchedOps(std::string_view spec);

//----------------------------------------------------------------------------
//! Render an operation set in the explicit form, "none" for an empty mask.
//! The counterpart of ParseSchedOps, and what a caller wants whenever the
//! preset names would name the wrong thing.
//----------------------------------------------------------------------------
std::string FormatSchedOps(FsOpMask mask);

//----------------------------------------------------------------------------
//! Parse a scheduling specification into a mask. Accepts either a preset name
//! ("rw", "internal", ...) or the explicit form, see ParseSchedOps.
//!
//! @param spec specification string
//!
//! @return the mask, or nullopt if the spec is malformed
//----------------------------------------------------------------------------
std::optional<FsOpMask> ParseSchedSpec(std::string_view spec);

//----------------------------------------------------------------------------
//! Render a mask. Returns the preset name when the mask is exactly one of the
//! presets, otherwise the explicit form.
//----------------------------------------------------------------------------
std::string FormatSchedMask(FsOpMask mask);

//----------------------------------------------------------------------------
//! Translate a legacy ConfigStatus into a mask. Every one of the six statuses
//! has an exact equivalent; the values that did not - "draindead",
//! "groupdrain" and "unknown" - no longer parse at all, so a caller reading a
//! stored legacy value fences the filesystem off at the parse instead.
//!
//! @param status legacy configuration status
//!
//! @return the equivalent mask
//----------------------------------------------------------------------------
FsOpMask DeriveMaskFromLegacy(ConfigStatus status);

//----------------------------------------------------------------------------
//! Project a mask back onto the legacy ConfigStatus that is still published
//! for the FST, the geotree engine, the capacity sums and monitoring.
//!
//! Reads the client bits first and falls back to kDrain: a mask that allows
//! only internal traffic has no legacy equivalent, and answering kOff there
//! would stop the FST booting the filesystem at all.
//!
//! @param mask permission mask
//! @param lifecycle_empty whether the filesystem is marked empty
//!
//! @return one of kRW, kWO, kRO, kDrain, kEmpty or kOff
//----------------------------------------------------------------------------
ConfigStatus DeriveLegacyConfigStatus(FsOpMask mask, bool lifecycle_empty);

EOSCOMMONNAMESPACE_END

#endif // EOS_COMMON_FS_OPS_HH
