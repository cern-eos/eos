//------------------------------------------------------------------------------
// File: FsOps.cc
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

#include "common/FsOps.hh"
#include "common/FileSystem.hh"
#include <array>
#include <utility>

EOSCOMMONNAMESPACE_BEGIN

namespace {
constexpr FsOpMask kMaskClientAll = MaskOfActivity(SchedActivity::kClient);
constexpr FsOpMask kMaskInternalAll = MaskOfActivity(SchedActivity::kInternal);
constexpr FsOpMask kMaskAllReads = MaskOfDirection(SchedDirection::kRead);
constexpr FsOpMask kMaskAllUpdates = MaskOfDirection(SchedDirection::kUpdate);
constexpr FsOpMask kMaskAllCreates = MaskOfDirection(SchedDirection::kCreate);

constexpr FsOpMask kMaskClientRead =
    (SchedOp{SchedActivity::kClient, SchedDirection::kRead}).Bit();

//------------------------------------------------------------------------------
//! Named presets, in preference order: FormatSchedMask returns the first entry
//! whose mask matches, so "ro" wins over the identically shaped "drain". The
//! two differ only in whether draining is requested, which is a separate key.
//------------------------------------------------------------------------------
constexpr std::array<std::pair<std::string_view, FsOpMask>, 7> kPresets{
    {{"rw", kMaskAll},
     {"ro", kMaskAllReads},
     {"wo", static_cast<FsOpMask>(kMaskAllUpdates | kMaskAllCreates)},
     {"none", kMaskNone},
     {"internal", kMaskInternalAll},
     {"clientro", static_cast<FsOpMask>(kMaskClientRead | kMaskInternalAll)},
     {"drain", kMaskAllReads}}};

//------------------------------------------------------------------------------
//! Translate the direction letters of one term into a mask for that class
//!
//! @param activity traffic class the term names
//! @param letters direction letters, 'r' 'u' 'c' or 'w' for both writes
//! @param out mask to OR the result into
//!
//! @return true if every letter was recognised, otherwise false
//------------------------------------------------------------------------------
bool
LettersToMask(SchedActivity activity, std::string_view letters, FsOpMask& out)
{
  if (letters.empty()) {
    return false;
  }

  FsOpMask mask = kMaskNone;

  for (const char letter : letters) {
    switch (letter) {
    case 'r':
      mask |= (SchedOp{activity, SchedDirection::kRead}).Bit();
      break;

    case 'u':
      mask |= (SchedOp{activity, SchedDirection::kUpdate}).Bit();
      break;

    case 'c':
      mask |= (SchedOp{activity, SchedDirection::kCreate}).Bit();
      break;

    case 'w':
      mask |= (SchedOp{activity, SchedDirection::kUpdate}).Bit();
      mask |= (SchedOp{activity, SchedDirection::kCreate}).Bit();
      break;

    default:
      return false;
    }
  }

  out |= mask;
  return true;
}

//------------------------------------------------------------------------------
//! Render the direction letters a mask sets for one traffic class
//------------------------------------------------------------------------------
std::string
MaskToLetters(FsOpMask mask, SchedActivity activity)
{
  std::string letters;

  if (AllowsOp(mask, SchedOp{activity, SchedDirection::kRead})) {
    letters += 'r';
  }

  if (AllowsOp(mask, SchedOp{activity, SchedDirection::kUpdate})) {
    letters += 'u';
  }

  if (AllowsOp(mask, SchedOp{activity, SchedDirection::kCreate})) {
    letters += 'c';
  }

  return letters;
}
} // namespace

//------------------------------------------------------------------------------
// Parse the explicit form of an operation set
//------------------------------------------------------------------------------
std::optional<FsOpMask>
ParseSchedOps(std::string_view spec)
{
  if (spec.empty()) {
    return std::nullopt;
  }

  FsOpMask result = kMaskNone;
  size_t pos = 0;

  while (pos <= spec.size()) {
    const size_t comma = spec.find(',', pos);
    const std::string_view term = spec.substr(
        pos, (comma == std::string_view::npos) ? std::string_view::npos : comma - pos);
    const size_t colon = term.find(':');

    if (colon == std::string_view::npos) {
      return std::nullopt;
    }

    const std::string_view class_name = term.substr(0, colon);
    const std::string_view letters = term.substr(colon + 1);
    SchedActivity activity;

    if (class_name == "client") {
      activity = SchedActivity::kClient;
    } else if (class_name == "internal") {
      activity = SchedActivity::kInternal;
    } else {
      return std::nullopt;
    }

    if (!LettersToMask(activity, letters, result)) {
      return std::nullopt;
    }

    if (comma == std::string_view::npos) {
      break;
    }

    pos = comma + 1;
  }

  return result;
}

//------------------------------------------------------------------------------
// Parse a scheduling specification into a mask
//------------------------------------------------------------------------------
std::optional<FsOpMask>
ParseSchedSpec(std::string_view spec)
{
  for (const auto& [name, mask] : kPresets) {
    if (spec == name) {
      return mask;
    }
  }

  return ParseSchedOps(spec);
}

//------------------------------------------------------------------------------
// Render an operation set in the explicit form
//------------------------------------------------------------------------------
std::string
FormatSchedOps(FsOpMask mask)
{
  mask &= kMaskAll;

  if (mask == kMaskNone) {
    return "none";
  }

  const std::string client = MaskToLetters(mask, SchedActivity::kClient);
  const std::string internal = MaskToLetters(mask, SchedActivity::kInternal);
  std::string out;

  if (!client.empty()) {
    out += "client:";
    out += client;
  }

  if (!internal.empty()) {
    if (!out.empty()) {
      out += ',';
    }

    out += "internal:";
    out += internal;
  }

  return out;
}

//------------------------------------------------------------------------------
// Render a mask
//------------------------------------------------------------------------------
std::string
FormatSchedMask(FsOpMask mask)
{
  mask &= kMaskAll;

  for (const auto& [name, preset] : kPresets) {
    if (mask == preset) {
      return std::string(name);
    }
  }

  return FormatSchedOps(mask);
}

//------------------------------------------------------------------------------
// Translate a legacy ConfigStatus into a mask
//------------------------------------------------------------------------------
std::optional<FsOpMask>
DeriveMaskFromLegacy(ConfigStatus status)
{
  switch (status) {
  case ConfigStatus::kRW:
    return kMaskAll;

  case ConfigStatus::kRO:
  case ConfigStatus::kDrain:
    return kMaskAllReads;

  case ConfigStatus::kWO:
    return static_cast<FsOpMask>(kMaskAllUpdates | kMaskAllCreates);

  case ConfigStatus::kEmpty:
  case ConfigStatus::kOff:
    return kMaskNone;

  default:
    // A status with no successor. The caller treats this as unset and fences
    // the filesystem off rather than guessing at an intent that no longer
    // exists in the model.
    return std::nullopt;
  }
}

//------------------------------------------------------------------------------
// Project a mask back onto the legacy ConfigStatus
//------------------------------------------------------------------------------
ConfigStatus
DeriveLegacyConfigStatus(FsOpMask mask, bool lifecycle_empty)
{
  const bool client_read =
      AllowsOp(mask, SchedOp{SchedActivity::kClient, SchedDirection::kRead});
  const bool client_write =
      AllowsOp(mask, SchedOp{SchedActivity::kClient, SchedDirection::kUpdate}) ||
      AllowsOp(mask, SchedOp{SchedActivity::kClient, SchedDirection::kCreate});

  if (client_write) {
    return client_read ? ConfigStatus::kRW : ConfigStatus::kWO;
  }

  if (client_read) {
    return ConfigStatus::kRO;
  }

  if ((mask & kMaskInternalAll) != 0) {
    // No legacy value means "internal traffic only", and kOff would stop the
    // FST booting the filesystem, so kDrain is the honest projection: booted
    // and operational, but out of the client capacity sums.
    return ConfigStatus::kDrain;
  }

  return lifecycle_empty ? ConfigStatus::kEmpty : ConfigStatus::kOff;
}

EOSCOMMONNAMESPACE_END
