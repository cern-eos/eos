//------------------------------------------------------------------------------
//! @file TreeSizeSnapshotBuilder.hh
//! @brief Direct tree-size counter snapshots for recomputation reconciliation
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

#pragma once

#include "namespace/Namespace.hh"
#include <cstdint>
#include <unordered_map>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Direct counters for one container, excluding descendant aggregation
//------------------------------------------------------------------------------
struct TreeSizeDirectCounters {
  int64_t fileBytes = 0;
  int64_t fileCount = 0;
  int64_t childContainerCount = 0;
};

//------------------------------------------------------------------------------
//! File metadata needed to derive direct counters
//------------------------------------------------------------------------------
struct TreeSizeSnapshotFile {
  uint64_t id = 0;
  uint64_t size = 0;
  uint64_t sizeSnapshotSequence = 0;
};

//------------------------------------------------------------------------------
//! Container metadata needed to derive direct counters
//------------------------------------------------------------------------------
struct TreeSizeSnapshotContainer {
  uint64_t id = 0;
  uint64_t fileMembershipSnapshotSequence = 0;
  uint64_t childMembershipSnapshotSequence = 0;
  std::vector<TreeSizeSnapshotFile> files;
  std::vector<uint64_t> childContainerIds;
};

//------------------------------------------------------------------------------
//! Direct-counter snapshot and discovered child topology keyed by container id
//------------------------------------------------------------------------------
struct TreeSizeSnapshot {
  std::unordered_map<uint64_t, TreeSizeDirectCounters> directCounters;
  std::unordered_map<uint64_t, std::vector<uint64_t>> childContainerIds;
  std::unordered_map<uint64_t, uint64_t> fileMembershipSnapshotSequences;
  std::unordered_map<uint64_t, uint64_t> childMembershipSnapshotSequences;
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t>>
      fileSizeSnapshotSequences;
  uint64_t latestSnapshotSequence = 0;
};

//------------------------------------------------------------------------------
//! Builds direct tree-size counters from discovered container contents
//------------------------------------------------------------------------------
class TreeSizeSnapshotBuilder {
public:
  TreeSizeSnapshot Build(const std::vector<TreeSizeSnapshotContainer>& containers) const;
};

EOSNSNAMESPACE_END
