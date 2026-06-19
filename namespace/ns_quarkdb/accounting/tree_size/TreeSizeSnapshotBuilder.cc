//------------------------------------------------------------------------------
//! @file TreeSizeSnapshotBuilder.cc
//! @brief Direct tree-size counter snapshot builder
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeSnapshotBuilder.hh"
#include <algorithm>

EOSNSNAMESPACE_BEGIN

TreeSizeSnapshot
TreeSizeSnapshotBuilder::Build(
    const std::vector<TreeSizeSnapshotContainer>& containers) const
{
  TreeSizeSnapshot snapshot;

  for (const auto& container : containers) {
    TreeSizeDirectCounters counters;
    counters.fileCount = static_cast<int64_t>(container.files.size());
    counters.childContainerCount =
        static_cast<int64_t>(container.childContainerIds.size());
    snapshot.latestSnapshotSequence = std::max(snapshot.latestSnapshotSequence,
                                               container.fileMembershipSnapshotSequence);
    snapshot.latestSnapshotSequence = std::max(snapshot.latestSnapshotSequence,
                                               container.childMembershipSnapshotSequence);

    for (const auto& file : container.files) {
      counters.fileBytes += static_cast<int64_t>(file.size);
      snapshot.fileSizeSnapshotSequences[container.id][file.id] =
          file.sizeSnapshotSequence;
      snapshot.latestSnapshotSequence =
          std::max(snapshot.latestSnapshotSequence, file.sizeSnapshotSequence);
    }

    snapshot.directCounters[container.id] = counters;
    snapshot.childContainerIds[container.id] = container.childContainerIds;
    snapshot.fileMembershipSnapshotSequences[container.id] =
        container.fileMembershipSnapshotSequence;
    snapshot.childMembershipSnapshotSequences[container.id] =
        container.childMembershipSnapshotSequence;
  }

  return snapshot;
}

EOSNSNAMESPACE_END
