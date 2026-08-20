//------------------------------------------------------------------------------
//! @file ClusterBuilder.hh
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

#pragma once
#include "mgm/placement/ClusterMgr.hh"
#include <string>
#include <vector>

namespace eos::mgm::placement {

//! Name given to the bucket collecting the disks of a scheduling group that
//! carry no geotag. It is deliberately not a legal geotag atom, so it cannot be
//! confused with a site that happens to be named the same.
constexpr std::string_view kNoGeoTagBucket = "<nogeotag>";

//------------------------------------------------------------------------------
//! Struct FsDescription - everything the topology builder needs to know about
//! one file system. It is the cut between the builder and the FsView: the MGM
//! fills these in under the view lock, the builder works on nothing else, and
//! the tests can therefore describe a cluster without an MGM behind them.
//------------------------------------------------------------------------------
struct FsDescription {
  fsid_t fsid{0};                                     ///< File system identifier
  unsigned int group_index{0};                        ///< Index of the scheduling group
  std::string geotag;                                 ///< Geotag, atoms separated by "::"
  uint64_t capacity{0};                               ///< Raw capacity in bytes
  uint64_t free_bytes{0};                             ///< Free space in bytes
  uint8_t percent_used{0};                            ///< Fill level in percent
  //! Operations this file system accepts, see common/FsOps.hh
  FsOpMask ops{eos::common::kMaskNone};
  //! Active status, already folded together with the boot status
  ActiveStatus active_status{ActiveStatus::kUndefined};
};

//------------------------------------------------------------------------------
//! Get the placement weight of a disk from its raw capacity, one unit per TB
//!
//! @param capacity raw capacity in bytes
//!
//! @return weight, at least 1 so that a small disk still takes part
//------------------------------------------------------------------------------
uint8_t CapacityToWeight(uint64_t capacity);

//------------------------------------------------------------------------------
//! Build the topology of one space and commit it to the given cluster manager.
//!
//! The hierarchy is root -> scheduling group -> geotag levels -> disks. The
//! scheduling group sits above the geotag levels because the replicas of a file
//! must all live in one group, so the group is what a placement picks first and
//! the geo levels are what it spreads over inside that choice. It also keeps
//! the group identifiers, and with them a forced group index, untouched.
//!
//! @param mgr cluster manager to commit the topology to
//! @param fs_list file systems making up the space, in any order
//------------------------------------------------------------------------------
void BuildClusterData(ClusterMgr& mgr, const std::vector<FsDescription>& fs_list);

//------------------------------------------------------------------------------
//! Add one file system to a topology under construction, creating the root,
//! group and geo buckets it needs. The single definition of the per file
//! system build step, shared by BuildClusterData and the incremental insert
//! of FsScheduler::InsertFs so the two cannot drift apart.
//!
//! @param builder snapshot builder accumulating the topology
//! @param fs description of the file system
//!
//! @return true if the disk was added, otherwise false. In particular a group
//!         whose identifier was handed to a geo bucket by an earlier build is
//!         refused - only a full rebuild, which adds every group before any
//!         geo bucket, can serve it.
//------------------------------------------------------------------------------
bool AddFsToCluster(ClusterMgr::SnapshotBuilder& builder, const FsDescription& fs);

} // namespace eos::mgm::placement
