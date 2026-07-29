//------------------------------------------------------------------------------
//! @file ClusterDataFormatter.hh
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
#include <string>

//------------------------------------------------------------------------------
//! Human readable rendering of a topology snapshot, kept apart from the core
//! data types so that the hot placement path does not drag in the table
//! formatter. Free functions over the snapshot structures, declared here and
//! defined in ClusterDataFormatter.cc.
//------------------------------------------------------------------------------
namespace eos::mgm::placement {

struct Disk;
struct Bucket;
struct ClusterData;

//------------------------------------------------------------------------------
//! Get a human readable description of a disk
//!
//! @param disk disk to describe
//!
//! @return string representation
//------------------------------------------------------------------------------
std::string ToString(const Disk& disk);

//------------------------------------------------------------------------------
//! Get a human readable description of a bucket
//!
//! @param bucket bucket to describe
//!
//! @return string representation
//------------------------------------------------------------------------------
std::string ToString(const Bucket& bucket);

//------------------------------------------------------------------------------
//! Get the disk list of a snapshot as a formatted table with the columns
//! fsid | config | active | weight | used% | free | booked | geotag
//!
//! @param data topology snapshot to render
//!
//! @return string representation of the table
//------------------------------------------------------------------------------
std::string GetDisksAsString(const ClusterData& data);

//------------------------------------------------------------------------------
//! Get the bucket hierarchy of a snapshot as a formatted table with the columns
//! type | id | parent | level | group | geotag | disabled | weight | count |
//! items
//!
//! @param data topology snapshot to render
//!
//! @return string representation of the table
//------------------------------------------------------------------------------
std::string GetBucketsAsString(const ClusterData& data);

} // namespace eos::mgm::placement
