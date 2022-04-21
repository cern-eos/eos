//------------------------------------------------------------------------------
// File: FileSystemStatusUtils.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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
#include <vector>

namespace eos::mgm::fsutils {

/*!
 * Set the filesystem as drained, and if we're not shutting down, to empty as well
 * @param fsid the FSID to set the drained status to
 */
void ApplyDrainedStatus(eos::common::FileSystem::fsid_t fsid);

/*!
 * Set the filesystem to  drain fail
 * @param fsid the FSID for setting failed drain
 * @param numFailedJobs number of failed file entries
 */
void ApplyFailedDrainStatus(eos::common::FileSystem::fsid_t fsid,
                            uint64_t numFailedJobs);

/*!
 * Get a list of Filesystems with the supplied active/drain status
 * @param groupname
 * @param active_status defaults to Online
 * @param drain_status defaults to No Drain
 * @return a vector a filesystem IDs
 */
std::vector<eos::common::FileSystem::fsid_t>
FsidsinGroup(const std::string& groupname,
             eos::common::ActiveStatus active_status = eos::common::ActiveStatus::kOnline,
             eos::common::DrainStatus drain_status = eos::common::DrainStatus::kNoDrain);



} // eos::mgm::fsutils
