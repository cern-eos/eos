//------------------------------------------------------------------------------
// File: DrainProgressTracker.hh
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

#include <map>
#include <mutex>
#include <string>
#include "common/FileSystem.hh"

namespace eos::mgm {

//! A simple progress tracker that just tracks the total files on a FS and
//! has a simple increment method to increase the curr. files being drained
//! This method is thread safe to be called from multiple threads, and holds
//! 2 individual mutexes to the 2 internal maps for tracking total and current
//! file counters
class DrainProgressTracker
{
public:
  using fsid_t = eos::common::FileSystem::fsid_t;

  //! Set Total amount of files in a FSID
  //! This method only stores the value if we find that
  //! the value is greater than the one stored, so is safe to call multiple times
  //! in the course of a drain
  //! \param fsid the Filesystem ID
  //! \param total_files Num Files on FS
  void setTotalFiles(fsid_t fsid, uint64_t total_files);

  //! Increment the counter of drained files
  //! \param fsid FSID from which the drain was scheduled
  void increment(fsid_t fsid);

  //! Drop all associated values from a FSID, this will no longer be tracked
  void dropFsid(fsid_t fsid);

  //! Clear all the internal entries!
  void clear();

  //! Get the percentage completion of a drain
  //! \param fsid the FSID
  //! \return a float percent value of curr_files_drained/total_files
  float getDrainStatus(fsid_t fsid) const;

  //! Get the total files for a FSID
  uint64_t getTotalFiles(fsid_t fsid) const;

  //! Get the current value of File Counter. may exceed total files considering
  //! failures
  uint64_t getFileCounter(fsid_t fsid) const;
private:
  mutable std::mutex mFsTotalFilesMtx;
  mutable std::mutex mFsScheduledCtrMtx;
  std::map<fsid_t, uint64_t> mFsTotalfiles;
  std::map<fsid_t, uint64_t> mFsScheduledCounter;
};


} // namespace eos::mgm
