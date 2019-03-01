// ----------------------------------------------------------------------
// File: OpenFileTracker.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "fst/utils/OpenFileTracker.hh"
#include <inttypes.h>
#include "common/Assert.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
OpenFileTracker::OpenFileTracker() {}

//------------------------------------------------------------------------------
// Mark that the given file ID, on the given filesystem ID, was just opened
//------------------------------------------------------------------------------
void OpenFileTracker::up(eos::common::FileSystem::fsid_t fsid, uint64_t fid) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);
  mContents[fsid][fid]++;
}

//------------------------------------------------------------------------------
// Mark that the given file ID, on the given filesystem ID, was just closed
//
// Prints warning in the logs if the value was about to go negative - it will
// never go negative.
//------------------------------------------------------------------------------
void OpenFileTracker::down(eos::common::FileSystem::fsid_t fsid, uint64_t fid) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  auto fsit = mContents.find(fsid);
  if(fsit == mContents.end()) {
    // Can happen if OpenFileTracker is misused
    eos_static_crit("Could not find fsid=%" PRIu64 " when calling OpenFileTracker::down for fid=%" PRIu64, fsid, fid);
    return;
  }

  auto fidit = fsit->second.find(fid);
  if(fidit == fsit->second.end()) {
    // Can happen if OpenFileTracker is misused
    eos_static_crit("Could not find fid=%" PRIu64 " when calling OpenFileTracker::down for fsid=%" PRIu64, fid, fsid);
    return;
  }

  if(fidit->second == 1) {
    // Last use, remove from map
    fsit->second.erase(fidit);
    return;
  }

  if(fidit->second < 1) {
    eos_static_crit("Should never happen - encountered bogus value in OpenFileTracker::down for fsid=%" PRIu64 ", fid=%" PRIu64 " - dropping", fsid, fid);
    fsit->second.erase(fidit);
    return;
  }

  // Simply decrement
  fidit->second--;
}

//------------------------------------------------------------------------------
// Checks if the given file ID, on the given filesystem ID, is currently open
//------------------------------------------------------------------------------
bool OpenFileTracker::isOpen(eos::common::FileSystem::fsid_t fsid, uint64_t fid) const {
  return getUseCount(fsid, fid) > 0;
}

//----------------------------------------------------------------------------
// Checks if the given file ID, on the given filesystem ID, is currently open
//----------------------------------------------------------------------------
int32_t OpenFileTracker::getUseCount(eos::common::FileSystem::fsid_t fsid, uint64_t fid) const {
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  auto fsit = mContents.find(fsid);
  if(fsit == mContents.end()) {
    return 0;
  }

  auto fidit = fsit->second.find(fid);
  if(fidit == fsit->second.end()) {
    return 0;
  }

  return fidit->second;
}

EOSFSTNAMESPACE_END
