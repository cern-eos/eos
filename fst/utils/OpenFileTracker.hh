// ----------------------------------------------------------------------
// File: OpenFileCounter.hh
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

#ifndef EOS_FST_UTILS_OPENFILECOUNTER
#define EOS_FST_UTILS_OPENFILECOUNTER

#pragma once

#include "fst/Namespace.hh"
#include "common/FileSystem.hh"
#include <shared_mutex>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to track which files are open at any given moment, on a
//! filesystem-basis.
//!
//! Thread-safe.
//------------------------------------------------------------------------------
class OpenFileTracker {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  OpenFileTracker();

  //----------------------------------------------------------------------------
  //! Mark that the given file ID, on the given filesystem ID, was just opened
  //----------------------------------------------------------------------------
  void up(eos::common::FileSystem::fsid_t fsid, uint64_t fid);

  //----------------------------------------------------------------------------
  //! Mark that the given file ID, on the given filesystem ID, was just closed
  //!
  //! Prints warning in the logs if the value was about to go negative - it will
  //! never go negative.
  //----------------------------------------------------------------------------
  void down(eos::common::FileSystem::fsid_t fsid, uint64_t fid);

  //----------------------------------------------------------------------------
  //! Checks if the given file ID, on the given filesystem ID, is currently open
  //----------------------------------------------------------------------------
  bool isOpen(eos::common::FileSystem::fsid_t fsid, uint64_t fid) const;

  //----------------------------------------------------------------------------
  //! Checks if the given file ID, on the given filesystem ID, is currently open
  //----------------------------------------------------------------------------
  int32_t getUseCount(eos::common::FileSystem::fsid_t fsid, uint64_t fid) const;

private:
  mutable std::shared_timed_mutex mMutex;
  std::map<eos::common::FileSystem::fsid_t, std::map<uint64_t, int32_t>> mContents;
};

EOSFSTNAMESPACE_END

#endif