// ----------------------------------------------------------------------
// File: OpenFileTracker.hh
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

#ifndef EOS_FST_UTILS_OPENFILETRACKER_H
#define EOS_FST_UTILS_OPENFILETRACKER_H

#include "fst/Namespace.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to track which files are open at any given moment, on a
//! filesystem-basis.
//!
//! Thread-safe. To track both "open-for-read" and "open-for-write" files,
//! use two different objects.
//------------------------------------------------------------------------------
class OpenFileTracker
{
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
  //! Wait for an excl open of a file and count up
  //----------------------------------------------------------------------------
  void waitExclOpen(eos::common::FileSystem::fsid_t fsid, uint64_t fid);

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
  //! Checks if there's _any_ operation currently in progress
  //----------------------------------------------------------------------------
  bool isAnyOpen() const;

  //----------------------------------------------------------------------------
  //! Checks if the given file ID, on the given filesystem ID, is currently open
  //----------------------------------------------------------------------------
  int32_t getUseCount(eos::common::FileSystem::fsid_t fsid, uint64_t fid) const;

  //----------------------------------------------------------------------------
  //! Get number of distinct open files by filesystem
  //----------------------------------------------------------------------------
  int32_t getOpenOnFilesystem(eos::common::FileSystem::fsid_t fsid) const;

  //----------------------------------------------------------------------------
  //! Get open file IDs of a filesystem, sorted by usecount
  //----------------------------------------------------------------------------
  std::map<size_t, std::set<uint64_t>> getSortedByUsecount(
                                      eos::common::FileSystem::fsid_t fsid) const;

  //----------------------------------------------------------------------------
  //! Get top hot files on current filesystem
  //----------------------------------------------------------------------------
  struct HotEntry {
    HotEntry(eos::common::FileSystem::fsid_t fs, uint64_t f, size_t u)
      : fsid(fs), fid(f), uses(u) {}

    HotEntry() {}

    bool operator==(const HotEntry& other) const
    {
      return fsid == other.fsid && fid == other.fid && uses == other.uses;
    }

    bool operator!=(const HotEntry& other) const
    {
      return !(*this == other);
    }

    eos::common::FileSystem::fsid_t fsid;
    uint64_t fid;
    size_t uses;
  };

  std::vector<HotEntry> getHotFiles(eos::common::FileSystem::fsid_t fsid,
                                    size_t maxEntries) const;

  //----------------------------------------------------------------------------
  //! Class acting as a barrier to avoid concurrent file creation interference
  //----------------------------------------------------------------------------
  class CreationBarrier
  {
  public:
    CreationBarrier(OpenFileTracker& tracker,
                    eos::common::FileSystem::fsid_t fsid,
                    uint64_t fid) : mTracker(tracker), mFsid(fsid), mFid(fid) , mReleased(false)
    {
      mTracker.waitExclOpen(fsid, fid);
    };

    ~CreationBarrier()
    {
      Release();
    }

    void Release()
    {
      if (!mReleased) {
        mTracker.down(mFsid, mFid);
      }

      mReleased = true;
    }
  private:
    OpenFileTracker& mTracker;
    eos::common::FileSystem::fsid_t mFsid;
    uint64_t mFid;
    bool mReleased;
  };

private:
  mutable eos::common::RWMutex mMutex;
  std::map<eos::common::FileSystem::fsid_t, std::map<uint64_t, int32_t>>
      mContents;
};

EOSFSTNAMESPACE_END

#endif
