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

//------------------------------------------------------------------------------
//! @brief Class tracking entries that were used by different
//! sub-systems during a reference period. e.g. draining / balancing
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------
#pragma once
#include "mgm/Namespace.hh"
#include "common/SteadyClock.hh"
#include <mutex>
#include <map>

EOSMGMNAMESPACE_BEGIN

//! Type of trackers
enum class TrackerType {
  All, Balance, Convert, Drain, Fsck
};

//------------------------------------------------------------------------------
//! Class IdTrackerWithValidity
//------------------------------------------------------------------------------
template <typename EntryT>
class IdTrackerWithValidity
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param clean_interval minimum interval after which a clean up of expired
  //!        entries is attempted
  //! @param entry_validity duration for which an entry is considered still
  //!        valid and not removed from the map
  //! @param fake_clock if true use synthetic clock for testing
  //----------------------------------------------------------------------------
  IdTrackerWithValidity(std::chrono::seconds clean_interval,
                        std::chrono::seconds entry_validity,
                        bool fake_clock = false):
    mCleanupInterval(clean_interval),
    mEntryValidity(entry_validity),
    mClock(fake_clock)
  {
    mCleanupTimestamp = eos::common::SteadyClock::now(&mClock) + mCleanupInterval;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~IdTrackerWithValidity() = default;

  //----------------------------------------------------------------------------
  //! Add entry with expiration
  //!
  //! @param entry
  //! @param tt tracker type
  //! @param validity validity for the newly added entry. If 0 then default
  //!         validity applies.
  //!
  //! @return true if entry added, otherwise false
  //----------------------------------------------------------------------------
  bool AddEntry(EntryT entry, TrackerType tt, std::chrono::seconds validity =
                  std::chrono::seconds::zero());

  //----------------------------------------------------------------------------
  //! Check if entry is already tracked
  //!
  //! @param entry
  //!
  //! @return true if entry in the map, otherwise false
  //----------------------------------------------------------------------------
  bool HasEntry(EntryT entry) const;

  //----------------------------------------------------------------------------
  //! Remove entry
  //----------------------------------------------------------------------------
  void RemoveEntry(EntryT entry);

  //----------------------------------------------------------------------------
  //! Clean up expired entries
  //!
  //! @param tt tracker type
  //----------------------------------------------------------------------------
  void DoCleanup(TrackerType tt);

  //----------------------------------------------------------------------------
  //! Clear all tracked entries
  //----------------------------------------------------------------------------
  void Clear(TrackerType tt)
  {
    std::unique_lock<std::mutex> lock(mMutex);

    if (tt == TrackerType::All) {
      mMap.clear();
    } else {
      auto it_tracker = mMap.find(tt);

      if (it_tracker != mMap.end()) {
        it_tracker->second.clear();
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Get clock reference for testing purposes
  //----------------------------------------------------------------------------
  inline eos::common::SteadyClock& GetClock()
  {
    return mClock;
  }

  //----------------------------------------------------------------------------
  //! Get statistics about the tracked files
  //!
  //! @param full if true print also the ids for each tracker
  //! @param monitor if true print info in monitor format
  //!
  //! @return string with the required information
  //----------------------------------------------------------------------------
  std::string PrintStats(bool full = false, bool monitor = false) const;

private:
  mutable std::mutex mMutex;
  std::map<TrackerType,
      std::map<EntryT, std::chrono::steady_clock::time_point>> mMap;
  //! Next cleanup timestamp
  std::chrono::steady_clock::time_point mCleanupTimestamp;
  std::chrono::seconds mCleanupInterval; ///< Interval when cleanup is performed
  std::chrono::seconds mEntryValidity; ///< Entry validity duration
  eos::common::SteadyClock mClock; ///< Clock wrapper also used for testing
};

//------------------------------------------------------------------------------
// Add entry with expiration
//------------------------------------------------------------------------------
template<typename EntryT>
bool
IdTrackerWithValidity<EntryT>::AddEntry(EntryT entry, TrackerType tt,
                                        std::chrono::seconds validity)
{
  if (tt == TrackerType::All) {
    return false;
  }

  std::unique_lock<std::mutex> lock(mMutex);

  // Check if entry already exists in any of the trackers
  for (const auto& pair : mMap) {
    if (pair.second.find(entry) != pair.second.end()) {
      return false;
    }
  }

  auto& tracker_map = mMap[tt]; // will create it if missing

  if (validity.count()) {
    tracker_map[entry] = eos::common::SteadyClock::now(&mClock) + validity;
  } else {
    tracker_map[entry] = eos::common::SteadyClock::now(&mClock) + mEntryValidity;
  }

  return true;
}

//------------------------------------------------------------------------------
// Clean up expired entries
//------------------------------------------------------------------------------
template<typename EntryT>
void
IdTrackerWithValidity<EntryT>::DoCleanup(TrackerType tt)
{
  using namespace std::chrono;
  auto now = eos::common::SteadyClock::now(&mClock);
  std::unique_lock<std::mutex> lock(mMutex);

  if (mCleanupTimestamp < now) {
    mCleanupTimestamp = now + mCleanupInterval;

    for (auto& pair : mMap) {
      if ((tt != TrackerType::All) && (pair.first != tt)) {
        continue;
      }

      auto& tracker_map = pair.second;

      for (auto it = tracker_map.begin(); it != tracker_map.end(); /*empty*/) {
        if (it->second < now) {
          auto it_del = it++;
          tracker_map.erase(it_del);
        } else {
          ++it;
        }
      }
    }
  }
}

//----------------------------------------------------------------------------
// Remove entry
//----------------------------------------------------------------------------
template<typename EntryT>
void
IdTrackerWithValidity<EntryT>::RemoveEntry(EntryT entry)
{
  std::unique_lock<std::mutex> lock(mMutex);

  for (auto& pair : mMap) {
    auto& tracker_map = pair.second;
    auto it = tracker_map.find(entry);

    if (it != tracker_map.end()) {
      tracker_map.erase(it);
      break;
    }
  }
}

//------------------------------------------------------------------------------
// Check if entry is already tracked
//------------------------------------------------------------------------------
template<typename EntryT>
bool
IdTrackerWithValidity<EntryT>::HasEntry(EntryT entry) const
{
  std::unique_lock<std::mutex> lock(mMutex);

  for (const auto& pair : mMap) {
    if (pair.second.find(entry) != pair.second.end()) {
      return true;
    }
  }

  return false;
}

//----------------------------------------------------------------------------
//! Get statistics about the tracked files
//----------------------------------------------------------------------------
template<typename EntryT>
std::string
IdTrackerWithValidity<EntryT>::PrintStats(bool full, bool monitor) const
{
  auto get_tracker_name = [](TrackerType tt) -> std::string {
    if (tt == TrackerType::Drain)
    {
      return "drain";
    } else if (tt == TrackerType::Balance)
    {
      return "balance";
    } else if (tt == TrackerType::Convert)
    {
      return "convert";
    } else if (tt == TrackerType::Fsck)
    {
      return "fsck";
    } else {
      return "unknown";
    }
  };
  std::ostringstream oss;
  std::unique_lock<std::mutex> lock(mMutex);

  for (const auto& pair : mMap) {
    if (monitor) {
      oss << "uid=all gid=all ";
    } else {
      oss << "ALL      tracker info                     ";
    }

    oss << "tracker=" << get_tracker_name(pair.first)
        << " size=" << pair.second.size();

    if (full) {
      oss << " ids=";

      for (const auto& elem : pair.second) {
        oss << elem.first << " ";
      }
    }

    oss << std::endl;
  }

  if (mMap.empty()) {
    oss << std::endl;
  }

  return oss.str();
}

EOSMGMNAMESPACE_END
