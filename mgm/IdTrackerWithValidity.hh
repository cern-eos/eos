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
//! @author Elvin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------
#pragma once
#include "mgm/Namespace.hh"
#include "common/RWMutex.hh"
#include "common/SteadyClock.hh"
#include <mutex>
#include <map>

EOSMGMNAMESPACE_BEGIN

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
  //! @param validity validity for the newly added entry. If 0 then default
  //!         validity applies.
  //----------------------------------------------------------------------------
  void AddEntry(EntryT entry, std::chrono::seconds validity =
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
  //----------------------------------------------------------------------------
  void DoCleanup();

  //----------------------------------------------------------------------------
  //! Clear all tracked entries
  //----------------------------------------------------------------------------
  void Clear()
  {
    eos::common::RWMutexWriteLock wr_lock(mRWMutex);
    mMap.clear();
  }

  //----------------------------------------------------------------------------
  //! Get clock reference for testing purposes
  //----------------------------------------------------------------------------
  inline eos::common::SteadyClock& GetClock()
  {
    return mClock;
  }

private:
  mutable eos::common::RWMutex mRWMutex;
  std::map<EntryT, std::chrono::steady_clock::time_point> mMap;
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
void
IdTrackerWithValidity<EntryT>::AddEntry(EntryT entry,
                                        std::chrono::seconds validity)
{
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);

  if (validity.count()) {
    mMap[entry] = eos::common::SteadyClock::now(&mClock) + validity;
  } else {
    mMap[entry] = eos::common::SteadyClock::now(&mClock) + mEntryValidity;
  }
}

//------------------------------------------------------------------------------
// Check if entry is already tracked
//------------------------------------------------------------------------------
template<typename EntryT>
bool
IdTrackerWithValidity<EntryT>::HasEntry(EntryT entry) const
{
  eos::common::RWMutexReadLock rd_lock(mRWMutex);
  return (mMap.find(entry) != mMap.end());
}

//------------------------------------------------------------------------------
// Clean up expired entries
//------------------------------------------------------------------------------
template<typename EntryT>
void
IdTrackerWithValidity<EntryT>::DoCleanup()
{
  using namespace std::chrono;
  auto now = eos::common::SteadyClock::now(&mClock);
  eos::common::RWMutexReadLock rd_lock(mRWMutex);

  if (mCleanupTimestamp < now) {
    rd_lock.Release();
    eos::common::RWMutexWriteLock wr_lock(mRWMutex);
    mCleanupTimestamp = now + mCleanupInterval;

    for (auto it = mMap.begin(); it != mMap.end(); /*empty*/) {
      if (it->second < now) {
        auto it_del = it++;
        mMap.erase(it_del);
      } else {
        ++it;
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
  eos::common::RWMutexWriteLock wr_lock(mRWMutex);
  auto it = mMap.find(entry);

  if (it != mMap.end()) {
    mMap.erase(it);
  }
}

EOSMGMNAMESPACE_END
