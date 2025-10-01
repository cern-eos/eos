// ----------------------------------------------------------------------
// File: SystemClock.hh
// Author: Elvin Sindrilaru - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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
#include <chrono>
#include <mutex>
#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! A clock which behaves similarly to std::chrono::system_clock, but can be
//! faked. During faking, you can advance time manually.
//------------------------------------------------------------------------------
class SystemClock
{
public:
  //----------------------------------------------------------------------------
  //! Constructor: Specify whether we're faking time, or not.
  //----------------------------------------------------------------------------
  SystemClock(bool fake_) : mFake(fake_) {}

  //----------------------------------------------------------------------------
  //! Default constructor - Sets fake to false
  //----------------------------------------------------------------------------
  SystemClock() : mFake(false) {}

  //----------------------------------------------------------------------------
  //! Static now function - it's also possible to pass a nullptr
  //----------------------------------------------------------------------------
  static std::chrono::system_clock::time_point now(SystemClock* clock)
  {
    if (clock == nullptr) {
      return std::chrono::system_clock::now();
    }

    return clock->GetTime();
  }

  //----------------------------------------------------------------------------
  //! Get current time.
  //----------------------------------------------------------------------------
  std::chrono::system_clock::time_point GetTime() const
  {
    if (mFake) {
      std::lock_guard<std::mutex> lock(mtx);
      return fakeTimepoint;
    }

    return std::chrono::system_clock::now();
  }

  //----------------------------------------------------------------------------
  //! Advance current time - only call if you're faking the clock, otherwise
  //! has no effect...
  //----------------------------------------------------------------------------
  template<typename T>
  void advance(T duration)
  {
    std::lock_guard<std::mutex> lock(mtx);
    fakeTimepoint += duration;
  }

  //----------------------------------------------------------------------------
  //! Utility function to convert a time_point to seconds since epoch
  //----------------------------------------------------------------------------
  static std::chrono::seconds SecondsSinceEpoch(
    std::chrono::system_clock::time_point point)
  {
    return std::chrono::duration_cast<std::chrono::seconds>(
             point.time_since_epoch());
  }

  //----------------------------------------------------------------------------
  //! Check if this is a "fake" clock
  //----------------------------------------------------------------------------
  inline bool IsFake() const
  {
    return mFake;
  }

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  bool mFake;
  mutable std::mutex mtx;
  std::chrono::system_clock::time_point fakeTimepoint;
};

EOSCOMMONNAMESPACE_END
