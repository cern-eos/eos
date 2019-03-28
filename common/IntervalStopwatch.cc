// ----------------------------------------------------------------------
// File: IntervalStopwatch.cc
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

#include "common/IntervalStopwatch.hh"
#include "common/SteadyClock.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Constructor. It's possible to pass a fake clock for testing - if none is
//! passed, a default one will be used.
//!
//! The first cycle starts as soon as the constructor is called, with the
//! given duration. (zero by default)
//------------------------------------------------------------------------------
IntervalStopwatch::IntervalStopwatch(SteadyClock *clock,
  std::chrono::milliseconds initialCycleDuration) : mClock(clock) {

  startCycle(initialCycleDuration);
}

//------------------------------------------------------------------------------
// Start a cycle from this point onwards, with the given duration.
// The previously running cycle is discarded.
//------------------------------------------------------------------------------
void IntervalStopwatch::startCycle(std::chrono::milliseconds duration) {
  mCycleStart = common::SteadyClock::now(mClock);
  mCycleDuration = duration;
}

//------------------------------------------------------------------------------
// Get timepoint of cycle start
//------------------------------------------------------------------------------
std::chrono::steady_clock::time_point IntervalStopwatch::getCycleStart() const {
  return mCycleStart;
}

//------------------------------------------------------------------------------
// Return how much time has elapsed within this cycle, ie milliseconds since
// startCycle was called.
//------------------------------------------------------------------------------
std::chrono::milliseconds IntervalStopwatch::timeIntoCycle() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    common::SteadyClock::now(mClock) - mCycleStart);
}

//------------------------------------------------------------------------------
// Return how much time is remaining in this cycle. If more time has elapsed
// than the cycle duration, this function returns 0.
//------------------------------------------------------------------------------
std::chrono::milliseconds IntervalStopwatch::timeRemainingInCycle() const {
  std::chrono::milliseconds elapsed =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      common::SteadyClock::now(mClock) - mCycleStart);
  std::chrono::milliseconds remaining = mCycleDuration - elapsed;

  if(remaining < std::chrono::milliseconds(0)) {
    remaining = std::chrono::milliseconds(0);
  }

  return remaining;
}

EOSCOMMONNAMESPACE_END
