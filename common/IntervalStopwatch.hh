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
// author: Georgios Bitzes <georgios.bitzes@cern.ch>
// desc:   A stopwatch which measures duration of cyclic events
//------------------------------------------------------------------------------

#pragma once
#include "common/Namespace.hh"
#include <chrono>

EOSCOMMONNAMESPACE_BEGIN

class SteadyClock;

//------------------------------------------------------------------------------
//! We often have the following pattern for background threads:
//! - Start of event
//! - ....
//! - End of event
//! - Sleep
//!
//! We want "event" to happen every 1 hour, for example. If the duration of
//! "event" was 10 minutes, we would want to sleep for 50 miinutes before
//! starting the cycle once again. This class simplifies the above pattern,
//! essentially telling you for how long to sleep.
//------------------------------------------------------------------------------
class IntervalStopwatch {
public:
  //----------------------------------------------------------------------------
  //! Constructor. It's possible to pass a fake clock for testing - if none is
  //! passed, a default one will be used.
  //!
  //! The first cycle starts as soon as the constructor is called, with the
  //! given duration. (zero by default)
  //----------------------------------------------------------------------------
  IntervalStopwatch(SteadyClock *clock = nullptr,
    std::chrono::milliseconds initialCycleDuration = std::chrono::milliseconds(0));

  //----------------------------------------------------------------------------
  //! Start a cycle from this point onwards, with the given duration.
  //! The previously running cycle is discarded.
  //----------------------------------------------------------------------------
  void startCycle(std::chrono::milliseconds duration);

  //----------------------------------------------------------------------------
  //! Get timepoint of cycle start
  //----------------------------------------------------------------------------
  std::chrono::steady_clock::time_point getCycleStart() const;

  //----------------------------------------------------------------------------
  //! Return how much time has elapsed within this cycle, ie milliseconds since
  //! startCycle was called.
  //----------------------------------------------------------------------------
  std::chrono::milliseconds timeIntoCycle() const;

  //----------------------------------------------------------------------------
  //! Return how much time is remaining in this cycle. If more time has elapsed
  //! than the cycle duration, this function returns 0.
  //----------------------------------------------------------------------------
  std::chrono::milliseconds timeRemainingInCycle() const;

private:
  SteadyClock *mClock; //< The internal clock object of this class, can be null.
  std::chrono::steady_clock::time_point mCycleStart; //< The point at which the
                                                     //< current cycle started
  std::chrono::milliseconds mCycleDuration;          //< The current duration
};


EOSCOMMONNAMESPACE_END

