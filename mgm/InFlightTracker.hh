// ----------------------------------------------------------------------
// File: InFlightTracker.hh
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

#pragma once
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include <atomic>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! @brief Keep track of how many requests are currently in-flight.
//! It's also possible to use this as a barrier to further requests - useful
//! when shutting down.
//------------------------------------------------------------------------------
class InFlightTracker: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param accepting flag to mark that we should accept connections
  //----------------------------------------------------------------------------
  InFlightTracker(bool accepting = true) :
    mAcceptingRequests(accepting) {}

  //----------------------------------------------------------------------------
  //! Decide whether to account or not for a new connection. This helps to
  //! keep track of the number of threads inside a critical block of code.
  //----------------------------------------------------------------------------
  bool Up()
  {
    // This contraption (hopefully) ensures that after setAcceptingRequests(false)
    // takes effect, the following guarantees hold:
    // - Any subsequent calls to up() will not increase mInFlight.
    // - As soon as we observe an mInFlight value of zero, no further requests
    //   will be accepted.
    //
    // The second guarantee is necessary for wait(), which checks if mInFlight
    // is zero to tell whether all in-flight requests have been dispatched.

    // If setAcceptingRequests takes effect here, the request is rejected, as expected.
    if (!mAcceptingRequests) {
      return false;
    }

    // If setAcceptingRequests takes effect here, no problem. mInFlight will
    // temporarily jump, but the request will be rejected.
    ++mInFlight;

    // Same as before.
    if (!mAcceptingRequests) {
      // If we're here, it means setAcceptingRequests has already taken effect.
      --mInFlight;
      return false;
    }

    // If setAcceptingRequests takes effect here, no problem:
    // mInFlight can NOT be zero at this point, and the spinner will wait.
    std::unique_lock<std::mutex> scope_lock(mInFlightPidMutex);
    mInFlightPids[pthread_self()]++;
    return true;
  }

  //----------------------------------------------------------------------------
  //! Decrement number of inflight tracked requests
  //----------------------------------------------------------------------------
  void Down()
  {
    --mInFlight;
    assert(mInFlight >= 0);
    std::unique_lock<std::mutex> scope_lock(mInFlightPidMutex);
    if (!(--mInFlightPids[pthread_self()])) {
      mInFlightPids.erase(pthread_self());
    }
  }

  //----------------------------------------------------------------------------
  //! Set if we should accept tracking new requests or not
  //!
  //! @param value if true enable tracking, otherwise refuse
  //----------------------------------------------------------------------------
  void SetAcceptingRequests(bool value)
  {
    mAcceptingRequests = value;
  }

  //----------------------------------------------------------------------------
  //! Check if we're accepting requests
  //----------------------------------------------------------------------------
  bool IsAcceptingRequests() const
  {
    return mAcceptingRequests;
  }

  //----------------------------------------------------------------------------
  //! Wait that there are no more tracked requests
  //----------------------------------------------------------------------------
  void SpinUntilNoRequestsInFlight(bool print_log = false,
                                   const std::chrono::milliseconds wait_ms =
                                     std::chrono::milliseconds::zero()) const
  {
    assert(!mAcceptingRequests);
    auto num = GetInFlight();

    while (num) {
      if (print_log) {
        eos_info("msg=\"waiting for %li in-flight requests to finish\"", num);
      }

      if (wait_ms.count()) {
        std::this_thread::sleep_for(wait_ms);
      }

      num = GetInFlight();
    }
  }

  //----------------------------------------------------------------------------
  //! Get number of in-flight tracked requests
  //----------------------------------------------------------------------------
  int64_t GetInFlight() const
  {
    return mInFlight;
  }

  std::set<pthread_t> getInFlightThreads() {
    std::unique_lock<std::mutex> scope_lock(mInFlightPidMutex);
    std::set<pthread_t> inflight_threads;
    for ( auto it : mInFlightPids ) {
      inflight_threads.insert(it.first);
    }
    return inflight_threads;
  }

private:
  std::atomic<bool> mAcceptingRequests {true};
  std::atomic<int64_t> mInFlight {0};
  std::map<pthread_t, size_t> mInFlightPids;
  std::mutex mInFlightPidMutex;

};

//------------------------------------------------------------------------------
//! @brief Class InFlightRegistraction helper to account for in-flight
//! requests at scope level.
//------------------------------------------------------------------------------
class InFlightRegistration
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param tracke tracker object
  //----------------------------------------------------------------------------
  InFlightRegistration(InFlightTracker& tracker) :
    mInFlightTracker(tracker)
  {
    mSucceeded = mInFlightTracker.Up();
  }

  //----------------------------------------------------------------------------
  //! Destructor - take care of doing the proper accounting when getting out
  //! of scope.
  //------------------------------------------------------------------------------
  ~InFlightRegistration()
  {
    if (mSucceeded) {
      mInFlightTracker.Down();
    }
  }

  //------------------------------------------------------------------------------
  //! Check if current registration is actually tracked
  //!
  //! @return true if current registration is tracked, otherwise false
  //------------------------------------------------------------------------------
  bool IsOK()
  {
    return mSucceeded;
  }

  std::set<pthread_t> getThreads() {
    return mInFlightTracker.getInFlightThreads();
  }
private:
  InFlightTracker& mInFlightTracker;
  bool mSucceeded;
};

EOSMGMNAMESPACE_END
