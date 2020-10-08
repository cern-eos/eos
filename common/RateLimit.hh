//------------------------------------------------------------------------------
//! @file RateLimit.hh
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

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
#include "common/Namespace.hh"
#include "common/SteadyClock.hh"
#include "common/Logging.hh"
#include <atomic>
#include <thread>
#include <set>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Rate limit interface
//------------------------------------------------------------------------------
class IRateLimit
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  IRateLimit(bool fake_clock = false):
    mRate(0ull), mClock(fake_clock)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~IRateLimit() = default;

  //----------------------------------------------------------------------------
  //! Get rate
  //!
  //! @return enforced rate
  //----------------------------------------------------------------------------
  virtual unsigned long long GetRatePerSecond() const
  {
    return mRate;
  }

  //----------------------------------------------------------------------------
  //! Set rate
  //----------------------------------------------------------------------------
  virtual void SetRatePerSecond(unsigned long long rate)
  {
    mRate = rate;
  }

  //----------------------------------------------------------------------------
  //! Allow method that might delay the current thread until it can proceed
  //! processing prmits.
  //!
  //! @param permits number of permits requested
  //!
  //! @return microseconds that the current thread was delayed
  //----------------------------------------------------------------------------
  virtual uint64_t Allow(uint64_t permits = 1) = 0;

  //----------------------------------------------------------------------------
  //! Get clock reference for testing purposes
  //----------------------------------------------------------------------------
  inline eos::common::SteadyClock& GetClock()
  {
    return mClock;
  }

protected:
  std::atomic<unsigned long long> mRate; ///< Rate per second
  eos::common::SteadyClock mClock; ///< Clock wrapper also used for testing
};


//------------------------------------------------------------------------------
//! Requests per second rate limiter
//------------------------------------------------------------------------------
class RequestRateLimit: public IRateLimit, public LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RequestRateLimit(bool fake_clock = false):
    IRateLimit(fake_clock), mLastTimestampUs(0ull)
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RequestRateLimit() = default;

  //----------------------------------------------------------------------------
  //! Set rate
  //----------------------------------------------------------------------------
  void SetRatePerSecond(unsigned long long rate)
  {
    if (rate > 1000000) {
      eos_static_err("msg=\"attempt to set very high rate discarded\""
                     " current_rate=%llu failed_rate=%llu", mRate.load(), rate);
      return;
    }

    if (rate < 1) {
      rate = 1;
    }

    mRate = rate;
    mRateIntervalUs = 1000000 / rate;
  }

  //----------------------------------------------------------------------------
  //! Allow submitting a request
  //!
  //! @return milliseconds that the current thread was delayed
  //----------------------------------------------------------------------------
  uint64_t Allow(uint64_t permits = 1)
  {
    auto wait = GetDelay();

    // When testing skip the actual sleepp
    if (!mClock.IsFake()) {
      std::this_thread::sleep_for(wait);
    }

    return wait.count();
  }

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  //----------------------------------------------------------------------------
  //! Get delay for the current request
  //!
  //! @return delay in microseconds for the current requestor
  //----------------------------------------------------------------------------
  std::chrono::microseconds GetDelay();

  std::mutex mMutex; ///< Mutex protecting the set of timestamps
  ///< Set of request timestamps in microsec for each request
  std::set<uint64_t> mSetReqTimestamps;
  uint64_t mRateIntervalUs; ///< Interval in microsec corresp. to the given rate
  ///< Last timestamp of an entry added to the set - only for testing
  std::atomic<uint64_t> mLastTimestampUs;
};

EOSCOMMONNAMESPACE_END
