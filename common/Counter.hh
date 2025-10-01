// ----------------------------------------------------------------------
// File: Counter.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#ifndef EOS_COUNTER_HH
#define EOS_COUNTER_HH

#include "common/SteadyClock.hh"

namespace eos::common
{

class Counter
{
public:
  Counter() = default;

  Counter(SteadyClock* clock): mCounter(0), mSteadyClock(clock) {}


  void Init()
  {
    mCounter = 0;
    mStartTime = GetCurrentTime();
    mLastTime = mStartTime;
  }

  void Increment(uint64_t value = 1)
  {
    auto curr_time = GetCurrentTime();
    std::chrono::duration<double, std::milli> ms_elapsed = curr_time - mLastTime;

    if (ms_elapsed.count() > 0) {
      last_frequency = 1000 * value / ms_elapsed.count();
    }

    mLastTime = curr_time;
    mCounter += value;
    ms_elapsed = curr_time - mStartTime;

    if (ms_elapsed.count() > 0) {
      frequency = 1000 * mCounter / ms_elapsed.count();
    }
  }

  double
  GetFrequency()
  {
    return frequency;
  }

  double
  GetLastFrequency()
  {
    return last_frequency;
  }

  std::chrono::steady_clock::time_point GetStartTime()
  {
    return mStartTime;
  }

  uint64_t GetSecondsSinceStart()
  {
    using namespace std::chrono;
    return duration_cast<seconds>(GetCurrentTime() - mStartTime).count();
  }

private:
  std::chrono::steady_clock::time_point GetCurrentTime()
  {
    if (mSteadyClock != nullptr) {
      return mSteadyClock->GetTime();
    }

    return std::chrono::steady_clock::now();
  }

  uint64_t mCounter {0};
  std::chrono::steady_clock::time_point mLastTime;
  std::chrono::steady_clock::time_point mStartTime;
  SteadyClock* mSteadyClock{nullptr}; //<Only used for testing
  double last_frequency{0};
  double frequency{0};
};

} // namespace eos::common

#endif // EOS_COUNTER_HH
