// ----------------------------------------------------------------------
// File: WaitInterval.hh
// Author: Gianmaria Del Monte - CERN
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
#include <condition_variable>
#include <mutex>
#include <chrono>
#include "common/AssistedThread.hh"

// A common pattern in the EOS code for long running threads is:
//   while (!assistant.terminationRequested()) {
//     doSomeOperation();
//     assistant.wait_for(sleep_time);
//   }
//
// Another thread can modify the sleep_time.
// Let elapsed_time be the time elapsed when it was first called
// the sleep time, with elapsed_time <= sleep_time.
//
// In this case the wanted behavior would be:
// - if new_sleep_time > elapsed_time
//   wait only the new_sleep_time - elapsed_time
// - if new_sleep_time <= elapsed_time
//   no need to wait
//
// This class can then be used as:
// Thread 1:
//   while (!assistant.terminationRequested()) {
//     doSomeOperation();
//     interval.wait(assistant);
//   }
//
// Thread 2:
// interval.set(new_value)

class WaitInterval
{
public:
  WaitInterval(const uint64_t interval_sec): mIntervalSec(
      interval_sec) {};

  WaitInterval(const WaitInterval&) = delete;
  WaitInterval(WaitInterval&&) noexcept = delete;

  WaitInterval& operator=(const WaitInterval&) = delete;
  WaitInterval& operator=(WaitInterval&&) noexcept = delete;

  uint64_t get() const
  {
    const std::lock_guard lock(mMutex);
    return mIntervalSec;
  }

  void set(const uint64_t new_value_sec)
  {
    const std::lock_guard lock(mMutex);

    if (mIntervalSec != new_value_sec) {
      mIntervalSec = new_value_sec;
      mCv.notify_all();
    }
  }

  //------------------------------------------------------------------------------
  //! wait pauses the execution of the current thread until an amout of <interval>
  //! seconds has passed or the thread has been terminated.
  //! The wait time <interval> can be changed while one thread is sleeping. If
  //! this happens, the wait time will change accordingly.
  //! If the <interval> value is zero and the flag <zero_forever> is true
  //! the thread will sleep forever. If the flag is false (default bahaviour),
  //! the thread will not sleep.
  //!
  //! @param assistant thread running the job
  //! @param zero_forever if true indicates that if the <interval> value is 0
  //!                     the sleep time is forever
  //------------------------------------------------------------------------------
  void wait(ThreadAssistant& assistant, bool zero_forever = false) const
  {
    registerNotifyCallback(assistant);
    std::unique_lock lock(mMutex);

    while (true) {
      if (mIntervalSec == 0) {
        if (zero_forever) {
          mCv.wait(lock);
        } else {
          return;
        }
      }

      const auto start = std::chrono::steady_clock::now();
      uint64_t remaining = mIntervalSec;

      while (remaining > 0) {
        auto status = mCv.wait_for(lock, std::chrono::seconds(remaining));

        if (status == std::cv_status::timeout || assistant.terminationRequested()) {
          return;
        }

        if (mIntervalSec == 0) {
          break;
        }

        const auto elapsed = std::chrono::steady_clock::now() - start;
        remaining =  mIntervalSec - std::chrono::duration_cast<std::chrono::seconds>
                     (elapsed).count();
      }

      if (mIntervalSec > 0) {
        return;
      }
    }
  }

  //------------------------------------------------------------------------------
  //! wait_if_zero pauses the execution of the current thread forever. The only
  //! way of resuming the thread is to set a value for the interval different
  //! from zero, or requesting the termination of the thread.
  //!
  //! @param assistant thread running the job
  //! @return true if the thread waited, false if it didn't wait
  //------------------------------------------------------------------------------
  bool wait_if_zero(ThreadAssistant& assistant) const
  {
    registerNotifyCallback(assistant);
    std::unique_lock lock(mMutex);

    if (mIntervalSec == 0) {
      mCv.wait(lock);
      return true;
    }

    return false;
  }

private:
  uint64_t mIntervalSec;
  mutable std::condition_variable mCv;
  mutable std::mutex mMutex;
  mutable std::once_flag mRegistered;

  void registerNotifyCallback(ThreadAssistant& assistant) const
  {
    std::call_once(mRegistered, [this](ThreadAssistant & assistant) {
      assistant.registerCallback([this]() {
        mCv.notify_all();
      });
    }, assistant);
  }
};