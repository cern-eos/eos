// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2024 CERN/Switzerland                           *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************
//
#include "ThreadEpochCounter.hh"
#include "common/Logging.hh"

namespace eos::common {

std::array<std::atomic<bool>, EOS_MAX_THREADS> g_thread_in_use {false};
thread_local ThreadID tlocalID;

ThreadID::ThreadID()
{
  for (size_t i = 0; i < EOS_MAX_THREADS; ++i) {
    bool expected = false;
    if (!g_thread_in_use[i] &&
        g_thread_in_use[i].compare_exchange_strong(expected, true)) {
      tid = i;
      return;
    }
  }

  // COULD NOT FIND A FREE THREAD ID, PANIC!
  // assert(true); In the rare event we reach here, we can't guarantee EpochCounter
  // correctness, so we'll just log and move on!
  // Since the commonest user of this code path is the counter for getting the current scheduler
  // we can take the risky case when we are at 65k threads
  eos_static_alert("Could not find a free thread ID, panicking! You've more than %d threads: current_thread: %d %lu",
                   EOS_MAX_THREADS, pthread_self());
}

ThreadID::~ThreadID()
{
  g_thread_in_use[tid] = false;
}
} // namespace eos::common