// ----------------------------------------------------------------------
// File: Parallel.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/ASwitzerland                                  *
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

/**
 * @file   Parallel.hh
 *
 * @brief  Class providing a parallel for function using lambda functions/threads
 *
 *
 */

#include <thread>
#include <mutex>
#include <vector>
#include <cmath>
#include <features.h>
#ifndef __EOSCOMMON__PARALLEL__HH
#define __EOSCOMMON__PARALLEL__HH

EOSCOMMONNAMESPACE_BEGIN

#if __GNUC_PREREQ(4,8)

class Parallel
{

public:

  template<typename Index, typename Callable>
  static void For(Index start, Index end, Callable func)
  {
    // Estimate number of threads in the pool
    const static unsigned nb_threads_hint = std::thread::hardware_concurrency();
    const static unsigned nb_threads = (nb_threads_hint == 0u ? 8u :
                                        nb_threads_hint);
    // Size of a slice for the range functions
    Index n = end - start + 1;
    Index slice = (Index) std::round(n / static_cast<double>(nb_threads));
    slice = std::max(slice, Index(1));
    // [Helper] Inner loop
    auto launchRange = [&func](int k1, int k2) {
      for (Index k = k1; k < k2; k++) {
        func(k);
      }
    };
    // Create pool and launch jobs
    std::vector<std::thread> pool;
    pool.reserve(nb_threads);
    Index i1 = start;
    Index i2 = std::min(start + slice, end);

    for (unsigned i = 0; i + 1 < nb_threads && i1 < end; ++i) {
      pool.emplace_back(launchRange, i1, i2);
      i1 = i2;
      i2 = std::min(i2 + slice, end);
    }

    if (i1 < end) {
      pool.emplace_back(launchRange, i1, end);
    }

    // Wait for jobs to finish
    for (std::thread& t : pool) {
      if (t.joinable()) {
        t.join();
      }
    }
  }

  // Serial version for easy comparison
  template<typename Index, typename Callable>
  static void SequentialFor(Index start, Index end, Callable func)
  {
    for (Index i = start; i < end; i++) {
      func(i);
    }
  }
};

#endif

EOSCOMMONNAMESPACE_END

#endif
