// ----------------------------------------------------------------------
// File: RRSeed
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#ifndef EOS_RRSEED_HH
#define EOS_RRSEED_HH

#include <atomic>
#include <vector>

namespace eos::mgm::placement {

// A copyable atomic type! DO NOT use this as a synchronization primitive!
// This is only meant for storing a seed value for a random number generator
// wherein we only use this at the initialization phase to put onto a vector!
// Do not copy atomic types when you are using them to synchronize values!
template <typename T>
struct AtomicWrapper {
  AtomicWrapper(T t): value(t) {}
  AtomicWrapper(const AtomicWrapper<T>& other): value(other.value.load()) {}

  std::atomic<T> value;
};

// A simple round robin seed generator, stored as a list of atomic values,
// the use case of the list is when you'd need a 2-D round robin and you'd
// need to RR over the 2nd dimension. Under the hood this is nothing but just
// a 1-D counter incremented to a given size.
template <typename T=uint64_t>
class RRSeed {
public:

  // Currently the counter will wrap around to 0 if it reaches the max value
  // of type T, as is defined for unsigned integers. If you need to -ve values
  // rewrite carefully considering overflows!
  static_assert(std::is_integral<T>::value && std::is_unsigned<T>::value,
                "We expect only unsigned integer types, "
                "otherwise overflow would be Undefined Behaviour");

  // Initialization is not TS: we assume that this is only called once!!!
  explicit RRSeed(size_t max_items) : mSeeds(max_items, 0) {}

  // Get a seed at an index, also reserve n_items, so that the next seed is n_items
  // away. This will throw an std::out_of_range if you ask an out-of-bounds index!
  T get(size_t index, size_t n_items) {

    T ret = mSeeds.at(index).value.load(std::memory_order_relaxed);

    // Make sure that our round robin seed is not taken over by other threads!
    // ie in case some other thread beats us while we update the seed, we need
    // to ensure that our seed isn't the same, we do a CAS loop which should
    // update our rr_seed to the latest in case we fail so that multiple threads
    // wouldn't end up with the same seed.
    while(!mSeeds.at(index).value.compare_exchange_weak(ret, ret + n_items,
                                                        std::memory_order_relaxed))
    {}

    return ret;
  }

  size_t getNumSeeds() const { return mSeeds.size(); }
private:
  std::vector<AtomicWrapper<T>> mSeeds;
};


}

#endif // EOS_RRSEED_HH
