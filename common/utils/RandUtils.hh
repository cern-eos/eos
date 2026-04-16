// ----------------------------------------------------------------------
// File: RandUtils.hh
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/ASwitzerland                                 *
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
#include <cstdlib>
#include <optional>
#include <random>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Method to generate random number in the given interval - thread safe
//!
//! @param start start interval
//! @param end end interval
//! @param seed  optional seed for the generator (only effective on first call
//!              per thread due to thread_local storage)
//!
//! @return random number uniformly distributed in the given interval
//------------------------------------------------------------------------------
template <typename IntType = uint64_t>
auto
getRandom(IntType start = 0, IntType end = static_cast<IntType>(RAND_MAX),
          std::optional<uint64_t> seed = std::nullopt) -> IntType
{
  static_assert(std::is_integral<IntType>::value,
                "template argument must be an integral type");
  thread_local std::mt19937 generator(seed.has_value() ? *seed : std::random_device{}());
  std::uniform_int_distribution<IntType> distrib(start, end);
  return distrib(generator);
}

//------------------------------------------------------------------------------
//! Method to generate a random number following a normal distribution
//! (Gaussian) - thread safe
//!
//! @param mean   mean value of the distribution
//! @param stddev standard deviation of the distribution
//! @param seed  optional seed for the generator (only effective on first call
//!              per thread due to thread_local storage)
//!
//! @return random number normally distributed with the given mean and stddev
//------------------------------------------------------------------------------
template <typename FloatType = double>
auto
getRandomNormal(FloatType mean = 0.0, FloatType stddev = 1.0,
                std::optional<uint64_t> seed = std::nullopt) -> FloatType
{
  static_assert(std::is_floating_point<FloatType>::value,
                "template argument must be a floating point type");
  thread_local std::mt19937 generator(seed.has_value() ? *seed : std::random_device{}());
  std::normal_distribution<FloatType> distrib(mean, stddev);
  return distrib(generator);
}

EOSCOMMONNAMESPACE_END
