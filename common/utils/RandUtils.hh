// ----------------------------------------------------------------------
// File: RandUtils.cc
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
#include <random>
#include <cstdlib>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Method to generate random nuber in the given interval - thread safe
//!
//! @param start start interval
//! @param end end interval
//!
//! @return random number uniformly distributed in the given interval
//------------------------------------------------------------------------------
template <typename IntType=uint64_t>
auto getRandom(IntType start = 0, IntType end = RAND_MAX) -> IntType
{
  thread_local std::random_device tlrd;
  thread_local std::mt19937 generator(tlrd());
  std::uniform_int_distribution<IntType> distrib(start, end);
  return distrib(generator);
}

EOSCOMMONNAMESPACE_END
