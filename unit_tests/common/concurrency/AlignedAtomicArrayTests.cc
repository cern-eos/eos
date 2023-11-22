//------------------------------------------------------------------------------
// File: AlignedAtomicArrayTests.cc
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

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


#include "common/concurrency/AlignedArray.hh"
#include "gtest/gtest.h"

TEST(AlignedAtomicArray, ZeroInitialization)
{
  eos::common::AlignedAtomicArray<int64_t, 8> arr_signed{};
  eos::common::AlignedAtomicArray<int64_t, 8> arr_unsigned{};
  for (size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(0, arr_signed[i].load());
    EXPECT_EQ(0, arr_unsigned[i].load());
  }
}

TEST(AlignedAtomicArray, NonZeroInitialization)
{
  // This will not work with std::array
  // eos::common::AlignedAtomicArray<int64_t, 8> arr{{1}}; // only first element is set
  eos::common::AlignedAtomicArray<int64_t, 8> arr{1,1,1,1,1,1,1,1};
  for (size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(1, arr[i].load());
  }
}
