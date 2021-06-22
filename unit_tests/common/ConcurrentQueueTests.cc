//------------------------------------------------------------------------------
//! @file BufferManager.hh
//! @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "common/ConcurrentQueue.hh"

TEST(ConcurrentQueue, BasicFunctionality)
{
  eos::common::ConcurrentQueue<int> queue;

  for (int i = 0; i < 100; ++i) {
    queue.push(i);
  }

  ASSERT_EQ(100, queue.size());
  int random = 12345;
  ASSERT_FALSE(queue.push_size(random, 99));
  int val;

  for (int j = 0; j < 100; ++j) {
    ASSERT_TRUE(queue.try_pop(val));
    ASSERT_EQ(j, val);
  }

  ASSERT_TRUE(queue.empty());
}
