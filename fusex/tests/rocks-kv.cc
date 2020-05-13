//------------------------------------------------------------------------------
//! @file RocksKVTest.cc
//! @author Georgios Bitzes CERN
//! @brief tests for kv persistency class based on rocksdb
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#ifdef HAVE_ROCKSDB
#include "kv/RocksKV.hh"
#include "gtest/gtest.h"

TEST(RocksKV, BasicSanity)
{
  ASSERT_EQ(system("rm -rf /tmp/eos-fusex-tests"), 0);
  RocksKV kv;
  ASSERT_EQ(kv.connect("myprefix", "/tmp/eos-fusex-tests"), 0);
  ASSERT_EQ(kv.put("123", "asdf"), 0);
  std::string tmp;
  ASSERT_EQ(kv.get("123", tmp), 0);
  ASSERT_EQ(tmp, "asdf");
  uint64_t ret;
  ASSERT_EQ(kv.put("123", 4), 0);
  ASSERT_EQ(kv.get("123", ret), 0);
  ASSERT_EQ(ret, 4u);
  ASSERT_EQ(kv.put("test", "test"), 0);
  ASSERT_EQ(kv.get("test", ret), -1); // cannot convert "test" to uint64_t
  ASSERT_EQ(kv.put(1, "value", "l"), 0);
  ASSERT_EQ(kv.get(1, tmp, "l"), 0);
  ASSERT_EQ(tmp, "value");
  ASSERT_EQ(kv.put(10, 5, "asdf"), 0);
  ASSERT_EQ(kv.get(10, ret, "asdf"), 0);
  ASSERT_EQ(ret, 5);
  ASSERT_EQ(kv.erase(10, "asdf"), 0);
  ASSERT_EQ(kv.get(10, ret, "asdf"), 1);
  uint64_t increment = 10;
  ASSERT_EQ(kv.inc("my-counter", increment), 0);
  ASSERT_EQ(increment, 10);
  increment = 5;
  ASSERT_EQ(kv.inc("my-counter", increment), 0);
  ASSERT_EQ(increment, 15u);
  ASSERT_EQ(kv.get("my-counter", ret), 0);
  ASSERT_EQ(ret, 15u);
  ASSERT_EQ(kv.inc("test", increment), -1);
}

#endif // HAVE_ROCKSDB
