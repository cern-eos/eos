//------------------------------------------------------------------------------
// File: TgcUtilsTests.cc
// Author: Steven Murray <smurray at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "mgm/cta/Utils.hh"

#include <gtest/gtest.h>
#include <limits>
#include <unistd.h>

class TgcUtilsTest : public ::testing::Test
{
protected:

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, toUint64)
{
  using namespace eos::mgm;
  const std::string str = "12345";
  ASSERT_EQ(12345, CtaUtils::toUint64(str));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, toUint64_max)
{
  using namespace eos::mgm;
  const std::string str = "18446744073709551615";
  ASSERT_EQ(std::numeric_limits<std::uint64_t>::max(), CtaUtils::toUint64(str));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, toUint64_empty_string)
{
  using namespace eos::mgm;
  const std::string str;
  ASSERT_THROW(CtaUtils::toUint64(str), CtaUtils::EmptyString);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, toUint64_non_numerioc)
{
  using namespace eos::mgm;
  const std::string str = "12345a";
  ASSERT_THROW(CtaUtils::toUint64(str), CtaUtils::NonNumericChar);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, toUint64_out_of_range)
{
  using namespace eos::mgm;
  const std::string str = "18446744073709551616";
  ASSERT_THROW(CtaUtils::toUint64(str), CtaUtils::ParsedValueOutOfRange);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, divideAndRoundToNearest)
{
  using namespace eos::mgm;
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(1, 1));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(2, 1));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(3, 1));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(1, 2));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(2, 2));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(3, 2));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(4, 2));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(5, 2));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(6, 2));
  ASSERT_EQ(0, CtaUtils::divideAndRoundToNearest(1, 3));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(2, 3));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(3, 3));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(4, 3));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(5, 3));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(6, 3));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(7, 3));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(8, 3));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(9, 3));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(10, 3));
  ASSERT_EQ(0, CtaUtils::divideAndRoundToNearest(1, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(2, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(3, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(4, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundToNearest(5, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(6, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(7, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(8, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundToNearest(9, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(10, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(11, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(12, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundToNearest(13, 4));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, divideAndRoundUp)
{
  using namespace eos::mgm;
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(1, 1));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(2, 1));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(3, 1));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(1, 2));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(2, 2));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(3, 2));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(4, 2));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(5, 2));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(6, 2));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(1, 3));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(2, 3));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(3, 3));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(4, 3));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(5, 3));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(6, 3));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(7, 3));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(8, 3));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(9, 3));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(1, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(2, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(3, 4));
  ASSERT_EQ(1, CtaUtils::divideAndRoundUp(4, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(5, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(6, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(7, 4));
  ASSERT_EQ(2, CtaUtils::divideAndRoundUp(8, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(9, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(10, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(11, 4));
  ASSERT_EQ(3, CtaUtils::divideAndRoundUp(12, 4));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, bufToTimespec)
{
  using namespace eos::mgm;
  timespec src;
  src.tv_sec = 1234;
  src.tv_nsec = 5678;
  std::string buf((char*)&src, sizeof(src));
  const timespec result = CtaUtils::bufToTimespec(buf);
  ASSERT_EQ(src.tv_sec, result.tv_sec);
  ASSERT_EQ(src.tv_nsec, result.tv_nsec);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, bufToTimespec_BufSizeMismatch)
{
  using namespace eos::mgm;
  timespec src;
  src.tv_sec = 1234;
  src.tv_nsec = 5678;
  std::string buf((char*)&src, sizeof(src) - 1);
  ASSERT_THROW(CtaUtils::bufToTimespec(buf), CtaUtils::BufSizeMismatch);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, readFdIntoStr)
{
  using namespace eos::mgm;
  int pipeFds[2] = {0, 0};
  ASSERT_NE(-1, pipe(pipeFds));
  const char msg[] = "1234";
  const std::string msgStr = msg;
  ASSERT_EQ(sizeof(msg), ::write(pipeFds[1], msg, sizeof(msg)));
  const ssize_t maxStrLen = sizeof(msg) - 1;
  const std::string resultStr = CtaUtils::readFdIntoStr(pipeFds[0], maxStrLen);
  ASSERT_EQ(msgStr, resultStr);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, readFdIntoStr_write_gt_maxStrLen)
{
  using namespace eos::mgm;
  int pipeFds[2] = {0, 0};
  ASSERT_NE(-1, pipe(pipeFds));
  const char msg[] = "1234";
  ASSERT_GE(sizeof(msg), 2);
  ASSERT_EQ(sizeof(msg), ::write(pipeFds[1], msg, sizeof(msg)));
  const ssize_t maxStrLen = sizeof(msg) - 2; // Drop one char off the end
  const std::string resultStr = CtaUtils::readFdIntoStr(pipeFds[0], maxStrLen);
  const std::string expectedStr = "123";
  ASSERT_EQ(expectedStr, resultStr);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, readFdIntoStr_write_lt_maxStrLen)
{
  using namespace eos::mgm;
  int pipeFds[2] = {0, 0};
  ASSERT_NE(-1, pipe(pipeFds));
  const char msg[] = "1234";
  const std::string msgStr = msg;
  ASSERT_EQ(sizeof(msg), ::write(pipeFds[1], msg, sizeof(msg)));
  const ssize_t maxStrLen = sizeof(msg) + 1;
  const std::string resultStr = CtaUtils::readFdIntoStr(pipeFds[0], maxStrLen);
  ASSERT_EQ(msgStr, resultStr);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, readFdIntoStr_out_of_range)
{
  using namespace eos::mgm;
  EXPECT_THROW(CtaUtils::readFdIntoStr(0, 1LL<<33), std::out_of_range);
}
