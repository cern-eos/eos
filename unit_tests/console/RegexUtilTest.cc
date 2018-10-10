//------------------------------------------------------------------------------
//! @file RegexUtilTest.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
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

#include "gtest/gtest.h"
#include "console/RegexUtil.hh"

TEST(RegexUtil, BasicSanity)
{
  RegexUtil test;
  std::string origin("asdfasfsssstest12kksdjftestossskso");
  std::string temp;
  // Pass case
  test.SetOrigin(origin);
  test.SetRegex("test[0-9]+");
  test.initTokenizerMode();
  temp = test.Match();
  ASSERT_EQ(temp, "test12");
  temp = test.Match();
  ASSERT_EQ(temp, "test12");
}

TEST(RegexUtil, FailCases)
{
  std::string origin("asdfasfsssstest12kksdjftestossskso");
  RegexUtil test;
  (test.SetOrigin(origin));
  ASSERT_THROW(test.SetRegex("test[0-9"),  std::string);

  RegexUtil test2;
  ASSERT_THROW(test2.SetRegex("test[0-9"),  std::string);
  ASSERT_THROW(test2.initTokenizerMode(),  std::string);
}
