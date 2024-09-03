//------------------------------------------------------------------------------
// File: GlobTests.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "common/Glob.hh"
#include "gtest/gtest.h"

EOSCOMMONNAMESPACE_BEGIN

TEST(Glob, BasicFunctionality)
{
  using eos::common::Glob;
  Glob glob;
  ASSERT_TRUE(glob.Match("asdf*.txt", "asdf1.txt"));
  ASSERT_FALSE(glob.Match("asdf*.txt", "bsdf1.txt"));
  ASSERT_TRUE(glob.Match("*.txt", "asdf1.txt"));
  ASSERT_TRUE(glob.Match("a?df1.txt", "asdf1.txt"));
  ASSERT_FALSE(glob.Match("asdf?.txt", "bsdf1.txt"));
  ASSERT_FALSE(glob.Match("number{1..9}pattern","10"));
  ASSERT_FALSE(glob.Match("test",""));
  ASSERT_FALSE(glob.Match("regexx*p.tt\\.ern","regexx*p.tt\\.ern"));
}

TEST(Glob, Performance)
{
  using eos::common::Glob;
  Glob glob;
  char a[2];
  a[1]=0;
  for (auto it=0; it < 100000; it++) {
    a[0]=it%256;
    ASSERT_FALSE(glob.Match("asdf*.txt", a)) ;
    ASSERT_TRUE(glob.Match("asdf*.txt", "asdf1.txt"));
  }
}
EOSCOMMONNAMESPACE_END
