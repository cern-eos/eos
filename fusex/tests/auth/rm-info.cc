//------------------------------------------------------------------------------
//! @file rm-info.cc
//! @author Georgios Bitzes CERN
//! @brief tests for RmrfGuard class
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

#include <gtest/gtest.h>
#include "auth/RmInfo.hh"

TEST(RmInfo, BasicSanity1) {
  RmInfo parser("/usr/bin/rm", {"rm", "/eos/some-file" });
  ASSERT_TRUE(parser.isRm());
  ASSERT_FALSE(parser.isRecursive());
}

TEST(RmInfo, BasicSanity2) {
  RmInfo parser("/bin/rm", {"rm", "-r", "/eos/some-folder"});
  ASSERT_TRUE(parser.isRm());
  ASSERT_TRUE(parser.isRecursive());
}

TEST(RmInfo, BasicSanity3) {
  RmInfo parser("/usr/bin/git", {"git", "-r", "aaaa"});
  ASSERT_FALSE(parser.isRm());
  ASSERT_FALSE(parser.isRecursive());
}

TEST(RmInfo, BasicSanity4) {
  RmInfo parser("/usr/bin/rm", {"rm", "-rf", "."});
  ASSERT_TRUE(parser.isRm());
  ASSERT_TRUE(parser.isRecursive());
}
