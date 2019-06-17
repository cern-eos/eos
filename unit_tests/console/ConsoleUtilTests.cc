//------------------------------------------------------------------------------
//! @file ConsoleUtilTests.cc
//! @author Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "console/ConsoleMain.hh"

//------------------------------------------------------------------------------
// Test parsing of keyword identifiers (e.g.: fid, fxid)
//------------------------------------------------------------------------------
TEST(PathIdentifier, KeyWords)
{
  // Check the keywords
  ASSERT_STREQ(path_identifier("fid:100"), "fid:100");
  ASSERT_STREQ(path_identifier("fxid:100a"), "fxid:100a");
  ASSERT_STREQ(path_identifier("pid:100"), "pid:100");
  ASSERT_STREQ(path_identifier("pxid:bc1"), "pxid:bc1");

  // Check the encode flag doesn't affect keywords
  ASSERT_STREQ(path_identifier("fid:100", true), "fid:100");

  // Check keyword-similar relative path
  ASSERT_STREQ(path_identifier("fid100"), "/fid100");
  ASSERT_STREQ(path_identifier("pxidbc1"), "/pxidbc1");
}

//------------------------------------------------------------------------------
// Test parsing of absolute path identifier
//------------------------------------------------------------------------------
TEST(PathIdentifier, AbsolutePath)
{
  // Absolute path
  ASSERT_STREQ(path_identifier("/eos/instance/user/file"),
               "/eos/instance/user/file");

  // Absolute path with &
  ASSERT_STREQ(path_identifier("/eos/instance/user/file&with&symbols"),
               "/eos/instance/user/file&with&symbols");

  // Absolute path with &, encoded
  ASSERT_STREQ(path_identifier("/eos/instance/user/file&with&symbols", true),
               "/eos/instance/user/file#AND#with#AND#symbols");
}

//------------------------------------------------------------------------------
// Test parsing of relative path identifier
//------------------------------------------------------------------------------
TEST(PathIdentifier, RelativePath)
{
  gPwd = "/";

  // Relative path
  ASSERT_STREQ(path_identifier("file"), "/file");

  // Relative path with &
  ASSERT_STREQ(path_identifier("file&with&symbols"),
               "/file&with&symbols");

  // Relative path with &, encoded
  ASSERT_STREQ(path_identifier("file&with&symbols", true),
               "/file#AND#with#AND#symbols");

  gPwd = "/eos/dir&with&symbols/";

  // Relative path with &
  ASSERT_STREQ(path_identifier("file&with&symbols"),
               "/eos/dir&with&symbols/file&with&symbols");

  // Relative path with &, encoded
  ASSERT_STREQ(path_identifier("file&with&symbols", true),
               "/eos/dir#AND#with#AND#symbols/file#AND#with#AND#symbols");
}
