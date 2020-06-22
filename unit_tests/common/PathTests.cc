//------------------------------------------------------------------------------
// File: PathTests.cc
// Author: Mihai Patrascoiu - CERN
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

#include "common/Path.hh"
#include "common/ParseUtils.hh"
#include "Namespace.hh"
#include "gtest/gtest.h"

EOSCOMMONNAMESPACE_BEGIN

TEST(Path, BasicFunctionality)
{
  using eos::common::Path;
  Path path("/eos/example/file");
  ASSERT_STREQ(path.GetName(), "file");
  ASSERT_STREQ(path.GetPath(), "/eos/example/file");
  ASSERT_STRNE(path.GetParentPath(), "/eos/example");
  ASSERT_STREQ(path.GetParentPath(), "/eos/example/");
  ASSERT_STREQ(path.GetFullPath().c_str(), "/eos/example/file");
  ASSERT_STREQ(path.GetContractedPath().c_str(), "..eos..example..file");
  ASSERT_EQ(path.GetSubPathSize(), 3);
  ASSERT_STREQ(path.GetSubPath(2), "/eos/example/");
  ASSERT_STREQ(path.GetSubPath(1), "/eos/");
  ASSERT_STREQ(path.GetSubPath(0), "/");
  ASSERT_STREQ(path.GetSubPath(5), 0);
  path = "/eos/example/otherfile"s;
  ASSERT_STREQ(path.GetPath(), "/eos/example/otherfile");
  path = "/eos/example/"s;
  ASSERT_STREQ(path.GetName(), "example");
  ASSERT_STREQ(path.GetPath(), "/eos/example");
  ASSERT_STREQ(path.GetParentPath(), "/eos/");
  ASSERT_STREQ(path.GetFullPath().c_str(), "/eos/example");
}

TEST(Path, EmptyOrRootPath)
{
  using eos::common::Path;
  Path empty_path;
  ASSERT_STREQ(empty_path.GetName(), "");
  ASSERT_STREQ(empty_path.GetPath(), "");
  ASSERT_STREQ(empty_path.GetParentPath(), "/");
  ASSERT_STREQ(empty_path.GetFullPath().c_str(), "");
  ASSERT_EQ(empty_path.GetSubPathSize(), 0);
}

TEST(Path, RootPath)
{
  using eos::common::Path;
  Path root_path;
  ASSERT_STREQ(root_path.GetName(), "");
  ASSERT_STREQ(root_path.GetPath(), "");
  ASSERT_STREQ(root_path.GetParentPath(), "/");
  ASSERT_STREQ(root_path.GetFullPath().c_str(), "");
  ASSERT_EQ(root_path.GetSubPathSize(), 0);
}

TEST(Path, RelativePath)
{
  using eos::common::Path;
  Path path("eos/example/file");
  ASSERT_STREQ(path.GetName(), "eos/example/file");
  ASSERT_STREQ(path.GetPath(), "eos/example/file");
  ASSERT_STREQ(path.GetParentPath(), "/");
  ASSERT_STREQ(path.GetFullPath().c_str(), "eos/example/file");
  ASSERT_EQ(path.GetSubPathSize(), 0);
  path = "eos/example/file/"s;
  ASSERT_STREQ(path.GetName(), "eos/example/file");
  ASSERT_STREQ(path.GetPath(), "eos/example/file");
  ASSERT_STREQ(path.GetParentPath(), "/");
  ASSERT_STREQ(path.GetFullPath().c_str(), "eos/example/file");
  ASSERT_EQ(path.GetSubPathSize(), 0);
}

TEST(Path, PathParsing)
{
  using eos::common::Path;
  // Only dotted paths
  ASSERT_STREQ(Path("/.").GetPath(), "/");
  ASSERT_STREQ(Path("/./").GetPath(), "/");
  ASSERT_STREQ(Path("/..").GetPath(), "/");
  ASSERT_STREQ(Path("/../").GetPath(), "/");
  ASSERT_STREQ(Path("/../../").GetPath(), "/");
  ASSERT_STREQ(Path("/../../../").GetPath(), "/");
  // Mix of dots and directories
  ASSERT_STREQ(Path("/../eos/").GetPath(), "/eos");
  ASSERT_STREQ(Path("/./eos/../").GetPath(), "/");
  ASSERT_STREQ(Path("/eos/../unit/test/").GetPath(), "/unit/test");
  ASSERT_STREQ(Path("/eos/../unit/./test/./").GetPath(), "/unit/test");
  ASSERT_STREQ(Path("/eos/../unit/./test/../").GetPath(), "/unit/");
  // Trailing dots
  ASSERT_STREQ(Path("/eos/test/.").GetPath(), "/eos/test");
  ASSERT_STREQ(Path("/eos/test/..").GetPath(), "/eos/");
  ASSERT_STREQ(Path("/eos/test/dir/../../").GetPath(), "/eos/");
  ASSERT_STREQ(Path("/eos/test/dir/../../../").GetPath(), "/");
  ASSERT_STREQ(Path("/eos/test/dir/../../../../").GetPath(), "/");
  ASSERT_STREQ(Path("/eos/test/dir/.././../").GetPath(), "/eos/");
  ASSERT_STREQ(Path("/eos/test/dir/.././../../").GetPath(), "/");
  ASSERT_STREQ(Path("/eos/test/dir/.././.././../").GetPath(), "/");
  ASSERT_STREQ(Path("/eos/test/dir/subdir/.././.././../").GetPath(), "/eos/");
  Path path("//eos//example//file");
  ASSERT_STREQ(path.GetName(), "file");
  ASSERT_STREQ(path.GetPath(), "/eos/example/file");
  ASSERT_STREQ(path.GetParentPath(), "/eos/example/");
}

TEST(ParseUtils, ParseHostNamePort)
{
  int port = 0;
  std::string host;
  std::string input = "eospps.cern.ch";
  ASSERT_TRUE(eos::common::ParseHostNamePort(input, host, port));
  ASSERT_STREQ(host.c_str(), "eospps.cern.ch");
  ASSERT_EQ(port, 1094);
  input = "eospps.cern.ch:2020";
  ASSERT_TRUE(eos::common::ParseHostNamePort(input, host, port));
  ASSERT_STREQ(host.c_str(), "eospps.cern.ch");
  ASSERT_EQ(port, 2020);
}

EOSCOMMONNAMESPACE_END
