//------------------------------------------------------------------------------
// File: FileFsPathTests.cc
// Author: Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
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

#include "gtest/gtest.h"
#include "Namespace.hh"
#include "common/FileFsPath.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_in_memory/FileMD.hh"

EOSCOMMONTESTING_BEGIN

using namespace eos::common;

//------------------------------------------------------------------------------
// Test input validation
//------------------------------------------------------------------------------
TEST(FileFsPath, InputValidation)
{
  std::shared_ptr<eos::IFileMDSvc> fileSvc = 0;
  std::shared_ptr<eos::IFileMD> emptyFmd = 0,
      fmd = std::make_shared<eos::FileMD>(1, fileSvc.get());
  XrdOucString fidPath, path = "initial";
  int rc;

  // Empty file metadata
  FileFsPath::GetPhysicalPath(1, emptyFmd, path);
  ASSERT_STREQ(path.c_str(), "");

  // No extended attribute present
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", fidPath);
  fidPath.erasefromstart(8);

  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());

  // Empty local prefix
  FileFsPath::GetFullPhysicalPath(1, fmd, 0, path);
  ASSERT_STREQ(path.c_str(), "");

  // Empty file metadata
  rc = FileFsPath::GetFullPhysicalPath(1, emptyFmd, "/prefix/", path);
  ASSERT_STREQ(path.c_str(), "");
  ASSERT_EQ(rc, -1);
}

//------------------------------------------------------------------------------
// Test logical path storage and retrieval
//------------------------------------------------------------------------------
TEST(FileFsPath, LogicalPath)
{
  std::shared_ptr<eos::IFileMDSvc> fileSvc = 0;
  std::shared_ptr<eos::IFileMD> fmd =
      std::make_shared<eos::FileMD>(1, fileSvc.get());
  XrdOucString path, fidPath;

  // Single logical path
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");

  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", fidPath);
  fidPath.erasefromstart(8);

  FileFsPath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());

  FileFsPath::StorePhysicalPath(1, fmd, "path2");
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");

  // Multiple logical paths
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  FileFsPath::StorePhysicalPath(2, fmd, "path2");
  FileFsPath::StorePhysicalPath(3, fmd, "path3");
  FileFsPath::StorePhysicalPath(3, fmd, "path3");
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");
  FileFsPath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");
  FileFsPath::GetPhysicalPath(3, fmd, path);
  ASSERT_STREQ(path.c_str(), "path3");

  // Retrieve full path
  FileFsPath::GetFullPhysicalPath(1, fmd, "/prefix/", path);
  ASSERT_STREQ(path.c_str(), "/prefix/path1");
}

//------------------------------------------------------------------------------
// Test path-from-fid generation
//------------------------------------------------------------------------------
TEST(FileFsPath, PathFromFid)
{
  std::shared_ptr<eos::IFileMDSvc> fileSvc = 0;
  std::shared_ptr<eos::IFileMD> fmd =
      std::make_shared<eos::FileMD>(1, fileSvc.get());
  XrdOucString path, expected;

  // Path from fid
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", expected);
  expected.erasefromstart(8);

  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), expected.c_str());

  // Full path from fid
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", expected);
  FileFsPath::GetFullPhysicalPath(1, fmd, "/prefix/", path);
  ASSERT_STREQ(path.c_str(), expected.c_str());
}

//------------------------------------------------------------------------------
// Test building of physical path
//------------------------------------------------------------------------------
TEST(FileFsPath, BuildPath)
{
  XrdOucString path = "initial";
  XrdOucString expected = "/prefix/sufix";

  FileFsPath::BuildPhysicalPath("/prefix", "sufix", path);
  ASSERT_STREQ(path.c_str(), expected.c_str());

  FileFsPath::BuildPhysicalPath("/prefix/", "sufix", path);
  ASSERT_STREQ(path.c_str(), expected.c_str());

  FileFsPath::BuildPhysicalPath("/prefix", "/sufix", path);
  ASSERT_STREQ(path.c_str(), expected.c_str());

  FileFsPath::BuildPhysicalPath("/prefix/", "/sufix", path);
  ASSERT_STREQ(path.c_str(), expected.c_str());
}

EOSCOMMONTESTING_END
