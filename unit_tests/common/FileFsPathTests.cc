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
  rc = FileFsPath::GetPhysicalPath(1, emptyFmd, path);
  ASSERT_STREQ(path.c_str(), "");
  ASSERT_EQ(rc, -1);

  // No extended attribute present
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", fidPath);
  fidPath.erasefromstart(8);
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());

  // Empty local prefix
  rc = FileFsPath::GetFullPhysicalPath(1, fmd, 0, path);
  ASSERT_STREQ(path.c_str(), "");
  ASSERT_EQ(rc, -1);

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

  // No logical path
  ASSERT_FALSE(FileFsPath::HasLogicalPath(1, fmd));

  // Single logical path
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_TRUE(FileFsPath::HasLogicalPath(1, fmd));
  ASSERT_STREQ(path.c_str(), "path1");

  // Overwrite logical path
  FileFsPath::StorePhysicalPath(1, fmd, "path2");
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");

  // Retrieve physical path from fid
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", fidPath);
  fidPath.erasefromstart(8);
  FileFsPath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());

  // Multiple logical paths
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  FileFsPath::StorePhysicalPath(2, fmd, "path2");
  FileFsPath::StorePhysicalPath(3, fmd, "path3");
  FileFsPath::StorePhysicalPath(3, fmd, "path3");
  ASSERT_TRUE(FileFsPath::HasLogicalPath(1, fmd) &&
              FileFsPath::HasLogicalPath(2, fmd) &&
              FileFsPath::HasLogicalPath(3, fmd));
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
// Test logical path removal
//------------------------------------------------------------------------------
TEST(FileFsPath, LogicalPathRemoval)
{
  std::shared_ptr<eos::IFileMDSvc> fileSvc = 0;
  std::shared_ptr<eos::IFileMD> fmd =
      std::make_shared<eos::FileMD>(1, fileSvc.get());
  XrdOucString path, fidPath;

  // Generate path from fid
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(1).c_str(), "/prefix/", fidPath);
  fidPath.erasefromstart(8);

  // Store single logical path
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  ASSERT_TRUE(FileFsPath::HasLogicalPath(1, fmd));
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");

  // Remove single logical path
  FileFsPath::RemovePhysicalPath(1, fmd);
  ASSERT_FALSE(FileFsPath::HasLogicalPath(1, fmd));
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_FALSE(fmd->hasAttribute("sys.eos.lpath"));

  // Attempt removal on empty logical path mapping
  FileFsPath::RemovePhysicalPath(1, fmd);
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_FALSE(fmd->hasAttribute("sys.eos.lpath"));

  // Attempt removal of nonexistent logical path
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  FileFsPath::RemovePhysicalPath(2, fmd);
  FileFsPath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_TRUE(fmd->hasAttribute("sys.eos.lpath"));
  ASSERT_FALSE(FileFsPath::HasLogicalPath(2, fmd));

  // Store multiple logical paths
  FileFsPath::StorePhysicalPath(1, fmd, "path1");
  FileFsPath::StorePhysicalPath(2, fmd, "path2");
  FileFsPath::StorePhysicalPath(3, fmd, "path3");

  // Remove logical paths one by one
  FileFsPath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");
  FileFsPath::RemovePhysicalPath(2, fmd);
  FileFsPath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_TRUE(fmd->hasAttribute("sys.eos.lpath"));

  FileFsPath::GetPhysicalPath(3, fmd, path);
  ASSERT_STREQ(path.c_str(), "path3");
  FileFsPath::RemovePhysicalPath(3, fmd);
  FileFsPath::GetPhysicalPath(3, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_TRUE(fmd->hasAttribute("sys.eos.lpath"));

  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");
  FileFsPath::RemovePhysicalPath(1, fmd);
  FileFsPath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_FALSE(fmd->hasAttribute("sys.eos.lpath"));
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
