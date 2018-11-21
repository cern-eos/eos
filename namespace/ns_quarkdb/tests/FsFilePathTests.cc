//------------------------------------------------------------------------------
// File: FsFilePathTests.cc
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

#include <gtest/gtest.h>
#include "TestUtils.hh"
#include "Namespace.hh"
#include "MockFileMDSvc.hh"
#include "common/FileId.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/utils/FsFilePath.hh"

EOSNSTESTING_BEGIN

class FsFilePathTests : public eos::ns::testing::NsTestsFixture {};

//------------------------------------------------------------------------------
// Test input validation
//------------------------------------------------------------------------------
TEST_F(FsFilePathTests, InputValidation)
{
  std::shared_ptr<eos::IFileMD> fmd = view()->createFile("/file.txt");
  ASSERT_EQ(fmd->getId(), 1);

  std::string hexstring = eos::common::FileId::Fid2Hex(fmd->getId());
  std::shared_ptr<eos::IFileMD> emptyFmd = 0;
  XrdOucString fidPath, path = "initial";
  int rc;

  // Empty file metadata
  rc = FsFilePath::GetPhysicalPath(1, emptyFmd, path);
  ASSERT_STREQ(path.c_str(), "");
  ASSERT_EQ(rc, -1);

  // No extended attribute present
  eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), "/prefix/",
                                          fidPath);
  fidPath.erasefromstart(8);
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());

  // Empty local prefix
  rc = FsFilePath::GetFullPhysicalPath(1, fmd, 0, path);
  ASSERT_STREQ(path.c_str(), "");
  ASSERT_EQ(rc, -1);

  // Empty file metadata
  rc = FsFilePath::GetFullPhysicalPath(1, emptyFmd, "/prefix/", path);
  ASSERT_STREQ(path.c_str(), "");
  ASSERT_EQ(rc, -1);
}

//------------------------------------------------------------------------------
// Test logical path storage and retrieval
//------------------------------------------------------------------------------
TEST_F(FsFilePathTests, LogicalPath)
{
  std::shared_ptr<eos::IFileMD> fmd = view()->createFile("/file.txt");
  ASSERT_EQ(fmd->getId(), 1);

  std::string hexstring = eos::common::FileId::Fid2Hex(fmd->getId());
  XrdOucString path, fidPath;

  // No logical path
  ASSERT_FALSE(FsFilePath::HasLogicalPath(1, fmd));

  // Single logical path
  FsFilePath::StorePhysicalPath(1, fmd, "path1");
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_TRUE(FsFilePath::HasLogicalPath(1, fmd));
  ASSERT_STREQ(path.c_str(), "path1");

  // Overwrite logical path
  FsFilePath::StorePhysicalPath(1, fmd, "path2");
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");

  // Retrieve physical path from fid
  eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), "/prefix/",
                                          fidPath);
  fidPath.erasefromstart(8);
  FsFilePath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());

  // Multiple logical paths
  FsFilePath::StorePhysicalPath(1, fmd, "path1");
  FsFilePath::StorePhysicalPath(2, fmd, "path2");
  FsFilePath::StorePhysicalPath(3, fmd, "path3");
  FsFilePath::StorePhysicalPath(3, fmd, "path3");
  ASSERT_TRUE(FsFilePath::HasLogicalPath(1, fmd) &&
              FsFilePath::HasLogicalPath(2, fmd) &&
              FsFilePath::HasLogicalPath(3, fmd));
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");
  FsFilePath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");
  FsFilePath::GetPhysicalPath(3, fmd, path);
  ASSERT_STREQ(path.c_str(), "path3");

  // Retrieve full path
  FsFilePath::GetFullPhysicalPath(1, fmd, "/prefix/", path);
  ASSERT_STREQ(path.c_str(), "/prefix/path1");
}

//------------------------------------------------------------------------------
// Test logical path removal
//------------------------------------------------------------------------------
TEST_F(FsFilePathTests, LogicalPathRemoval)
{
  std::shared_ptr<eos::IFileMD> fmd = view()->createFile("/file.txt");
  ASSERT_EQ(fmd->getId(), 1);

  std::string hexstring = eos::common::FileId::Fid2Hex(fmd->getId());
  XrdOucString path, fidPath;

  // Generate path from fid
  eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), "/prefix/",
                                          fidPath);
  fidPath.erasefromstart(8);

  // Store single logical path
  FsFilePath::StorePhysicalPath(1, fmd, "path1");
  ASSERT_TRUE(FsFilePath::HasLogicalPath(1, fmd));
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");

  // Remove single logical path
  FsFilePath::RemovePhysicalPath(1, fmd);
  ASSERT_FALSE(FsFilePath::HasLogicalPath(1, fmd));
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_FALSE(fmd->hasAttribute("sys.eos.lpath"));

  // Attempt removal on empty logical path mapping
  FsFilePath::RemovePhysicalPath(1, fmd);
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_FALSE(fmd->hasAttribute("sys.eos.lpath"));

  // Attempt removal of nonexistent logical path
  FsFilePath::StorePhysicalPath(1, fmd, "path1");
  FsFilePath::RemovePhysicalPath(2, fmd);
  FsFilePath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_TRUE(fmd->hasAttribute("sys.eos.lpath"));
  ASSERT_FALSE(FsFilePath::HasLogicalPath(2, fmd));

  // Store multiple logical paths
  FsFilePath::StorePhysicalPath(1, fmd, "path1");
  FsFilePath::StorePhysicalPath(2, fmd, "path2");
  FsFilePath::StorePhysicalPath(3, fmd, "path3");

  // Remove logical paths one by one
  FsFilePath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), "path2");
  FsFilePath::RemovePhysicalPath(2, fmd);
  FsFilePath::GetPhysicalPath(2, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_TRUE(fmd->hasAttribute("sys.eos.lpath"));

  FsFilePath::GetPhysicalPath(3, fmd, path);
  ASSERT_STREQ(path.c_str(), "path3");
  FsFilePath::RemovePhysicalPath(3, fmd);
  FsFilePath::GetPhysicalPath(3, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_TRUE(fmd->hasAttribute("sys.eos.lpath"));

  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), "path1");
  FsFilePath::RemovePhysicalPath(1, fmd);
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), fidPath.c_str());
  ASSERT_FALSE(fmd->hasAttribute("sys.eos.lpath"));
}

//------------------------------------------------------------------------------
// Test path-from-fid generation
//------------------------------------------------------------------------------
TEST_F(FsFilePathTests, PathFromFid)
{
  std::shared_ptr<eos::IFileMD> fmd = view()->createFile("/file.txt");
  ASSERT_EQ(fmd->getId(), 1);

  std::string hexstring = eos::common::FileId::Fid2Hex(fmd->getId()).c_str();
  XrdOucString path, expected;

  // Path from fid
  eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), "/prefix/",
                                          expected);
  expected.erasefromstart(8);
  FsFilePath::GetPhysicalPath(1, fmd, path);
  ASSERT_STREQ(path.c_str(), expected.c_str());

  // Full path from fid
  eos::common::FileId::FidPrefix2FullPath(hexstring.c_str(), "/prefix/",
                                          expected);
  FsFilePath::GetFullPhysicalPath(1, fmd, "/prefix/", path);
  ASSERT_STREQ(path.c_str(), expected.c_str());
}

EOSNSTESTING_END
