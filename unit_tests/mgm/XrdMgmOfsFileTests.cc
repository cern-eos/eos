//------------------------------------------------------------------------------
// File: XrdMgmFileOfsTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "namespace/interface/IFileMD.hh"
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#define IN_TEST_HARNESS
#include "mgm/XrdMgmOfsFile.hh"
#undef IN_TEST_HARNESS
#include "gtest/gtest.h"

using namespace eos::mgm;

TEST(XrdMgmOfsFile, ParsingExcludFsids)
{
  XrdMgmOfsFile f;
  std::string opaque_info =
    "eos.excludefsid=2,4,6,8,10,144&eos.ruid=0&eos.rgid=0";
  f.openOpaque = new XrdOucEnv(opaque_info.c_str());
  std::vector<unsigned int> expect_result {2, 4, 6, 8, 10, 144};
  auto result = f.GetExcludedFsids();
  ASSERT_EQ(result.size(), expect_result.size());

  for (const auto& elem : expect_result) {
    ASSERT_TRUE(std::find(result.begin(), result.end(), elem) != result.end());
  }
}

TEST(XrdMgmOfsFile, GetClientApplicationName)
{
  ASSERT_STREQ("", XrdMgmOfsFile::GetClientApplicationName(nullptr, nullptr).c_str());
  std::string opaque_str = "&key1=val1&key2=val2&key3=val3";
  XrdOucEnv env(opaque_str.c_str());
  XrdSecEntity client("test");
  ASSERT_STREQ("",
               XrdMgmOfsFile::GetClientApplicationName(&env, &client).c_str());
  client.eaAPI->Add("xrd.appname", "xrd_tag");
  ASSERT_STREQ("xrd_tag",
               XrdMgmOfsFile::GetClientApplicationName(&env, &client).c_str());
  opaque_str = "&key1=val1&key2=val2&key3=val3&eos.app=eos_tag";
  XrdOucEnv env1(opaque_str.c_str());
  ASSERT_STREQ("eos_tag",
               XrdMgmOfsFile::GetClientApplicationName(&env1, &client).c_str());
  ASSERT_STREQ("eos_tag",
               XrdMgmOfsFile::GetClientApplicationName(&env1, nullptr).c_str());
}
