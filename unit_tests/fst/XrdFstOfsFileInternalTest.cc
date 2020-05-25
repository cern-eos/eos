//------------------------------------------------------------------------------
// File: XrdFstOfsFileTest.cc
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

#define IN_TEST_HARNESS
#include "fst/XrdFstOfsFile.hh"
#include "fst/XrdFstOfs.hh"
#undef IN_TEST_HARNESS
#include <memory>
#include "gtest/gtest.h"

using namespace eos::fst;

TEST(XrdFstOfsFileTest, FilterTags)
{
  std::set<std::string> tags {"xrdcl.secuid", "xrdcl.secgid"};
  std::string opaque =
    "eos.app=demo&oss.size=13&xrdcl.secuid=2134&xrdcl.secgid=99";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(), "eos.app=demo&oss.size=13");
  opaque = "eos.app=demo&oss.size=13";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(), "eos.app=demo&oss.size=13");
  opaque = "eos.app=demo&oss.size=13&xrdcl.secuid=2134&xrdcl.secgid=99&"
           "xrdcl.other=tag&eos.lfn=/some/dummy/path&";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(),
               "eos.app=demo&oss.size=13&xrdcl.other=tag&eos.lfn=/some/dummy/path");
  opaque = "";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(), "");
}

TEST(XrdFstOfsFileTest, GetHostFromTident)
{
  std::string hostname;
  std::string tident = "root.1.2@eospps.cern.ch";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root@eospps.ipv6.cern.ch";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root.1.1@eospps.dyndns.some.other.ipv6.cern.ch";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root.1.1@eospps";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root.1.1_eospps.dyndns.some.other.ipv6.cern.ch";
  ASSERT_FALSE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "");
  tident = "root.1.1@";
  ASSERT_FALSE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "");
  tident = "root.1.1";
  ASSERT_FALSE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "");
}
