//------------------------------------------------------------------------------
// File: MappingTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "common/Mapping.hh"

EOSCOMMONTESTING_BEGIN

TEST(Mapping, VidAssignOperator)
{
  using namespace eos::common;
  VirtualIdentity vid;
  vid.uid = 99;
  vid.gid = 99;
  vid.uid_string = "99";
  vid.gid_string = "99";
  vid.allowed_uids = {2, 3, 99};
  vid.allowed_gids = {2, 4, 99};
  vid.tident = "client:process_id:sockd_fd";
  vid.name = "dummy_user";
  vid.prot = "xrootd";
  vid.host = "localhost.localdomain";
  vid.grps = "some_random_grps";
  vid.role = "some_random_role";
  vid.dn = "some_random_dn";
  vid.geolocation = "some_random_geoloacation";
  vid.app = "some_random_app";
  vid.sudoer = true;
  VirtualIdentity copy_vid = vid;
  ASSERT_EQ(vid.uid, copy_vid.uid);
  ASSERT_EQ(vid.gid, copy_vid.gid);
  ASSERT_EQ(vid.uid_string, copy_vid.uid_string);
  ASSERT_EQ(vid.gid_string, copy_vid.gid_string);
  ASSERT_EQ(vid.allowed_uids, copy_vid.allowed_uids);
  ASSERT_EQ(vid.allowed_gids, copy_vid.allowed_gids);
  ASSERT_STREQ(vid.tident.c_str(), copy_vid.tident.c_str());
  ASSERT_STREQ(vid.name.c_str(),  copy_vid.name.c_str());
  ASSERT_STREQ(vid.prot.c_str(), copy_vid.prot.c_str());
  ASSERT_EQ(vid.host, copy_vid.host);
  ASSERT_EQ(vid.grps, copy_vid.grps);
  ASSERT_EQ(vid.role, copy_vid.role);
  ASSERT_EQ(vid.dn, copy_vid.dn);
  ASSERT_EQ(vid.geolocation, copy_vid.geolocation);
  ASSERT_EQ(vid.app, copy_vid.app);
  ASSERT_EQ(vid.sudoer, copy_vid.sudoer);
  ASSERT_TRUE(vid.hasUid(2));
  ASSERT_TRUE(copy_vid.hasUid(2));
  ASSERT_TRUE(vid.hasUid(3));
  ASSERT_TRUE(copy_vid.hasUid(3));
  ASSERT_TRUE(vid.hasUid(99));
  ASSERT_TRUE(copy_vid.hasUid(99));
  ASSERT_FALSE(vid.hasUid(4));
  ASSERT_FALSE(copy_vid.hasUid(4));
  ASSERT_TRUE(vid.hasGid(4));
  ASSERT_TRUE(copy_vid.hasGid(4));
  ASSERT_FALSE(vid.hasGid(3));
  ASSERT_FALSE(copy_vid.hasGid(3));
}

TEST(VirtualIdentity, IsLocalhost)
{
  VirtualIdentity vid;
  vid.host = "localhost";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "localhost6";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "localhost.localdomain";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "localhost6.localdomain6";
  ASSERT_TRUE(vid.isLocalhost());
  vid.host = "pickles";
  ASSERT_FALSE(vid.isLocalhost());
  vid.host = "asdf";
  ASSERT_FALSE(vid.isLocalhost());
}

EOSCOMMONTESTING_END
