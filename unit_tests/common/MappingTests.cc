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

#include <gtest/gtest.h>
#include "Namespace.hh"
#include "common/Mapping.hh"

EOSCOMMONTESTING_BEGIN

TEST(Mapping, VidAssignOperator)
{
  using namespace eos::common;
  Mapping::VirtualIdentity vid;
  vid.uid = 99;
  vid.gid = 99;
  vid.uid_string = "99";
  vid.gid_string = "99";
  vid.uid_list = {2, 3, 99};
  vid.gid_list = {2, 4, 99};
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
  Mapping::VirtualIdentity copy_vid = vid;
  ASSERT_TRUE(vid.uid == copy_vid.uid);
  ASSERT_TRUE(vid.gid == copy_vid.gid);
  ASSERT_TRUE(vid.uid_string == copy_vid.uid_string);
  ASSERT_TRUE(vid.gid_string == copy_vid.gid_string);
  ASSERT_TRUE(vid.uid_list.size() == copy_vid.uid_list.size());

  for (const auto& elem : vid.uid_list) {
    ASSERT_TRUE(std::find(copy_vid.uid_list.begin(), copy_vid.uid_list.end(),
                          elem) != copy_vid.uid_list.end());
  }

  ASSERT_TRUE(vid.gid_list.size() == copy_vid.gid_list.size());

  for (const auto& elem : vid.gid_list) {
    ASSERT_TRUE(std::find(copy_vid.gid_list.begin(), copy_vid.gid_list.end(),
                          elem) != copy_vid.gid_list.end());
  }

  ASSERT_TRUE(vid.tident == copy_vid.tident);
  ASSERT_TRUE(vid.name == copy_vid.name);
  ASSERT_TRUE(vid.prot == copy_vid.prot);
  ASSERT_TRUE(vid.host == copy_vid.host);
  ASSERT_TRUE(vid.grps == copy_vid.grps);
  ASSERT_TRUE(vid.role == copy_vid.role);
  ASSERT_TRUE(vid.dn == copy_vid.dn);
  ASSERT_TRUE(vid.geolocation == copy_vid.geolocation);
  ASSERT_TRUE(vid.app == copy_vid.app);
  ASSERT_TRUE(vid.sudoer == copy_vid.sudoer);
}

EOSCOMMONTESTING_END
