/************************************************************************
  * EOS - the CERN Disk Storage System                                   *
  * Copyright (C) 2022 CERN/Switzerland                           *
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
  ************************************************************************
*/

#include "mgm/utils/AttrHelper.hh"
#include "mgm/Constants.hh"
#include "gtest/gtest.h"

using namespace eos::mgm;
using eos::mgm::attr::checkStickyDirOwner;

TEST(checkStickyDirOwner, EmptyMap)
{
  eos::common::VirtualIdentity vid;
  ASSERT_FALSE(checkStickyDirOwner({}, {}, {}, vid, nullptr));
}

TEST(checkStickyDirOwner, StickyOwner)
{
  eos::common::VirtualIdentity vid;
  eos::IContainerMD::XAttrMap xattrs {{SYS_OWNER_AUTH, "*"}};
  ASSERT_TRUE(checkStickyDirOwner(xattrs, {}, {}, vid, nullptr));
}

TEST(checkStickyDirOwner, dirOwner)
{
  eos::common::VirtualIdentity vid;
  vid.uid = 23;
  vid.gid = 23;
  uid_t dir_uid = 46;
  gid_t dir_gid = 46;

  vid.prot = "krb5";
  vid.uid_string = "testuser";
  eos::IContainerMD::XAttrMap  xattrs {{SYS_OWNER_AUTH, "sss:operator,krb5:testuser"}};
  ASSERT_FALSE(checkStickyDirOwner(xattrs, dir_uid, dir_gid, vid, nullptr));
  ASSERT_EQ(vid.uid, dir_uid);
  ASSERT_EQ(vid.gid, dir_gid);
}
