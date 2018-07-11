//------------------------------------------------------------------------------
//! @file AclCmdTest.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#include "mgm/proc/user/AclCmd.hh"

EOSMGMNAMESPACE_BEGIN

TEST(AclCmd, CheckId)
{
  eos::console::RequestProto req;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);

  AclCmd test(std::move(req), vid);
  ASSERT_TRUE(test.CheckCorrectId("u:user"));
  ASSERT_TRUE(test.CheckCorrectId("g:group"));
  ASSERT_TRUE(test.CheckCorrectId("egroup:gssroup"));
  ASSERT_FALSE(test.CheckCorrectId("gr:gro@up"));
  ASSERT_FALSE(test.CheckCorrectId("ug:group"));
  ASSERT_FALSE(test.CheckCorrectId(":a$4uggroup"));
  ASSERT_FALSE(test.CheckCorrectId("egro:gro"));
}

TEST(AclCmd, GetRuleBitmask)
{
  eos::console::RequestProto req;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);

  AclCmd test(std::move(req), vid);

  ASSERT_TRUE(test.GetRuleBitmask("wr!u+d-!u", true));
  ASSERT_EQ(test.GetAddRule(), 67u);
  ASSERT_EQ(test.GetRmRule(), 128u);

  ASSERT_TRUE(test.GetRuleBitmask("+++++++d!urwxxxxxx!u", true));
  ASSERT_EQ(test.GetAddRule(), 199u);
  ASSERT_EQ(test.GetRmRule(), 0u);

  ASSERT_TRUE(test.GetRuleBitmask("+rw+d-!u", false));
  ASSERT_EQ(test.GetAddRule(), 67u);
  ASSERT_EQ(test.GetRmRule(), 128u);

  ASSERT_TRUE(test.GetRuleBitmask("+rw+d-!u", false));
  ASSERT_EQ(test.GetAddRule(), 67u);
  ASSERT_EQ(test.GetRmRule(), 128u);

  ASSERT_FALSE(test.GetRuleBitmask("+rw!u+d-!u$%@", false));
  ASSERT_FALSE(test.GetRuleBitmask("rw!u+d-!u", false));
}

TEST(AclCmd, AclRuleFromString)
{
  // Method AclRuleFromString is called to parse acl data which MGM Node
  // sends. So string in incorrect format is not possible, hence there is
  // no checking for that.
  Rule temp;

  eos::console::RequestProto req;
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);

  AclCmd test(std::move(req), vid);
  temp = test.GetRuleFromString("u:user1:rwx!u");
  ASSERT_EQ(temp.first, "u:user1");
  ASSERT_EQ(temp.second, 135);
  temp = test.GetRuleFromString("g:group1:wx!u");
  ASSERT_EQ(temp.first, "g:group1");
  ASSERT_EQ(temp.second, 134);
  temp = test.GetRuleFromString("egroup:group1:rx!u");
  ASSERT_EQ(temp.first, "egroup:group1");
  ASSERT_EQ(temp.second, 133);
}

EOSMGMNAMESPACE_END
