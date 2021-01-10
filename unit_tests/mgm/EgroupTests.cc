//------------------------------------------------------------------------------
// File: EgroupTests.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
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
#include "mgm/Egroup.hh"

using namespace eos::mgm;
using namespace eos::common;

//------------------------------------------------------------------------------
// Test basic Egroup functionality
//------------------------------------------------------------------------------
TEST(Egroup, Functional) {
  //----------------------------------------------------------------------------
  // Yeah, yeah, this is not a unit test.. and maybe not appropriate to contact
  // the CERN LDAP server from here, who knows where this might be executing.
  // Feel free to delete the entire test if it creates problems.
  //----------------------------------------------------------------------------
  Egroup egroup;
  ASSERT_TRUE(egroup.Member("esindril", "it-dep"));
  ASSERT_FALSE(egroup.Member("esindril", "be-dep"));
  ASSERT_FALSE(egroup.Member("this-user-does-not-exist", "it-dep"));
  ASSERT_FALSE(egroup.Member("esindril", "this-group-does-not-exist"));
  ASSERT_TRUE(egroup.Member("esindril", "cern-accounts-primary"));
}

TEST(Egroup, BasicSanity) {
  SteadyClock clock(true);
  Egroup egroup(&clock);

  egroup.inject("user1", "awesome-users", Egroup::Status::kMember);
  egroup.inject("user2", "groovy-users", Egroup::Status::kMember);
  egroup.inject("user3", "awesome-users", Egroup::Status::kMember);
  egroup.inject("user3", "groovy-users", Egroup::Status::kMember);

  ASSERT_EQ(egroup.DumpMember("user1", "awesome-users"),
    "egroup=awesome-users user=user1 member=true lifetime=1800");

  ASSERT_EQ(egroup.DumpMember("user1", "groovy-users"),
    "egroup=groovy-users user=user1 member=false lifetime=1800");

  ASSERT_EQ(egroup.DumpMember("user2", "groovy-users"),
    "egroup=groovy-users user=user2 member=true lifetime=1800");

  ASSERT_EQ(egroup.DumpMember("user2", "awesome-users"),
    "egroup=awesome-users user=user2 member=false lifetime=1800");

  ASSERT_EQ(egroup.DumpMember("user3", "groovy-users"),
    "egroup=groovy-users user=user3 member=true lifetime=1800");

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=1800");

  clock.advance(std::chrono::seconds(10));

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=1790");

  clock.advance(std::chrono::seconds(1789));

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=1");

  clock.advance(std::chrono::seconds(1));

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=0");

  clock.advance(std::chrono::seconds(1));

  // cache update, wait until the asynchronous thread makes progress
  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=-1");

  while(egroup.getPendingQueueSize() != 0) {}

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=1800");

  // By official decree, user3 is no longer awesome. The cache will take a
  // while to reflect this, though.
  egroup.inject("user3", "awesome-users", Egroup::Status::kNotMember);

  clock.advance(std::chrono::seconds(100));

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=1700");

  clock.advance(std::chrono::seconds(10000));

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=true lifetime=-8300");

  while(egroup.getPendingQueueSize() != 0) { }

  ASSERT_EQ(egroup.DumpMember("user3", "awesome-users"),
    "egroup=awesome-users user=user3 member=false lifetime=1800");

  ASSERT_EQ(egroup.DumpMembers(),
    "egroup=awesome-users user=user1 member=true lifetime=-10101\n"
    "egroup=awesome-users user=user2 member=false lifetime=-10101\n"
    "egroup=awesome-users user=user3 member=false lifetime=1800\n"
    "egroup=groovy-users user=user1 member=false lifetime=-10101\n"
    "egroup=groovy-users user=user2 member=true lifetime=-10101\n"
    "egroup=groovy-users user=user3 member=true lifetime=-10101\n");
}

TEST(Egroup, ExplicitRefresh) {
  SteadyClock clock(true);
  Egroup egroup(&clock);

  egroup.inject("user1", "awesome-users", Egroup::Status::kNotMember);

  ASSERT_EQ(egroup.DumpMember("user1", "awesome-users"),
    "egroup=awesome-users user=user1 member=false lifetime=1800");

  clock.advance(std::chrono::seconds(10));

  ASSERT_EQ(egroup.DumpMember("user1", "awesome-users"),
    "egroup=awesome-users user=user1 member=false lifetime=1790");

  egroup.inject("user1", "awesome-users", Egroup::Status::kMember);

  clock.advance(std::chrono::seconds(10));

  ASSERT_EQ(egroup.DumpMember("user1", "awesome-users"),
    "egroup=awesome-users user=user1 member=false lifetime=1780");

  egroup.refresh("user1", "awesome-users");

  ASSERT_EQ(egroup.DumpMember("user1", "awesome-users"),
    "egroup=awesome-users user=user1 member=true lifetime=1800");
}

