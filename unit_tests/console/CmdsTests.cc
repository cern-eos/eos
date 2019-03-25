//------------------------------------------------------------------------------
//! @file CmdsTests.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "console/commands/helpers/AclHelper.hh"
#include "console/commands/helpers/RecycleHelper.hh"
#include "unistd.h"

TEST(AclHelper, RouteFromPathAppeneded)
{
  AclHelper acl;
  acl.ParseCommand("--user u:1001=rwx /eos/devtest/");
  acl.mMgmExec.InjectSimulated("mgm.cmd.proto=Eh0IAioKdToxMDAxPXJ3eDINL2Vvcy9kZXZ0ZXN0Lw==&eos.route=/eos/devtest/", {"", "", 0});
  ASSERT_EQ(acl.Execute(true, true), 0);
  // Setting EOSHOME env variable should make no difference
  setenv("EOSHOME", "/eos/home/test/", 1);
  acl.mMgmExec.InjectSimulated("mgm.cmd.proto=Eh0IAioKdToxMDAxPXJ3eDINL2Vvcy9kZXZ0ZXN0Lw==&eos.route=/eos/devtest/", {"", "", 0});
  ASSERT_EQ(acl.Execute(true, true), 0);
  unsetenv("EOSHOME");
}

TEST(RecycleHelper, RouteFromEnvAppended)
{
  // By default /eos/user/username[0]/username is added to the eos.route
  const std::string username = cuserid(nullptr);
  std::ostringstream oss_route;
  oss_route << "/eos/user/" << username[0] << "/" << username << "/";
  RecycleHelper recycle;
  recycle.ParseCommand("ls");

  if (getenv("USER")) {
    recycle.mMgmExec.InjectSimulated("mgm.cmd.proto=UgQKAggB&eos.route=" +
                                     oss_route.str(),
    {"", "", 0});
  } else {
    // Inside the docker container the USER env is not set
    recycle.mMgmExec.InjectSimulated("mgm.cmd.proto=UgQKAggB", {"", "", 0});
  }

  ASSERT_EQ(recycle.Execute(false, true), 0);
  // Setting EOSHOME env variable should update the eos.route
  setenv("EOSHOME", "/eos/home/test/", 1);
  recycle.mMgmExec.InjectSimulated("mgm.cmd.proto=UgQKAggB&eos.route=/eos/home/test/",
  {"", "", 0});
  ASSERT_EQ(recycle.Execute(false, true), 0);
  unsetenv("EOSHOME");
  // Setting EOSUSER env variable should update eos.route to point to the old
  // /eos/user/username[0]/username/ where username=getenv("EOSUSER")
  setenv("EOSUSER", "dummy", 1);
  unsetenv("USER"); // otherwise USER has precedence
  recycle.mMgmExec.InjectSimulated("mgm.cmd.proto=UgQKAggB&eos.route=/eos/user/d/dummy/",
  {"", "", 0});
  ASSERT_EQ(recycle.Execute(false, true), 0);
  unsetenv("EOSUSER");
  // The same should happend if USER is set
  setenv("USER", "other_dummy", 1);
  recycle.mMgmExec.InjectSimulated("mgm.cmd.proto=UgQKAggB&eos.route=/eos/user/o/other_dummy/",
  {"", "", 0});
  ASSERT_EQ(recycle.Execute(false, true), 0);
  unsetenv("USER");
}
