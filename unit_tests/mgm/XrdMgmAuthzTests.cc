//------------------------------------------------------------------------------
// File: XrdMgmAuthzTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "mgm/authz/XrdMgmAuthz.hh"
#include "gtest/gtest.h"
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>

// XrdMgmAuthz is the last plugin of the token authorization chain. It
// authorizes a client that has a name of its own, or a user name that a token
// plugin put in the "request.name" attribute.

TEST(XrdMgmAuthz, NoEntityIsRefused)
{
  XrdMgmAuthz authz;
  EXPECT_EQ(XrdAccPriv_None, authz.Access(nullptr, "/eos/f", AOP_Read, nullptr));
}

TEST(XrdMgmAuthz, EntityWithNameIsAllowed)
{
  XrdMgmAuthz authz;
  XrdSecEntity entity("https");
  char name[] = "alice";
  entity.name = name;
  EXPECT_EQ(XrdAccPriv_All, authz.Access(&entity, "/eos/f", AOP_Read, nullptr));
}

TEST(XrdMgmAuthz, EntityWithoutNameIsRefused)
{
  XrdMgmAuthz authz;
  XrdSecEntity entity("https");
  EXPECT_EQ(XrdAccPriv_None, authz.Access(&entity, "/eos/f", AOP_Read, nullptr));
}

TEST(XrdMgmAuthz, EntityWithTokenUserNameIsAllowed)
{
  XrdMgmAuthz authz;
  XrdSecEntity entity("https");
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "alice", true));
  EXPECT_EQ(XrdAccPriv_All, authz.Access(&entity, "/eos/f", AOP_Read, nullptr));
}

// An empty token user name is no user name.
TEST(XrdMgmAuthz, EntityWithEmptyTokenUserNameIsRefused)
{
  XrdMgmAuthz authz;
  XrdSecEntity entity("https");
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "", true));
  EXPECT_EQ(XrdAccPriv_None, authz.Access(&entity, "/eos/f", AOP_Read, nullptr));
}
