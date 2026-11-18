//------------------------------------------------------------------------------
// File: SecEntityTests.cc
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

#include "common/SecEntity.hh"
#include "gtest/gtest.h"
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <string>

using eos::common::SecEntity;

TEST(SecEntity, GetRequestNameWithoutEntity)
{
  std::string name;
  EXPECT_FALSE(SecEntity::GetRequestName(nullptr, name));
  EXPECT_TRUE(name.empty());
}

TEST(SecEntity, GetRequestNameWithoutAttribute)
{
  XrdSecEntity entity("https");
  std::string name;
  EXPECT_FALSE(SecEntity::GetRequestName(&entity, name));
  EXPECT_TRUE(name.empty());
}

TEST(SecEntity, GetRequestNameWithUserName)
{
  XrdSecEntity entity("https");
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "alice", true));
  std::string name;
  EXPECT_TRUE(SecEntity::GetRequestName(&entity, name));
  EXPECT_EQ("alice", name);
}

// An empty user name is no user name: the caller must fall back to the
// entity's own identity.
TEST(SecEntity, GetRequestNameWithEmptyUserName)
{
  XrdSecEntity entity("https");
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "", true));
  std::string name;
  EXPECT_FALSE(SecEntity::GetRequestName(&entity, name));
  EXPECT_TRUE(name.empty());
}

// The value set last counts.
TEST(SecEntity, GetRequestNameAfterReplacement)
{
  XrdSecEntity entity("https");
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "alice", true));
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "", true));
  std::string name;
  EXPECT_FALSE(SecEntity::GetRequestName(&entity, name));
  EXPECT_TRUE(name.empty());
}

TEST(SecEntity, ClearRequestNameWithoutEntity) { SecEntity::ClearRequestName(nullptr); }

TEST(SecEntity, ClearRequestNameRemovesTheUserName)
{
  XrdSecEntity entity("https");
  ASSERT_TRUE(entity.eaAPI->Add("request.name", "alice", true));
  SecEntity::ClearRequestName(&entity);
  std::string name;
  EXPECT_FALSE(SecEntity::GetRequestName(&entity, name));
  EXPECT_TRUE(name.empty());
}
