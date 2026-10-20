//------------------------------------------------------------------------------
//! @file TokenHelperTest.cc
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

#include "console/commands/helpers/TokenHelper.hh"
#include "gtest/gtest.h"

TEST(TokenHelper, MissingScopeOperandIsRejected)
{
  const GlobalOptions opts;

  for (const char* command :
       {"--scope", "--path /eos/ --scope", "--path /eos/ --scope   ",
        "--path /eos/ --owner 0 --group 0 --tree --scope",
        "--path /eos/ --scope grpc.quota.read --scope", "--path /eos/ --scope \"\""}) {
    SCOPED_TRACE(command);
    TokenHelper helper(opts);
    EXPECT_FALSE(helper.ParseCommand(command));
  }
}

TEST(TokenHelper, QuotaScopesArePreserved)
{
  const GlobalOptions opts;
  TokenHelper helper(opts);
  ASSERT_TRUE(helper.ParseCommand("--path /eos/ --owner 0 --group 0 --tree "
                                  "--scope grpc.quota.read "
                                  "--scope grpc.quota.write"));
  const auto& token = helper.GetRequest().token();
  ASSERT_EQ(2, token.scopes_size());
  EXPECT_EQ("grpc.quota.read", token.scopes(0));
  EXPECT_EQ("grpc.quota.write", token.scopes(1));
  EXPECT_EQ("0", token.owner());
  EXPECT_EQ("0", token.group());
  EXPECT_TRUE(token.allowtree());
}

TEST(TokenHelper, UnknownOptionsAreRejected)
{
  const GlobalOptions opts;

  for (const char* command :
       {"--path /eos/ --scopes grpc.quota.read", "--path /eos/ --Scope grpc.quota.read",
        "--path /eos/ -scope grpc.quota.read", "--path /eos/ scope grpc.quota.read",
        "--path /eos/ --scope grpc.quota.read --unknown value"}) {
    SCOPED_TRACE(command);
    TokenHelper helper(opts);
    EXPECT_FALSE(helper.ParseCommand(command));
  }
}

TEST(TokenHelper, LegacyUnscopedTokensAreUnchanged)
{
  const GlobalOptions opts;
  TokenHelper helper(opts);
  ASSERT_TRUE(helper.ParseCommand("--path /eos/ --owner 0 --group 0 --tree"));
  EXPECT_EQ(0, helper.GetRequest().token().scopes_size());
}
