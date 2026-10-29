//------------------------------------------------------------------------------
// File: JwtTokenTests.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "common/token/EosTok.hh"
#include "common/token/JwtTok.hh"
#include "gtest/gtest.h"
#include <algorithm>

TEST(JwtTokenTests, RoundTrip)
{
  eos::common::JwtTok token;
  token.SetPath("/eos/token/test/", true);
  token.SetPermission("rwx!d");
  token.SetOwner("myuser");
  token.SetGroup("mygroup");
  token.SetExpires(time(nullptr) + 60);
  token.SetGeneration(1);
  token.SetRequester("uid:0");
  token.AddOrigin("host(.*)", "(.*)", "sss");
  ASSERT_EQ(token.ValidateOrigins(), 0);

  const std::string key = "1234567890";
  const std::string wire = token.Write(key);
  ASSERT_FALSE(wire.empty());
  ASSERT_TRUE(eos::common::JwtTok::IsEosJwt(wire));
  ASSERT_TRUE(eos::common::JwtTok::IsEosJwt(std::string("Bearer ") + wire));
  ASSERT_TRUE(eos::common::EosTok::IsEosCredential(wire));
  ASSERT_TRUE(eos::common::EosTok::IsEosTokenPath((std::string("/") + wire).c_str()));
  ASSERT_EQ(std::count(wire.begin(), wire.end(), '.'), 2);
  ASSERT_EQ(wire.find("wlcg.ver"), std::string::npos);

  eos::common::JwtTok decoded;
  ASSERT_EQ(decoded.Read(wire, key, 1, false), 0);
  ASSERT_TRUE(decoded.Valid());
  ASSERT_EQ(decoded.Path(), "/eos/token/test/");
  ASSERT_TRUE(decoded.TreeToken());
  ASSERT_EQ(decoded.Permission(), "rwx!d");
  ASSERT_EQ(decoded.Owner(), "myuser");
  ASSERT_EQ(decoded.Group(), "mygroup");
  ASSERT_EQ(decoded.Requester(), "uid:0");
  ASSERT_EQ(decoded.Generation(), 1);
  ASSERT_EQ(decoded.Voucher(), token.Voucher());
  ASSERT_EQ(decoded.ValidatePath("/eos/token/test/file"), 0);
  ASSERT_NE(decoded.ValidatePath("/eos/other"), 0);
  ASSERT_EQ(decoded.VerifyOrigin("host.cern.ch", "root", "sss"), 0);
  ASSERT_NE(decoded.VerifyOrigin("tosty.cern.ch", "root", "sss"), 0);

  ASSERT_EQ(decoded.Read(std::string("Bearer ") + wire, key, 1, false), 0);
  ASSERT_NE(decoded.Read(wire, key + "z", 1, false), 0);
  ASSERT_NE(decoded.Read(wire, key, 2, false), 0);

  std::string faulty = wire;
  faulty[faulty.size() - 1] = (faulty.back() == 'a') ? 'b' : 'a';
  ASSERT_NE(decoded.Read(faulty, key, 1, false), 0);
}

TEST(JwtTokenTests, ExactPathAndExpiry)
{
  eos::common::JwtTok token;
  token.SetPath("/eos/token/file", false);
  token.SetPermission("rx");
  token.SetExpires(time(nullptr) - 5);
  token.SetGeneration(0);
  const std::string key = "1234567890";
  const std::string wire = token.Write(key);
  ASSERT_FALSE(wire.empty());

  eos::common::JwtTok decoded;
  ASSERT_NE(decoded.Read(wire, key, 0, false), 0);
  ASSERT_EQ(decoded.Read(wire, key, 0, true), 0);
  ASSERT_EQ(decoded.ValidatePath("/eos/token/file"), 0);
  ASSERT_NE(decoded.ValidatePath("/eos/token/file/extra"), 0);
  ASSERT_FALSE(decoded.TreeToken());
}

TEST(JwtTokenTests, RejectsOtherJwts)
{
  const std::string other =
    "eyJhbGciOiJub25lIn0."
    "eyJpc3MiOiJodHRwczovL2RlbW8uc2NpdG9rZW5zLm9yZyIsInN1YiI6ImpvZSIsImV4cCI6NDEwMjQ0NDgwMH0.";
  ASSERT_FALSE(eos::common::JwtTok::IsEosJwt(other));
  ASSERT_FALSE(eos::common::JwtTok::IsEosJwt("zteos64:abc"));
  ASSERT_TRUE(eos::common::EosTok::IsEosCredential("zteos64:abc"));
  ASSERT_TRUE(eos::common::EosTok::IsEosCredential("Bearer zteos64:abc"));
  ASSERT_TRUE(eos::common::EosTok::IsEosTokenPath("/zteos64:abc"));
  ASSERT_FALSE(eos::common::EosTok::IsEosTokenPath("/eos/file"));
  ASSERT_FALSE(eos::common::EosTok::IsEosTokenPath(nullptr));
}

TEST(JwtTokenTests, InvalidOrigin)
{
  eos::common::JwtTok token;
  token.AddOrigin("[", "(.*)", "(.*)");
  ASSERT_NE(token.ValidateOrigins(), 0);
}
