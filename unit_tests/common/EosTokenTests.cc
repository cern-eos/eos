//------------------------------------------------------------------------------
// File: EosTokenTests.cc
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
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
#include "common/token/EosTok.hh"

//------------------------------------------------------------------------------
// Test Tokens
//------------------------------------------------------------------------------
TEST(EosTokenTests, Tokens) {
  eos::common::EosTok token;
  std::string path = "/eos/token/test/";
  token.SetPath(path, true);
  std::string perm = "rwx";
  token.SetPermission(perm);
  std::string owner = "myuser";
  std::string group = "mygroup";
  token.SetOwner(owner);
  token.SetGroup(group);
  time_t now = time(NULL);
  token.SetExpires(now + 10);
  token.SetGeneration(0);
  std::string dump;
  token.Dump(dump);
  token.AddOrigin("*","*","*");
  
  //  std::cout << dump.c_str() << std::endl;
  std::string key = "1234567890";
  std::string btoken = token.Write(key);

  token.Dump(dump);
  //  std::cout << dump << btoken << std::endl;

  eos::common::EosTok reversetoken;
  
  // Test Token writing
  ASSERT_TRUE(reversetoken.Read(btoken, key, 0)==0);

  std::string reversedump;
  reversetoken.Dump(reversedump);
  //  std::cout << reversedump << std::endl;

  // Test Token reading
  ASSERT_EQ(dump, reversedump);

  reversetoken.Reset();
  reversetoken.Dump(reversedump);
  //  std::cout << reversedump << std::endl;

  std::string expectreversedump = 
    "{\n"
    " \"signature\": \"\",\n"
    " \"serialized\": \"\",\n"
    " \"seed\": 0\n"
    "}\n";


  // Test Token Reset
  ASSERT_EQ(reversedump, expectreversedump);

  // Test Token Reading with new generation
  ASSERT_TRUE(reversetoken.Read(btoken, key, 1)!=0);

  // Test Token Reading with wrong key
  ASSERT_TRUE(reversetoken.Read(btoken, key+"z", 0)!=0);

  // Test Token Reading with wrong token
  std::string faultytoken = btoken.erase(10,1);
  ASSERT_TRUE(reversetoken.Read(faultytoken, key, 0)!=0);


  // Test Token expired;
  now = time(NULL);
  token.SetExpires(now+1);
  btoken = token.Write(key);
  ASSERT_TRUE(reversetoken.Read(btoken, key, 0)==0);
  sleep(2);
  ASSERT_TRUE(reversetoken.Read(btoken, key, 0)!=0);
}


TEST(EosTokenTests, Origins) {
  eos::common::EosTok token;

  ASSERT_TRUE(!token.VerifyOrigin("eos.cern.ch","admin","sss"));
  token.AddOrigin("(.*)","(.*)","(.*)");

  ASSERT_TRUE(!token.VerifyOrigin("eos.cern.ch","root","sss"));
  
  token.Reset();
  
  token.AddOrigin("host(.*)", "(.*)","(.*)");
  ASSERT_TRUE(!token.VerifyOrigin("host.cern.ch","root","sss"));
  ASSERT_TRUE(!token.VerifyOrigin("hosty.cern.ch","root","sss"));
  ASSERT_TRUE(token.VerifyOrigin("tosty.cern.ch","root","sss"));
}
