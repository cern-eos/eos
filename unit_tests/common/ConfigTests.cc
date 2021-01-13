//------------------------------------------------------------------------------
// File: ConfigTests.cc
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "common/Config.hh"
#include "common/StringConversion.hh"

//------------------------------------------------------------------------------
// Test Tokens
//------------------------------------------------------------------------------
TEST(ConfigTests, Configs) {
  system("mkdir -p /etc/eos/config/test/");
  std::string configname = "/etc/eos/config/test/default";

  std::string s;
  s = "[global]\nfirst line\nsecond line\nthird line\n[test]\nverify";

  eos::common::StringConversion::SaveStringIntoFile(configname.c_str(), s);

  eos::common::Config cfg;

  cfg.Load("failing","default");

  ASSERT_TRUE(cfg.getErrc() == 2);
  ASSERT_EQ(cfg.getMsg(),"error: unable to load '/etc/eos/config/failing/default' : No such file or directory");
  ASSERT_TRUE(!cfg.ok());

  cfg.Load("test","default");

  ASSERT_TRUE(cfg.ok());
  ASSERT_TRUE( (cfg["test"].size() == 1) && (cfg["test"][0] == "verify"));
  ASSERT_TRUE(cfg["global"].size() == 3);

  eos::common::Config cfgenoent;
  cfgenoent.Load("test","faulty");

  ASSERT_TRUE(!cfgenoent.ok());

  eos::common::Config cfgempty;
  s = "line without chapter";

  eos::common::StringConversion::SaveStringIntoFile(configname.c_str(), s);
  cfgempty.Load("test","default");

  ASSERT_TRUE(!cfgempty.ok());

  eos::common::Config cfgsub;
  s = "[sysconfig]\na=100\nb=$a\nc=$b\n[xconf]\n$a $b c d";

  eos::common::StringConversion::SaveStringIntoFile(configname.c_str(), s);
  cfgsub.Load("test","default");

  std::string r = cfgsub.Dump("xconf", true);
  ASSERT_TRUE( r == "100 100 c d\n");

  char** env = cfgsub.Env("sysconfig");

  ASSERT_TRUE( std::string(env[0]) == "a=100" );
  ASSERT_TRUE( std::string(env[1]) == "b=100" );
  ASSERT_TRUE( std::string(env[2]) == "c=100" );
  ASSERT_TRUE( env[3] == 0 );

  std::string s1="ASDF";
  std::string s2="$ASDF";
  std::string s3="${ASDF}";
  std::string s4="1234${ASDF}1234";
  std::string s5="1234${ASDF";
  std::string s6="123456${ASDF}";
  std::string s7="123456$ASDF 1234";
  size_t p1,p2;

  s = cfgsub.ParseVariable(s1,p1,p2);
  ASSERT_TRUE( s.empty() );
  ASSERT_EQ(p1,0);
  ASSERT_EQ(p2,0);

  s = cfgsub.ParseVariable(s2,p1,p2);
  ASSERT_TRUE( s == "ASDF" );
  ASSERT_EQ(p1,0);
  ASSERT_EQ(p2,5);

  s = cfgsub.ParseVariable(s3,p1,p2);
  ASSERT_TRUE( s == "ASDF" );
  ASSERT_EQ(p1,0);
  ASSERT_EQ(p2,7);

  s = cfgsub.ParseVariable(s4,p1,p2);
  ASSERT_TRUE( s == "ASDF" );
  ASSERT_EQ(p1,4);
  ASSERT_EQ(p2,11);

  s = cfgsub.ParseVariable(s5,p1,p2);
  ASSERT_TRUE( s.empty() );
  ASSERT_EQ(p1,0);
  ASSERT_EQ(p2,0);

  s = cfgsub.ParseVariable(s6,p1,p2);
  ASSERT_TRUE( s == "ASDF" );
  ASSERT_EQ(p1,6);
  ASSERT_EQ(p2,13);

  s = cfgsub.ParseVariable(s7,p1,p2);
  ASSERT_TRUE( s == "ASDF" );
  ASSERT_EQ(p1,6);
  ASSERT_EQ(p2,11);
}
