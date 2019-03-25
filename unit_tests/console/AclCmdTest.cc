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
#include "console/MgmExecute.hh"

EOSMGMNAMESPACE_BEGIN

TEST(MgmExecute, ResponseParsingFull)
{
  MgmExecute exec;
  ASSERT_EQ(exec.process("mgm.proc.stdout=123&mgm.proc.stderr=345&mgm.proc.retc=3"), 3);
  ASSERT_EQ(exec.GetResult(), "123");
  ASSERT_EQ(exec.GetError(), "345");
  ASSERT_EQ(exec.GetErrc(), 3);
}

TEST(MgmExecute, ResponseParsingStdoutAndErrc)
{
  MgmExecute exec;
  ASSERT_EQ(exec.process("mgm.proc.stdout=123&mgm.proc.retc=999"), 999);
  ASSERT_EQ(exec.GetResult(), "123");
  ASSERT_EQ(exec.GetError(), "");
  ASSERT_EQ(exec.GetErrc(), 999);
}

TEST(MgmExecute, ResponseParsingStderrAndErrc)
{
  MgmExecute exec;
  ASSERT_EQ(exec.process("&mgm.proc.stderr=this is stderr&mgm.proc.retc=2"), 2);
  ASSERT_EQ(exec.GetResult(), "");
  ASSERT_EQ(exec.GetError(), "this is stderr");
  ASSERT_EQ(exec.GetErrc(), 2);
}

TEST(MgmExecute, ResponseParsingEmptyStdout)
{
  MgmExecute exec;
  ASSERT_EQ(exec.process("mgm.proc.stdout=&mgm.proc.stderr=345&mgm.proc.retc=3"), 3);
  ASSERT_EQ(exec.GetResult(), "");
  ASSERT_EQ(exec.GetError(), "345");
  ASSERT_EQ(exec.GetErrc(), 3);
}

TEST(MgmExecute, ResponseParsingEmptyStderr)
{
  MgmExecute exec;
  ASSERT_EQ(exec.process("mgm.proc.stdout=123&mgm.proc.stderr=&mgm.proc.retc=3"), 3);
  ASSERT_EQ(exec.GetResult(), "123");
  ASSERT_EQ(exec.GetError(), "");
  ASSERT_EQ(exec.GetErrc(), 3);
}

TEST(MgmExecute, ResponseParsingPlain)
{
  MgmExecute exec;
  ASSERT_EQ(exec.process("aaaaaaa"), 0);
  ASSERT_EQ(exec.GetResult(), "aaaaaaa");
  ASSERT_EQ(exec.GetError(), "");
  ASSERT_EQ(exec.GetErrc(), 0);
}

TEST(MgmExecute, SimpleSimulation)
{
  // Note: This only tests the faking capabilities of MgmExecute.
  MgmExecute exec;
  std::string message;

  exec.InjectSimulated("mgm.cmd=ayy&mgm.subcmd=lmao", {"12345"} );
  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));
  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy&mgm.subcmd=lmao", true), 0);
  ASSERT_EQ(exec.GetResult(), "12345");
  ASSERT_EQ(exec.GetError(), "");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_TRUE(exec.CheckSimulationSuccessful(message));
}

TEST(MgmExecute, ComplexSimulation)
{
  // Note: This only tests the faking capabilities of MgmExecute.
  MgmExecute exec;
  std::string message;

  exec.InjectSimulated("mgm.cmd=ayy1&mgm.subcmd=lmao1", {"12345", "some error"} );
  exec.InjectSimulated("mgm.cmd=ayy2&mgm.subcmd=lmao2", {"23456"} );
  exec.InjectSimulated("mgm.cmd=ayy2&mgm.subcmd=lmao2", {"999", "error 2"} );
  exec.InjectSimulated("mgm.cmd=ayy3&mgm.subcmd=lmao3", {"888", "error 3", 987} );
  exec.InjectSimulated("mgm.cmd=ayy1&mgm.subcmd=lmao1", {"234567"} );
  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy1&mgm.subcmd=lmao1", true), 0);
  ASSERT_EQ(exec.GetResult(), "12345");
  ASSERT_EQ(exec.GetError(), "some error");
  ASSERT_EQ(exec.GetErrc(), 0);

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy2&mgm.subcmd=lmao2", true), 0);
  ASSERT_EQ(exec.GetResult(), "23456");
  ASSERT_EQ(exec.GetError(), "");
  ASSERT_EQ(exec.GetErrc(), 0);

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy2&mgm.subcmd=lmao2", true), 0);
  ASSERT_EQ(exec.GetResult(), "999");
  ASSERT_EQ(exec.GetError(), "error 2");
  ASSERT_EQ(exec.GetErrc(), 0);

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy3&mgm.subcmd=lmao3", true), 987);
  ASSERT_EQ(exec.GetResult(), "888");
  ASSERT_EQ(exec.GetError(), "error 3");
  ASSERT_EQ(exec.GetErrc(), 987);

  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy1&mgm.subcmd=lmao1", true), 0);
  ASSERT_EQ(exec.GetResult(), "234567");
  ASSERT_EQ(exec.GetError(), "");
  ASSERT_EQ(exec.GetErrc(), 0);

  ASSERT_TRUE(exec.CheckSimulationSuccessful(message));
}

TEST(MgmExecute, FailedSimulation)
{
  // Note: This only tests the faking capabilities of MgmExecute.
  MgmExecute exec;
  std::string message;

  exec.InjectSimulated("mgm.cmd=ayy1&mgm.subcmd=lmao1", {"12345", "some error"} );
  exec.InjectSimulated("mgm.cmd=ayy2&mgm.subcmd=lmao2", {"23456"} );

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy1&mgm.subcmd=lmao1", true), 0);
  ASSERT_EQ(exec.GetResult(), "12345");
  ASSERT_EQ(exec.GetError(), "some error");
  ASSERT_EQ(exec.GetErrc(), 0);

  ASSERT_EQ(exec.ExecuteCommand("mgm.cmd=ayy3&mgm.subcmd=lmao3", true), EIO);
  std::cout << message << std::endl;
  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));
}

TEST(AclCmd, CheckId)
{
  eos::console::RequestProto req;
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

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
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

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
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

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
