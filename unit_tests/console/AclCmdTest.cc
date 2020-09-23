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
#define IN_TEST_HARNESS
#include "console/commands/helpers/AclHelper.hh"
#undef IN_TEST_HARNESS

EOSMGMNAMESPACE_BEGIN

GlobalOptions opts;

TEST(ICmdHelper, ResponseParsingFull)
{
  // Any helper would do
  AclHelper exec(opts);
  ASSERT_EQ(
    exec.ProcessResponse("mgm.proc.stdout=123&mgm.proc.stderr=345&mgm.proc.retc=3"),
    3);
  ASSERT_EQ(exec.GetResult(), "123\n");
  ASSERT_EQ(exec.GetError(), "345\n");
  ASSERT_EQ(exec.GetErrc(), 3);
}

TEST(ICmdHelper, ResponseParsingStdoutAndErrc)
{
  AclHelper exec(opts);
  ASSERT_EQ(exec.ProcessResponse("mgm.proc.stdout=123&mgm.proc.retc=999"), 999);
  ASSERT_EQ(exec.GetResult(), "123\n");
  ASSERT_EQ(exec.GetError(), "\n");
  ASSERT_EQ(exec.GetErrc(), 999);
}

TEST(ICmdHelper, ResponseParsingStderrAndErrc)
{
  AclHelper exec(opts);
  ASSERT_EQ(
    exec.ProcessResponse("&mgm.proc.stderr=this is stderr&mgm.proc.retc=2"), 2);
  ASSERT_EQ(exec.GetResult(), "\n");
  ASSERT_EQ(exec.GetError(), "this is stderr\n");
  ASSERT_EQ(exec.GetErrc(), 2);
}

TEST(ICmdHelper, ResponseParsingEmptyStdout)
{
  AclHelper exec(opts);
  ASSERT_EQ(
    exec.ProcessResponse("mgm.proc.stdout=&mgm.proc.stderr=345&mgm.proc.retc=3"),
    3);
  ASSERT_EQ(exec.GetResult(), "\n");
  ASSERT_EQ(exec.GetError(), "345\n");
  ASSERT_EQ(exec.GetErrc(), 3);
}

TEST(ICmdHelper, ResponseParsingEmptyStderr)
{
  AclHelper exec(opts);
  ASSERT_EQ(
    exec.ProcessResponse("mgm.proc.stdout=123&mgm.proc.stderr=&mgm.proc.retc=3"),
    3);
  ASSERT_EQ(exec.GetResult(), "123\n");
  ASSERT_EQ(exec.GetError(), "\n");
  ASSERT_EQ(exec.GetErrc(), 3);
}

TEST(ICmdHelper, ResponseParsingPlain)
{
  AclHelper exec(opts);
  ASSERT_EQ(exec.ProcessResponse("aaaaaaa"), 0);
  ASSERT_EQ(exec.GetResult(), "aaaaaaa\n");
  ASSERT_EQ(exec.GetError(), "\n");
  ASSERT_EQ(exec.GetErrc(), 0);
}

TEST(ICmdHelper, SimpleSimulation)
{
  // Note: This only tests the faking capabilities of ICmdHelper.
  AclHelper exec(opts);
  std::string message;
  exec.InjectSimulated("mgm.cmd=ayy&mgm.subcmd=lmao", {"12345"});
  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy&mgm.subcmd=lmao"), 0);
  ASSERT_EQ(exec.GetResult(), "12345\n");
  ASSERT_EQ(exec.GetError(), "\n");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_TRUE(exec.CheckSimulationSuccessful(message));
}

TEST(ICmdHelper, ComplexSimulation)
{
  // Note: This only tests the faking capabilities of ICmdHelper.
  AclHelper exec(opts);
  std::string message;
  exec.InjectSimulated("mgm.cmd=ayy1&mgm.subcmd=lmao1", {"12345", "some error"});
  exec.InjectSimulated("mgm.cmd=ayy2&mgm.subcmd=lmao2", {"23456"});
  exec.InjectSimulated("mgm.cmd=ayy2&mgm.subcmd=lmao2", {"999", "error 2"});
  exec.InjectSimulated("mgm.cmd=ayy3&mgm.subcmd=lmao3", {"888", "error 3", 987});
  exec.InjectSimulated("mgm.cmd=ayy1&mgm.subcmd=lmao1", {"234567"});
  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy1&mgm.subcmd=lmao1"), 0);
  ASSERT_EQ(exec.GetResult(), "12345\n");
  ASSERT_EQ(exec.GetError(), "some error\n");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy2&mgm.subcmd=lmao2"), 0);
  ASSERT_EQ(exec.GetResult(), "23456\n");
  ASSERT_EQ(exec.GetError(), "\n");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy2&mgm.subcmd=lmao2"), 0);
  ASSERT_EQ(exec.GetResult(), "999\n");
  ASSERT_EQ(exec.GetError(), "error 2\n");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy3&mgm.subcmd=lmao3"), 987);
  ASSERT_EQ(exec.GetResult(), "888\n");
  ASSERT_EQ(exec.GetError(), "error 3\n");
  ASSERT_EQ(exec.GetErrc(), 987);
  ASSERT_FALSE(exec.CheckSimulationSuccessful(message));
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy1&mgm.subcmd=lmao1"), 0);
  ASSERT_EQ(exec.GetResult(), "234567\n");
  ASSERT_EQ(exec.GetError(), "\n");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_TRUE(exec.CheckSimulationSuccessful(message));
}

TEST(ICmdHelper, FailedSimulation)
{
  // Note: This only tests the faking capabilities of ICmdHelper.
  AclHelper exec(opts);
  std::string message;
  exec.InjectSimulated("mgm.cmd=ayy1&mgm.subcmd=lmao1", {"12345", "some error"});
  exec.InjectSimulated("mgm.cmd=ayy2&mgm.subcmd=lmao2", {"23456"});
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy1&mgm.subcmd=lmao1"), 0);
  ASSERT_EQ(exec.GetResult(), "12345\n");
  ASSERT_EQ(exec.GetError(), "some error\n");
  ASSERT_EQ(exec.GetErrc(), 0);
  ASSERT_EQ(exec.RawExecute("mgm.cmd=ayy3&mgm.subcmd=lmao3"), EIO);
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

TEST(AclHelper, TestParseCommand)
{
  AclHelper acl(opts);
  ASSERT_EQ(acl.ParseCommand("--sys u:1001:-w /eos/test"), true);
  ASSERT_EQ(acl.ParseCommand("--user u:1001:-w /eos/test"), true);
  ASSERT_EQ(acl.ParseCommand("--sys -l /eos/test"), true);
  ASSERT_EQ(acl.ParseCommand("--user -lR /eos/test"), true);
  ASSERT_EQ(acl.ParseCommand("--sys u:1001:-w /eos/test"), true);
  ASSERT_EQ(acl.ParseCommand("--user -R --recursive u:1001:-w /eos/test"), true);
  ASSERT_EQ(acl.ParseCommand("-FD --recursive u:1001:-w /eos/test"), false);
  ASSERT_EQ(acl.ParseCommand("-Rgg --recursive u:1001:-w /eos/test"), false);
}

EOSMGMNAMESPACE_END
