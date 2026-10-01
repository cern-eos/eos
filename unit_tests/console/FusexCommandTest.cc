//------------------------------------------------------------------------------
//! @file FusexCommandTest.cc
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

#include "console/CommandFramework.hh"
#include "gtest/gtest.h"

#include <cerrno>
#include <utility>

void RegisterFusexNativeCommand();

namespace {
class FusexCommandTest : public ::testing::Test {
protected:
  static void
  SetUpTestSuite()
  {
    RegisterFusexNativeCommand();
  }

  void
  SetUp() override
  {
    instance = this;
    savedRetc = global_retc;
    command = CommandRegistry::instance().find("fusex");
    ASSERT_NE(command, nullptr);
    context.clientCommand = [](XrdOucString& in, bool admin, std::string*) {
      instance->request = in.c_str();
      instance->isAdmin = admin;
      ++instance->requestCount;
      return static_cast<XrdOucEnv*>(nullptr);
    };
    context.outputResult = [](XrdOucEnv*, bool) { return 0; };
  }

  void
  TearDown() override
  {
    global_retc = savedRetc;
    instance = nullptr;
  }

  void
  run(const std::vector<std::string>& args)
  {
    request.clear();
    requestCount = 0;
    global_retc = 0;
    EXPECT_EQ(command->run(args, context), 0);
  }

  static FusexCommandTest* instance;
  CommandContext context{};
  IConsoleCommand* command = nullptr;
  std::string request;
  bool isAdmin = false;
  unsigned requestCount = 0;
  int savedRetc = 0;
};

FusexCommandTest* FusexCommandTest::instance = nullptr;
} // namespace

TEST_F(FusexCommandTest, DefaultListing)
{
  run({"ls"});
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(requestCount, 1u);
  EXPECT_TRUE(isAdmin);
  EXPECT_EQ(request, "mgm.cmd=fusex&mgm.subcmd=ls");
}

TEST_F(FusexCommandTest, ForwardsEachListingFlag)
{
  for (const auto* flag : {"-m", "-l", "-f", "-k"}) {
    SCOPED_TRACE(flag);
    run({"ls", flag});
    EXPECT_EQ(global_retc, 0);
    EXPECT_EQ(requestCount, 1u);
    EXPECT_TRUE(isAdmin);
    EXPECT_EQ(request, std::string("mgm.cmd=fusex&mgm.subcmd=ls&mgm.option=") + flag[1]);
  }
}

TEST_F(FusexCommandTest, ForwardsCombinedListingFlags)
{
  const std::vector<std::pair<std::vector<std::string>, std::string>> cases = {
      {{"ls", "-m", "-l"}, "lm"},
      {{"ls", "-ml"}, "lm"},
      {{"ls", "-m", "-k", "-f", "-l"}, "lfkm"},
      {{"ls", "-lfkm"}, "lfkm"},
      {{"ls", "-m", "-m"}, "m"}};
  for (const auto& item : cases) {
    SCOPED_TRACE(::testing::PrintToString(item.first));
    run(item.first);
    EXPECT_EQ(global_retc, 0);
    EXPECT_EQ(requestCount, 1u);
    EXPECT_EQ(request, "mgm.cmd=fusex&mgm.subcmd=ls&mgm.option=" + item.second);
  }
}

TEST_F(FusexCommandTest, RejectsInvalidListingArgumentsWithoutSendingRequest)
{
  for (const auto* argument : {"-x", "--unknown", "unexpected", "-mx"}) {
    SCOPED_TRACE(argument);
    run({"ls", argument});
    EXPECT_EQ(global_retc, EINVAL);
    EXPECT_EQ(requestCount, 0u);
    EXPECT_TRUE(request.empty());
  }
  run({"ls", "-m", "unexpected"});
  EXPECT_EQ(global_retc, EINVAL);
  EXPECT_EQ(requestCount, 0u);
}

TEST_F(FusexCommandTest, PreservesOtherSubcommandArguments)
{
  run({"caps", "-i", "^0000abcd$"});
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(
      request,
      "mgm.cmd=fusex&mgm.subcmd=caps&mgm.option=i&mgm.filter=/#curl#%5E0000abcd%24");
  run({"conf", "10", "30", "256", "@b[67]"});
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(request,
            "mgm.cmd=fusex&mgm.subcmd=conf&mgm.fusex.hb=10&mgm.fusex.qc=30&mgm.fusex.bc."
            "max=256&mgm.fusex.bc.match.set=1&mgm.fusex.bc.match=@b[67]");
}
