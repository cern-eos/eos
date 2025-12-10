//------------------------------------------------------------------------------
//! @file FileCmdTest.cc
//! @author Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 ************************************************************************/

#include "gtest/gtest.h"
#include "mgm/proc/user/FileCmd.hh"

#define IN_TEST_HARNESS
#include "console/commands/helpers/FileHelper.hh"
#undef IN_TEST_HARNESS

EOSMGMNAMESPACE_BEGIN

GlobalOptions gGlobalOpts;

//------------------------------------------------------------------------------
// FileHelper parsing tests
//------------------------------------------------------------------------------

TEST(FileHelper, ParseCommandBasic)
{
  FileHelper file(gGlobalOpts);
  EXPECT_TRUE(file.ParseCommand("info /eos/test/file"));
}

TEST(FileHelper, ParseCommandWithFid)
{
  FileHelper file(gGlobalOpts);
  EXPECT_TRUE(file.ParseCommand("info fid:123456"));
  EXPECT_TRUE(file.ParseCommand("info fxid:1a2b3c"));
}

TEST(FileHelper, ParseCommandInvalid)
{
  FileHelper file(gGlobalOpts);
  EXPECT_FALSE(file.ParseCommand(""));
  EXPECT_FALSE(file.ParseCommand("unknowncmd /eos/test"));
  EXPECT_FALSE(file.ParseCommand("copy onlyoneside"));
}

TEST(FileHelper, ParseCommandCopy)
{
  FileHelper file(gGlobalOpts);
  EXPECT_TRUE(file.ParseCommand("copy /eos/src /eos/dst"));
  EXPECT_TRUE(file.ParseCommand("copy -f -s /eos/src /eos/dst"));
  EXPECT_FALSE(file.ParseCommand("copy -Z /eos/src /eos/dst"));
}

//------------------------------------------------------------------------------
// ICmdHelper simulation
//------------------------------------------------------------------------------

TEST(FileHelper, SimpleSimulation)
{
  FileHelper file(gGlobalOpts);
  std::string message;
  file.InjectSimulated("mgm.cmd=file&mgm.subcmd=info", {"OK"});
  ASSERT_FALSE(file.CheckSimulationSuccessful(message));
  ASSERT_EQ(file.RawExecute("mgm.cmd=file&mgm.subcmd=info"), 0);
  EXPECT_EQ(file.GetResult(), "OK\n");
  EXPECT_EQ(file.GetError(), "\n");
  EXPECT_EQ(file.GetErrc(), 0);
  ASSERT_TRUE(file.CheckSimulationSuccessful(message));
}

TEST(FileHelper, ComplexSimulation)
{
  FileHelper file(gGlobalOpts);
  std::string message;
  file.InjectSimulated("mgm.cmd=file&mgm.subcmd=info", {"123"});
  file.InjectSimulated("mgm.cmd=file&mgm.subcmd=info", {"456", "warning"});
  file.InjectSimulated("mgm.cmd=file&mgm.subcmd=copy", {"", "copy failed", EIO});
  ASSERT_EQ(file.RawExecute("mgm.cmd=file&mgm.subcmd=info"), 0);
  EXPECT_EQ(file.GetResult(), "123\n");
  EXPECT_EQ(file.GetError(), "\n");
  ASSERT_EQ(file.RawExecute("mgm.cmd=file&mgm.subcmd=info"), 0);
  EXPECT_EQ(file.GetResult(), "456\n");
  EXPECT_EQ(file.GetError(), "warning\n");
  ASSERT_EQ(file.RawExecute("mgm.cmd=file&mgm.subcmd=copy"), EIO);
  EXPECT_EQ(file.GetError(), "copy failed\n");
  EXPECT_EQ(file.GetErrc(), EIO);
  ASSERT_TRUE(file.CheckSimulationSuccessful(message));
}

TEST(FileHelper, FailedSimulation)
{
  FileHelper file(gGlobalOpts);
  std::string message;
  file.InjectSimulated("mgm.cmd=file&mgm.subcmd=info", {"123"});
  ASSERT_EQ(file.RawExecute("mgm.cmd=file&mgm.subcmd=info"), 0);
  // no simulation registered
  ASSERT_EQ(file.RawExecute("mgm.cmd=file&mgm.subcmd=drop"), EIO);
  ASSERT_FALSE(file.CheckSimulationSuccessful(message));
}

//------------------------------------------------------------------------------
// Specific subcommand parsing
//------------------------------------------------------------------------------

TEST(FileHelper, TouchCommand)
{
  FileHelper file(gGlobalOpts);
  EXPECT_TRUE(file.ParseCommand("touch /eos/test/file"));
  EXPECT_TRUE(file.ParseCommand("touch -0 /eos/test/file"));
  EXPECT_TRUE(file.ParseCommand("touch -a /eos/test/file /external/file"));
}

TEST(FileHelper, VerifyCommand)
{
  FileHelper file(gGlobalOpts);
  EXPECT_TRUE(file.ParseCommand("verify /eos/test/file"));
  EXPECT_TRUE(file.ParseCommand("verify /eos/test/file -checksum"));
  EXPECT_TRUE(file.ParseCommand("verify fid:123 -commitchecksum"));
  EXPECT_FALSE(file.ParseCommand("verify -Z /eos/test/file"));
}

EOSMGMNAMESPACE_END
