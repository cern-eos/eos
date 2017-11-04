//------------------------------------------------------------------------------
// File: process-info.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
#include "auth/ProcessInfo.hh"

TEST(ProcessInfoProvider, BasicSanity)
{
  std::string
  sampleProc("10823 (zsh) S 10815 10823 10823 34819 10874 4194304 3022 2685 0 0 8 4 0 0 20 0 1 0 70104 47996928 1870 18446744073709551615 93955198316544 93955199085420 140720349285888 0 0 0 2 3686404 134295555 1 0 0 17 1 0 0 0 0 0 93955201186664 93955201214728 93955201884160 140720349292924 140720349292928 140720349292928 140720349294575 0");
  ProcessInfo pinfo;
  ASSERT_TRUE(ProcessInfoProvider::fromString(sampleProc, "", pinfo));
  ASSERT_EQ(pinfo.getPid(), 10823);
  ASSERT_EQ(pinfo.getParentId(), 10815);
  ASSERT_EQ(pinfo.getSid(), 10823);
  ASSERT_EQ(pinfo.getStartTime(), 70104);
}

TEST(ProcessInfoProvider, BasicSanity2)
{
  std::string
  sampleProc("9631 (vim) S 9593 9631 9593 34825 9631 4194304 1731 0 0 0 18 1 0 0 20 0 1 0 28017391 186519552 4535 18446744073709551615 94905521688576 94905524542468 140735046390256 0 0 0 0 12288 1837256447 1 0 0 17 0 0 0 0 0 0 94905526642120 94905526801172 94905547104256 140735046398239 140735046398243 140735046398243 140735046402027 0");
  ProcessInfo pinfo;
  ASSERT_TRUE(ProcessInfoProvider::fromString(sampleProc, "", pinfo));
  ASSERT_EQ(pinfo.getPid(), 9631);
  ASSERT_EQ(pinfo.getParentId(), 9593);
  ASSERT_EQ(pinfo.getSid(), 9593);
  ASSERT_EQ(pinfo.getStartTime(), 28017391);
}

TEST(ProcessInfoProvider, ParseBroken)
{
  ProcessInfo pinfo;
  std::string broken1("9631 (vim) S 9593 9631 9593");
  ASSERT_FALSE(ProcessInfoProvider::fromString(broken1, "", pinfo));
  std::string broken2("adfadfasd");
  ASSERT_FALSE(ProcessInfoProvider::fromString(broken2, "", pinfo));
  std::string
  broken3("9631 (vim) S 9593 9631 9593 34825 9631 4194304 1731 0 0 0 18 1 0 0 20 0 1 0");
  ASSERT_FALSE(ProcessInfoProvider::fromString(broken3, "", pinfo));
  ASSERT_TRUE(pinfo.isEmpty());
  std::string
  good("9631 (vim) S 9593 9631 9593 34825 9631 4194304 1731 0 0 0 18 1 0 0 20 0 1 0 28017391");
  ASSERT_TRUE(ProcessInfoProvider::fromString(good, "", pinfo));
  ASSERT_EQ(pinfo.getPid(), 9631);
  ASSERT_EQ(pinfo.getParentId(), 9593);
  ASSERT_EQ(pinfo.getSid(), 9593);
  ASSERT_EQ(pinfo.getStartTime(), 28017391);
}

TEST(ProcessInfoProvider, ParseCmdline)
{
  ProcessInfo pinfo;
  std::string
  sampleProc("23829 (vim) S 23713 23829 23713 34817 23829 4194304 8131 917 0 0 26 4 0 0 20 0 1 0 28202761 187371520 4651 18446744073709551615 94763168460800 94763171314692 140721547023136 0 0 0 0 12288 1837256447 1 0 0 17 1 0 0 0 0 0 94763173414344 94763173573396 94763190026240 140721547026699 140721547026715 140721547026715 140721547030507 0");
  std::string cmdline(SSTR("vim" << '\0' << "eos.spec.in"));
  ASSERT_TRUE(ProcessInfoProvider::fromString(sampleProc, cmdline, pinfo));
  ASSERT_EQ(pinfo.getPid(), 23829);
  ASSERT_EQ(pinfo.getParentId(), 23713);
  ASSERT_EQ(pinfo.getSid(), 23713);
  ASSERT_EQ(pinfo.getStartTime(), 28202761);
  std::vector<std::string> tmp { "vim", "eos.spec.in" };
  ASSERT_EQ(pinfo.getCmd(), tmp);
}

#ifndef __APPLE__
TEST(ProcessInfoProvider, GetMyProcessInfo)
{
  ProcessInfo myself;
  ASSERT_TRUE(ProcessInfoProvider::retrieveFull(getpid(), myself));
  ASSERT_FALSE(myself.isEmpty());
  ASSERT_THROW(ProcessInfoProvider::retrieveFull(getpid(), myself),
               FatalException);
  ProcessInfo parent;
  ASSERT_TRUE(ProcessInfoProvider::retrieveFull(getppid(), parent));
  ASSERT_EQ(myself.getParentId(), parent.getPid());
  std::cerr << "My cmdline: " << myself.cmdStr << std::endl;
  std::cerr << "Parent's cmdline: " << parent.cmdStr << std::endl;
}
#endif
