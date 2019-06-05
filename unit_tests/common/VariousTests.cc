//------------------------------------------------------------------------------
// File: VariousTests.cc
// Author: Georgios Bitzes - CERN
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

#include "common/Statfs.hh"
#include "common/FileSystem.hh"
#include "Namespace.hh"
#include "gtest/gtest.h"
#include <list>

EOSCOMMONTESTING_BEGIN

TEST(StatFs, BasicSanity)
{
  std::unique_ptr<eos::common::Statfs> statfs =
    eos::common::Statfs::DoStatfs("/");
  ASSERT_NE(statfs, nullptr);

  statfs = eos::common::Statfs::DoStatfs("aaaaaaaa");
  ASSERT_EQ(statfs, nullptr);
}

TEST(FileSystemLocator, BasicSanity) {
  FileSystemLocator locator;
  ASSERT_TRUE(FileSystemLocator::fromQueuePath("/eos/somehost.cern.ch:1095/fst/data05", locator));

  ASSERT_EQ(locator.getHost(), "somehost.cern.ch");
  ASSERT_EQ(locator.getPort(), 1095);
  ASSERT_EQ(locator.getLocalPath(), "/data05");

  ASSERT_EQ(locator.getHostPort(), "somehost.cern.ch:1095");
  ASSERT_EQ(locator.getQueuePath(), "/eos/somehost.cern.ch:1095/fst/data05");
  ASSERT_EQ(locator.getFSTQueue(), "/eos/somehost.cern.ch:1095/fst");
  ASSERT_EQ(locator.getTransientChannel(), "filesystem-transient||somehost.cern.ch:1095||/data05");
}

TEST(FileSystemLocator, ParsingFailure) {
  FileSystemLocator locator;
  ASSERT_FALSE(FileSystemLocator::fromQueuePath("/fst/somehost.cern.ch:1095/fst/data05", locator));
  ASSERT_FALSE(FileSystemLocator::fromQueuePath("/eos/somehost.cern.ch:1095/mgm/data07", locator));
  ASSERT_FALSE(FileSystemLocator::fromQueuePath("/eos/somehost.cern.ch:1095/mgm/data07", locator));
  ASSERT_FALSE(FileSystemLocator::fromQueuePath("/eos/somehost.cern.ch/fst/data05", locator));
  ASSERT_FALSE(FileSystemLocator::fromQueuePath("/eos/fst:999/data05", locator));
  ASSERT_FALSE(FileSystemLocator::fromQueuePath("/eos/somehost.cern.ch:1096/fst/", locator));
}

TEST(GroupLocator, BasicSanity) {
  GroupLocator locator;
  ASSERT_TRUE(GroupLocator::parseGroup("default.1337", locator));
  ASSERT_EQ(locator.getSpace(), "default");
  ASSERT_EQ(locator.getIndex(), 1337);

  ASSERT_TRUE(GroupLocator::parseGroup("spare", locator));
  ASSERT_EQ(locator.getSpace(), "spare");
  ASSERT_EQ(locator.getGroup(), "spare");
  ASSERT_EQ(locator.getIndex(), 0);

  ASSERT_FALSE(GroupLocator::parseGroup("aaa.bbb", locator));
  ASSERT_EQ(locator.getSpace(), "aaa");
  ASSERT_EQ(locator.getGroup(), "aaa.bbb");
  ASSERT_EQ(locator.getIndex(), 0);

  ASSERT_TRUE(GroupLocator::parseGroup("default.0", locator));
  ASSERT_EQ(locator.getSpace(), "default");
  ASSERT_EQ(locator.getGroup(), "default.0");
  ASSERT_EQ(locator.getIndex(), 0);

  ASSERT_FALSE(GroupLocator::parseGroup("onlyspace", locator));
  ASSERT_EQ(locator.getSpace(), "onlyspace");
  ASSERT_EQ(locator.getGroup(), "onlyspace");
  ASSERT_EQ(locator.getIndex(), 0);

  ASSERT_FALSE(GroupLocator::parseGroup("", locator));
  ASSERT_EQ(locator.getSpace(), "");
  ASSERT_EQ(locator.getGroup(), "");
  ASSERT_EQ(locator.getIndex(), 0);
}

EOSCOMMONTESTING_END
