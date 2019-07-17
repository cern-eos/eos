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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @breif NextInodeProvider tests
//------------------------------------------------------------------------------
#include "namespace/ns_quarkdb/persistency/NextInodeProvider.hh"
#include "TestUtils.hh"
#include "qclient/structures/QHash.hh"
#include "Namespace.hh"
#include <gtest/gtest.h>
#include <vector>

EOSNSTESTING_BEGIN

class NextInodeProviderTest : public NsTestsFixture {};

TEST_F(NextInodeProviderTest, BasicSanity)
{
  std::unique_ptr<qclient::QClient> qcl = createQClient();

  qclient::QHash myhash;
  myhash.setKey("ns-tests-next-inode-provider");
  myhash.setClient(*qcl.get());
  myhash.hdel("counter");
  constexpr size_t firstRunLimit = 50000;
  constexpr size_t secondRunLimit = 100000;
  {
    NextInodeProvider inodeProvider;
    inodeProvider.configure(myhash, "counter");

    for (size_t i = 1; i < firstRunLimit; i++) {
      ASSERT_EQ(inodeProvider.getFirstFreeId(), i);
      ASSERT_EQ(inodeProvider.reserve(), i);
    }
  }
  {
    NextInodeProvider inodeProvider;
    inodeProvider.configure(myhash, "counter");
    size_t continuation = inodeProvider.getFirstFreeId();
    ASSERT_TRUE(firstRunLimit <= continuation);
    std::cerr << "Wasted " << continuation - firstRunLimit << " inodes." <<
              std::endl;

    for (size_t i = continuation; i < secondRunLimit; i++) {
      ASSERT_EQ(inodeProvider.getFirstFreeId(), i);
      ASSERT_EQ(inodeProvider.reserve(), i);
    }
  }
  qcl->del("ns-tests-next-inode-provider");
}

TEST_F(NextInodeProviderTest, Blacklisting)
{
  std::unique_ptr<qclient::QClient> qcl = createQClient();

  qclient::QHash myhash;
  myhash.setKey("ns-tests-next-inode-provider");
  myhash.setClient(*qcl.get());
  myhash.hdel("counter");

  NextInodeProvider inodeProvider;
  inodeProvider.configure(myhash, "counter");

  ASSERT_EQ(inodeProvider.reserve(), 1);
  ASSERT_EQ(inodeProvider.reserve(), 2);
  ASSERT_EQ(inodeProvider.reserve(), 3);

  inodeProvider.blacklistBelow(4);
  ASSERT_EQ("7", myhash.hget("counter"));

  ASSERT_EQ(inodeProvider.reserve(), 5);
  ASSERT_EQ(inodeProvider.reserve(), 6);
  ASSERT_EQ(inodeProvider.reserve(), 7);

  inodeProvider.blacklistBelow(1);
  inodeProvider.blacklistBelow(6);
  inodeProvider.blacklistBelow(7);

  for(size_t i = 8; i < 5000; i++) {
    ASSERT_EQ(inodeProvider.reserve(), i);
  }

  inodeProvider.blacklistBelow(10000);
  ASSERT_EQ("10101", myhash.hget("counter"));

  for(size_t i = 10001; i < 10100; i++) {
    ASSERT_EQ(inodeProvider.reserve(), i);
  }
}

EOSNSTESTING_END
