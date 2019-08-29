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
#include "common/TransferQueue.hh"
#include "common/Locators.hh"
#include "common/InstanceName.hh"
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
  ASSERT_EQ(locator.getStoragePath(), "/data05");
  ASSERT_EQ(locator.getStorageType(), FileSystemLocator::StorageType::Local);
  ASSERT_TRUE(locator.isLocal());

  ASSERT_EQ(locator.getHostPort(), "somehost.cern.ch:1095");
  ASSERT_EQ(locator.getQueuePath(), "/eos/somehost.cern.ch:1095/fst/data05");
  ASSERT_EQ(locator.getFSTQueue(), "/eos/somehost.cern.ch:1095/fst");
  ASSERT_EQ(locator.getTransientChannel(), "filesystem-transient||somehost.cern.ch:1095||/data05");
}

TEST(FileSystemLocator, ParseStorageType) {
  ASSERT_EQ(FileSystemLocator::parseStorageType("/data"), FileSystemLocator::StorageType::Local);
  ASSERT_EQ(FileSystemLocator::parseStorageType("root://root.example.cern.ch:1094//"), FileSystemLocator::StorageType::Xrd);
  ASSERT_EQ(FileSystemLocator::parseStorageType("s3://s3.example.cern.ch//"), FileSystemLocator::StorageType::S3);
  ASSERT_EQ(FileSystemLocator::parseStorageType("dav://webdav.example.cern.ch/"), FileSystemLocator::StorageType::WebDav);
  ASSERT_EQ(FileSystemLocator::parseStorageType("http://web.example.cern.ch/"), FileSystemLocator::StorageType::HTTP);
  ASSERT_EQ(FileSystemLocator::parseStorageType("https://webs.example.cern.ch/"), FileSystemLocator::StorageType::HTTPS);

  ASSERT_EQ(FileSystemLocator::parseStorageType("root:/invalid.example"), FileSystemLocator::StorageType::Unknown);
  ASSERT_EQ(FileSystemLocator::parseStorageType("local/path"), FileSystemLocator::StorageType::Unknown);
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

TEST(FileSystemLocator, RemoteFileSystem) {
  FileSystemLocator locator;
  ASSERT_TRUE(FileSystemLocator::fromQueuePath("/eos/example-host.cern.ch:1095/fsthttps://remote.example.cern.ch/path/", locator));
  ASSERT_EQ(locator.getHost(), "example-host.cern.ch");
  ASSERT_EQ(locator.getPort(), 1095);
  ASSERT_EQ(locator.getStoragePath(), "https://remote.example.cern.ch/path/");
  ASSERT_EQ(locator.getStorageType(), FileSystemLocator::StorageType::HTTPS);
  ASSERT_FALSE(locator.isLocal());

  ASSERT_EQ(locator.getHostPort(), "example-host.cern.ch:1095");
  ASSERT_EQ(locator.getQueuePath(), "/eos/example-host.cern.ch:1095/fsthttps://remote.example.cern.ch/path/");
  ASSERT_EQ(locator.getFSTQueue(), "/eos/example-host.cern.ch:1095/fst");
  ASSERT_EQ(locator.getTransientChannel(), "filesystem-transient||example-host.cern.ch:1095||https://remote.example.cern.ch/path/");
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

TEST(TransferQueueLocator, BasicSanity) {
  FileSystemLocator fsLocator("example-host.cern.ch", 1095, "/some/path");
  ASSERT_EQ(fsLocator.getQueuePath(), "/eos/example-host.cern.ch:1095/fst/some/path");

  TransferQueueLocator locator(fsLocator, "drainq");
  ASSERT_EQ(locator.getQueue(), "/eos/example-host.cern.ch:1095/fst");
  ASSERT_EQ(locator.getQueuePath(), "/eos/example-host.cern.ch:1095/fst/some/path/txqueue/drainq");
  ASSERT_EQ(locator.getQDBKey(), "txqueue-filesystem||example-host.cern.ch:1095||/some/path||drainq");
}

TEST(TransferQueueLocator, txq) {
  TransferQueueLocator locator("/eos/example-host.cern.ch:1095/fst", "txq");

  ASSERT_EQ(locator.getQueue(), "/eos/example-host.cern.ch:1095/fst");
  ASSERT_EQ(locator.getQueuePath(), "/eos/example-host.cern.ch:1095/fst/gw/txqueue/txq");
  ASSERT_EQ(locator.getQDBKey(), "txqueue-fst||example-host.cern.ch:1095||txq");
}

TEST(FstLocator, BasicSanity) {
  FstLocator locator("example.com", 999);
  ASSERT_EQ(locator.getHost(), "example.com");
  ASSERT_EQ(locator.getPort(), 999);
  ASSERT_EQ(locator.getHostPort(), "example.com:999");
  ASSERT_EQ(locator.getQueuePath(), "/eos/example.com:999/fst");
}

TEST(FstLocator, FromQueuePath) {
  FstLocator locator;
  ASSERT_TRUE(FstLocator::fromQueuePath("/eos/example.com:1111/fst", locator));
  ASSERT_EQ(locator.getHost(), "example.com");
  ASSERT_EQ(locator.getPort(), 1111);
  ASSERT_EQ(locator.getHostPort(), "example.com:1111");
  ASSERT_EQ(locator.getQueuePath(), "/eos/example.com:1111/fst");
}

TEST(SharedHashLocator, BasicSanity) {
  SharedHashLocator locator("eosdev", SharedHashLocator::Type::kSpace, "default");
  ASSERT_EQ(locator.getConfigQueue(), "/config/eosdev/space/default");
  ASSERT_EQ(locator.getBroadcastQueue(), "/eos/*/mgm");

  locator = SharedHashLocator("eosdev", SharedHashLocator::Type::kGroup, "default.0");
  ASSERT_EQ(locator.getConfigQueue(), "/config/eosdev/group/default.0");
  ASSERT_EQ(locator.getBroadcastQueue(), "/eos/*/mgm");

  locator = SharedHashLocator("eosdev", SharedHashLocator::Type::kNode, "/eos/example.com:3003/fst");
  ASSERT_EQ(locator.getConfigQueue(), "/config/eosdev/node/example.com:3003");
  ASSERT_EQ(locator.getBroadcastQueue(), "/eos/example.com:3003/fst");
}

TEST(InstanceName, BasicSanity) {
  common::InstanceName::set("eosdev");
  ASSERT_EQ(common::InstanceName::get(), "eosdev");
}

TEST(SharedHashLocator, AutoInstanceName) {
  common::InstanceName::set("eosdev");

  SharedHashLocator locator(SharedHashLocator::Type::kSpace, "default");
  ASSERT_EQ(locator.getConfigQueue(), "/config/eosdev/space/default");
  ASSERT_EQ(locator.getBroadcastQueue(), "/eos/*/mgm");
}

EOSCOMMONTESTING_END
