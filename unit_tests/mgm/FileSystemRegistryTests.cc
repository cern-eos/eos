//------------------------------------------------------------------------------
// File: FileSystemRegistryTests.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
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
#include "mgm/utils/FileSystemRegistry.hh"

using namespace eos::mgm;
using namespace eos::common;

//------------------------------------------------------------------------------
// Test basic FileSystemRegistry functionality
//------------------------------------------------------------------------------
TEST(FileSystemRegistry, BasicSanity) {
  FileSystemRegistry registry;

  FileSystemLocator locator1("example.com", 1111, "/path1");
  FileSystemLocator locator2("example.com", 1111, "/path2");
  FileSystemLocator locator3("example.com", 1111, "/path3");
  FileSystemLocator locator4("example.com", 1111, "/path4");




  ASSERT_TRUE(registry.registerFileSystem(locator1, 1, (eos::mgm::FileSystem*) 0x01));

  // No duplicates
  ASSERT_FALSE(registry.registerFileSystem(locator1, 1, (eos::mgm::FileSystem*) 0x01));
  ASSERT_FALSE(registry.registerFileSystem(locator2, 2, (eos::mgm::FileSystem*) 0x01));
  ASSERT_FALSE(registry.registerFileSystem(locator1, 1, (eos::mgm::FileSystem*) 0x02));

  ASSERT_EQ(registry.lookupByID(1), (eos::mgm::FileSystem*) 0x01);
  ASSERT_EQ(registry.lookupByID(2), nullptr);

  ASSERT_EQ(registry.lookupByPtr( (eos::mgm::FileSystem*) 0x01), 1);
  ASSERT_EQ(registry.lookupByPtr( (eos::mgm::FileSystem*) 0x02), 0);
  ASSERT_EQ(registry.lookupByPtr(nullptr), 0);

  ASSERT_EQ(registry.size(), 1u);
  ASSERT_FALSE(registry.eraseById(2));
  ASSERT_FALSE(registry.eraseByPtr( (eos::mgm::FileSystem*) 0x02));

  ASSERT_TRUE(registry.registerFileSystem(locator2,  2, (eos::mgm::FileSystem*) 0x02));
  ASSERT_TRUE(registry.registerFileSystem(locator3, 3, (eos::mgm::FileSystem*) 0x03));
  ASSERT_TRUE(registry.registerFileSystem(locator4, 4, (eos::mgm::FileSystem*) 0x04));

  ASSERT_EQ(registry.size(), 4u);

  ASSERT_EQ(registry.lookupByID(3), (eos::mgm::FileSystem*) 0x03);
  ASSERT_EQ(registry.lookupByPtr( (eos::mgm::FileSystem*) 0x03), 3);

  ASSERT_TRUE(registry.eraseById(3));

  ASSERT_EQ(registry.size(), 3u);
  ASSERT_EQ(registry.lookupByID(3), nullptr);
  ASSERT_EQ(registry.lookupByPtr( (eos::mgm::FileSystem*) 0x03), 0);

  ASSERT_TRUE(registry.eraseByPtr( (eos::mgm::FileSystem*) 0x04));

  ASSERT_EQ(registry.size(), 2u);
  ASSERT_EQ(registry.lookupByID(4), nullptr);
  ASSERT_EQ(registry.lookupByPtr( (eos::mgm::FileSystem*) 0x04), 0);

  registry.clear();

  ASSERT_EQ(registry.size(), 0u);
  ASSERT_EQ(registry.lookupByID(2), nullptr);
  ASSERT_EQ(registry.lookupByPtr( (eos::mgm::FileSystem*) 0x02), 0);
}
