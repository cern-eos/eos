//------------------------------------------------------------------------------
// File: InodeTests.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "Namespace.hh"
#include "common/FileId.hh"
#include <iostream>
#include <random>

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl

EOSCOMMONTESTING_BEGIN

using namespace eos::common;

TEST(Inode, ValidateLegacyEncodingRange) {
  ASSERT_EQ(FileId::LegacyIsFileInode(1), false);
  ASSERT_EQ(FileId::LegacyIsFileInode(2), false);
  ASSERT_EQ(FileId::LegacyIsFileInode(268435454), false);
  ASSERT_EQ(FileId::LegacyIsFileInode(268435455), false);

  ASSERT_EQ(FileId::LegacyIsFileInode(268435456), true);
  ASSERT_EQ(FileId::LegacyIsFileInode(268435457), true);
  ASSERT_EQ(FileId::LegacyIsFileInode(268435458), true);
  ASSERT_EQ(FileId::LegacyIsFileInode(20000000000), true);

  // From this point on, the legacy scheme only uses 1 inode per 256M ...
  ASSERT_EQ(FileId::LegacyFidToInode(1), 268435456);
  ASSERT_EQ(FileId::LegacyFidToInode(2), 536870912);
  ASSERT_EQ(FileId::LegacyFidToInode(3), 805306368);
  ASSERT_EQ(FileId::LegacyFidToInode(4), 1073741824);
  ASSERT_EQ(FileId::LegacyFidToInode(5), 1342177280);
  ASSERT_EQ(FileId::LegacyFidToInode(6), 1610612736);
}

TEST(Inode, ValidateNewEncodingRange) {
  ASSERT_EQ(FileId::NewIsFileInode(1), false);
  ASSERT_EQ(FileId::NewIsFileInode(2), false);

  ASSERT_EQ(FileId::LegacyIsFileInode(1), false);
  ASSERT_EQ(FileId::LegacyIsFileInode(2), false);
  ASSERT_EQ(FileId::LegacyIsFileInode(268435454), false);
  ASSERT_EQ(FileId::LegacyIsFileInode(268435455), false);

  ASSERT_EQ(FileId::NewIsFileInode(268435456), false);
  ASSERT_EQ(FileId::NewIsFileInode(268435457), false);
  ASSERT_EQ(FileId::NewIsFileInode(268435458), false);
  ASSERT_EQ(FileId::NewIsFileInode(268435459), false);
  ASSERT_EQ(FileId::NewIsFileInode(268435460), false);

  ASSERT_EQ(FileId::NewFidToInode(1), 9223372036854775809ull);
  ASSERT_EQ(FileId::NewFidToInode(2), 9223372036854775810ull);
  ASSERT_EQ(FileId::NewFidToInode(3), 9223372036854775811ull);
  ASSERT_EQ(FileId::NewFidToInode(4), 9223372036854775812ull);
  ASSERT_EQ(FileId::NewFidToInode(5), 9223372036854775813ull);
  ASSERT_EQ(FileId::NewFidToInode(6), 9223372036854775814ull);

  ASSERT_FALSE(FileId::NewIsFileInode(1ull));
  ASSERT_FALSE(FileId::NewIsFileInode(2ull));
  ASSERT_FALSE(FileId::NewIsFileInode(3ull));
  ASSERT_FALSE(FileId::NewIsFileInode(4ull));

  ASSERT_FALSE(FileId::NewIsFileInode(9223372036854775807ull));
  ASSERT_TRUE(FileId::NewIsFileInode(9223372036854775808ull));
  ASSERT_TRUE(FileId::NewIsFileInode(9223372036854775809ull));
  ASSERT_TRUE(FileId::NewIsFileInode(9223372036854775810ull));
  ASSERT_TRUE(FileId::NewIsFileInode(9223372036854775811ull));
  ASSERT_TRUE(FileId::NewIsFileInode(9223372036854775812ull));
}

TEST(Inode, ValidateCollisionsBetweenLegacyAndNew) {
  // For the first 256M directories, the two implementations of IsFileInode
  // are compatible.
  ASSERT_EQ(FileId::NewIsFileInode(1ull), FileId::LegacyIsFileInode(1ull));
  ASSERT_EQ(FileId::NewIsFileInode(2ull), FileId::LegacyIsFileInode(2ull));
  ASSERT_EQ(FileId::NewIsFileInode(3ull), FileId::LegacyIsFileInode(3ull));
  ASSERT_EQ(FileId::NewIsFileInode(4ull), FileId::LegacyIsFileInode(4ull));
  ASSERT_EQ(FileId::NewIsFileInode(5ull), FileId::LegacyIsFileInode(5ull));
  ASSERT_EQ(FileId::NewIsFileInode(6ull), FileId::LegacyIsFileInode(6ull));
  ASSERT_EQ(FileId::NewIsFileInode(7ull), FileId::LegacyIsFileInode(7ull));
  ASSERT_EQ(FileId::NewIsFileInode(268435454ull), FileId::LegacyIsFileInode(268435454ull));
  ASSERT_EQ(FileId::NewIsFileInode(268435455ull), FileId::LegacyIsFileInode(268435455ull));

  // Compatibility breaks down after 256M directories, as expected
  ASSERT_NE(FileId::NewIsFileInode(268435456ull), FileId::LegacyIsFileInode(268435456ull));
  ASSERT_NE(FileId::NewIsFileInode(268435457ull), FileId::LegacyIsFileInode(268435457ull));
  ASSERT_NE(FileId::NewIsFileInode(268435458ull), FileId::LegacyIsFileInode(268435458ull));

  // At which point file inodes collide?
  ASSERT_EQ(FileId::NewFidToInode(1), 9223372036854775809ull);

  // 2^35 is the highest safe number of files we can go and maintain compatibility
  // between the two schemes.
  ASSERT_EQ(FileId::LegacyFidToInode(34359738368ull), FileId::NewFidToInode(1) - 1);

  // LegacyFidToInode works for 34359738368ull - what about LegacyInodeToFid?
  ASSERT_EQ(FileId::LegacyInodeToFid(FileId::LegacyInodeToFid(34359738368ull)), 0ull);

  // Nope! It overflows at exactly the same point where the new encoding scheme
  // takes effect. (wasting one more bit of its theoretical capacity)
  // There's zero collisions for files between old encoding scheme and new one.

  // At exactly 2^36 (68B files), the legacy encoding scheme breaks down completely,
  // including FidToInode.
  ASSERT_EQ(FileId::LegacyFidToInode(68719476735ull), 18446744073441116160ull); // 2^36 - 1 files
  ASSERT_EQ(FileId::LegacyFidToInode(68719476736ull), 0ull); // 2^36 files
}

TEST(Inode, InodeToFidCompatibility) {
  // InodeToFid dispatches to the appropriate function, depending which scheme
  // is being used. Validate it's able to handle both encodings.

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(1ull)), 1ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(1ull)), 1ull);

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(2ull)), 2ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(2ull)), 2ull);

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(3ull)), 3ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(3ull)), 3ull);

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(4ull)), 4ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(4ull)), 4ull);

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(5ull)), 5ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(5ull)), 5ull);

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(6ull)), 6ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(6ull)), 6ull);

  ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(34359738367ull)), 34359738367ull);
  ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(34359738367ull)), 34359738367ull);

  // Randomize testing by generating random numbers between 1 and the maximum
  // valid inode supported by legacy encoding: 34359738368ull.

  std::mt19937_64 gen(12345678);
  std::uniform_int_distribution<uint64_t> dist(1ull,  34359738368ull);

  for(size_t i = 0; i < 10'000'000; i++) { // 10M rounds
    uint64_t randomID = dist(gen);
    ASSERT_EQ(FileId::InodeToFid(FileId::NewFidToInode(randomID)), randomID);
    ASSERT_EQ(FileId::InodeToFid(FileId::LegacyFidToInode(randomID)), randomID);

    ASSERT_TRUE(FileId::IsFileInode(FileId::NewFidToInode(randomID)));
    ASSERT_TRUE(FileId::IsFileInode(FileId::LegacyFidToInode(randomID)));
  }

}

EOSCOMMONTESTING_END
