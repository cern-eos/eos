//------------------------------------------------------------------------------
// File: ChecksumGroupTests.cc
// Author: Elvin Sindrilaru - CERN
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

#include "fst/checksum/ChecksumGroup.hh"
#include "gtest/gtest.h"
#include <vector>

//------------------------------------------------------------------------------
// Build a group holding a single adler checksum
//------------------------------------------------------------------------------
static std::unique_ptr<eos::fst::ChecksumGroup>
MakeAdlerGroup()
{
  auto group = std::make_unique<eos::fst::ChecksumGroup>();
  group->SetDefault(
      eos::fst::ChecksumPlugins::GetXsObj(eos::common::LayoutId::eChecksum::kAdler),
      eos::common::LayoutId::eChecksum::kAdler);
  return group;
}

//------------------------------------------------------------------------------
// A valid chunk is accepted and leaves the group clean
//------------------------------------------------------------------------------
TEST(ChecksumGroup, AddValidChunk)
{
  auto group = MakeAdlerGroup();
  ASSERT_TRUE(group->HasChecksums());
  std::vector<char> buffer(4096, 'a');
  ASSERT_TRUE(group->Add(buffer.data(), buffer.size(), 0));
  ASSERT_FALSE(group->NeedsRecalculation());
}

//------------------------------------------------------------------------------
// A negative read return value cast to size_t must be rejected rather than
// making the checksum routines run off the end of the buffer. This is the
// regression test for the FST crash in XrdFstOfsFile::read where an error
// return of -1 was stored in an unsigned variable and forwarded as length.
//------------------------------------------------------------------------------
TEST(ChecksumGroup, RejectNegativeLength)
{
  auto group = MakeAdlerGroup();
  std::vector<char> buffer(4096, 'a');
  const size_t bogus_length = static_cast<size_t>(-1);
  ASSERT_FALSE(group->Add(buffer.data(), bogus_length, 0));
  // The group must be marked dirty so that a checksum computed from partial
  // data is never reported as a genuine mismatch
  ASSERT_TRUE(group->NeedsRecalculation());
}

//------------------------------------------------------------------------------
// A null buffer is rejected as well
//------------------------------------------------------------------------------
TEST(ChecksumGroup, RejectNullBuffer)
{
  auto group = MakeAdlerGroup();
  ASSERT_FALSE(group->Add(nullptr, 4096, 0));
  ASSERT_TRUE(group->NeedsRecalculation());
}

//------------------------------------------------------------------------------
// Any length beyond the 32-bit transfer size bound is rejected
//------------------------------------------------------------------------------
TEST(ChecksumGroup, RejectOversizedLength)
{
  auto group = MakeAdlerGroup();
  std::vector<char> buffer(4096, 'a');
  ASSERT_FALSE(group->Add(buffer.data(), (1ull << 32) + 1, 0));
  ASSERT_TRUE(group->NeedsRecalculation());
}
