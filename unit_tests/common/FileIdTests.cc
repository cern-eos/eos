//------------------------------------------------------------------------------
// File: FileIdTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
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
#define IN_TEST_HARNESS
#include "common/FileId.hh"
#undef IN_TEST_HARNESS

const uint64_t GB = (uint64_t)std::pow(2, 30);

TEST(FileId, EstimateTpcTimeout)
{
  using eos::common::FileId;
  ASSERT_EQ(FileId::EstimateTpcTimeout(1).count(), 1800);
  ASSERT_EQ(FileId::EstimateTpcTimeout(50 * GB).count(), 1800);
  ASSERT_EQ(FileId::EstimateTpcTimeout(60 * GB).count(), 2048);
  ASSERT_EQ(FileId::EstimateTpcTimeout(100 * GB, 100).count(), 1800);
  ASSERT_EQ(FileId::EstimateTpcTimeout(250 * GB, 100).count(), 2560);
}


TEST(FileId, DefaultConstruction)
{
  eos::common::FileId::fileid_t fid;
  ASSERT_EQ(fid, 0);
}
