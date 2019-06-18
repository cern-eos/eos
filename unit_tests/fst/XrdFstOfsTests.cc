//------------------------------------------------------------------------------
// File: XrdFstOfsTests.cc
// Author: Elvin Sindrilaru - CERN
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
#include "fst/XrdFstOfs.hh"
#undef IN_TEST_HARNESS

//------------------------------------------------------------------------------
// Test parsing of simulation error offset
//------------------------------------------------------------------------------
TEST(XrdFstOfs, ParseSimulationErrOffset)
{
  eos::fst::XrdFstOfs ofs;
  ASSERT_EQ(ofs.GetSimulationErrorOffset("dummy"), 0);
  ASSERT_EQ(ofs.GetSimulationErrorOffset("io_read"), 0);
  ASSERT_EQ(ofs.GetSimulationErrorOffset("io_read_10"), 10);
  ASSERT_EQ(ofs.GetSimulationErrorOffset("io_read_10B"), 10);
  ASSERT_EQ(ofs.GetSimulationErrorOffset("io_read_10k"), 10000);
  ASSERT_EQ(ofs.GetSimulationErrorOffset("io_write_10M"), 10000000);
  ASSERT_EQ(ofs.GetSimulationErrorOffset("io_write_4G"), 4000000000);
  ofs.SetSimulationError("dummy");
  ASSERT_FALSE(ofs.mSimIoReadErr);
  ASSERT_EQ(ofs.mSimErrIoReadOff, 0ull);
  ASSERT_FALSE(ofs.mSimIoWriteErr);
  ASSERT_EQ(ofs.mSimErrIoWriteOff, 0ull);
  ASSERT_FALSE(ofs.mSimXsReadErr);
  ASSERT_FALSE(ofs.mSimXsWriteErr);
  ofs.SetSimulationError("io_read_4M");
  ASSERT_TRUE(ofs.mSimIoReadErr);
  ASSERT_EQ(ofs.mSimErrIoReadOff, 4000000);
  ofs.SetSimulationError("io_write_5B");
  ASSERT_TRUE(ofs.mSimIoWriteErr);
  ASSERT_EQ(ofs.mSimErrIoWriteOff, 5);
  ofs.SetSimulationError("xs_read");
  ASSERT_TRUE(ofs.mSimXsReadErr);
  ofs.SetSimulationError("xs_write");
  ASSERT_TRUE(ofs.mSimXsWriteErr);
  ofs.SetSimulationError("fmd_open");
  ASSERT_TRUE(ofs.mSimFmdOpenErr);
}
