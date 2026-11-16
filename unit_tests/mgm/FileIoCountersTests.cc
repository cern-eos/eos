//------------------------------------------------------------------------------
//! @brief In-memory file traffic counters
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

#include "namespace/ns_quarkdb/FileIoCounters.hh"
#include <gtest/gtest.h>

TEST(FileIoCounters, PendingReadAndUpdate)
{
  eos::FileIoCounters counters(nullptr, nullptr);
  counters.add(7, 100, 0);
  eos::FileIoTotals totals = counters.get(7);
  EXPECT_EQ(totals.rbytes, 100u);
  EXPECT_EQ(totals.rbc, 1u);
  EXPECT_EQ(totals.ubytes, 0u);
  EXPECT_EQ(totals.ubc, 0u);
  counters.add(7, 5, 40);
  totals = counters.get(7);
  EXPECT_EQ(totals.rbytes, 105u);
  EXPECT_EQ(totals.rbc, 2u);
  EXPECT_EQ(totals.ubytes, 40u);
  EXPECT_EQ(totals.ubc, 1u);
}

TEST(FileIoCounters, ZeroAddIsIgnored)
{
  eos::FileIoCounters counters(nullptr, nullptr);
  counters.add(0, 10, 10);
  counters.add(3, 0, 0);
  EXPECT_EQ(counters.get(0).rbytes, 0u);
  EXPECT_EQ(counters.get(3).rbytes, 0u);
  EXPECT_EQ(counters.get(3).ubytes, 0u);
}

TEST(FileIoCounters, FilesStaySeparate)
{
  eos::FileIoCounters counters(nullptr, nullptr);
  counters.add(1, 10, 0);
  counters.add(2, 0, 20);
  EXPECT_EQ(counters.get(1).rbytes, 10u);
  EXPECT_EQ(counters.get(1).ubytes, 0u);
  EXPECT_EQ(counters.get(1).rbc, 1u);
  EXPECT_EQ(counters.get(2).ubytes, 20u);
  EXPECT_EQ(counters.get(2).ubc, 1u);
  EXPECT_EQ(counters.get(2).rbc, 0u);
}

TEST(FileIoCounters, DropForgetsPending)
{
  eos::FileIoCounters counters(nullptr, nullptr);
  counters.add(9, 1, 2);
  counters.drop(9);
  const eos::FileIoTotals totals = counters.get(9);
  EXPECT_EQ(totals.rbytes, 0u);
  EXPECT_EQ(totals.rbc, 0u);
  EXPECT_EQ(totals.ubytes, 0u);
  EXPECT_EQ(totals.ubc, 0u);
}

TEST(FileIoCounters, FlushWithoutBackendKeepsPending)
{
  eos::FileIoCounters counters(nullptr, nullptr);
  counters.add(4, 8, 0);
  counters.flush(true);
  EXPECT_EQ(counters.get(4).rbytes, 8u);
  EXPECT_EQ(counters.get(4).rbc, 1u);
}
