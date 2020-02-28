//------------------------------------------------------------------------------
// File: TgcFreedBytesHistogramTests.cc
// Author: Steven Murray <smurray at cern dot ch>
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

#include "mgm/tgc/Constants.hh"
#include "mgm/tgc/DummyClock.hh"
#include "mgm/tgc/FreedBytesHistogram.hh"
#include "mgm/tgc/RealClock.hh"

#include <gtest/gtest.h>

class TgcFreedBytesHistogramTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, constructor)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 600;
  const std::uint32_t binWidthSecs = 1;

  RealClock clock;
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  const time_t historyLimitSecs = nbBins * binWidthSecs;
  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(lastNbSecs));
  }
  ASSERT_THROW(histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs + 1), FreedBytesHistogram::TooFarBackInTime);

  for(std::uint32_t binIndex = 0; binIndex < nbBins; binIndex++) {
    ASSERT_EQ(0, histogram.getFreedBytesInBin(binIndex));
  }
  ASSERT_THROW(histogram.getFreedBytesInBin(nbBins), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, getTotalBytesFreed_default)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = TGC_FREED_BYTES_HISTOGRAM_NB_BINS;
  const std::uint32_t binWidthSecs = TGC_DEFAULT_FREED_BYTES_HISTOGRAM_BIN_WIDTH_SECS;

  DummyClock clock(1000);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  const std::uint32_t historyLimitSecs = nbBins * binWidthSecs;
  std::uint64_t totalFreedBytes = 0;
  for (std::uint32_t i = 1; i <= historyLimitSecs; i++) {
    clock.setTime(999 + i);
    histogram.bytesFreed(i);
    totalFreedBytes += i;
  }
  ASSERT_THROW(histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs + 1), FreedBytesHistogram::TooFarBackInTime);

  ASSERT_EQ(totalFreedBytes, histogram.getTotalBytesFreed());

  for (std::uint32_t i = 1; i <= historyLimitSecs; i++) {
    const std::uint32_t binIndex = i - 1;
    const std::uint32_t expectedFreedBytes = historyLimitSecs - binIndex;
    ASSERT_EQ(expectedFreedBytes, histogram.getFreedBytesInBin(binIndex));
  }

  ASSERT_THROW(histogram.getFreedBytesInBin(historyLimitSecs), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, constructor_nbBins_0)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 0;
  const std::uint32_t binWidthSecs = 1;

  RealClock clock;
  ASSERT_THROW(FreedBytesHistogram(nbBins, binWidthSecs, clock), FreedBytesHistogram::InvalidNbBins);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, constructor_nbBins_too_big)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = TGC_FREED_BYTES_HISTOGRAM_MAX_NB_BINS + 1;
  const std::uint32_t binWidthSecs = 1;

  RealClock clock;
  ASSERT_THROW(FreedBytesHistogram(nbBins, binWidthSecs, clock), FreedBytesHistogram::InvalidNbBins);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, constructor_binWidthSecs_0)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 600;
  const std::uint32_t binWidthSecs = 0;

  RealClock clock;
  ASSERT_THROW(FreedBytesHistogram(nbBins, binWidthSecs, clock), FreedBytesHistogram::InvalidBinWidth);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, constructor_binWidthSecs_too_big)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 600;
  const std::uint32_t binWidthSecs = TGC_FREED_BYTES_HISTOGRAM_MAX_BIN_WIDTH_SECS + 1;

  RealClock clock;
  ASSERT_THROW(FreedBytesHistogram(nbBins, binWidthSecs, clock), FreedBytesHistogram::InvalidBinWidth);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidthSecs_0)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 600;
  const std::uint32_t binWidthSecs = 1;

  RealClock clock;
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_THROW(histogram.setBinWidthSecs(0), FreedBytesHistogram::InvalidBinWidth);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidthSecs_too_big)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 600;
  const std::uint32_t binWidthSecs = 1;

  RealClock clock;
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_THROW(histogram.setBinWidthSecs(TGC_FREED_BYTES_HISTOGRAM_MAX_BIN_WIDTH_SECS + 1),
    FreedBytesHistogram::InvalidBinWidth);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, bytesFreed)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(6, histogram.getTotalBytesFreed());

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(0));
  ASSERT_EQ(3, histogram.getNbBytesFreedInLastNbSecs(1));
  ASSERT_EQ(3, histogram.getNbBytesFreedInLastNbSecs(2));
  ASSERT_EQ(3, histogram.getNbBytesFreedInLastNbSecs(3));
  ASSERT_EQ(5, histogram.getNbBytesFreedInLastNbSecs(4));
  ASSERT_EQ(5, histogram.getNbBytesFreedInLastNbSecs(5));
  ASSERT_EQ(5, histogram.getNbBytesFreedInLastNbSecs(6));
  ASSERT_EQ(6, histogram.getNbBytesFreedInLastNbSecs(7));
  ASSERT_EQ(6, histogram.getNbBytesFreedInLastNbSecs(8));
  ASSERT_EQ(6, histogram.getNbBytesFreedInLastNbSecs(9));
  ASSERT_THROW(histogram.getNbBytesFreedInLastNbSecs(10), FreedBytesHistogram::TooFarBackInTime);

  clock.setTime(1009); histogram.bytesFreed(4);
  clock.setTime(1012); histogram.bytesFreed(5);
  clock.setTime(1015); histogram.bytesFreed(6);

  ASSERT_EQ(15, histogram.getTotalBytesFreed());

  ASSERT_EQ(6, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(5, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(4, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  ASSERT_EQ( 0, histogram.getNbBytesFreedInLastNbSecs(0));
  ASSERT_EQ( 6, histogram.getNbBytesFreedInLastNbSecs(1));
  ASSERT_EQ( 6, histogram.getNbBytesFreedInLastNbSecs(2));
  ASSERT_EQ( 6, histogram.getNbBytesFreedInLastNbSecs(3));
  ASSERT_EQ(11, histogram.getNbBytesFreedInLastNbSecs(4));
  ASSERT_EQ(11, histogram.getNbBytesFreedInLastNbSecs(5));
  ASSERT_EQ(11, histogram.getNbBytesFreedInLastNbSecs(6));
  ASSERT_EQ(15, histogram.getNbBytesFreedInLastNbSecs(7));
  ASSERT_EQ(15, histogram.getNbBytesFreedInLastNbSecs(8));
  ASSERT_EQ(15, histogram.getNbBytesFreedInLastNbSecs(9));
  ASSERT_THROW(histogram.getNbBytesFreedInLastNbSecs(10), FreedBytesHistogram::TooFarBackInTime);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidth_from_3_to_4)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  const std::uint32_t newBinWidthSecs = 4;
  histogram.setBinWidthSecs(newBinWidthSecs);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(newBinWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(4, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(0, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidth_from_3_to_5)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  const std::uint32_t newBinWidthSecs = 5;
  histogram.setBinWidthSecs(newBinWidthSecs);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(newBinWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(5, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(0, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidth_from_3_to_6)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  const std::uint32_t newBinWidthSecs = 6;
  histogram.setBinWidthSecs(newBinWidthSecs);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(newBinWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(6, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(0, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(0, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidth_from_3_to_2)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  const std::uint32_t newBinWidthSecs = 2;
  histogram.setBinWidthSecs(newBinWidthSecs);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(newBinWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(2, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, setBinWidth_from_3_to_1)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  const std::uint32_t newBinWidthSecs = 1;
  histogram.setBinWidthSecs(newBinWidthSecs);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(newBinWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(1, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, multiple_passes)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  clock.setTime(1000); histogram.bytesFreed(1);
  clock.setTime(1003); histogram.bytesFreed(2);
  clock.setTime(1006); histogram.bytesFreed(3);

  ASSERT_EQ(3, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(2, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(1, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  clock.setTime(1009); histogram.bytesFreed(4);
  clock.setTime(1012); histogram.bytesFreed(5);
  clock.setTime(1015); histogram.bytesFreed(6);

  ASSERT_EQ(6, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(5, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(4, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);

  clock.setTime(1018); histogram.bytesFreed(7);
  clock.setTime(1021); histogram.bytesFreed(8);
  clock.setTime(1024); histogram.bytesFreed(9);

  ASSERT_EQ(9, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(8, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(7, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcFreedBytesHistogramTest, bytesFreed_many_times_same_bin)
{
  using namespace eos::mgm::tgc;

  const std::size_t nbBins = 3;
  const std::uint32_t binWidthSecs = 3;
  const time_t historyLimitSecs = nbBins * binWidthSecs;
  const std::time_t initialTime = 1000;
  DummyClock clock(initialTime);
  FreedBytesHistogram histogram(nbBins, binWidthSecs, clock);

  ASSERT_EQ(nbBins, histogram.getNbBins());
  ASSERT_EQ(binWidthSecs, histogram.getBinWidthSecs());

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (time_t lastNbSecs = 0; lastNbSecs <= historyLimitSecs; lastNbSecs++) {
    ASSERT_EQ(0, histogram.getNbBytesFreedInLastNbSecs(historyLimitSecs));
  }

  ASSERT_EQ(0, histogram.getTotalBytesFreed());

  for (int i = 0; i < 100; i++) {
    histogram.bytesFreed(1);
  }

  ASSERT_EQ(100, histogram.getFreedBytesInBin(0));
  ASSERT_EQ(0, histogram.getFreedBytesInBin(1));
  ASSERT_EQ(0, histogram.getFreedBytesInBin(2));
  ASSERT_THROW(histogram.getFreedBytesInBin(4), FreedBytesHistogram::InvalidBinIndex);
}
