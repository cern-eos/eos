#include "common/shaping/SlidingWindowStats.hh"

#include <gtest/gtest.h>

TEST(SlidingWindowStats, UsesActualElapsedTimeForRate)
{
  eos::common::traffic_shaping::SlidingWindowStats stats(60.0, 1.0);

  stats.Add(1350);
  stats.Tick(1.35);

  EXPECT_DOUBLE_EQ(stats.GetRate(1.0), 1000.0);
}

TEST(SlidingWindowStats, FixedTickRateIsBackwardCompatible)
{
  eos::common::traffic_shaping::SlidingWindowStats stats(60.0, 1.0);

  stats.Add(1000);
  stats.Tick();

  EXPECT_DOUBLE_EQ(stats.GetRate(1.0), 1000.0);
}
