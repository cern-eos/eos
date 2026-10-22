#include "mgm/shaping/AutomaticDetailRecovery.hh"

#include "gtest/gtest.h"

#include <chrono>
#include <cstdint>
#include <limits>

namespace {
using eos::mgm::traffic_shaping::AutomaticDetailRecovery;
using namespace std::chrono_literals;
const auto start = AutomaticDetailRecovery::Clock::time_point{};
} // namespace

TEST(AutomaticDetailRecovery, ColdStartUsesExistingLowThreshold)
{
  AutomaticDetailRecovery recovery;
  EXPECT_TRUE(recovery.CanRecover(0, 7, start, start, 200ms, 300s));
  EXPECT_TRUE(recovery.CanRecover(7, 7, start, start, 200ms, 300s));
  EXPECT_FALSE(recovery.CanRecover(8, 7, start, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, SameIdentityFilesystemExpansionPreventsOscillation)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 1, start);
  EXPECT_EQ(30u, recovery.EstimateFilesystemStreams(1));
  EXPECT_FALSE(recovery.CanRecover(1, 7, start + 3600s, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, ResetAndPartialReportsDoNotTriggerEarlyRecovery)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 10, start);
  EXPECT_FALSE(recovery.CanRecover(0, 7, start + 3600s, start, 200ms, 0s));
  EXPECT_FALSE(recovery.CanRecover(1, 7, start + 599ms, start, 200ms, 0s));
  EXPECT_TRUE(recovery.CanRecover(1, 7, start + 600ms, start, 200ms, 0s));
  EXPECT_FALSE(recovery.CanRecover(1, 7, start + 600ms, start, 200ms, 300s));
  EXPECT_TRUE(recovery.CanRecover(1, 7, start + 300s, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, LaterRuntimeClearRestartsReportSettling)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 10, start);
  const auto first_accepted_after_clear = start + 3600s;
  EXPECT_FALSE(recovery.CanRecover(1, 7, first_accepted_after_clear,
                                   first_accepted_after_clear, 200ms, 0s));
  EXPECT_FALSE(recovery.CanRecover(1, 7, first_accepted_after_clear + 599ms,
                                   first_accepted_after_clear, 200ms, 0s));
  EXPECT_TRUE(recovery.CanRecover(1, 7, first_accepted_after_clear + 600ms,
                                  first_accepted_after_clear, 200ms, 0s));
}

TEST(AutomaticDetailRecovery, EstimatedCardinalityPreservesBandAndAllowsLoadFall)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 10, start);
  EXPECT_FALSE(recovery.CanRecover(4, 10, start + 300s, start, 200ms, 300s));
  EXPECT_TRUE(recovery.CanRecover(3, 10, start + 300s, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, FractionalExpansionRoundsUp)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(20, 3, start);
  EXPECT_EQ(7u, recovery.EstimateFilesystemStreams(1));
  EXPECT_EQ(14u, recovery.EstimateFilesystemStreams(2));
  EXPECT_EQ(20u, recovery.EstimateFilesystemStreams(3));
}

TEST(AutomaticDetailRecovery, FailedRecoveryProbeDoublesHoldoff)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 10, start);
  ASSERT_TRUE(recovery.CanRecover(2, 7, start + 300s, start, 200ms, 300s));
  recovery.RecordRecoveryAttempt();
  recovery.RecordFallback(30, 10, start + 310s);
  EXPECT_FALSE(recovery.CanRecover(2, 7, start + 610s, start, 200ms, 300s));
  EXPECT_TRUE(recovery.CanRecover(2, 7, start + 910s, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, LearnedExpansionNeverDecreasesAcrossFallbacks)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(20, 10, start);
  recovery.RecordFallback(30, 10, start);
  recovery.RecordFallback(15, 10, start);
  EXPECT_EQ(30u, recovery.EstimateFilesystemStreams(10));
}

TEST(AutomaticDetailRecovery, UnknownCalibrationLatchesUntilExplicitReset)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 0, start);
  EXPECT_FALSE(recovery.CanRecover(1, 7, start + 3600s, start, 200ms, 300s));
  recovery.RecordFallback(30, 10, start + 3600s);
  EXPECT_FALSE(recovery.CanRecover(1, 7, start + 7200s, start, 200ms, 300s));
  recovery.Reset();
  EXPECT_TRUE(recovery.CanRecover(1, 7, start + 7200s, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, ExplicitResetDiscardsExpansionAndProbeBackoff)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 1, start);
  recovery.RecordRecoveryAttempt();
  recovery.RecordFallback(30, 1, start + 300s);
  recovery.Reset();
  EXPECT_EQ(1u, recovery.EstimateFilesystemStreams(1));
  EXPECT_TRUE(recovery.CanRecover(1, 7, start + 300s, start, 200ms, 300s));
}

TEST(AutomaticDetailRecovery, EstimateSaturatesInsteadOfOverflowing)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 10, start);
  EXPECT_EQ(std::numeric_limits<uint64_t>::max(),
            recovery.EstimateFilesystemStreams(std::numeric_limits<uint64_t>::max()));
}

TEST(AutomaticDetailRecovery, ClockBeforeFallbackDoesNotLookLikeElapsedHoldoff)
{
  AutomaticDetailRecovery recovery;
  recovery.RecordFallback(30, 10, start + 300s);
  EXPECT_FALSE(recovery.CanRecover(1, 7, start, start, 200ms, 0s));
}
