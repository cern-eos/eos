#include "gtest/gtest.h"
#include "mgm/groupdrainer/RetryTracker.hh"


TEST(RetryTracker, basic)
{
  using namespace eos::mgm;
  using namespace std::chrono_literals;
  RetryTracker tracker;
  std::chrono::time_point<std::chrono::steady_clock> time0 {};
  ASSERT_EQ(tracker.count, 0);
  EXPECT_EQ(tracker.last_run_time, time0);
  ASSERT_TRUE(tracker.need_update());
  tracker.update();
  ASSERT_EQ(tracker.count, 1);
  EXPECT_NE(tracker.last_run_time, time0);
  eos::common::SteadyClock test_clock(true);
  ASSERT_FALSE(tracker.need_update(900));
  test_clock.advance(eos::common::SteadyClock::SecondsSinceEpoch(
                       std::chrono::steady_clock::now()));
  test_clock.advance(std::chrono::seconds(902));
  ASSERT_TRUE(tracker.need_update(900, &test_clock));
}
