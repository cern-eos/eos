#include "gtest/gtest.h"
#include "mgm/GroupDrainer.hh"


TEST(RetryTracker, basic)
{
  using namespace eos::mgm;
  using namespace std::chrono_literals;
  GroupDrainer::RetryTracker tracker;
  std::chrono::time_point<std::chrono::steady_clock> time0 {};
  ASSERT_EQ(tracker.count, 0);
  EXPECT_EQ(tracker.last_run_time, time0);
  ASSERT_TRUE(tracker.need_update());
  tracker.update();
  ASSERT_EQ(tracker.count, 1);
  auto updated_time = tracker.last_run_time;
  EXPECT_NE(tracker.last_run_time, time0);
  eos::common::SteadyClock test_clock(true);
  ASSERT_FALSE(tracker.need_update(900));
  test_clock.advance(eos::common::SteadyClock::secondsSinceEpoch(std::chrono::steady_clock::now()));
  test_clock.advance(std::chrono::seconds(902));
  auto diff = std::chrono::duration_cast<std::chrono::seconds>(test_clock.getTime() - updated_time);
  ASSERT_TRUE(tracker.need_update(900, &test_clock));
}