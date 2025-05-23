#pragma once
#include "common/SteadyClock.hh"

namespace eos::mgm
{

constexpr uint64_t DEFAULT_RETRY_INTERVAL = 4 * 3600;


struct RetryTracker {
  uint16_t count;
  std::chrono::time_point<std::chrono::steady_clock> last_run_time {};

  RetryTracker() : count(0) {}

  bool need_update(uint64_t retry_interval = DEFAULT_RETRY_INTERVAL,
                   eos::common::SteadyClock* clock = nullptr) const
  {
    if (count == 0) {
      return true;
    }

    auto curr_time  = eos::common::SteadyClock::now(clock);
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>
                   (curr_time - last_run_time);
    return (elapsed.count() > (int64_t)retry_interval);
  }

  void update(eos::common::SteadyClock* clock = nullptr)
  {
    ++count;
    last_run_time = eos::common::SteadyClock::now(clock);
  }
};

} // eos::mgm
