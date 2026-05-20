#pragma once

#include <cstdint>
#include <vector>

namespace eos::common::traffic_shaping {

// Circular-buffer sliding window for computing rates over variable time windows.
// Not thread-safe on its own; callers must hold any required locks.
class SlidingWindowStats {
public:
  SlidingWindowStats() = default;

  SlidingWindowStats(double max_history_seconds, double tick_interval_seconds);

  void Add(uint64_t bytes);

  void Tick();

  void Tick(double elapsed_seconds);

  double GetRate(double seconds) const;

  uint64_t GetMax(bool ignore_zeroes = false) const;

  uint64_t GetMin(bool ignore_zeroes = false) const;

  double GetMean(bool ignore_zeroes = false) const;

  double GetMedian(bool ignore_zeroes = false) const;

private:
  double mTickIntervalSec{};
  int mHistorySize{};
  std::vector<uint64_t> mBuffer{};
  std::vector<double> mDurationBuffer{};
  int mHead{};
};

} // namespace eos::common::traffic_shaping
