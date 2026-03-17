#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

namespace eos::common::traffic_shaping {

// Circular-buffer sliding window for computing rates over variable time windows.
// Not thread-safe on its own; callers must hold any required locks.
class SlidingWindowStats {
public:
  SlidingWindowStats() = default;

  SlidingWindowStats(const double max_history_seconds, const double tick_interval_seconds)
      : mTickIntervalSec(tick_interval_seconds)
      , mHistorySize(
            std::max(1, static_cast<int>(max_history_seconds / tick_interval_seconds)))
      , mBuffer(mHistorySize, 0)
  {
  }

  void
  Add(const uint64_t bytes)
  {
    mBuffer[mHead] += bytes;
  }

  void
  Tick()
  {
    mHead = (mHead + 1) % mHistorySize;
    mBuffer[mHead] = 0;
  }

  double GetRate(double seconds) const;

  uint64_t GetMax(bool ignore_zeroes = false) const;

  uint64_t GetMin(bool ignore_zeroes = false) const;

  double GetMean(bool ignore_zeroes = false) const;

  double GetMedian(bool ignore_zeroes = false) const;

private:
  double mTickIntervalSec{};
  int mHistorySize{};
  std::vector<uint64_t> mBuffer{};
  int mHead{};
};

inline double
SlidingWindowStats::GetRate(const double seconds) const
{
  if (seconds <= 0.0) {
    return 0.0;
  }

  int num_buckets = static_cast<int>(std::round(seconds / mTickIntervalSec));
  if (num_buckets <= 0) {
    num_buckets = 1;
  }
  if (num_buckets > mHistorySize) {
    num_buckets = mHistorySize;
  }

  uint64_t sum = 0;
  int idx = mHead;

  for (int i = 0; i < num_buckets; ++i) {
    sum += mBuffer[idx];

    if (--idx < 0) {
      idx = mHistorySize - 1;
    }
  }

  const double actual_window_sec = static_cast<double>(num_buckets) * mTickIntervalSec;
  return static_cast<double>(sum) / actual_window_sec;
}

inline uint64_t
SlidingWindowStats::GetMax(const bool ignore_zeroes) const
{
  uint64_t max_val = 0;
  for (int i = 0; i < mHistorySize; ++i) {
    if (i == mHead) {
      continue;
    }
    if (mBuffer[i] == 0 && ignore_zeroes) {
      continue;
    }
    if (mBuffer[i] > max_val) {
      max_val = mBuffer[i];
    }
  }
  return max_val;
}

inline uint64_t
SlidingWindowStats::GetMin(const bool ignore_zeroes) const
{
  uint64_t min_val = UINT64_MAX;
  for (int i = 0; i < mHistorySize; ++i) {
    if (i == mHead) {
      continue;
    }
    if (mBuffer[i] == 0 && ignore_zeroes) {
      continue;
    }
    if (mBuffer[i] < min_val) {
      min_val = mBuffer[i];
    }
  }
  return min_val == UINT64_MAX ? 0 : min_val;
}

inline double
SlidingWindowStats::GetMean(const bool ignore_zeroes) const
{
  uint64_t sum = 0;
  int count = 0;
  for (int i = 0; i < mHistorySize; ++i) {
    if (i == mHead) {
      continue;
    }
    if (mBuffer[i] == 0 && ignore_zeroes) {
      continue;
    }
    sum += mBuffer[i];
    count++;
  }
  return count == 0 ? 0.0 : static_cast<double>(sum) / count;
}

inline double
SlidingWindowStats::GetMedian(const bool ignore_zeroes) const
{
  std::vector<uint64_t> valid_values;
  valid_values.reserve(mHistorySize);

  for (int i = 0; i < mHistorySize; ++i) {
    if (i == mHead) {
      continue;
    }
    if (mBuffer[i] == 0 && ignore_zeroes) {
      continue;
    }
    valid_values.push_back(mBuffer[i]);
  }

  if (valid_values.empty()) {
    return 0.0;
  }

  std::sort(valid_values.begin(), valid_values.end());

  const size_t size = valid_values.size();
  const size_t mid = size / 2;

  if (size % 2 == 0) {
    return (static_cast<double>(valid_values[mid - 1]) +
            static_cast<double>(valid_values[mid])) /
           2.0;
  }
  return static_cast<double>(valid_values[mid]);
}

} // namespace eos::common::traffic_shaping
