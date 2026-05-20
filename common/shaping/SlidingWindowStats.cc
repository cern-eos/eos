#include "common/shaping/SlidingWindowStats.hh"

#include <algorithm>

namespace eos::common::traffic_shaping {

SlidingWindowStats::SlidingWindowStats(const double max_history_seconds,
                                       const double tick_interval_seconds)
    : mTickIntervalSec(tick_interval_seconds)
    , mHistorySize(
          std::max(1, static_cast<int>(max_history_seconds / tick_interval_seconds)))
    , mBuffer(mHistorySize, 0)
    , mDurationBuffer(mHistorySize, tick_interval_seconds)
{
}

void
SlidingWindowStats::Add(const uint64_t bytes)
{
  mBuffer[mHead] += bytes;
}

void
SlidingWindowStats::Tick()
{
  Tick(mTickIntervalSec);
}

void
SlidingWindowStats::Tick(const double elapsed_seconds)
{
  if (mHistorySize <= 0 || mBuffer.empty()) {
    return;
  }

  mDurationBuffer[mHead] = elapsed_seconds > 0.0 ? elapsed_seconds : mTickIntervalSec;
  mHead = (mHead + 1) % mHistorySize;
  mBuffer[mHead] = 0;
  mDurationBuffer[mHead] = mTickIntervalSec;
}

double
SlidingWindowStats::GetRate(const double seconds) const
{
  if (seconds <= 0.0 || mHistorySize <= 0 || mBuffer.empty()) {
    return 0.0;
  }

  uint64_t sum = 0;
  double duration_sum = 0.0;
  int idx = mHead - 1;

  if (idx < 0) {
    idx = mHistorySize - 1;
  }

  for (int i = 0; i < mHistorySize; ++i) {
    sum += mBuffer[idx];
    duration_sum += mDurationBuffer[idx] > 0.0 ? mDurationBuffer[idx] : mTickIntervalSec;

    if (duration_sum >= seconds) {
      break;
    }

    if (--idx < 0) {
      idx = mHistorySize - 1;
    }
  }

  const double actual_window_sec = std::max(seconds, duration_sum);
  return static_cast<double>(sum) / actual_window_sec;
}

uint64_t
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

uint64_t
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

double
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

double
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
