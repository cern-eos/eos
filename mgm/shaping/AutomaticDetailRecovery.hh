#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <numeric>

namespace eos::mgm::traffic_shaping {

// Aggregate reports cannot reveal a change in filesystem fan-out. Retain a
// conservative learned expansion and back off probes that exceed the real
// filesystem guard again; this estimate never replaces that guard.
class AutomaticDetailRecovery {
public:
  using Clock = std::chrono::steady_clock;

  void
  Reset()
  {
    *this = AutomaticDetailRecovery{};
  }

  void
  RecordFallback(const uint64_t filesystem_streams, const uint64_t aggregate_streams,
                 const Clock::time_point now)
  {
    if (mHasFallback && mRecoveryAttempted) {
      mFailedProbes = std::min<uint32_t>(mFailedProbes + 1, 6);
    }
    mHasFallback = true;
    mRecoveryAttempted = false;
    mLastFallback = now;

    if (aggregate_streams == 0 || filesystem_streams < aggregate_streams) {
      mUnknownExpansion = true;
      return;
    }

    if (static_cast<long double>(filesystem_streams) / aggregate_streams >
        static_cast<long double>(mExpansionNumerator) / mExpansionDenominator) {
      const uint64_t divisor = std::gcd(filesystem_streams, aggregate_streams);
      mExpansionNumerator = filesystem_streams / divisor;
      mExpansionDenominator = aggregate_streams / divisor;
    }
  }

  void
  RecordRecoveryAttempt()
  {
    mRecoveryAttempted = true;
  }

  uint64_t
  EstimateFilesystemStreams(const uint64_t aggregate_streams) const
  {
    const auto maximum = std::numeric_limits<uint64_t>::max();
    if (mUnknownExpansion || aggregate_streams > maximum / mExpansionNumerator) {
      return maximum;
    }
    const uint64_t product = aggregate_streams * mExpansionNumerator;
    return std::max(aggregate_streams, product / mExpansionDenominator +
                                           (product % mExpansionDenominator != 0));
  }

  bool
  CanRecover(const uint64_t aggregate_streams, const uint64_t low_cardinality,
             const Clock::time_point now, const Clock::time_point stream_state_started_at,
             const std::chrono::milliseconds report_period,
             const std::chrono::seconds aggregate_holdoff) const
  {
    if (!mHasFallback) {
      return aggregate_streams <= low_cardinality;
    }
    if (aggregate_streams == 0 || mUnknownExpansion) {
      return false;
    }

    // This settling floor is independent of a shortened GC holdoff: a cleared
    // map and the first partial report are not evidence of reduced load.
    const auto settling = std::max(std::chrono::milliseconds(1), report_period) * 3;
    const auto base_delay =
        std::max(std::chrono::duration_cast<std::chrono::milliseconds>(aggregate_holdoff),
                 settling);
    const auto retry_delay = base_delay * (int64_t{1} << mFailedProbes);
    return now - mLastFallback >= retry_delay &&
           now - stream_state_started_at >= settling &&
           EstimateFilesystemStreams(aggregate_streams) <= low_cardinality;
  }

private:
  bool mHasFallback = false;
  bool mUnknownExpansion = false;
  bool mRecoveryAttempted = false;
  uint32_t mFailedProbes = 0;
  uint64_t mExpansionNumerator = 1;
  uint64_t mExpansionDenominator = 1;
  Clock::time_point mLastFallback{};
};

} // namespace eos::mgm::traffic_shaping
