#include "mgm/shaping/TrafficShaping.hh"
#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"

#include "proto/TrafficShaping.pb.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <google/protobuf/util/json_util.h>
#include <limits>
#include <map>
#include <new>
#include <numeric>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <utility>

namespace eos::mgm::traffic_shaping {

namespace {
constexpr double kMinReservationDeficitFraction = 0.05;
constexpr double kMinReservationDeficitBps = 16.0 * 1024.0 * 1024.0;
constexpr auto kControllerLimitTtl = std::chrono::minutes(5);
constexpr auto kControllerLimitRefreshInterval = std::chrono::minutes(1);
constexpr auto kControllerResponseInterval = std::chrono::seconds(10);
constexpr auto kControllerSuppressionInterval = std::chrono::seconds(15);
constexpr auto kControllerMaxSuppressionInterval = std::chrono::seconds(300);
constexpr auto kControllerUpdateInterval = std::chrono::milliseconds(500);
constexpr auto kFstIoDelayConfigRefreshInterval = std::chrono::seconds(5);
constexpr uint32_t kControllerEngageSamples = 2;
constexpr double kMaxControllerTightenFraction = 0.5;
constexpr double kControllerRetuneGain = 0.5;
constexpr double kMinControllerResponseFraction = 0.10;
constexpr uint32_t kMaxControllerBackoffExponent = 5;
constexpr uint64_t kMaxDelayUs = 2000000;
constexpr double kNodeEntityActivityMinAgeSeconds = 5.0;
// FST disk load is sampled every 15s and published on a randomized 5-15s
// cadence.  This window tolerates normal jitter while rejecting stopped or
// substantially clock-skewed publishers.
constexpr int64_t kFilesystemPressureMaxAgeMs = 45'000;
constexpr int64_t kFilesystemPressureMaxFutureSkewMs = 5'000;
constexpr int64_t kFstReportMaxFutureSkewMs = 5'000;
constexpr auto kReportQueueWarningInterval = std::chrono::seconds(1);
constexpr std::size_t kMaxQueuedReports = 500;
constexpr std::size_t kMaxFstReportEntries = 8192;
constexpr std::size_t kMaxFstReportIdentityBytes =
    eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES;
constexpr std::size_t kMaxFstIoEntryWireBytes = 4096;
constexpr std::size_t kMaxFstIoEntryWireFields = 16;
constexpr std::size_t kMaxFstReportWireFields = kMaxFstReportEntries + 16;
constexpr std::size_t kMaxFstTrackedNodes = 4096;
constexpr std::size_t kMaxFstStreamsPerNode = 8192;
constexpr std::size_t kMaxFstStreamStates = 8192;
constexpr std::size_t kMaxFstStreamStateEstimatedBytes = 256ULL * 1024ULL * 1024ULL;
// A delta-bearing stream can populate global aggregate and filesystem keys,
// detailed and disk maps, and (in the one-stream-per-node worst case) a node
// map. At the supported 50 ms estimator period those histories alone approach
// 85 KiB. Charge their worst-case downstream state at baseline admission time,
// including map/key headroom, so a later detail or period change cannot turn
// admitted baselines into an unbounded allocation burst.
constexpr std::size_t kEstimatedFstStreamStateBaseBytes = 128ULL * 1024ULL;
constexpr std::size_t kEstimatedFstStreamIdentityByteCopies = 8;
constexpr std::size_t kMaxQueuedReportEstimatedBytes = 64ULL * 1024ULL * 1024ULL;
constexpr std::size_t kEstimatedFstReportEntryOverheadBytes = 128;
constexpr std::size_t kReportQueueHighWater = kMaxQueuedReports * 4 / 5;
constexpr std::size_t kReportQueueEstimatedBytesHighWater =
    kMaxQueuedReportEstimatedBytes * 4 / 5;
static_assert(kReportQueueHighWater < kMaxQueuedReports);

int
NodeEntityActivityMaxAgeSeconds(const double report_interval_seconds)
{
  return static_cast<int>(std::ceil(
      std::max(2.0 * report_interval_seconds, kNodeEntityActivityMinAgeSeconds)));
}

uint64_t
MonotonicNowNs()
{
  const auto count = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count();
  return static_cast<uint64_t>(std::max<int64_t>(0, count));
}

size_t
EstimateFstStreamStateBytes(const std::string& node_id, const std::string& app)
{
  return kEstimatedFstStreamStateBaseBytes +
         kEstimatedFstStreamIdentityByteCopies * (node_id.size() + app.size());
}

bool
ShouldEmitRateLimitedWarning(std::atomic<uint64_t>& last_warning_ns)
{
  const uint64_t now_ns = MonotonicNowNs();
  const uint64_t interval_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(kReportQueueWarningInterval)
          .count());
  uint64_t previous_ns = last_warning_ns.load(std::memory_order_relaxed);
  while (previous_ns == 0 ||
         (now_ns >= previous_ns && now_ns - previous_ns >= interval_ns)) {
    if (last_warning_ns.compare_exchange_weak(previous_ns, now_ns,
                                              std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

bool
ReadProtobufVarint(const std::string_view wire, size_t& position,
                   uint64_t& value) noexcept
{
  value = 0;
  for (uint32_t byte_index = 0; byte_index < 10; ++byte_index) {
    if (position >= wire.size()) {
      return false;
    }
    const uint8_t byte = static_cast<uint8_t>(wire[position++]);
    if (byte_index == 9 && byte > 1) {
      return false;
    }
    value |= static_cast<uint64_t>(byte & 0x7fU) << (byte_index * 7U);
    if ((byte & 0x80U) == 0) {
      return true;
    }
  }
  return false;
}

bool
ReadProtobufLength(const std::string_view wire, size_t& position, size_t& length) noexcept
{
  uint64_t encoded_length = 0;
  if (!ReadProtobufVarint(wire, position, encoded_length) ||
      encoded_length > wire.size() - position) {
    return false;
  }
  length = static_cast<size_t>(encoded_length);
  return true;
}

bool
SkipProtobufField(const std::string_view wire, size_t& position,
                  const uint32_t wire_type) noexcept
{
  uint64_t ignored = 0;
  size_t length = 0;
  switch (wire_type) {
  case 0:
    return ReadProtobufVarint(wire, position, ignored);
  case 1:
    if (wire.size() - position < 8) {
      return false;
    }
    position += 8;
    return true;
  case 2:
    if (!ReadProtobufLength(wire, position, length)) {
      return false;
    }
    position += length;
    return true;
  case 5:
    if (wire.size() - position < 4) {
      return false;
    }
    position += 4;
    return true;
  default:
    // Groups are deprecated and are not emitted by the FST schema. Rejecting
    // them keeps this allocation-free preflight simple and unambiguous.
    return false;
  }
}

bool
ValidateFstIoEntryWire(const std::string_view wire) noexcept
{
  size_t position = 0;
  size_t field_count = 0;
  size_t app_name_count = 0;
  while (position < wire.size()) {
    if (++field_count > kMaxFstIoEntryWireFields) {
      return false;
    }
    uint64_t key = 0;
    if (!ReadProtobufVarint(wire, position, key) || (key >> 3U) == 0) {
      return false;
    }
    const uint32_t field_number = static_cast<uint32_t>(key >> 3U);
    const uint32_t wire_type = static_cast<uint32_t>(key & 0x7U);
    if (field_number == 1 && wire_type == 2) {
      size_t length = 0;
      if (++app_name_count > 1 || !ReadProtobufLength(wire, position, length) ||
          length > kMaxFstReportIdentityBytes) {
        return false;
      }
      position += length;
    } else if (!SkipProtobufField(wire, position, wire_type)) {
      return false;
    }
  }
  return true;
}

bool
ValidateFstIoReportWire(const std::string_view wire) noexcept
{
  size_t position = 0;
  size_t field_count = 0;
  size_t node_id_count = 0;
  size_t entry_count = 0;
  while (position < wire.size()) {
    if (++field_count > kMaxFstReportWireFields) {
      return false;
    }
    uint64_t key = 0;
    if (!ReadProtobufVarint(wire, position, key) || (key >> 3U) == 0) {
      return false;
    }
    const uint32_t field_number = static_cast<uint32_t>(key >> 3U);
    const uint32_t wire_type = static_cast<uint32_t>(key & 0x7U);
    if (field_number == 1 && wire_type == 2) {
      size_t length = 0;
      if (++node_id_count > 1 || !ReadProtobufLength(wire, position, length) ||
          length > kMaxFstReportIdentityBytes) {
        return false;
      }
      position += length;
    } else if (field_number == 4 && wire_type == 2) {
      size_t length = 0;
      if (++entry_count > kMaxFstReportEntries ||
          !ReadProtobufLength(wire, position, length) ||
          length > kMaxFstIoEntryWireBytes ||
          !ValidateFstIoEntryWire(wire.substr(position, length))) {
        return false;
      }
      position += length;
    } else if (!SkipProtobufField(wire, position, wire_type)) {
      return false;
    }
  }
  return true;
}

double
SanitizeRate(const double value)
{
  return std::isfinite(value) && value > 0.0 ? value : 0.0;
}

uint64_t
ClampRateToUint64(const long double value)
{
  if (!std::isfinite(value) || value <= 0.0L) {
    return 0;
  }

  constexpr auto max_value = std::numeric_limits<uint64_t>::max();
  if (value >= static_cast<long double>(max_value)) {
    return max_value;
  }

  return static_cast<uint64_t>(value + 0.5L);
}

uint64_t
EffectiveReservation(const TrafficShapingPolicy& policy, const bool is_write,
                     const bool include_controller_limit = true)
{
  if (!policy.is_enabled) {
    return 0;
  }

  uint64_t reservation = is_write ? policy.reservation_write_bytes_per_sec
                                  : policy.reservation_read_bytes_per_sec;
  const uint64_t user_limit =
      is_write ? policy.limit_write_bytes_per_sec : policy.limit_read_bytes_per_sec;
  const uint64_t controller_limit =
      include_controller_limit ? (is_write ? policy.controller_limit_write_bytes_per_sec
                                           : policy.controller_limit_read_bytes_per_sec)
                               : 0;
  uint64_t ceiling = user_limit;
  if (controller_limit > 0) {
    ceiling = ceiling > 0 ? std::min(ceiling, controller_limit) : controller_limit;
  }
  if (ceiling > 0) {
    reservation = std::min(reservation, ceiling);
  }
  return reservation;
}

template <typename DelayMap, typename Key>
void
ScaleDelayForLimitChange(DelayMap* delays, const Key& key, const uint64_t old_limit,
                         const uint64_t new_limit)
{
  if (old_limit == new_limit) {
    return;
  }

  if (new_limit == 0) {
    delays->erase(key);
    return;
  }

  if (old_limit == 0) {
    return;
  }

  auto it = delays->find(key);
  if (it == delays->end() || it->second == 0) {
    return;
  }

  const long double scaled_delay =
      static_cast<long double>(it->second) * old_limit / new_limit;
  it->second = std::clamp<uint64_t>(ClampRateToUint64(scaled_delay), 1, kMaxDelayUs);
}

std::string
NormalizeFstNodeId(const std::string& node_id)
{
  if (node_id.rfind("/eos/", 0) == 0) {
    return node_id;
  }

  return SSTR("/eos/" << node_id << "/fst");
}

TrafficShapingPolicy
PreparePolicyForSet(const TrafficShapingPolicy& policy,
                    const TrafficShapingPolicy* old_policy)
{
  auto next_policy = policy;
  const auto now = std::chrono::steady_clock::now();

  if (!next_policy.is_enabled) {
    next_policy.controller_limit_write_bytes_per_sec = 0;
    next_policy.controller_limit_read_bytes_per_sec = 0;
    next_policy.controller_limit_write_update_time = {};
    next_policy.controller_limit_read_update_time = {};
  }

  auto refresh_timestamp =
      [now](const uint64_t limit, std::chrono::steady_clock::time_point& update_time,
            const TrafficShapingPolicy* old_policy, const uint64_t old_limit,
            const std::chrono::steady_clock::time_point old_update_time) {
        if (limit == 0) {
          update_time = {};
          return;
        }

        if (old_policy == nullptr || limit != old_limit ||
            update_time == std::chrono::steady_clock::time_point{}) {
          update_time = now;
          return;
        }

        if (update_time == old_update_time) {
          return;
        }
      };

  refresh_timestamp(next_policy.controller_limit_write_bytes_per_sec,
                    next_policy.controller_limit_write_update_time, old_policy,
                    old_policy ? old_policy->controller_limit_write_bytes_per_sec : 0,
                    old_policy ? old_policy->controller_limit_write_update_time
                               : std::chrono::steady_clock::time_point{});
  refresh_timestamp(next_policy.controller_limit_read_bytes_per_sec,
                    next_policy.controller_limit_read_update_time, old_policy,
                    old_policy ? old_policy->controller_limit_read_bytes_per_sec : 0,
                    old_policy ? old_policy->controller_limit_read_update_time
                               : std::chrono::steady_clock::time_point{});

  return next_policy;
}

bool
PolicyRuntimeStateChanged(const TrafficShapingPolicy& lhs,
                          const TrafficShapingPolicy& rhs)
{
  return lhs != rhs ||
         lhs.controller_limit_write_bytes_per_sec !=
             rhs.controller_limit_write_bytes_per_sec ||
         lhs.controller_limit_read_bytes_per_sec !=
             rhs.controller_limit_read_bytes_per_sec ||
         lhs.controller_limit_write_update_time !=
             rhs.controller_limit_write_update_time ||
         lhs.controller_limit_read_update_time != rhs.controller_limit_read_update_time;
}

bool
HasControllerFeedback(const ReservationControllerState::Direction& state)
{
  return state.consecutive_deficit_samples != 0 || !state.protected_apps.empty() ||
         state.applied_reduction_bps != 0.0 || state.ineffective_probe_count != 0 ||
         !state.failed_protected_apps.empty() ||
         state.last_observed_protected_gain_bps != 0.0 ||
         state.last_response_ratio != 0.0 ||
         state.last_adjustment_time != std::chrono::steady_clock::time_point{} ||
         state.healthy_since != std::chrono::steady_clock::time_point{} ||
         state.suppressed_until != std::chrono::steady_clock::time_point{};
}

void
ResetInactiveReservationControllerDirection(
    ReservationControllerState::Direction& state,
    const std::chrono::steady_clock::time_point now)
{
  const bool preserve_suppression =
      state.ineffective_probe_count > 0 &&
      state.suppressed_until != std::chrono::steady_clock::time_point{} &&
      now < state.suppressed_until;
  if (!preserve_suppression) {
    state = {};
    return;
  }

  // A qualification or active intervention cannot span a transfer gap. Keep
  // only the bounded negative-probe evidence so the next burst cannot restart
  // immediately with a full-strength cut.
  state.consecutive_deficit_samples = 0;
  state.protected_apps.clear();
  state.applied_reduction_bps = 0.0;
  state.last_adjustment_time = {};
  state.healthy_since = {};
}
} // namespace

CumulativeRateHistory::CumulativeRateHistory(const double max_history_seconds,
                                             const double tick_interval_seconds)
{
  Reset(max_history_seconds, tick_interval_seconds);
}

void
CumulativeRateHistory::Reset(const double max_history_seconds,
                             const double tick_interval_seconds)
{
  mTickIntervalSec = std::max(0.001, tick_interval_seconds);
  mMediumIntervalSec = std::max(1.0, mTickIntervalSec);
  mCoarseIntervalSec = std::max(5.0, mTickIntervalSec);
  const double bounded_history_seconds = std::max(0.0, max_history_seconds);
  const auto fine_capacity = static_cast<size_t>(std::ceil(
      std::min(bounded_history_seconds, kFineHistorySeconds) / mTickIntervalSec));
  const auto medium_capacity = static_cast<size_t>(std::ceil(
      std::min(bounded_history_seconds, kMediumHistorySeconds) / mMediumIntervalSec));
  const auto coarse_capacity =
      static_cast<size_t>(std::ceil(bounded_history_seconds / mCoarseIntervalSec));
  // Keep one checkpoint immediately before each longest requested window so a
  // full-window rate remains available after either ring starts wrapping.
  mFineHistory.Reset(std::max<size_t>(2, fine_capacity + 1));
  mMediumHistory.Reset(std::max<size_t>(2, medium_capacity + 1));
  mCoarseHistory.Reset(std::max<size_t>(2, coarse_capacity + 1));
  mLastMediumCheckpointSec = 0.0;
  mLastCoarseCheckpointSec = 0.0;
  mCumulativeSample = {};
}

void
CumulativeRateHistory::HistoryRing::Reset(const size_t capacity)
{
  samples.clear();
  samples.reserve(capacity);
  this->capacity = capacity;
  head = 0;
  size = 0;
  base = {};
}

void
CumulativeRateHistory::HistoryRing::Push(const Sample& sample)
{
  if (size < capacity) {
    samples.push_back(sample);
    ++size;
    if (size == capacity) {
      head = 0;
    }
    return;
  }

  base = samples[head];
  samples[head] = sample;
  head = (head + 1) % capacity;
}

const CumulativeRateHistory::Sample&
CumulativeRateHistory::HistoryRing::GetLogicalSample(const size_t index) const
{
  const size_t oldest = size == capacity ? head : 0;
  return samples[(oldest + index) % size];
}

CumulativeRateHistory::Sample
CumulativeRateHistory::HistoryRing::FindBoundary(const double target_elapsed) const
{
  Sample lower = base;
  size_t low = 0;
  size_t high = size;
  while (low < high) {
    const size_t mid = low + (high - low) / 2;
    if (GetLogicalSample(mid).elapsed_seconds <= target_elapsed) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  if (low > 0) {
    lower = GetLogicalSample(low - 1);
  }
  if (low == size || target_elapsed <= lower.elapsed_seconds) {
    return lower;
  }

  const Sample& upper = GetLogicalSample(low);
  const double interval = upper.elapsed_seconds - lower.elapsed_seconds;
  if (interval <= 0.0) {
    return lower;
  }

  const double fraction =
      std::clamp((target_elapsed - lower.elapsed_seconds) / interval, 0.0, 1.0);
  auto interpolate = [fraction](const double lower_counter, const double upper_counter) {
    return lower_counter + (upper_counter - lower_counter) * fraction;
  };

  return {{interpolate(lower.counters.bytes_read, upper.counters.bytes_read),
           interpolate(lower.counters.bytes_written, upper.counters.bytes_written),
           interpolate(lower.counters.read_ops, upper.counters.read_ops),
           interpolate(lower.counters.write_ops, upper.counters.write_ops)},
          target_elapsed};
}

void
CumulativeRateHistory::Push(const RateCounters& counters, const double elapsed_seconds)
{
  const double duration = elapsed_seconds > 0.0 && std::isfinite(elapsed_seconds)
                              ? elapsed_seconds
                              : mTickIntervalSec;
  mCumulativeSample.counters.bytes_read += counters.bytes_read;
  mCumulativeSample.counters.bytes_written += counters.bytes_written;
  mCumulativeSample.counters.read_ops += counters.read_ops;
  mCumulativeSample.counters.write_ops += counters.write_ops;
  mCumulativeSample.elapsed_seconds += duration;

  mFineHistory.Push(mCumulativeSample);
  if (mMediumHistory.size == 0 ||
      mCumulativeSample.elapsed_seconds - mLastMediumCheckpointSec >=
          mMediumIntervalSec) {
    mMediumHistory.Push(mCumulativeSample);
    mLastMediumCheckpointSec = mCumulativeSample.elapsed_seconds;
  }
  if (mCoarseHistory.size == 0 ||
      mCumulativeSample.elapsed_seconds - mLastCoarseCheckpointSec >=
          mCoarseIntervalSec) {
    mCoarseHistory.Push(mCumulativeSample);
    mLastCoarseCheckpointSec = mCumulativeSample.elapsed_seconds;
  }
}

RateMetrics
CumulativeRateHistory::GetRate(const double seconds) const
{
  if (seconds <= 0.0 || mFineHistory.size == 0) {
    return {};
  }

  const Sample& newest = mCumulativeSample;
  const double target_elapsed = newest.elapsed_seconds - seconds;
  auto covers_target = [target_elapsed](const HistoryRing& history) {
    return target_elapsed <= 0.0 || history.base.elapsed_seconds <= target_elapsed;
  };

  const HistoryRing* history = &mCoarseHistory;
  if (seconds <= kFineHistorySeconds && covers_target(mFineHistory)) {
    history = &mFineHistory;
  } else if (seconds <= kMediumHistorySeconds && covers_target(mMediumHistory)) {
    history = &mMediumHistory;
  } else if (!covers_target(mCoarseHistory)) {
    history = covers_target(mMediumHistory) ? &mMediumHistory : &mFineHistory;
  }
  const Sample boundary = history->FindBoundary(target_elapsed);

  const double actual_window_seconds = newest.elapsed_seconds - boundary.elapsed_seconds;
  const double denominator = target_elapsed < 0.0 ? seconds : actual_window_seconds;
  if (denominator <= 0.0) {
    return {};
  }

  return {
      static_cast<double>(newest.counters.bytes_read - boundary.counters.bytes_read) /
          denominator,
      static_cast<double>(newest.counters.bytes_written -
                          boundary.counters.bytes_written) /
          denominator,
      static_cast<double>(newest.counters.read_ops - boundary.counters.read_ops) /
          denominator,
      static_cast<double>(newest.counters.write_ops - boundary.counters.write_ops) /
          denominator,
  };
}

uint64_t
TrafficShapingPolicy::GetEffectiveWriteLimit() const
{
  if (!is_enabled) {
    return 0;
  }
  const uint64_t active_user_limit = limit_write_bytes_per_sec;
  if (active_user_limit > 0 && controller_limit_write_bytes_per_sec > 0) {
    return std::min(active_user_limit, controller_limit_write_bytes_per_sec);
  }
  return active_user_limit > 0 ? active_user_limit : controller_limit_write_bytes_per_sec;
}

uint64_t
TrafficShapingPolicy::GetEffectiveReadLimit() const
{
  if (!is_enabled) {
    return 0;
  }
  const uint64_t active_user_limit = limit_read_bytes_per_sec;
  if (active_user_limit > 0 && controller_limit_read_bytes_per_sec > 0) {
    return std::min(active_user_limit, controller_limit_read_bytes_per_sec);
  }
  return active_user_limit > 0 ? active_user_limit : controller_limit_read_bytes_per_sec;
}

bool
TrafficShapingPolicy::IsReservationConfigurationFeasible() const
{
  if (!is_enabled) {
    return true;
  }

  const auto has_conflict = [](const uint64_t reservation, const uint64_t user_limit,
                               const uint64_t controller_limit) {
    return (user_limit > 0 && user_limit < reservation) ||
           (controller_limit > 0 && controller_limit < reservation);
  };
  return !has_conflict(reservation_read_bytes_per_sec, limit_read_bytes_per_sec,
                       controller_limit_read_bytes_per_sec) &&
         !has_conflict(reservation_write_bytes_per_sec, limit_write_bytes_per_sec,
                       controller_limit_write_bytes_per_sec);
}

bool
TrafficShapingPolicy::IsEmpty() const
{
  return limit_write_bytes_per_sec == 0 && limit_read_bytes_per_sec == 0 &&
         reservation_write_bytes_per_sec == 0 && reservation_read_bytes_per_sec == 0 &&
         controller_limit_write_bytes_per_sec == 0 &&
         controller_limit_read_bytes_per_sec == 0;
}

bool
TrafficShapingPolicy::IsActive() const
{
  if (!is_enabled) {
    return false;
  }
  const bool has_user_rules =
      limit_write_bytes_per_sec > 0 || limit_read_bytes_per_sec > 0 ||
      reservation_write_bytes_per_sec > 0 || reservation_read_bytes_per_sec > 0;
  const bool has_controller_rules =
      controller_limit_write_bytes_per_sec > 0 || controller_limit_read_bytes_per_sec > 0;
  return has_controller_rules || has_user_rules;
}

bool
TrafficShapingPolicy::operator==(const TrafficShapingPolicy& policy) const
{
  return limit_write_bytes_per_sec == policy.limit_write_bytes_per_sec &&
         limit_read_bytes_per_sec == policy.limit_read_bytes_per_sec &&
         reservation_write_bytes_per_sec == policy.reservation_write_bytes_per_sec &&
         reservation_read_bytes_per_sec == policy.reservation_read_bytes_per_sec &&
         is_enabled == policy.is_enabled;
}

bool
TrafficShapingPolicy::operator!=(const TrafficShapingPolicy& policy) const
{
  return !(*this == policy);
}

std::string
TrafficShapingPolicy::ToString() const
{
  std::ostringstream oss;
  oss << (is_enabled ? "Enabled" : "Disabled") << ", "
      << "Read Limit: " << limit_read_bytes_per_sec << " Bps, "
      << "Write Limit: " << limit_write_bytes_per_sec << " Bps, "
      << "Read Reservation: " << reservation_read_bytes_per_sec << " Bps, "
      << "Write Reservation: " << reservation_write_bytes_per_sec << " Bps, "
      << "Controller Read Limit: " << controller_limit_read_bytes_per_sec << " Bps, "
      << "Controller Write Limit: " << controller_limit_write_bytes_per_sec << " Bps";
  return oss.str();
}

TrafficShapingManager::TrafficShapingManager() = default;

TrafficShapingManager::~TrafficShapingManager() { Clear(); }

void
AddCumulativeStats(RateSnapshot& snapshot, const uint64_t bytes_read,
                   const uint64_t bytes_written, const uint64_t read_ops,
                   const uint64_t write_ops, const time_t now_unix)
{
  snapshot.bytes_read_total += bytes_read;
  snapshot.bytes_written_total += bytes_written;
  snapshot.read_ops_total += read_ops;
  snapshot.write_ops_total += write_ops;
  snapshot.last_activity_time = now_unix;
}

bool
TrafficShapingManager::ApplyThreadConfig(const uint32_t estimators_period_ms,
                                         const uint32_t fst_policy_period_ms,
                                         const uint32_t fst_report_period_ms,
                                         const uint32_t window_seconds) noexcept
{
  try {
    const uint32_t bounded_estimators_period_ms =
        std::clamp(estimators_period_ms, kMinThreadPeriodMs, kMaxThreadPeriodMs);
    const uint32_t bounded_policy_period_ms =
        std::clamp(fst_policy_period_ms, kMinThreadPeriodMs, kMaxThreadPeriodMs);
    const uint32_t bounded_report_period_ms =
        std::clamp(fst_report_period_ms, kMinThreadPeriodMs, kMaxThreadPeriodMs);
    const uint32_t bounded_window_seconds =
        std::clamp(window_seconds, kMinSystemStatsWindowSec, kMaxSystemStatsWindowSec);
    const double estimator_tick_sec = bounded_estimators_period_ms * 0.001;
    const double policy_tick_sec = bounded_policy_period_ms * 0.001;
    const double report_tick_sec = bounded_report_period_ms * 0.001;

    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock lock(mMutex);

    const bool initialized = estimators_update_loop_micro_sec.has_value();
    const bool estimator_changed = mEstimatorsTickIntervalSec != estimator_tick_sec;
    const bool policy_changed = mFstPolicyTickIntervalSec != policy_tick_sec;
    const bool report_changed = mFstReportTickIntervalSec != report_tick_sec;
    const bool window_changed = mSystemStatsWindowSeconds != bounded_window_seconds;
    if (initialized && !estimator_changed && !policy_changed && !report_changed &&
        !window_changed) {
      return true;
    }

    if (mFailNextThreadConfigPreparation.exchange(false, std::memory_order_relaxed)) {
      throw std::bad_alloc{};
    }

    using SlidingWindowStats = eos::common::traffic_shaping::SlidingWindowStats;
    static_assert(std::is_nothrow_move_assignable_v<MultiWindowRate>);
    static_assert(std::is_nothrow_move_assignable_v<std::optional<SlidingWindowStats>>);

    std::optional<MultiWindowRate> replacement_total_stats;
    if (!initialized || estimator_changed) {
      replacement_total_stats.emplace(estimator_tick_sec);
    }
    std::optional<SlidingWindowStats> replacement_estimator_loop_stats{
        std::in_place, bounded_window_seconds, estimator_tick_sec};
    std::optional<SlidingWindowStats> replacement_report_rate_stats{
        std::in_place, bounded_window_seconds, estimator_tick_sec};
    std::optional<SlidingWindowStats> replacement_controller_loop_stats{
        std::in_place, bounded_window_seconds, policy_tick_sec};
    std::optional<SlidingWindowStats> replacement_limit_loop_stats{
        std::in_place, bounded_window_seconds, policy_tick_sec};

    // A cadence change invalidates every fixed-window history. Rebuild these
    // bounded runtime entries lazily instead of allocating a new history for
    // every populated key while holding the manager lock. Cumulative counters
    // and report baselines remain intact.
    if (replacement_total_stats.has_value()) {
      mGlobalStats.clear();
      mNodeStats.clear();
      mDiskStats.clear();
      mDetailedStats.clear();
      mTotalStats = std::move(*replacement_total_stats);
    }

    estimators_update_loop_micro_sec = std::move(replacement_estimator_loop_stats);
    fst_reports_processed_per_second = std::move(replacement_report_rate_stats);
    reservation_controller_update_loop_micro_sec =
        std::move(replacement_controller_loop_stats);
    fst_limits_update_loop_micro_sec = std::move(replacement_limit_loop_stats);
    mSystemStatsWindowSeconds = bounded_window_seconds;
    mEstimatorsTickIntervalSec = estimator_tick_sec;
    mFstPolicyTickIntervalSec = policy_tick_sec;
    mFstReportTickIntervalSec = report_tick_sec;
    ++mControllerInputRevision;
    return true;
  } catch (const std::exception& error) {
    try {
      eos_static_err("msg=\"failed to apply Traffic Shaping thread config; keeping "
                     "previous config\" error=\"%s\"",
                     error.what());
    } catch (...) {
      // Error reporting is best-effort at this noexcept boundary.
    }
    return false;
  } catch (...) {
    try {
      eos_static_err("%s", "msg=\"failed to apply Traffic Shaping thread config due to "
                           "unknown exception; keeping previous config\"");
    } catch (...) {
      // Error reporting is best-effort at this noexcept boundary.
    }
    return false;
  }
}

void
TrafficShapingManager::SetFilesystemDetailEnabled(const bool enabled)
{
  mFilesystemDetailEnabled.store(enabled, std::memory_order_relaxed);
}

double
TrafficShapingManager::CalculateEma(const double current_val, const double prev_ema,
                                    const double alpha)
{
  return (alpha * current_val) + ((1.0 - alpha) * prev_ema);
}

namespace {
struct EntityRateMaps {
  std::unordered_map<std::string, double> app_read;
  std::unordered_map<std::string, double> app_write;
  std::unordered_map<uint32_t, double> uid_read;
  std::unordered_map<uint32_t, double> uid_write;
  std::unordered_map<uint32_t, double> gid_read;
  std::unordered_map<uint32_t, double> gid_write;
};

struct IoPressureSnapshot {
  std::unordered_map<std::string, double> nodes;
  std::unordered_map<DiskKey, double, DiskKeyHash> filesystems;
};

std::optional<double>
ParseFreshFilesystemIoPressure(const std::string& load_value,
                               const std::string& publish_timestamp_value,
                               const int64_t now_ms)
{
  if (load_value.empty() || publish_timestamp_value.empty() || now_ms < 0) {
    return std::nullopt;
  }

  errno = 0;
  char* end = nullptr;
  const double pressure = std::strtod(load_value.c_str(), &end);
  if (errno == ERANGE || end == load_value.c_str() || *end != '\0' ||
      !std::isfinite(pressure)) {
    return std::nullopt;
  }

  uint64_t publish_timestamp_ms = 0;
  const char* const timestamp_begin = publish_timestamp_value.data();
  const char* const timestamp_end = timestamp_begin + publish_timestamp_value.size();
  const auto [timestamp_parse_end, timestamp_error] =
      std::from_chars(timestamp_begin, timestamp_end, publish_timestamp_ms);
  if (timestamp_error != std::errc{} || timestamp_parse_end != timestamp_end) {
    return std::nullopt;
  }

  const uint64_t current_timestamp_ms = static_cast<uint64_t>(now_ms);
  if (publish_timestamp_ms > current_timestamp_ms) {
    if (publish_timestamp_ms - current_timestamp_ms >
        static_cast<uint64_t>(kFilesystemPressureMaxFutureSkewMs)) {
      return std::nullopt;
    }
  } else if (current_timestamp_ms - publish_timestamp_ms >
             static_cast<uint64_t>(kFilesystemPressureMaxAgeMs)) {
    return std::nullopt;
  }

  return std::clamp(pressure, 0.0, 1.0);
}

std::optional<double>
ReadFilesystemIoPressure(FileSystem* fs, const int64_t now_ms)
{
  return ParseFreshFilesystemIoPressure(fs->GetString("stat.disk.load"),
                                        fs->GetString("stat.publishtimestamp"), now_ms);
}

IoPressureSnapshot
CollectIoPressure(std::vector<std::string>* online_nodes = nullptr,
                  const bool include_filesystems = false)
{
  IoPressureSnapshot pressure;
  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  std::vector<std::pair<DiskKey, double>> filesystem_samples;

  {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    if (include_filesystems) {
      filesystem_samples.reserve(FsView::gFsView.mIdView.size());
    }
    for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
      if (node_view->GetStatus() != "online") {
        continue;
      }

      if (online_nodes) {
        online_nodes->push_back(node_name);
      }

      bool have_pressure_sample = false;
      double max_disk_load = 0.0;

      for (auto fsid_it = node_view->begin(); fsid_it != node_view->end(); ++fsid_it) {
        auto* fs = FsView::gFsView.mIdView.lookupByID(*fsid_it);
        if (!fs || !BaseView::ConsiderForStatistics(fs)) {
          continue;
        }

        const auto disk_load = ReadFilesystemIoPressure(fs, now_ms);
        if (!disk_load.has_value()) {
          continue;
        }

        if (include_filesystems) {
          filesystem_samples.emplace_back(
              DiskKey{node_name, static_cast<uint64_t>(*fsid_it)}, *disk_load);
        }
        max_disk_load = std::max(max_disk_load, *disk_load);
        have_pressure_sample = true;
      }

      if (have_pressure_sample) {
        pressure.nodes[node_name] = max_disk_load;
      }
    }
  }

  if (include_filesystems) {
    pressure.filesystems.reserve(filesystem_samples.size());
    for (auto& [key, sample] : filesystem_samples) {
      pressure.filesystems.emplace(std::move(key), sample);
    }
  }

  return pressure;
}

void
UpdateAppIoPressure(AppIoPressureSnapshot& app_pressure, const RateMetrics& metrics,
                    const double node_pressure,
                    const uint64_t active_node_rate_threshold_bps)
{
  if (metrics.read_rate_bps >= active_node_rate_threshold_bps) {
    app_pressure.read = std::max(app_pressure.read, node_pressure);
    app_pressure.has_read = true;
  }

  if (metrics.write_rate_bps >= active_node_rate_threshold_bps) {
    app_pressure.write = std::max(app_pressure.write, node_pressure);
    app_pressure.has_write = true;
  }
}

void
AddStreamRates(EntityRateMaps& rates, const StreamKey& key, const RateMetrics& metrics)
{
  const double read_rate_bps = SanitizeRate(metrics.read_rate_bps);
  const double write_rate_bps = SanitizeRate(metrics.write_rate_bps);
  rates.app_read[key.app] += read_rate_bps;
  rates.app_write[key.app] += write_rate_bps;
  rates.uid_read[key.uid] += read_rate_bps;
  rates.uid_write[key.uid] += write_rate_bps;
  rates.gid_read[key.gid] += read_rate_bps;
  rates.gid_write[key.gid] += write_rate_bps;
}

template <typename Key>
void
MergeMaxRates(std::unordered_map<Key, double>& fast_rates,
              const std::unordered_map<Key, double>& stable_rates)
{
  fast_rates.reserve(std::max(fast_rates.size(), stable_rates.size()));
  for (const auto& [key, stable_rate] : stable_rates) {
    auto [it, inserted] = fast_rates.try_emplace(key, stable_rate);
    if (!inserted) {
      it->second = std::max(it->second, stable_rate);
    }
  }
}

void
MergeMaxRates(EntityRateMaps& fast_rates, const EntityRateMaps& stable_rates)
{
  // Select the stronger window only after aggregating every stream. A per-stream
  // maximum would combine unrelated fast/stable peaks from different fsids.
  MergeMaxRates(fast_rates.app_read, stable_rates.app_read);
  MergeMaxRates(fast_rates.app_write, stable_rates.app_write);
  MergeMaxRates(fast_rates.uid_read, stable_rates.uid_read);
  MergeMaxRates(fast_rates.uid_write, stable_rates.uid_write);
  MergeMaxRates(fast_rates.gid_read, stable_rates.gid_read);
  MergeMaxRates(fast_rates.gid_write, stable_rates.gid_write);
}

template <typename RateState>
void
AddDeltas(RateState& stats, const double delta_bytes_read,
          const double delta_bytes_written, const double delta_read_iops,
          const double delta_write_iops, const time_t now_unix)
{
  stats.bytes_read_accumulator += delta_bytes_read;
  stats.bytes_written_accumulator += delta_bytes_written;
  stats.read_iops_accumulator += delta_read_iops;
  stats.write_iops_accumulator += delta_write_iops;
  stats.last_activity_time = now_unix;
}
} // namespace

std::optional<double>
ParseFreshFilesystemIoPressureForTest(const std::string& load_value,
                                      const std::string& publish_timestamp_value,
                                      const int64_t now_ms)
{
  return ParseFreshFilesystemIoPressure(load_value, publish_timestamp_value, now_ms);
}

void
TrafficShapingManager::ProcessReport(const eos::traffic_shaping::FstIoReport& report)
try {
  if (report.node_id().size() > kMaxFstReportIdentityBytes ||
      static_cast<size_t>(report.entries_size()) > kMaxFstReportEntries ||
      std::any_of(report.entries().begin(), report.entries().end(),
                  [](const auto& entry) {
                    return entry.app_name().size() > kMaxFstReportIdentityBytes;
                  })) {
    if (ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
      eos_static_warning(
          "msg=\"Rejecting Traffic Shaping FST report outside safety bounds\" "
          "node_bytes=%zu entries=%d",
          report.node_id().size(), report.entries_size());
    }
    return;
  }
  const std::string node_id = NormalizeFstNodeId(report.node_id());
  const int64_t now_system_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
  const bool has_future_source_timestamp =
      report.timestamp_ms() > now_system_ms + kFstReportMaxFutureSkewMs;
  const int64_t report_timestamp_ms =
      has_future_source_timestamp ? 0 : report.timestamp_ms();
  if (has_future_source_timestamp &&
      ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
    eos_static_warning(
        "msg=\"Ignoring future-skewed Traffic Shaping FST report timestamp\" "
        "node=\"%s\" report_timestamp_ms=%lld now_ms=%lld max_future_skew_ms=%lld",
        node_id.c_str(), static_cast<long long>(report.timestamp_ms()),
        static_cast<long long>(now_system_ms),
        static_cast<long long>(kFstReportMaxFutureSkewMs));
  }

  const auto now_steady = std::chrono::steady_clock::now();
  const time_t now_unix = time(nullptr);

  std::unique_lock lock(mMutex);

  auto node_it = mNodeStates.find(node_id);
  bool inserted_node = false;
  if (node_it == mNodeStates.end()) {
    // Empty heartbeats do not carry controller input. Avoid allowing them to
    // consume the finite tracked-node budget ahead of real streams.
    if (report.entries().empty()) {
      return;
    }
    if (mNodeStates.size() >= kMaxFstTrackedNodes) {
      const size_t tracked_nodes = mNodeStates.size();
      mFstStreamStatesRejectedTotal.fetch_add(
          static_cast<uint64_t>(report.entries_size()), std::memory_order_relaxed);
      lock.unlock();
      if (ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
        eos_static_warning("msg=\"Rejecting new Traffic Shaping FST node at cardinality "
                           "limit\" node=\"%s\" tracked_nodes=%zu max_nodes=%zu",
                           node_id.c_str(), tracked_nodes, kMaxFstTrackedNodes);
      }
      return;
    }
    auto [candidate_it, inserted] = mNodeStates.try_emplace(node_id);
    node_it = candidate_it;
    inserted_node = inserted;
  }
  NodeData& node_data = node_it->second;
  NodeStateMap& node_map = node_data.streams;

  const int64_t report_node_start_time_ms = report.node_start_time_ms();
  if (report_node_start_time_ms != 0 &&
      report_node_start_time_ms != node_data.node_start_time_ms) {
    if (std::find(node_data.retired_node_start_times.begin(),
                  node_data.retired_node_start_times.end(), report_node_start_time_ms) !=
        node_data.retired_node_start_times.end()) {
      return;
    }

    if (node_data.node_start_time_ms != 0) {
      node_data.retired_node_start_times[node_data.next_retired_node_start_time] =
          node_data.node_start_time_ms;
      node_data.next_retired_node_start_time =
          (node_data.next_retired_node_start_time + 1) %
          node_data.retired_node_start_times.size();
    }
    for (const auto& [stream, _] : node_map) {
      const size_t estimated_stream_bytes =
          EstimateFstStreamStateBytes(node_id, stream.app);
      mFstStreamStateCount -= std::min<size_t>(mFstStreamStateCount, 1);
      mFstStreamStateEstimatedBytes -=
          std::min(mFstStreamStateEstimatedBytes, estimated_stream_bytes);
    }
    node_map.clear();
    node_data.last_report_time = {};
    node_data.last_source_report_timestamp_ms = 0;
    node_data.node_start_time_ms = report_node_start_time_ms;
  }

  // Recover automatically from an anchor admitted by an older process that did
  // not reject future-skewed source timestamps.
  if (node_data.last_source_report_timestamp_ms >
      now_system_ms + kFstReportMaxFutureSkewMs) {
    node_data.last_source_report_timestamp_ms = 0;
  }

  const bool is_first_node_contact =
      (node_data.last_report_time == std::chrono::steady_clock::time_point{});

  double node_elapsed_sec = 0.0;
  if (!is_first_node_contact) {
    node_elapsed_sec =
        std::chrono::duration<double>(now_steady - node_data.last_report_time).count();
  }
  node_data.last_report_time = now_steady;

  double report_elapsed_sec = node_elapsed_sec;
  bool is_historical_node_report = false;
  if (report_timestamp_ms > 0) {
    if (node_data.last_source_report_timestamp_ms > 0) {
      if (report_timestamp_ms > node_data.last_source_report_timestamp_ms) {
        report_elapsed_sec =
            (report_timestamp_ms - node_data.last_source_report_timestamp_ms) * 0.001;
      } else {
        // Keep recovering stream deltas from an out-of-order node envelope, but
        // never present historical traffic as a current-rate sample.
        is_historical_node_report = true;
        report_elapsed_sec = 0.0;
      }
    }
    node_data.last_source_report_timestamp_ms =
        std::max(node_data.last_source_report_timestamp_ms, report_timestamp_ms);
  }

  const double expected_report_period_sec = std::max(0.001, mFstReportTickIntervalSec);
  const double estimator_weight =
      is_historical_node_report
          ? 0.0
          : (std::isfinite(report_elapsed_sec) && report_elapsed_sec > 0.0 &&
                     report_elapsed_sec > expected_report_period_sec
                 ? expected_report_period_sec / report_elapsed_sec
                 : 1.0);

  uint64_t total_node_raw_delta_bytes_read = 0;
  uint64_t total_node_raw_delta_bytes_written = 0;
  uint64_t total_node_raw_delta_read_iops = 0;
  uint64_t total_node_raw_delta_write_iops = 0;
  double total_node_rate_delta_bytes_read = 0.0;
  double total_node_rate_delta_bytes_written = 0.0;
  double total_node_rate_delta_read_iops = 0.0;
  double total_node_rate_delta_write_iops = 0.0;
  size_t rejected_new_streams = 0;
  size_t stale_stream_entries = 0;
  size_t baseline_stream_entries = 0;

  for (const auto& entry : report.entries()) {
    const StreamKey stream_key{entry.app_name(), entry.uid(), entry.gid(), entry.fsid()};
    const StreamKey stats_key =
        mFilesystemDetailEnabled.load(std::memory_order_relaxed)
            ? stream_key
            : StreamKey{entry.app_name(), entry.uid(), entry.gid(), 0};

    auto stream_it = node_map.find(stream_key);
    if (stream_it == node_map.end()) {
      const size_t estimated_stream_bytes =
          EstimateFstStreamStateBytes(node_id, entry.app_name());
      if (node_map.size() >= kMaxFstStreamsPerNode ||
          mFstStreamStateCount >= kMaxFstStreamStates ||
          estimated_stream_bytes > kMaxFstStreamStateEstimatedBytes -
                                       std::min(mFstStreamStateEstimatedBytes,
                                                kMaxFstStreamStateEstimatedBytes)) {
        ++rejected_new_streams;
        continue;
      }
      auto [candidate_it, inserted] = node_map.try_emplace(stream_key);
      stream_it = candidate_it;
      if (inserted) {
        ++mFstStreamStateCount;
        mFstStreamStateEstimatedBytes += estimated_stream_bytes;
      }
    }
    StreamState& state = stream_it->second;

    const bool is_first_stream_contact =
        state.last_update_time == std::chrono::steady_clock::time_point{};
    const bool is_same_generation =
        !is_first_stream_contact && state.generation_id == entry.generation_id();
    // Generation IDs are opaque uniqueness tokens, not ordered counters. Use
    // the source timestamp to reject a delayed report from another generation.
    const bool is_stale_generation =
        !is_first_stream_contact && !is_same_generation && report_timestamp_ms > 0 &&
        state.last_report_timestamp_ms > 0 &&
        report_timestamp_ms <= state.last_report_timestamp_ms;
    const bool is_stale_snapshot = is_same_generation && report_timestamp_ms > 0 &&
                                   state.last_report_timestamp_ms > 0 &&
                                   report_timestamp_ms <= state.last_report_timestamp_ms;

    // FST reports contain cumulative snapshots and can arrive out of order. A
    // generation ID is the stream creation timestamp, while timestamp_ms orders
    // snapshots within that generation. Rejecting before touching the baseline
    // prevents the next fresh report from recounting bytes and IOPS.
    if (is_stale_generation || is_stale_snapshot) {
      ++stale_stream_entries;
      continue;
    }

    uint64_t delta_bytes_read = 0;
    uint64_t delta_bytes_written = 0;
    uint64_t delta_read_iops = 0;
    uint64_t delta_write_iops = 0;
    // Handle New Streams, MGM Restarts, and FST Restarts
    if (!is_same_generation) {
      state.generation_id = entry.generation_id();

      // If the stream was created more than 3 seconds ago, it is a ghost from the past
      bool is_old_stream = false;
      if (now_system_ms > 0 &&
          static_cast<uint64_t>(now_system_ms) > entry.generation_id() &&
          (static_cast<uint64_t>(now_system_ms) - entry.generation_id() > 3000)) {
        is_old_stream = true;
      }

      // Baseline streams that predate this MGM process, but count a fresh stream
      // immediately. In particular, an ignored empty heartbeat must not cause
      // the first subsequent traffic report to be swallowed.
      if (is_first_stream_contact && is_old_stream) {
        delta_bytes_read = 0;
        delta_bytes_written = 0;
        delta_read_iops = 0;
        delta_write_iops = 0;

        ++baseline_stream_entries;

      } else {
        delta_bytes_read = entry.total_bytes_read();
        delta_bytes_written = entry.total_bytes_written();
        delta_read_iops = entry.total_read_ops();
        delta_write_iops = entry.total_write_ops();
      }
    } else {
      if (entry.total_bytes_read() >= state.last_bytes_read) {
        delta_bytes_read = entry.total_bytes_read() - state.last_bytes_read;
      }
      if (entry.total_bytes_written() >= state.last_bytes_written) {
        delta_bytes_written = entry.total_bytes_written() - state.last_bytes_written;
      }
      if (entry.total_read_ops() >= state.last_iops_read) {
        delta_read_iops = entry.total_read_ops() - state.last_iops_read;
      }
      if (entry.total_write_ops() >= state.last_iops_write) {
        delta_write_iops = entry.total_write_ops() - state.last_iops_write;
      }
    }

    const double rate_delta_bytes_read = delta_bytes_read * estimator_weight;
    const double rate_delta_bytes_written = delta_bytes_written * estimator_weight;
    const double rate_delta_read_iops = delta_read_iops * estimator_weight;
    const double rate_delta_write_iops = delta_write_iops * estimator_weight;

    total_node_raw_delta_bytes_read += delta_bytes_read;
    total_node_raw_delta_bytes_written += delta_bytes_written;
    total_node_raw_delta_read_iops += delta_read_iops;
    total_node_raw_delta_write_iops += delta_write_iops;
    total_node_rate_delta_bytes_read += rate_delta_bytes_read;
    total_node_rate_delta_bytes_written += rate_delta_bytes_written;
    total_node_rate_delta_read_iops += rate_delta_read_iops;
    total_node_rate_delta_write_iops += rate_delta_write_iops;

    if (is_same_generation) {
      // Timestamp zero is accepted for legacy producers. Keep every cumulative
      // counter monotonic independently so an unordered zero-timestamp snapshot
      // still cannot regress a baseline.
      state.last_bytes_read = std::max(state.last_bytes_read, entry.total_bytes_read());
      state.last_bytes_written =
          std::max(state.last_bytes_written, entry.total_bytes_written());
      state.last_iops_read = std::max(state.last_iops_read, entry.total_read_ops());
      state.last_iops_write = std::max(state.last_iops_write, entry.total_write_ops());
    } else {
      state.last_bytes_read = entry.total_bytes_read();
      state.last_bytes_written = entry.total_bytes_written();
      state.last_iops_read = entry.total_read_ops();
      state.last_iops_write = entry.total_write_ops();
    }
    if (report_timestamp_ms > 0) {
      state.last_report_timestamp_ms = report_timestamp_ms;
    }
    state.last_update_time = now_steady;

    if (delta_bytes_read > 0 || delta_bytes_written > 0 || delta_read_iops > 0 ||
        delta_write_iops > 0) {

      auto [it, inserted] =
          mGlobalStats.try_emplace(stats_key, mEstimatorsTickIntervalSec);
      MultiWindowRate& global = it->second;

      AddDeltas(global, rate_delta_bytes_read, rate_delta_bytes_written,
                rate_delta_read_iops, rate_delta_write_iops, now_unix);
      AddCumulativeStats(mGlobalCumulativeStats[stats_key], delta_bytes_read,
                         delta_bytes_written, delta_read_iops, delta_write_iops,
                         now_unix);
      AddCumulativeStats(mProjectionCumulativeStats.app[stream_key.app], delta_bytes_read,
                         delta_bytes_written, delta_read_iops, delta_write_iops,
                         now_unix);
      AddCumulativeStats(mProjectionCumulativeStats.uid[stream_key.uid], delta_bytes_read,
                         delta_bytes_written, delta_read_iops, delta_write_iops,
                         now_unix);
      AddCumulativeStats(mProjectionCumulativeStats.gid[stream_key.gid], delta_bytes_read,
                         delta_bytes_written, delta_read_iops, delta_write_iops,
                         now_unix);

      DetailedKey node_entity_key{node_id,
                                  {stream_key.app, stream_key.uid, stream_key.gid, 0}};
      auto [node_entity_it, node_entity_inserted] =
          mNodeEntityStats.try_emplace(node_entity_key);
      EmaRate& node_entity = node_entity_it->second;

      AddDeltas(node_entity, rate_delta_bytes_read, rate_delta_bytes_written,
                rate_delta_read_iops, rate_delta_write_iops, now_unix);

      if (mFilesystemDetailEnabled.load(std::memory_order_relaxed) && entry.fsid() != 0) {
        DetailedKey detailed_key{node_id, stream_key};
        auto [detailed_it, detailed_inserted] =
            mDetailedStats.try_emplace(detailed_key, mEstimatorsTickIntervalSec);
        MultiWindowRate& detailed = detailed_it->second;

        AddDeltas(detailed, rate_delta_bytes_read, rate_delta_bytes_written,
                  rate_delta_read_iops, rate_delta_write_iops, now_unix);
        AddCumulativeStats(mDetailedCumulativeStats[detailed_key], delta_bytes_read,
                           delta_bytes_written, delta_read_iops, delta_write_iops,
                           now_unix);

        DiskKey disk_key{node_id, entry.fsid()};
        auto [disk_it, disk_inserted] =
            mDiskStats.try_emplace(disk_key, mEstimatorsTickIntervalSec);
        MultiWindowRate& disk = disk_it->second;

        AddDeltas(disk, rate_delta_bytes_read, rate_delta_bytes_written,
                  rate_delta_read_iops, rate_delta_write_iops, now_unix);
        AddCumulativeStats(mDiskCumulativeStats[disk_key], delta_bytes_read,
                           delta_bytes_written, delta_read_iops, delta_write_iops,
                           now_unix);
      }
    }
  }

  if (total_node_raw_delta_bytes_read > 0 || total_node_raw_delta_bytes_written > 0 ||
      total_node_raw_delta_read_iops > 0 || total_node_raw_delta_write_iops > 0) {

    auto [it, inserted] = mNodeStats.try_emplace(node_id, mEstimatorsTickIntervalSec);
    MultiWindowRate& node_stat = it->second;

    AddDeltas(node_stat, total_node_rate_delta_bytes_read,
              total_node_rate_delta_bytes_written, total_node_rate_delta_read_iops,
              total_node_rate_delta_write_iops, now_unix);
    AddCumulativeStats(mNodeCumulativeStats[node_id], total_node_raw_delta_bytes_read,
                       total_node_raw_delta_bytes_written, total_node_raw_delta_read_iops,
                       total_node_raw_delta_write_iops, now_unix);
    AddCumulativeStats(mProjectionCumulativeStats.node[node_id],
                       total_node_raw_delta_bytes_read,
                       total_node_raw_delta_bytes_written, total_node_raw_delta_read_iops,
                       total_node_raw_delta_write_iops, now_unix);

    AddDeltas(mTotalStats, total_node_rate_delta_bytes_read,
              total_node_rate_delta_bytes_written, total_node_rate_delta_read_iops,
              total_node_rate_delta_write_iops, now_unix);
    AddCumulativeStats(mCumulativeTotalStats, total_node_raw_delta_bytes_read,
                       total_node_raw_delta_bytes_written, total_node_raw_delta_read_iops,
                       total_node_raw_delta_write_iops, now_unix);
  }

  const size_t node_streams = node_map.size();
  const size_t total_streams = mFstStreamStateCount;
  const size_t total_stream_estimated_bytes = mFstStreamStateEstimatedBytes;
  mFstStreamStatesRejectedTotal.fetch_add(static_cast<uint64_t>(rejected_new_streams),
                                          std::memory_order_relaxed);
  if (inserted_node && node_map.empty()) {
    mNodeStates.erase(node_it);
  }
  lock.unlock();

  if (rejected_new_streams > 0 &&
      ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
    eos_static_warning("msg=\"Rejecting new Traffic Shaping FST streams at memory or "
                       "cardinality limit\" node=\"%s\" rejected=%zu node_streams=%zu "
                       "total_streams=%zu estimated_bytes=%zu max_node_streams=%zu "
                       "max_total_streams=%zu max_estimated_bytes=%zu",
                       node_id.c_str(), rejected_new_streams, node_streams, total_streams,
                       total_stream_estimated_bytes, kMaxFstStreamsPerNode,
                       kMaxFstStreamStates, kMaxFstStreamStateEstimatedBytes);
  } else if (report_elapsed_sec > 2.0 * expected_report_period_sec &&
             !report.entries().empty() &&
             ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
    eos_static_warning(
        "msg=\"Large delay in FST report, normalizing estimator contribution\" "
        "node=%s report_elapsed_sec=%.3f estimator_weight=%.6f",
        node_id.c_str(), report_elapsed_sec, estimator_weight);
  } else if ((stale_stream_entries > 0 || baseline_stream_entries > 0) &&
             ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
    eos_static_debug("msg=\"Traffic Shaping FST report entries ignored or baselined\" "
                     "node=\"%s\" entries=%d stale=%zu baselined=%zu first_node=%d",
                     node_id.c_str(), report.entries_size(), stale_stream_entries,
                     baseline_stream_entries, is_first_node_contact);
  }
} catch (const std::exception& error) {
  if (ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
    eos_static_err("msg=\"Traffic Shaping FST report processing aborted by exception\" "
                   "error=\"%s\"",
                   error.what());
  }
} catch (...) {
  if (ShouldEmitRateLimitedWarning(mLastFstReportStateWarningMonotonicNs)) {
    eos_static_err("%s", "msg=\"Traffic Shaping FST report processing aborted by "
                         "unknown exception\"");
  }
}

double
ComputeEmaAlpha(const double window_seconds, const double time_delta_seconds)
{
  if (time_delta_seconds <= 0.0 || window_seconds <= 0.0) {
    return 1.0;
  }

  // Preserve the existing EMA response while preventing delayed ticks from
  // producing alpha > 1, which would make the decay oscillate below zero.
  return std::min(1.0,
                  (2.0 * time_delta_seconds) / (window_seconds + time_delta_seconds));
}

void
TrafficShapingManager::UpdateEstimators(const double time_delta_seconds)
{
  if (!std::isfinite(time_delta_seconds) || time_delta_seconds <= 0.001 /* 1 ms */) {
    eos_static_err(
        "msg=\"Skipping estimator update due to problem time delta seconds: %f\"",
        time_delta_seconds);
    return;
  }

  std::unique_lock lock(mMutex);

  std::array<double, EmaWindowSec.size()> ema_alphas{};
  for (size_t i = 0; i < EmaWindowSec.size(); ++i) {
    ema_alphas[i] = ComputeEmaAlpha(EmaWindowSec[i], time_delta_seconds);
  }

  auto take_counters = [](auto& stats) {
    const RateCounters counters{
        stats.bytes_read_accumulator, stats.bytes_written_accumulator,
        stats.read_iops_accumulator, stats.write_iops_accumulator};
    stats.bytes_read_accumulator = 0;
    stats.bytes_written_accumulator = 0;
    stats.read_iops_accumulator = 0;
    stats.write_iops_accumulator = 0;
    return counters;
  };

  auto current_metrics = [time_delta_seconds](const RateCounters& counters) {
    return RateMetrics{
        static_cast<double>(counters.bytes_read) / time_delta_seconds,
        static_cast<double>(counters.bytes_written) / time_delta_seconds,
        static_cast<double>(counters.read_ops) / time_delta_seconds,
        static_cast<double>(counters.write_ops) / time_delta_seconds,
    };
  };

  auto update_ema = [&](RateMetrics& ema, const RateMetrics& current,
                        const double alpha) {
    ema.read_rate_bps = CalculateEma(current.read_rate_bps, ema.read_rate_bps, alpha);
    ema.write_rate_bps = CalculateEma(current.write_rate_bps, ema.write_rate_bps, alpha);
    ema.read_iops = CalculateEma(current.read_iops, ema.read_iops, alpha);
    ema.write_iops = CalculateEma(current.write_iops, ema.write_iops, alpha);
  };

  auto process_rate = [&](MultiWindowRate& stats) {
    const RateCounters counters = take_counters(stats);
    const RateMetrics current = current_metrics(counters);

    for (size_t i = 0; i < EmaWindowSec.size(); ++i) {
      update_ema(stats.ema[i], current, ema_alphas[i]);
    }

    stats.rate_history.Push(counters, time_delta_seconds);

    for (size_t i = 0; i < SmaWindowSec.size(); ++i) {
      stats.sma[i] = stats.rate_history.GetRate(SmaWindowSec[i]);
    }
  };

  auto process_ema_rate = [&](EmaRate& stats) {
    const RateCounters counters = take_counters(stats);
    update_ema(stats.ema, current_metrics(counters), ema_alphas[Ema1s]);
  };

  for (auto& [key, stats] : mGlobalStats) {
    process_rate(stats);
  }
  for (auto& [node_id, stats] : mNodeStats) {
    process_rate(stats);
  }
  for (auto& [disk_key, stats] : mDiskStats) {
    process_rate(stats);
  }
  for (auto& [detailed_key, stats] : mDetailedStats) {
    process_rate(stats);
  }
  for (auto& [node_entity_key, stats] : mNodeEntityStats) {
    process_ema_rate(stats);
  }
  process_rate(mTotalStats);
}

uint64_t
TrafficShapingManager::CalculateDelayUs(
    const double limit_bps, const double current_rate_bps,
    const uint64_t current_delay_us, const double io_pressure, const bool has_rate_sample,
    const bool allow_idle_release, const double delay_reference_bps,
    const double io_pressure_threshold)
{
  if (!std::isfinite(limit_bps) || limit_bps <= 0.0) {
    return 0;
  }

  constexpr uint64_t kDelayReferenceBytes = 1024 * 1024;
  constexpr uint64_t kIdleReleaseStepDownUs = 250000;
  constexpr uint64_t kSparseSampleStepDownUs = 80000;
  constexpr double kIdleRatio = 0.01;
  constexpr double kLowerDeadbandRatio = 0.94;
  constexpr double kUpperDeadbandRatio = 1.02;

  uint64_t delay_us = std::min(current_delay_us, kMaxDelayUs);
  const double safe_current_rate_bps = SanitizeRate(current_rate_bps);
  const double reference_bps =
      std::isfinite(delay_reference_bps) && delay_reference_bps > 0.0
          ? delay_reference_bps
          : limit_bps;
  const uint64_t seed_delay_us = std::min<uint64_t>(
      kMaxDelayUs,
      static_cast<uint64_t>((kDelayReferenceBytes * 1000000.0) / reference_bps));
  const double ratio = safe_current_rate_bps / limit_bps;
  const double safe_io_pressure =
      std::isfinite(io_pressure) ? std::clamp(io_pressure, 0.0, 1.0) : 0.0;
  const double safe_io_pressure_threshold =
      std::isfinite(io_pressure_threshold) ? std::clamp(io_pressure_threshold, 0.0, 1.0)
                                           : kDefaultIoPressureThreshold;

  // 0. Idle seed: when traffic is sparse, use the current limit's baseline
  // delay. This shapes the first transfer and lets raised limits release old
  // delay even before dense samples arrive. If EOS already measures no storage
  // pressure for this FST, this is a controller-only limit, and we have seen
  // this entity on the node, release the idle delay instead of keeping a sparse
  // low-demand app throttled indefinitely. Do not release explicit user limits
  // or a pre-traffic seed, otherwise a hard policy can decay away before or
  // during the first transfer. For explicit limits, shed existing delay slowly
  // while the entity still has a rate sample; a delayed stream can briefly report
  // near zero rate, and resetting straight to the seed creates a burst/collapse
  // cycle.
  if (ratio < kIdleRatio) {
    if (allow_idle_release && has_rate_sample &&
        safe_io_pressure < safe_io_pressure_threshold) {
      return delay_us > kIdleReleaseStepDownUs ? delay_us - kIdleReleaseStepDownUs : 0;
    }

    if (has_rate_sample) {
      const uint64_t released_delay_us =
          delay_us > kSparseSampleStepDownUs ? delay_us - kSparseSampleStepDownUs : 0;
      return std::max(released_delay_us, seed_delay_us);
    }

    return seed_delay_us;
  }

  if (delay_us == 0 && ratio > kUpperDeadbandRatio) {
    delay_us = std::max<uint64_t>(1000, seed_delay_us);
  }

  if (ratio > kUpperDeadbandRatio) {
    const double excess = std::min(4.0, ratio - 1.0);
    const uint64_t proportional_step = static_cast<uint64_t>(
        static_cast<double>(std::max(delay_us, seed_delay_us)) * (0.30 * excess));
    const uint64_t seed_step =
        static_cast<uint64_t>(static_cast<double>(seed_delay_us) * (3.5 * excess));
    const uint64_t step = std::clamp<uint64_t>(
        std::max<uint64_t>({proportional_step, seed_step, 1000}), 1000, 160000);
    delay_us = std::min<uint64_t>(kMaxDelayUs, delay_us + step);
  } else if (ratio < kLowerDeadbandRatio) {
    const double deficit = std::min(1.0, 1.0 - ratio);
    const uint64_t proportional_step =
        static_cast<uint64_t>(static_cast<double>(delay_us) * (0.025 + 0.05 * deficit));
    const uint64_t step = std::clamp<uint64_t>(
        std::max<uint64_t>(proportional_step, seed_delay_us / 5), 250, 25000);
    delay_us = delay_us > step ? delay_us - step : 0;
  } else if (delay_us < seed_delay_us) {
    delay_us = seed_delay_us;
  }

  if (delay_us < 10 && ratio < kLowerDeadbandRatio) {
    delay_us = 0;
  }

  return std::min(delay_us, kMaxDelayUs);
}

void
TrafficShapingManager::ApplyDefaultReservationController(
    std::vector<AppState>& apps, const bool reservations_enabled,
    const uint64_t controller_min_limit_bps, const double io_pressure_threshold,
    const uint64_t active_node_rate_threshold_bps,
    ReservationControllerState* controller_state,
    std::chrono::steady_clock::time_point now,
    const std::unordered_map<std::string, double>* competition_write_rates,
    const std::unordered_map<std::string, double>* competition_read_rates,
    const bool deficits_prequalified)
{
  const double pressure_threshold = std::isfinite(io_pressure_threshold)
                                        ? std::clamp(io_pressure_threshold, 0.0, 1.0)
                                        : kDefaultIoPressureThreshold;
  if (controller_state && now == std::chrono::steady_clock::time_point{}) {
    now = std::chrono::steady_clock::now();
  }

  // Keep views into the application state so the controller hot path does not
  // copy and hash-allocate every application twice on each node update.
  std::vector<std::string_view> resolved_app_names;
  resolved_app_names.reserve(apps.size());
  std::vector<std::string> synthetic_app_names;
  std::unordered_map<std::string_view, size_t> app_indices;
  app_indices.reserve(apps.size());
  for (size_t i = 0; i < apps.size(); ++i) {
    std::string_view name = apps[i].app_name;

    if (name.empty() || app_indices.find(name) != app_indices.end()) {
      if (synthetic_app_names.empty()) {
        synthetic_app_names.reserve(apps.size());
      }
      synthetic_app_names.push_back("\x1f"
                                    "controller-index:" +
                                    std::to_string(i));
      while (app_indices.find(synthetic_app_names.back()) != app_indices.end()) {
        synthetic_app_names.back().push_back('\x1f');
      }
      name = synthetic_app_names.back();
    }
    resolved_app_names.push_back(name);
    app_indices.emplace(name, i);
  }

  auto update_direction = [&](std::vector<AppState>& apps, const bool is_write) {
    auto reservation = [is_write](const AppState& app) {
      return is_write ? app.reservation_write_bps : app.reservation_read_bps;
    };
    auto controller_limit = [is_write](const AppState& app) {
      return is_write ? app.controller_limit_write_bps : app.controller_limit_read_bps;
    };
    auto current_rate = [is_write](const AppState& app) {
      return SanitizeRate(is_write ? app.current_write_bps : app.current_read_bps);
    };
    auto competition_rate = [is_write, competition_write_rates,
                             competition_read_rates](const AppState& app) {
      const auto* rates = is_write ? competition_write_rates : competition_read_rates;
      if (rates == nullptr) {
        return SanitizeRate(is_write ? app.current_write_bps : app.current_read_bps);
      }

      const auto it = rates->find(app.app_name);
      return it == rates->end() ? 0.0 : SanitizeRate(it->second);
    };
    auto has_pressure = [is_write](const AppState& app) {
      return is_write ? app.has_write_io_pressure : app.has_read_io_pressure;
    };
    auto io_pressure = [is_write](const AppState& app) {
      const double pressure =
          is_write ? app.current_write_io_pressure : app.current_read_io_pressure;
      return std::isfinite(pressure) ? std::clamp(pressure, 0.0, 1.0) : 0.0;
    };
    auto has_reservation_competition = [is_write](const AppState& app) {
      return is_write ? app.has_write_reservation_competition
                      : app.has_read_reservation_competition;
    };
    auto set_controller_limit = [is_write](AppState& app, const uint64_t limit) {
      if (is_write) {
        app.new_controller_limit_write_bps = limit;
        app.update_write = true;
      } else {
        app.new_controller_limit_read_bps = limit;
        app.update_read = true;
      }
    };

    struct Competitor {
      AppState* app = nullptr;
      double baseline_bps = 0.0;
      double floor_bps = 0.0;
      double headroom_bps = 0.0;
    };

    struct ProtectedDeficit {
      std::string name;
      double target_bps = 0.0;
      double baseline_rate_bps = 0.0;
      double deficit_bps = 0.0;
    };

    auto* direction_state =
        controller_state ? (is_write ? &controller_state->write : &controller_state->read)
                         : nullptr;

    double total_reservation_deficit_bps = 0.0;
    std::vector<ProtectedDeficit> protected_deficits;
    protected_deficits.reserve(apps.size());
    bool has_pressure_active_reservation = false;

    for (size_t i = 0; i < apps.size(); ++i) {
      const auto& app = apps[i];
      const uint64_t reservation_bps = reservation(app);
      if (!reservations_enabled || reservation_bps == 0 || !has_pressure(app) ||
          io_pressure(app) < pressure_threshold) {
        continue;
      }

      has_pressure_active_reservation = true;
      const double rate_bps = current_rate(app);

      const double deficit_bps =
          std::max(0.0, static_cast<double>(reservation_bps) - rate_bps);
      const double meaningful_deficit_bps =
          deficits_prequalified
              ? 0.0
              : std::max(kMinReservationDeficitBps, static_cast<double>(reservation_bps) *
                                                        kMinReservationDeficitFraction);
      if (deficit_bps > 0.0 && deficit_bps >= meaningful_deficit_bps) {
        total_reservation_deficit_bps += deficit_bps;
        protected_deficits.push_back(
            {std::string(resolved_app_names[i]), static_cast<double>(reservation_bps),
             std::min(rate_bps, static_cast<double>(reservation_bps)), deficit_bps});
      }
    }

    auto hold_limits = [&]() {
      for (auto& app : apps) {
        if (controller_limit(app) > 0) {
          set_controller_limit(app, controller_limit(app));
        }
      }
    };

    auto release_limits = [&](const bool immediate) {
      bool has_remaining_limit = false;
      for (auto& app : apps) {
        const uint64_t old_limit = controller_limit(app);
        if (old_limit == 0) {
          continue;
        }

        uint64_t desired_limit = 0;
        if (!immediate) {
          constexpr double kReleaseFraction = 0.20;
          const uint64_t release_step =
              std::max<uint64_t>(kMinReservationDeficitBps,
                                 static_cast<uint64_t>(old_limit * kReleaseFraction));
          const uint64_t probed_limit =
              old_limit > std::numeric_limits<uint64_t>::max() - release_step
                  ? std::numeric_limits<uint64_t>::max()
                  : old_limit + release_step;
          if (competition_rate(app) >= old_limit * 0.80) {
            desired_limit = probed_limit;
            has_remaining_limit = true;
          }
        }

        set_controller_limit(app, desired_limit);
      }
      return has_remaining_limit;
    };

    auto protected_rate = [&](const std::string& name, const double target_bps) {
      const auto it = app_indices.find(std::string_view(name));
      if (it == app_indices.end()) {
        return 0.0;
      }
      return std::min(current_rate(apps[it->second]), target_bps);
    };

    auto minimum_protected_response = [](const double assigned_reduction_bps) {
      if (assigned_reduction_bps <= 0.0) {
        return std::numeric_limits<double>::infinity();
      }
      return std::min(
          assigned_reduction_bps,
          std::max(1.0, assigned_reduction_bps * kMinControllerResponseFraction));
    };

    auto clear_active_action = [&]() {
      if (!direction_state) {
        return;
      }
      direction_state->protected_apps.clear();
      direction_state->applied_reduction_bps = 0.0;
      direction_state->last_adjustment_time = {};
    };

    auto evaluate_protected_response = [&](auto& rates_at_response) {
      if (!direction_state || direction_state->protected_apps.empty()) {
        if (direction_state) {
          direction_state->last_observed_protected_gain_bps = 0.0;
          direction_state->last_response_ratio = 0.0;
        }
        return false;
      }

      bool every_app_responded = true;
      double total_gain_bps = 0.0;
      double conservative_ratio = std::numeric_limits<double>::infinity();
      for (const auto& [name, action] : direction_state->protected_apps) {
        const double rate_bps = protected_rate(name, action.target_bps);
        rates_at_response.emplace(name, rate_bps);
        const double gain_bps = std::max(0.0, rate_bps - action.baseline_rate_bps);
        total_gain_bps += gain_bps;

        if (action.assigned_reduction_bps <= 0.0) {
          every_app_responded = false;
          conservative_ratio = 0.0;
          continue;
        }

        const double response_ratio = gain_bps / action.assigned_reduction_bps;
        conservative_ratio = std::min(conservative_ratio, response_ratio);
        const double minimum_response_bps =
            minimum_protected_response(action.assigned_reduction_bps);
        every_app_responded &= gain_bps >= minimum_response_bps;
      }

      direction_state->last_observed_protected_gain_bps = total_gain_bps;
      direction_state->last_response_ratio =
          std::isfinite(conservative_ratio) ? std::max(0.0, conservative_ratio) : 0.0;
      return every_app_responded;
    };

    if (total_reservation_deficit_bps <= 0.0) {
      if (!direction_state) {
        release_limits(true);
        return;
      }

      direction_state->consecutive_deficit_samples = 0;
      const bool has_existing_limit =
          std::any_of(apps.begin(), apps.end(),
                      [&](const auto& app) { return controller_limit(app) > 0; });
      if (!has_existing_limit) {
        ResetInactiveReservationControllerDirection(*direction_state, now);
        return;
      }

      if (!reservations_enabled || !has_pressure_active_reservation) {
        release_limits(true);
        ResetInactiveReservationControllerDirection(*direction_state, now);
        return;
      }

      if (direction_state->healthy_since == std::chrono::steady_clock::time_point{}) {
        direction_state->healthy_since = now;
      }
      if (now - direction_state->healthy_since < kControllerResponseInterval) {
        hold_limits();
        return;
      }

      // Sustained health is positive evidence that the latest intervention
      // worked. Forget earlier failed probes before gradually releasing the
      // limit so that a later deficit starts with the normal controller gain.
      direction_state->ineffective_probe_count = 0;
      direction_state->failed_protected_apps.clear();
      direction_state->suppressed_until = {};
      const bool probe_remains = release_limits(false);
      clear_active_action();
      direction_state->last_observed_protected_gain_bps = 0.0;
      direction_state->last_response_ratio = 0.0;
      if (probe_remains) {
        direction_state->healthy_since = now;
      } else {
        *direction_state = {};
      }
      return;
    }

    if (direction_state &&
        direction_state->suppressed_until != std::chrono::steady_clock::time_point{}) {
      if (now < direction_state->suppressed_until) {
        bool failed_cohort_has_retry_evidence =
            !direction_state->failed_protected_apps.empty();
        for (const auto& [name, failed] : direction_state->failed_protected_apps) {
          const double minimum_response_bps =
              minimum_protected_response(failed.assigned_reduction_bps);
          const double retry_evidence_bps = std::isfinite(minimum_response_bps)
                                                ? minimum_response_bps
                                                : kMinReservationDeficitBps;
          if (protected_rate(name, failed.target_bps) <
              failed.rate_at_failure_bps + retry_evidence_bps) {
            failed_cohort_has_retry_evidence = false;
            break;
          }
        }
        if (!failed_cohort_has_retry_evidence) {
          direction_state->consecutive_deficit_samples = 0;
          direction_state->healthy_since = {};
          release_limits(true);
          return;
        }
        direction_state->ineffective_probe_count = 0;
        direction_state->failed_protected_apps.clear();
        direction_state->last_observed_protected_gain_bps = 0.0;
        direction_state->last_response_ratio = 0.0;
      }
      direction_state->suppressed_until = {};
      direction_state->failed_protected_apps.clear();
    }

    if (direction_state) {
      direction_state->healthy_since = {};
      ++direction_state->consecutive_deficit_samples;
      if (direction_state->consecutive_deficit_samples < kControllerEngageSamples) {
        return;
      }
    }

    std::vector<Competitor> competitors;
    competitors.reserve(apps.size());
    double total_headroom_bps = 0.0;
    bool has_existing_limits = false;

    for (auto& app : apps) {
      const uint64_t old_limit = controller_limit(app);

      if (!has_reservation_competition(app) ||
          (competition_rate(app) < active_node_rate_threshold_bps && old_limit == 0)) {
        continue;
      }

      has_existing_limits = has_existing_limits || old_limit > 0;

      const double baseline_bps =
          old_limit > 0 ? static_cast<double>(old_limit) : competition_rate(app);
      const uint64_t floor_limit_bps =
          std::max(reservation(app), controller_min_limit_bps);
      const double floor_bps = static_cast<double>(floor_limit_bps);
      const double headroom_bps = std::max(0.0, baseline_bps - floor_bps);
      if (headroom_bps <= 0.0) {
        if (old_limit > 0) {
          set_controller_limit(app, std::max(old_limit, floor_limit_bps));
        }
        continue;
      }

      competitors.push_back({&app, baseline_bps, floor_bps, headroom_bps});
      total_headroom_bps += headroom_bps;
    }

    if (competitors.empty() || total_headroom_bps <= 0.0) {
      for (auto& app : apps) {
        if (controller_limit(app) > 0) {
          const bool was_updated = is_write ? app.update_write : app.update_read;
          if (!was_updated) {
            set_controller_limit(
                app, has_reservation_competition(app) ? controller_limit(app) : 0);
          }
        }
      }
      clear_active_action();
      if (direction_state) {
        direction_state->failed_protected_apps.clear();
        direction_state->last_observed_protected_gain_bps = 0.0;
        direction_state->last_response_ratio = 0.0;
      }
      return;
    }

    // Stateless callers use this helper for deterministic one-step decisions.
    // Treat an existing limit as an already-applied decision rather than applying
    // the same deficit repeatedly to an already-throttled observed rate.
    if (!direction_state && has_existing_limits) {
      for (auto& app : apps) {
        if (controller_limit(app) > 0) {
          const bool was_updated = is_write ? app.update_write : app.update_read;
          if (!was_updated) {
            set_controller_limit(
                app, has_reservation_competition(app) ? controller_limit(app) : 0);
          }
        }
      }
      return;
    }

    std::optional<double> successful_response_ratio;
    if (direction_state && has_existing_limits &&
        direction_state->last_adjustment_time !=
            std::chrono::steady_clock::time_point{}) {
      if (now - direction_state->last_adjustment_time < kControllerResponseInterval) {
        for (auto& app : apps) {
          if (controller_limit(app) > 0) {
            const bool was_updated = is_write ? app.update_write : app.update_read;
            if (!was_updated) {
              set_controller_limit(
                  app, has_reservation_competition(app) ? controller_limit(app) : 0);
            }
          }
        }
        return;
      }

      std::unordered_map<std::string, double> rates_at_response;
      const bool every_app_responded = evaluate_protected_response(rates_at_response);
      if (direction_state->applied_reduction_bps > 0.0 && !every_app_responded) {
        if (direction_state->ineffective_probe_count <
            std::numeric_limits<uint32_t>::max()) {
          ++direction_state->ineffective_probe_count;
        }
        const uint32_t backoff_exponent = std::min(
            direction_state->ineffective_probe_count - 1, kMaxControllerBackoffExponent);
        const auto suppression_interval =
            std::min(kControllerSuppressionInterval * (int64_t{1} << backoff_exponent),
                     kControllerMaxSuppressionInterval);
        direction_state->suppressed_until = now + suppression_interval;
        direction_state->failed_protected_apps.clear();
        for (const auto& [name, action] : direction_state->protected_apps) {
          const auto rate_it = rates_at_response.find(name);
          const double rate_bps =
              rate_it == rates_at_response.end() ? 0.0 : rate_it->second;
          const double gain_bps = std::max(0.0, rate_bps - action.baseline_rate_bps);
          const double minimum_response_bps =
              minimum_protected_response(action.assigned_reduction_bps);
          if (gain_bps < minimum_response_bps) {
            direction_state->failed_protected_apps.emplace(
                name, ReservationControllerState::FailedProtectedApp{
                          action.target_bps, action.baseline_rate_bps,
                          action.assigned_reduction_bps, rate_bps});
          }
        }
        direction_state->consecutive_deficit_samples = 0;
        clear_active_action();
        release_limits(true);
        return;
      }
      if (direction_state->applied_reduction_bps > 0.0) {
        successful_response_ratio = direction_state->last_response_ratio;
        direction_state->ineffective_probe_count = 0;
        direction_state->failed_protected_apps.clear();
        direction_state->suppressed_until = {};
      }
    }

    double retune_gain = has_existing_limits ? kControllerRetuneGain : 1.0;
    if (successful_response_ratio.has_value()) {
      constexpr double kMarginalResponseRetuneGain = 0.25;
      constexpr double kFullResponseRatio = 0.50;
      const double bounded_response = std::clamp(
          *successful_response_ratio, kMinControllerResponseFraction, kFullResponseRatio);
      const double response_progress =
          (bounded_response - kMinControllerResponseFraction) /
          (kFullResponseRatio - kMinControllerResponseFraction);
      retune_gain = kMarginalResponseRetuneGain +
                    response_progress * (1.0 - kMarginalResponseRetuneGain);
    }
    const uint32_t retry_exponent =
        direction_state ? std::min(direction_state->ineffective_probe_count,
                                   kMaxControllerBackoffExponent)
                        : 0;
    const double retry_gain = 1.0 / static_cast<double>(1u << retry_exponent);
    const double requested_reduction_bps = std::min(
        {total_reservation_deficit_bps * retune_gain * retry_gain, total_headroom_bps,
         total_headroom_bps * kMaxControllerTightenFraction});
    double applied_reduction_bps = 0.0;

    for (auto& competitor : competitors) {
      const double share = competitor.headroom_bps / total_headroom_bps;
      const double reduction_bps = requested_reduction_bps * share;
      const uint64_t desired_limit = ClampRateToUint64(
          std::max(competitor.floor_bps, competitor.baseline_bps - reduction_bps));
      applied_reduction_bps +=
          competitor.baseline_bps - static_cast<double>(desired_limit);
      set_controller_limit(*competitor.app, desired_limit);
    }

    for (auto& app : apps) {
      if (controller_limit(app) == 0) {
        continue;
      }
      const bool was_updated = is_write ? app.update_write : app.update_read;
      if (!was_updated) {
        set_controller_limit(app, 0);
      }
    }

    if (direction_state) {
      direction_state->applied_reduction_bps = applied_reduction_bps;
      direction_state->protected_apps.clear();
      direction_state->failed_protected_apps.clear();
      if (applied_reduction_bps > 0.0 && total_reservation_deficit_bps > 0.0) {
        for (const auto& deficit : protected_deficits) {
          const double assigned_reduction_bps =
              applied_reduction_bps * deficit.deficit_bps / total_reservation_deficit_bps;
          direction_state->protected_apps.emplace(
              deficit.name,
              ReservationControllerState::ProtectedAppAction{
                  deficit.target_bps, deficit.baseline_rate_bps, assigned_reduction_bps});
        }
        direction_state->last_adjustment_time = now;
      } else {
        direction_state->last_adjustment_time = {};
      }
    }
  };

  update_direction(apps, true);
  update_direction(apps, false);
}

bool
TrafficShapingManager::ShouldEmitDelayForPolicy(
    const TrafficShapingPolicy& policy, const bool is_write, const double node_rate_bps,
    const double io_pressure, const bool node_has_pressured_reservation,
    const bool limits_enabled, const bool reservations_enabled,
    const double io_pressure_threshold, const uint64_t active_node_rate_threshold_bps)
{
  if (!limits_enabled || !policy.is_enabled) {
    return false;
  }

  const bool has_explicit_limit = is_write ? policy.limit_write_bytes_per_sec > 0
                                           : policy.limit_read_bytes_per_sec > 0;
  if (has_explicit_limit) {
    return true;
  }

  if (!reservations_enabled) {
    return false;
  }

  const bool has_controller_limit = is_write
                                        ? policy.controller_limit_write_bytes_per_sec > 0
                                        : policy.controller_limit_read_bytes_per_sec > 0;
  if (!has_controller_limit) {
    return false;
  }

  if (!node_has_pressured_reservation) {
    return false;
  }

  const bool has_reservation = is_write ? policy.reservation_write_bytes_per_sec > 0
                                        : policy.reservation_read_bytes_per_sec > 0;
  if (!has_reservation) {
    return true;
  }

  return node_rate_bps >= active_node_rate_threshold_bps &&
         io_pressure >= std::max(0.0, std::min(1.0, io_pressure_threshold));
}

void
TrafficShapingManager::UpdateTrafficShapingController(
    const std::unordered_map<std::string, double>& node_io_pressure) noexcept
{
  try {
    UpdateTrafficShapingControllerImpl(node_io_pressure);
  } catch (const std::exception& error) {
    try {
      eos_static_err("msg=\"Traffic shaping controller update aborted by host exception; "
                     "preserving prior state\" error=\"%s\"",
                     error.what());
    } catch (...) {
    }
  } catch (...) {
    try {
      eos_static_err("%s",
                     "msg=\"Traffic shaping controller update aborted by unknown host "
                     "exception; preserving prior state\"");
    } catch (...) {
    }
  }
}

void
TrafficShapingManager::UpdateTrafficShapingControllerImpl(
    const std::unordered_map<std::string, double>& node_io_pressure)
{
  const auto now = std::chrono::steady_clock::now();
  ExpireControllerLimits(now);

  struct AppNodeRates {
    double read_bps = 0.0;
    double write_bps = 0.0;
  };
  struct LocalReservationTarget {
    uint64_t read_bps = 0;
    uint64_t write_bps = 0;
  };
  struct ReservationCeiling {
    uint64_t read_bps = 0;
    uint64_t write_bps = 0;
  };
  struct NodeDeficitState {
    bool read = false;
    bool write = false;
  };

  std::unordered_map<std::string, AppState> app_states;
  EntityRateMaps global_controller_rates;
  EntityRateMaps global_controller_stable_rates;
  std::unordered_map<std::string, std::unordered_map<std::string, AppNodeRates>>
      node_rates;
  std::unordered_map<std::string, NodeReservationControllerRuntime> node_runtimes;
  std::unordered_map<std::string, ReservationCeiling> reservation_ceilings;
  std::unordered_set<std::string> disabled_apps;
  bool limits_enabled = false;
  bool reservations_enabled = false;
  uint64_t controller_min_limit_bps = 0;
  double io_pressure_threshold = 0.0;
  uint64_t active_node_rate_threshold_bps = 0;
  uint64_t policy_revision = 0;
  {
    std::shared_lock lock(mMutex);
    limits_enabled = mLimitsEnabled.load(std::memory_order_relaxed);
    reservations_enabled = mReservationsEnabled.load(std::memory_order_relaxed);
    controller_min_limit_bps = mControllerMinLimitBps.load(std::memory_order_relaxed);
    io_pressure_threshold = mIoPressureThreshold.load(std::memory_order_relaxed);
    active_node_rate_threshold_bps =
        mActiveNodeRateThresholdBps.load(std::memory_order_relaxed);
    app_states.reserve(mAppPolicies.size());

    for (const auto& [app, policy] : mAppPolicies) {
      if (!policy.is_enabled) {
        disabled_apps.insert(app);
        continue;
      }
      auto& state = app_states[app];
      state.reservation_write_bps = policy.reservation_write_bytes_per_sec;
      state.reservation_read_bps = policy.reservation_read_bytes_per_sec;
      auto& ceiling = reservation_ceilings[app];
      ceiling.write_bps = EffectiveReservation(policy, true);
      ceiling.read_bps = EffectiveReservation(policy, false);
    }

    for (const auto& [key, stats] : mGlobalStats) {
      if (disabled_apps.find(key.app) != disabled_apps.end()) {
        continue;
      }
      AddStreamRates(global_controller_rates, key, stats.ema[Ema1s]);
      AddStreamRates(global_controller_stable_rates, key, stats.ema[Ema5s]);
    }

    node_rates.reserve(mNodeStates.size());
    for (const auto& [key, stats] : mNodeEntityStats) {
      if (disabled_apps.find(key.stream.app) != disabled_apps.end()) {
        continue;
      }
      auto& rates = node_rates[key.node_id][key.stream.app];
      rates.read_bps += stats.ema.read_rate_bps;
      rates.write_bps += stats.ema.write_rate_bps;
    }

    policy_revision = mControllerInputRevision;
    node_runtimes = mNodeReservationControllers;
  }
  MergeMaxRates(global_controller_rates, global_controller_stable_rates);

  if (limits_enabled && reservations_enabled) {
    // Index active node rates by app once. Reservation allocation then touches
    // only nodes that actually carry the app instead of rescanning every node
    // for every app and direction.
    std::unordered_map<std::string, std::vector<std::pair<std::string, AppNodeRates>>>
        app_node_rates;
    for (const auto& [node_id, rates_by_app] : node_rates) {
      for (const auto& [app, rates] : rates_by_app) {
        app_node_rates[app].emplace_back(node_id, rates);
      }
    }

    std::unordered_map<std::string,
                       std::unordered_map<std::string, LocalReservationTarget>>
        local_targets;
    std::unordered_map<std::string, NodeDeficitState> node_deficits;

    auto clamp_rate_to_uint64 = [](const double value) {
      if (value <= 0.0 || !std::isfinite(value)) {
        return uint64_t{0};
      }
      constexpr auto max_value = std::numeric_limits<uint64_t>::max();
      if (static_cast<long double>(value) >= static_cast<long double>(max_value)) {
        return max_value;
      }
      return static_cast<uint64_t>(static_cast<long double>(value) + 0.5L);
    };

    auto allocate_direction = [&](const std::string& app, const uint64_t reservation_bps,
                                  const bool is_write) {
      if (reservation_bps == 0) {
        return;
      }

      struct EligibleNode {
        std::string node_id;
        double rate_bps = 0.0;
        uint64_t allocation_bps = 0;
        double remainder = 0.0;
      };

      const auto& global_rates =
          is_write ? global_controller_rates.app_write : global_controller_rates.app_read;
      const auto global_rate_it = global_rates.find(app);
      const double global_rate_bps = global_rate_it == global_rates.end()
                                         ? 0.0
                                         : SanitizeRate(global_rate_it->second);
      double active_node_rate_bps = 0.0;
      std::vector<std::pair<std::string, double>> active_nodes;
      std::vector<EligibleNode> eligible_nodes;
      const auto app_nodes_it = app_node_rates.find(app);
      if (app_nodes_it == app_node_rates.end()) {
        return;
      }
      for (const auto& [node_id, rates] : app_nodes_it->second) {
        const double rate_bps = is_write ? rates.write_bps : rates.read_bps;
        if (rate_bps <= 0.0 || !std::isfinite(rate_bps)) {
          continue;
        }
        active_node_rate_bps += rate_bps;
        active_nodes.emplace_back(node_id, rate_bps);

        const auto pressure_it = node_io_pressure.find(node_id);
        if (rate_bps >= active_node_rate_threshold_bps &&
            pressure_it != node_io_pressure.end() &&
            pressure_it->second >= io_pressure_threshold) {
          eligible_nodes.push_back({node_id, rate_bps});
        }
      }

      if (!std::isfinite(active_node_rate_bps)) {
        return;
      }

      // When the reservation is globally healthy, protect each node's
      // proportional share. During a deficit, protect observed local throughput
      // and add the globally conserved deficit allocation below.
      for (const auto& [node_id, rate_bps] : active_nodes) {
        const double protected_rate_bps =
            global_rate_bps >= static_cast<double>(reservation_bps) &&
                    active_node_rate_bps > 0.0
                ? static_cast<double>(reservation_bps) * rate_bps / active_node_rate_bps
                : rate_bps;
        auto& target = local_targets[node_id][app];
        if (is_write) {
          target.write_bps = clamp_rate_to_uint64(protected_rate_bps);
        } else {
          target.read_bps = clamp_rate_to_uint64(protected_rate_bps);
        }
      }

      const double deficit_bps =
          std::max(0.0, static_cast<double>(reservation_bps) - global_rate_bps);
      const double meaningful_deficit_bps =
          std::max(kMinReservationDeficitBps,
                   static_cast<double>(reservation_bps) * kMinReservationDeficitFraction);
      if (deficit_bps < meaningful_deficit_bps || eligible_nodes.empty()) {
        return;
      }

      std::sort(
          eligible_nodes.begin(), eligible_nodes.end(),
          [](const auto& lhs, const auto& rhs) { return lhs.node_id < rhs.node_id; });
      const double eligible_rate_bps = std::accumulate(
          eligible_nodes.begin(), eligible_nodes.end(), 0.0,
          [](const double sum, const auto& node) { return sum + node.rate_bps; });
      if (eligible_rate_bps <= 0.0 || !std::isfinite(eligible_rate_bps)) {
        return;
      }
      const uint64_t deficit_to_allocate = clamp_rate_to_uint64(deficit_bps);
      uint64_t assigned_bps = 0;
      for (auto& node : eligible_nodes) {
        const long double exact_allocation =
            static_cast<long double>(deficit_to_allocate) * node.rate_bps /
            eligible_rate_bps;
        const uint64_t available_bps = deficit_to_allocate - assigned_bps;
        node.allocation_bps =
            std::min(available_bps,
                     exact_allocation >= static_cast<long double>(deficit_to_allocate)
                         ? deficit_to_allocate
                         : static_cast<uint64_t>(std::max(0.0L, exact_allocation)));
        node.remainder = static_cast<double>(exact_allocation - node.allocation_bps);
        assigned_bps += node.allocation_bps;
      }

      std::stable_sort(
          eligible_nodes.begin(), eligible_nodes.end(),
          [](const auto& lhs, const auto& rhs) { return lhs.remainder > rhs.remainder; });
      uint64_t remaining_bps = deficit_to_allocate - assigned_bps;
      const uint64_t common_remainder = remaining_bps / eligible_nodes.size();
      if (common_remainder > 0) {
        for (auto& node : eligible_nodes) {
          node.allocation_bps += common_remainder;
        }
        remaining_bps %= eligible_nodes.size();
      }
      for (size_t i = 0; i < remaining_bps; ++i) {
        ++eligible_nodes[i].allocation_bps;
      }

      for (const auto& node : eligible_nodes) {
        auto& target = local_targets[node.node_id][app];
        uint64_t& target_bps = is_write ? target.write_bps : target.read_bps;
        target_bps =
            target_bps > std::numeric_limits<uint64_t>::max() - node.allocation_bps
                ? std::numeric_limits<uint64_t>::max()
                : target_bps + node.allocation_bps;
        if (node.allocation_bps > 0) {
          auto& deficit = node_deficits[node.node_id];
          (is_write ? deficit.write : deficit.read) = true;
        }
      }
    };

    for (const auto& [app, state] : app_states) {
      uint64_t write_reservation_bps = state.reservation_write_bps;
      uint64_t read_reservation_bps = state.reservation_read_bps;
      if (const auto it = reservation_ceilings.find(app);
          it != reservation_ceilings.end()) {
        if (it->second.write_bps > 0) {
          write_reservation_bps = std::min(write_reservation_bps, it->second.write_bps);
        }
        if (it->second.read_bps > 0) {
          read_reservation_bps = std::min(read_reservation_bps, it->second.read_bps);
        }
      }
      allocate_direction(app, write_reservation_bps, true);
      allocate_direction(app, read_reservation_bps, false);
    }

    std::set<std::string> nodes_to_process;
    for (const auto& [node_id, _] : node_deficits) {
      nodes_to_process.insert(node_id);
    }
    for (const auto& [node_id, _] : node_runtimes) {
      nodes_to_process.insert(node_id);
    }

    for (const auto& node_id : nodes_to_process) {
      auto& runtime = node_runtimes[node_id];
      const auto rates_it = node_rates.find(node_id);
      const auto targets_it = local_targets.find(node_id);
      const NodeDeficitState deficit = node_deficits[node_id];
      if (!deficit.read) {
        for (auto& [_, limit] : runtime.app_limits) {
          limit.read_bps = 0;
          limit.read_update_time = {};
        }
        ResetInactiveReservationControllerDirection(runtime.feedback.read, now);
      }
      if (!deficit.write) {
        for (auto& [_, limit] : runtime.app_limits) {
          limit.write_bps = 0;
          limit.write_update_time = {};
        }
        ResetInactiveReservationControllerDirection(runtime.feedback.write, now);
      }
      std::set<std::string> apps;
      if (rates_it != node_rates.end()) {
        for (const auto& [app, _] : rates_it->second) {
          apps.insert(app);
        }
      }
      if (targets_it != local_targets.end()) {
        for (const auto& [app, _] : targets_it->second) {
          apps.insert(app);
        }
      }
      for (const auto& [app, _] : runtime.app_limits) {
        apps.insert(app);
      }

      std::vector<AppState> node_apps;
      node_apps.reserve(apps.size());
      const auto pressure_it = node_io_pressure.find(node_id);
      const bool has_pressure = pressure_it != node_io_pressure.end();
      const double pressure = has_pressure ? pressure_it->second : 0.0;
      for (const auto& app : apps) {
        AppState state{};
        state.app_name = app;

        if (rates_it != node_rates.end()) {
          if (const auto it = rates_it->second.find(app); it != rates_it->second.end()) {
            state.current_read_bps = it->second.read_bps;
            state.current_write_bps = it->second.write_bps;
          }
        }
        if (targets_it != local_targets.end()) {
          if (const auto it = targets_it->second.find(app);
              it != targets_it->second.end()) {
            state.reservation_read_bps = it->second.read_bps;
            state.reservation_write_bps = it->second.write_bps;
          }
        }
        if (const auto it = runtime.app_limits.find(app);
            it != runtime.app_limits.end()) {
          state.controller_limit_read_bps = it->second.read_bps;
          state.controller_limit_write_bps = it->second.write_bps;
        }

        state.has_read_io_pressure = has_pressure && state.reservation_read_bps > 0;
        state.has_write_io_pressure = has_pressure && state.reservation_write_bps > 0;
        state.current_read_io_pressure = pressure;
        state.current_write_io_pressure = pressure;
        state.has_read_reservation_competition =
            deficit.read && state.current_read_bps >= active_node_rate_threshold_bps;
        state.has_write_reservation_competition =
            deficit.write && state.current_write_bps >= active_node_rate_threshold_bps;
        node_apps.push_back(std::move(state));
      }

      ApplyDefaultReservationController(
          node_apps, true, controller_min_limit_bps, io_pressure_threshold,
          active_node_rate_threshold_bps, &runtime.feedback, now, nullptr, nullptr, true);

      for (size_t i = 0; i < node_apps.size(); ++i) {
        const auto& state = node_apps[i];
        const auto& app = state.app_name;
        auto& limit = runtime.app_limits[app];
        auto apply_limit = [&](const bool update, const uint64_t next_limit,
                               uint64_t& current_limit,
                               std::chrono::steady_clock::time_point& update_time) {
          if (!update) {
            return;
          }
          const bool refresh_due =
              update_time == std::chrono::steady_clock::time_point{} ||
              now - update_time >= kControllerLimitRefreshInterval;
          if (next_limit != current_limit || (next_limit > 0 && refresh_due)) {
            current_limit = next_limit;
            update_time = next_limit > 0 ? now : std::chrono::steady_clock::time_point{};
          }
        };
        apply_limit(deficit.read && state.update_read,
                    state.new_controller_limit_read_bps, limit.read_bps,
                    limit.read_update_time);
        apply_limit(deficit.write && state.update_write,
                    state.new_controller_limit_write_bps, limit.write_bps,
                    limit.write_update_time);
      }

      for (auto it = runtime.app_limits.begin(); it != runtime.app_limits.end();) {
        if (it->second.read_bps == 0 && it->second.write_bps == 0) {
          it = runtime.app_limits.erase(it);
        } else {
          ++it;
        }
      }
      if (runtime.app_limits.empty() && !HasControllerFeedback(runtime.feedback.read) &&
          !HasControllerFeedback(runtime.feedback.write)) {
        node_runtimes.erase(node_id);
      }
    }
  } else {
    node_runtimes.clear();
  }

  auto limits_equal = [](const auto& lhs, const auto& rhs) {
    auto count_limits = [](const auto& runtimes) {
      size_t count = 0;
      for (const auto& [_, runtime] : runtimes) {
        count += runtime.app_limits.size();
      }
      return count;
    };
    if (count_limits(lhs) != count_limits(rhs)) {
      return false;
    }
    for (const auto& [node_id, runtime] : lhs) {
      const auto rhs_node = rhs.find(node_id);
      if (rhs_node == rhs.end()) {
        if (!runtime.app_limits.empty()) {
          return false;
        }
        continue;
      }
      for (const auto& [app, limit] : runtime.app_limits) {
        const auto rhs_app = rhs_node->second.app_limits.find(app);
        if (rhs_app == rhs_node->second.app_limits.end() ||
            rhs_app->second.read_bps != limit.read_bps ||
            rhs_app->second.write_bps != limit.write_bps) {
          return false;
        }
      }
    }
    return true;
  };

  // Build every allocation-bearing part of the publication before changing
  // live state. Once staged, all commit operations are non-throwing swaps.
  decltype(mNodeAppDelayStates) next_node_app_delay_states;
  decltype(mNodeFstIoDelayConfigs) next_fst_io_delay_configs;
  static_assert(std::is_nothrow_swappable_v<decltype(next_node_app_delay_states)>);
  static_assert(std::is_nothrow_swappable_v<decltype(next_fst_io_delay_configs)>);
  static_assert(std::is_nothrow_swappable_v<decltype(node_runtimes)>);

  if (mPauseControllerBeforePublication.load(std::memory_order_acquire)) {
    mControllerPublicationPaused.store(true, std::memory_order_release);
    while (mPauseControllerBeforePublication.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    mControllerPublicationPaused.store(false, std::memory_order_release);
  }

  {
    std::unique_lock publish_lock(mFstConfigPublishMutex);
    std::unique_lock lock(mMutex);
    if (policy_revision != mControllerInputRevision) {
      return;
    }

    const bool limits_changed = !limits_equal(mNodeReservationControllers, node_runtimes);
    if (limits_changed) {
      next_node_app_delay_states = mNodeAppDelayStates;
      next_fst_io_delay_configs = mNodeFstIoDelayConfigs;
      std::set<std::string> nodes;
      for (const auto& [node, _] : mNodeReservationControllers) {
        nodes.insert(node);
      }
      for (const auto& [node, _] : node_runtimes) {
        nodes.insert(node);
      }
      for (const auto& node : nodes) {
        std::set<std::string> apps;
        const auto old_node = mNodeReservationControllers.find(node);
        const auto new_node = node_runtimes.find(node);
        if (old_node != mNodeReservationControllers.end()) {
          for (const auto& [app, _] : old_node->second.app_limits) {
            apps.insert(app);
          }
        }
        if (new_node != node_runtimes.end()) {
          for (const auto& [app, _] : new_node->second.app_limits) {
            apps.insert(app);
          }
        }

        auto get_limit = [](const auto node_it, const auto node_end,
                            const std::string& app, const bool is_write) {
          if (node_it == node_end) {
            return uint64_t{0};
          }
          const auto app_it = node_it->second.app_limits.find(app);
          if (app_it == node_it->second.app_limits.end()) {
            return uint64_t{0};
          }
          return is_write ? app_it->second.write_bps : app_it->second.read_bps;
        };
        auto& delays = next_node_app_delay_states[node];
        for (const auto& app : apps) {
          const uint64_t old_write =
              get_limit(old_node, mNodeReservationControllers.end(), app, true);
          const uint64_t new_write = get_limit(new_node, node_runtimes.end(), app, true);
          if (old_write != new_write) {
            if (old_write > 0 && new_write > 0) {
              ScaleDelayForLimitChange(&delays.reservation_write, app, old_write,
                                       new_write);
            } else {
              delays.reservation_write.erase(app);
            }
            next_fst_io_delay_configs[node].mutable_app_write_delay()->erase(app);
          }

          const uint64_t old_read =
              get_limit(old_node, mNodeReservationControllers.end(), app, false);
          const uint64_t new_read = get_limit(new_node, node_runtimes.end(), app, false);
          if (old_read != new_read) {
            if (old_read > 0 && new_read > 0) {
              ScaleDelayForLimitChange(&delays.reservation_read, app, old_read, new_read);
            } else {
              delays.reservation_read.erase(app);
            }
            next_fst_io_delay_configs[node].mutable_app_read_delay()->erase(app);
          }
        }
      }
    }

    if (mFailNextControllerPublication.exchange(false, std::memory_order_relaxed)) {
      throw std::bad_alloc{};
    }

    if (limits_changed) {
      mNodeAppDelayStates.swap(next_node_app_delay_states);
      mNodeFstIoDelayConfigs.swap(next_fst_io_delay_configs);
    }
    mNodeReservationControllers.swap(node_runtimes);
    if (limits_changed) {
      ++mControllerInputRevision;
    }
  }
}

void
TrafficShapingManager::SetLimitsEnabled(const bool enabled)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  const bool old_value = mLimitsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (old_value != enabled) {
    std::unique_lock lock(mMutex);
    if (!enabled) {
      ClearControllerLimitsUnlocked();
    }
    mNodeReservationControllers.clear();
    mNodeAppDelayStates.clear();
    ++mControllerInputRevision;
    mNodeFstIoDelayConfigs.clear();
  }
}

bool
TrafficShapingManager::GetLimitsEnabled() const
{
  return mLimitsEnabled.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetReservationsEnabled(const bool enabled)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  const bool old_value =
      mReservationsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (old_value != enabled) {
    std::unique_lock lock(mMutex);
    if (!enabled) {
      ClearControllerLimitsUnlocked();
    }
    mNodeReservationControllers.clear();
    mNodeAppDelayStates.clear();
    ++mControllerInputRevision;
    mNodeFstIoDelayConfigs.clear();
  }
}

bool
TrafficShapingManager::GetReservationsEnabled() const
{
  return mReservationsEnabled.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetControllerMinLimit(const uint64_t limit_bps)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  const uint64_t old_value =
      mControllerMinLimitBps.exchange(limit_bps, std::memory_order_relaxed);

  if (old_value != limit_bps) {
    std::unique_lock lock(mMutex);
    mNodeReservationControllers.clear();
    mNodeAppDelayStates.clear();
    ++mControllerInputRevision;
    mNodeFstIoDelayConfigs.clear();
  }
}

uint64_t
TrafficShapingManager::GetControllerMinLimit() const
{
  return mControllerMinLimitBps.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetActiveNodeRateThreshold(const uint64_t threshold_bps)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  const uint64_t old_value =
      mActiveNodeRateThresholdBps.exchange(threshold_bps, std::memory_order_relaxed);

  if (old_value != threshold_bps) {
    std::unique_lock lock(mMutex);
    mNodeReservationControllers.clear();
    mNodeAppDelayStates.clear();
    ++mControllerInputRevision;
    mNodeFstIoDelayConfigs.clear();
  }
}

uint64_t
TrafficShapingManager::GetActiveNodeRateThreshold() const
{
  return mActiveNodeRateThresholdBps.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetIoPressureThreshold(const double threshold)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  const double clamped_threshold = std::max(0.0, std::min(1.0, threshold));
  const double old_value =
      mIoPressureThreshold.exchange(clamped_threshold, std::memory_order_relaxed);

  if (old_value != clamped_threshold) {
    std::unique_lock lock(mMutex);
    mNodeReservationControllers.clear();
    mNodeAppDelayStates.clear();
    ++mControllerInputRevision;
    mNodeFstIoDelayConfigs.clear();
  }
}

double
TrafficShapingManager::GetIoPressureThreshold() const
{
  return mIoPressureThreshold.load(std::memory_order_relaxed);
}

size_t
TrafficShapingManager::ClearControllerLimits()
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  size_t cleared = 0;
  {
    std::unique_lock lock(mMutex);
    cleared = ClearControllerLimitsUnlocked();

    if (cleared > 0) {
      mNodeAppDelayStates.clear();
      ++mControllerInputRevision;
      mNodeFstIoDelayConfigs.clear();
    }
  }

  return cleared;
}

size_t
TrafficShapingManager::ClearControllerLimitsUnlocked()
{
  size_t cleared = 0;

  auto clear_map = [&cleared](auto& policies) {
    for (auto it = policies.begin(); it != policies.end();) {
      auto& policy = it->second;
      const bool had_controller_limit = policy.controller_limit_write_bytes_per_sec > 0 ||
                                        policy.controller_limit_read_bytes_per_sec > 0;

      if (!had_controller_limit) {
        ++it;
        continue;
      }

      policy.controller_limit_write_bytes_per_sec = 0;
      policy.controller_limit_read_bytes_per_sec = 0;
      policy.controller_limit_write_update_time = {};
      policy.controller_limit_read_update_time = {};
      ++cleared;

      if (policy.IsEmpty()) {
        it = policies.erase(it);
      } else {
        ++it;
      }
    }
  };

  clear_map(mUidPolicies);
  clear_map(mGidPolicies);
  clear_map(mAppPolicies);
  const bool had_node_runtime = !mNodeReservationControllers.empty();
  for (const auto& [_, runtime] : mNodeReservationControllers) {
    cleared += runtime.app_limits.size();
  }
  mNodeReservationControllers.clear();
  if (had_node_runtime && cleared == 0) {
    ++cleared;
  }

  return cleared;
}

size_t
TrafficShapingManager::ExpireControllerLimits(
    const std::chrono::steady_clock::time_point now)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  std::unique_lock lock(mMutex);
  size_t expired = 0;

  auto expire_map = [&expired, now](auto& policies, const auto& on_expire) {
    for (auto it = policies.begin(); it != policies.end();) {
      auto& policy = it->second;

      auto expire_limit = [now](uint64_t& limit,
                                std::chrono::steady_clock::time_point& update_time) {
        if (limit == 0) {
          update_time = {};
          return false;
        }

        if (update_time == std::chrono::steady_clock::time_point{} ||
            now - update_time >= kControllerLimitTtl) {
          limit = 0;
          update_time = {};
          return true;
        }

        return false;
      };

      const bool write_expired = expire_limit(policy.controller_limit_write_bytes_per_sec,
                                              policy.controller_limit_write_update_time);
      const bool read_expired = expire_limit(policy.controller_limit_read_bytes_per_sec,
                                             policy.controller_limit_read_update_time);
      const bool policy_expired = write_expired || read_expired;

      if (policy_expired) {
        ++expired;
        on_expire(it->first, write_expired, read_expired);
      }

      if (policy.IsEmpty()) {
        it = policies.erase(it);
      } else {
        ++it;
      }
    }
  };

  const auto ignore_expiry = [](const auto&, const bool, const bool) {};
  expire_map(mUidPolicies, ignore_expiry);
  expire_map(mGidPolicies, ignore_expiry);
  expire_map(mAppPolicies, [&](const std::string& app, const bool write_expired,
                               const bool read_expired) {
    for (auto& [_, delays] : mNodeAppDelayStates) {
      if (write_expired) {
        delays.global_write.erase(app);
      }
      if (read_expired) {
        delays.global_read.erase(app);
      }
    }
  });

  auto expire_limit = [now](uint64_t& limit,
                            std::chrono::steady_clock::time_point& update_time) {
    if (limit == 0) {
      update_time = {};
      return false;
    }
    if (update_time == std::chrono::steady_clock::time_point{} ||
        now - update_time >= kControllerLimitTtl) {
      limit = 0;
      update_time = {};
      return true;
    }
    return false;
  };
  for (auto runtime_it = mNodeReservationControllers.begin();
       runtime_it != mNodeReservationControllers.end();) {
    bool read_expired = false;
    bool write_expired = false;
    for (auto app_it = runtime_it->second.app_limits.begin();
         app_it != runtime_it->second.app_limits.end();) {
      const bool app_write_expired =
          expire_limit(app_it->second.write_bps, app_it->second.write_update_time);
      const bool app_read_expired =
          expire_limit(app_it->second.read_bps, app_it->second.read_update_time);
      write_expired |= app_write_expired;
      read_expired |= app_read_expired;
      if (const auto delays_it = mNodeAppDelayStates.find(runtime_it->first);
          delays_it != mNodeAppDelayStates.end()) {
        if (app_write_expired) {
          delays_it->second.reservation_write.erase(app_it->first);
        }
        if (app_read_expired) {
          delays_it->second.reservation_read.erase(app_it->first);
        }
      }
      if (app_write_expired || app_read_expired) {
        ++expired;
      }
      if (app_it->second.write_bps == 0 && app_it->second.read_bps == 0) {
        app_it = runtime_it->second.app_limits.erase(app_it);
      } else {
        ++app_it;
      }
    }
    if (write_expired) {
      runtime_it->second.feedback.write = {};
    }
    if (read_expired) {
      runtime_it->second.feedback.read = {};
    }
    if (runtime_it->second.app_limits.empty() &&
        !HasControllerFeedback(runtime_it->second.feedback.read) &&
        !HasControllerFeedback(runtime_it->second.feedback.write)) {
      runtime_it = mNodeReservationControllers.erase(runtime_it);
    } else {
      ++runtime_it;
    }
  }

  if (expired > 0) {
    ++mControllerInputRevision;
    mNodeFstIoDelayConfigs.clear();
  }

  return expired;
}

void
TrafficShapingManager::UpdateLimits(
    const std::unordered_map<std::string, double>& node_io_pressure,
    const std::vector<std::string>& online_nodes)
{
  bool limits_enabled = false;
  bool reservations_enabled = false;
  double io_pressure_threshold = 0.0;
  uint64_t active_node_rate_threshold_bps = 0;
  uint64_t input_revision = 0;

  auto calculate_delay =
      [&](const double limit_bps, const double global_rate, const double node_rate,
          const bool has_global_rate_sample, const bool has_node_rate_sample,
          const bool use_node_rate_for_control, const bool allow_idle_release,
          const uint64_t current_delay_us, const std::string& node_id,
          const char* entity_type, const std::string& entity_id, const char* op_type,
          const double io_pressure) -> uint64_t {
    if (limit_bps <= 0) {
      return 0;
    }

    const uint64_t old_delay = current_delay_us;
    uint64_t delay_us = current_delay_us;
    const double control_limit_bps = limit_bps;
    const double control_rate_bps = use_node_rate_for_control ? node_rate : global_rate;
    const bool has_control_rate_sample =
        use_node_rate_for_control ? has_node_rate_sample : has_global_rate_sample;
    const double ratio = control_rate_bps / control_limit_bps;
    const double delay_reference_bps = control_limit_bps;
    delay_us = CalculateDelayUs(control_limit_bps, control_rate_bps, old_delay,
                                io_pressure, has_control_rate_sample, allow_idle_release,
                                delay_reference_bps, io_pressure_threshold);
    delay_us = std::min<uint64_t>(delay_us, kMaxDelayUs);

    eos_static_debug(
        "msg=\"throttle evaluation\" node=\"%s\" type=\"%s\" id=\"%s\" op=\"%s\" "
        "limit_bps=%.0f global_rate_bps=%.0f node_rate_bps=%.0f ratio=%.3f "
        "control_scope=%s control_limit_bps=%.0f control_rate_bps=%.0f "
        "delay_reference_bps=%.0f io_pressure=%.3f "
        "allow_idle_release=%d old_delay_us=%lu new_delay_us=%lu",
        node_id.c_str(), entity_type, entity_id.c_str(), op_type, limit_bps, global_rate,
        node_rate, ratio, use_node_rate_for_control ? "node" : "global",
        control_limit_bps, control_rate_bps, delay_reference_bps, io_pressure,
        allow_idle_release, old_delay, delay_us);
    return delay_us;
  };

  // Helper lambda to do a single-pass map lookup
  auto get_rate = [](const auto& map, const auto& key) {
    if (auto it = map.find(key); it != map.end()) {
      return std::pair{it->second, true};
    }
    return std::pair{0.0, false};
  };

  auto get_delay = [](const auto& map, const auto& key) {
    if (const auto it = map.find(key); it != map.end()) {
      return it->second;
    }
    return uint64_t{0};
  };

  auto effective_limit = [&](const TrafficShapingPolicy& policy, const bool is_write) {
    if (!limits_enabled) {
      return uint64_t{0};
    }

    const uint64_t user_limit = policy.is_enabled
                                    ? (is_write ? policy.limit_write_bytes_per_sec
                                                : policy.limit_read_bytes_per_sec)
                                    : 0;
    const uint64_t controller_limit =
        reservations_enabled ? (is_write ? policy.controller_limit_write_bytes_per_sec
                                         : policy.controller_limit_read_bytes_per_sec)
                             : 0;

    if (user_limit > 0 && controller_limit > 0) {
      return std::min(user_limit, controller_limit);
    }

    return user_limit > 0 ? user_limit : controller_limit;
  };

  auto get_pressure = [&](const std::string& node_id) {
    if (auto it = node_io_pressure.find(node_id); it != node_io_pressure.end()) {
      return it->second;
    }
    // Missing pressure is unknown, not evidence of saturation. Explicit user
    // limits remain active, but reservation-driven limits must not engage.
    return 0.0;
  };

  std::unordered_map<std::string, eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      fst_io_delay_configs;
  std::unordered_map<std::string, TrafficShapingPolicy> app_policies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> uid_policies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> gid_policies;
  std::unordered_map<std::string, NodeReservationControllerRuntime> node_controllers;
  std::unordered_map<std::string, NodeAppDelayState> previous_app_delay_states;
  std::unordered_map<std::string, eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      previous_fst_configs;
  std::unordered_map<std::string, NodeAppDelayState> next_app_delay_states;
  std::unordered_map<std::string, EntityRateMaps> node_rates;
  EntityRateMaps global_rates;
  EntityRateMaps reservation_global_rates;
  EntityRateMaps reservation_global_stable_rates;
  bool has_active_policies = false;

  // Snapshot only the inputs needed for this tick. High-cardinality policy
  // evaluation and delay calculation run after releasing the manager lock.
  {
    std::shared_lock lock(mMutex);
    limits_enabled = mLimitsEnabled.load(std::memory_order_relaxed);
    reservations_enabled = mReservationsEnabled.load(std::memory_order_relaxed);
    io_pressure_threshold = mIoPressureThreshold.load(std::memory_order_relaxed);
    active_node_rate_threshold_bps =
        mActiveNodeRateThresholdBps.load(std::memory_order_relaxed);
    input_revision = mControllerInputRevision;
    app_policies = mAppPolicies;
    uid_policies = mUidPolicies;
    gid_policies = mGidPolicies;
    node_controllers = mNodeReservationControllers;
    previous_app_delay_states = mNodeAppDelayStates;
    previous_fst_configs = mNodeFstIoDelayConfigs;

    auto has_active_policy = [](const auto& policies) {
      for (const auto& [id, policy] : policies) {
        if (policy.IsActive()) {
          return true;
        }
      }

      return false;
    };

    const bool has_node_controller_limits =
        std::any_of(node_controllers.begin(), node_controllers.end(),
                    [](const auto& entry) { return !entry.second.app_limits.empty(); });
    has_active_policies = has_active_policy(app_policies) ||
                          has_active_policy(uid_policies) ||
                          has_active_policy(gid_policies) || has_node_controller_limits;

    if (has_active_policies) {
      for (const auto& [node_entity_key, stats] : mNodeEntityStats) {
        AddStreamRates(node_rates[node_entity_key.node_id], node_entity_key.stream,
                       stats.ema);
      }
      for (const auto& [key, stats] : mGlobalStats) {
        AddStreamRates(global_rates, key, stats.ema[Ema1s]);
        AddStreamRates(reservation_global_rates, key, stats.ema[Ema1s]);
        AddStreamRates(reservation_global_stable_rates, key, stats.ema[Ema5s]);
      }
    }
  }
  MergeMaxRates(reservation_global_rates, reservation_global_stable_rates);

  if (!has_active_policies) {
    for (const auto& node_id : online_nodes) {
      fst_io_delay_configs[node_id] =
          eos::traffic_shaping::TrafficShapingFstIoDelayConfig{};
    }
  } else {

    auto has_pressured_app_reservation =
        [&](const EntityRateMaps& rates, const bool is_write, const double io_pressure) {
          if (!reservations_enabled || io_pressure < io_pressure_threshold) {
            return false;
          }

          for (const auto& [app, policy] : app_policies) {
            if (!policy.is_enabled) {
              continue;
            }
            const uint64_t reservation_bps = EffectiveReservation(policy, is_write);
            if (reservation_bps == 0) {
              continue;
            }

            const auto [global_rate_bps, _] =
                get_rate(is_write ? reservation_global_rates.app_write
                                  : reservation_global_rates.app_read,
                         app);
            const auto [node_rate_bps, has_node_rate_sample] =
                get_rate(is_write ? rates.app_write : rates.app_read, app);

            if (!has_node_rate_sample || node_rate_bps < active_node_rate_threshold_bps) {
              continue;
            }

            const double reserved_deficit_bps =
                std::max(0.0, static_cast<double>(reservation_bps) - global_rate_bps);
            const double min_meaningful_deficit_bps =
                std::max(kMinReservationDeficitBps, static_cast<double>(reservation_bps) *
                                                        kMinReservationDeficitFraction);

            if (reserved_deficit_bps >= min_meaningful_deficit_bps) {
              return true;
            }
          }

          return false;
        };

    const EntityRateMaps empty_rates;
    const NodeAppDelayState empty_app_delays;
    const eos::traffic_shaping::TrafficShapingFstIoDelayConfig empty_config;
    for (const auto& node_id : online_nodes) {
      const auto rates_it = node_rates.find(node_id);
      const EntityRateMaps& rates =
          rates_it == node_rates.end() ? empty_rates : rates_it->second;
      const double io_pressure = get_pressure(node_id);
      const bool node_has_pressured_write_reservation =
          has_pressured_app_reservation(rates, true, io_pressure);
      const bool node_has_pressured_read_reservation =
          has_pressured_app_reservation(rates, false, io_pressure);
      const auto previous_config_it = previous_fst_configs.find(node_id);
      const auto& previous_config = previous_config_it == previous_fst_configs.end()
                                        ? empty_config
                                        : previous_config_it->second;
      const auto previous_app_delays_it = previous_app_delay_states.find(node_id);
      const auto& previous_app_delays =
          previous_app_delays_it == previous_app_delay_states.end()
              ? empty_app_delays
              : previous_app_delays_it->second;
      NodeAppDelayState next_app_delays;
      eos::traffic_shaping::TrafficShapingFstIoDelayConfig next_config;

      auto* app_write_map = next_config.mutable_app_write_delay();
      auto* app_read_map = next_config.mutable_app_read_delay();
      auto* uid_write_map = next_config.mutable_uid_write_delay();
      auto* uid_read_map = next_config.mutable_uid_read_delay();
      auto* gid_write_map = next_config.mutable_gid_write_delay();
      auto* gid_read_map = next_config.mutable_gid_read_delay();

      for (const auto& [app, policy] : app_policies) {
        if (!policy.IsActive()) {
          eos_static_debug(
              "msg=\"skipping inactive policy\" node=\"%s\" type=\"app\" id=\"%s\"",
              node_id.c_str(), app.c_str());
          continue;
        }

        const auto [global_write_rate, has_global_write_rate] =
            get_rate(global_rates.app_write, app);
        const auto [node_write_rate, has_node_write_rate] =
            get_rate(rates.app_write, app);
        const bool allow_write_idle_release =
            !(policy.is_enabled && policy.limit_write_bytes_per_sec > 0);
        if (ShouldEmitDelayForPolicy(policy, true, node_write_rate, io_pressure,
                                     node_has_pressured_write_reservation, limits_enabled,
                                     reservations_enabled, io_pressure_threshold,
                                     active_node_rate_threshold_bps)) {
          const uint64_t delay = calculate_delay(
              static_cast<double>(effective_limit(policy, true)), global_write_rate,
              node_write_rate, has_global_write_rate, has_node_write_rate, false,
              allow_write_idle_release, get_delay(previous_app_delays.global_write, app),
              node_id, "app", app, "write", io_pressure);
          if (delay > 0) {
            (*app_write_map)[app] = delay;
            next_app_delays.global_write[app] = delay;
          }
        }

        const auto [global_read_rate, has_global_read_rate] =
            get_rate(global_rates.app_read, app);
        const auto [node_read_rate, has_node_read_rate] = get_rate(rates.app_read, app);
        const bool allow_read_idle_release =
            !(policy.is_enabled && policy.limit_read_bytes_per_sec > 0);
        if (ShouldEmitDelayForPolicy(policy, false, node_read_rate, io_pressure,
                                     node_has_pressured_read_reservation, limits_enabled,
                                     reservations_enabled, io_pressure_threshold,
                                     active_node_rate_threshold_bps)) {
          const uint64_t delay = calculate_delay(
              static_cast<double>(effective_limit(policy, false)), global_read_rate,
              node_read_rate, has_global_read_rate, has_node_read_rate, false,
              allow_read_idle_release, get_delay(previous_app_delays.global_read, app),
              node_id, "app", app, "read", io_pressure);
          if (delay > 0) {
            (*app_read_map)[app] = delay;
            next_app_delays.global_read[app] = delay;
          }
        }
      }

      // Built-in reservation limits are owned by a node-local feedback loop.
      // Evaluate them against that node's rate and merge with any persistent
      // global constraint by keeping the stricter delay.
      if (const auto runtime_it = node_controllers.find(node_id);
          runtime_it != node_controllers.end()) {
        for (const auto& [app, local_limit] : runtime_it->second.app_limits) {
          if (const auto policy_it = app_policies.find(app);
              policy_it != app_policies.end() && !policy_it->second.is_enabled) {
            continue;
          }

          const auto [global_write_rate, has_global_write_rate] =
              get_rate(global_rates.app_write, app);
          const auto [node_write_rate, has_node_write_rate] =
              get_rate(rates.app_write, app);
          if (local_limit.write_bps > 0) {
            const uint64_t delay = calculate_delay(
                static_cast<double>(local_limit.write_bps), global_write_rate,
                node_write_rate, has_global_write_rate, has_node_write_rate, true, true,
                get_delay(previous_app_delays.reservation_write, app), node_id, "app",
                app, "write", io_pressure);
            if (delay > 0) {
              next_app_delays.reservation_write[app] = delay;
              (*app_write_map)[app] = std::max(get_delay(*app_write_map, app), delay);
            }
          }

          const auto [global_read_rate, has_global_read_rate] =
              get_rate(global_rates.app_read, app);
          const auto [node_read_rate, has_node_read_rate] = get_rate(rates.app_read, app);
          if (local_limit.read_bps > 0) {
            const uint64_t delay = calculate_delay(
                static_cast<double>(local_limit.read_bps), global_read_rate,
                node_read_rate, has_global_read_rate, has_node_read_rate, true, true,
                get_delay(previous_app_delays.reservation_read, app), node_id, "app", app,
                "read", io_pressure);
            if (delay > 0) {
              next_app_delays.reservation_read[app] = delay;
              (*app_read_map)[app] = std::max(get_delay(*app_read_map, app), delay);
            }
          }
        }
      }

      for (const auto& [uid, policy] : uid_policies) {
        if (!policy.IsActive()) {
          eos_static_debug(
              "msg=\"skipping inactive policy\" node=\"%s\" type=\"uid\" id=\"%u\"",
              node_id.c_str(), uid);
          continue;
        }

        const auto [global_write_rate, has_global_write_rate] =
            get_rate(global_rates.uid_write, uid);
        const auto [node_write_rate, has_node_write_rate] =
            get_rate(rates.uid_write, uid);
        const bool allow_write_idle_release =
            !(policy.is_enabled && policy.limit_write_bytes_per_sec > 0);
        if (ShouldEmitDelayForPolicy(policy, true, node_write_rate, io_pressure,
                                     node_has_pressured_write_reservation, limits_enabled,
                                     reservations_enabled, io_pressure_threshold,
                                     active_node_rate_threshold_bps)) {
          const uint64_t delay = calculate_delay(
              static_cast<double>(effective_limit(policy, true)), global_write_rate,
              node_write_rate, has_global_write_rate, has_node_write_rate, false,
              allow_write_idle_release, get_delay(previous_config.uid_write_delay(), uid),
              node_id, "uid", std::to_string(uid), "write", io_pressure);
          if (delay > 0) {
            (*uid_write_map)[uid] = delay;
          }
        }

        const auto [global_read_rate, has_global_read_rate] =
            get_rate(global_rates.uid_read, uid);
        const auto [node_read_rate, has_node_read_rate] = get_rate(rates.uid_read, uid);
        const bool allow_read_idle_release =
            !(policy.is_enabled && policy.limit_read_bytes_per_sec > 0);
        if (ShouldEmitDelayForPolicy(policy, false, node_read_rate, io_pressure,
                                     node_has_pressured_read_reservation, limits_enabled,
                                     reservations_enabled, io_pressure_threshold,
                                     active_node_rate_threshold_bps)) {
          const uint64_t delay = calculate_delay(
              static_cast<double>(effective_limit(policy, false)), global_read_rate,
              node_read_rate, has_global_read_rate, has_node_read_rate, false,
              allow_read_idle_release, get_delay(previous_config.uid_read_delay(), uid),
              node_id, "uid", std::to_string(uid), "read", io_pressure);
          if (delay > 0) {
            (*uid_read_map)[uid] = delay;
          }
        }
      }

      for (const auto& [gid, policy] : gid_policies) {
        if (!policy.IsActive()) {
          eos_static_debug(
              "msg=\"skipping inactive policy\" node=\"%s\" type=\"gid\" id=\"%u\"",
              node_id.c_str(), gid);
          continue;
        }

        const auto [global_write_rate, has_global_write_rate] =
            get_rate(global_rates.gid_write, gid);
        const auto [node_write_rate, has_node_write_rate] =
            get_rate(rates.gid_write, gid);
        const bool allow_write_idle_release =
            !(policy.is_enabled && policy.limit_write_bytes_per_sec > 0);
        if (ShouldEmitDelayForPolicy(policy, true, node_write_rate, io_pressure,
                                     node_has_pressured_write_reservation, limits_enabled,
                                     reservations_enabled, io_pressure_threshold,
                                     active_node_rate_threshold_bps)) {
          const uint64_t delay = calculate_delay(
              static_cast<double>(effective_limit(policy, true)), global_write_rate,
              node_write_rate, has_global_write_rate, has_node_write_rate, false,
              allow_write_idle_release, get_delay(previous_config.gid_write_delay(), gid),
              node_id, "gid", std::to_string(gid), "write", io_pressure);
          if (delay > 0) {
            (*gid_write_map)[gid] = delay;
          }
        }

        const auto [global_read_rate, has_global_read_rate] =
            get_rate(global_rates.gid_read, gid);
        const auto [node_read_rate, has_node_read_rate] = get_rate(rates.gid_read, gid);
        const bool allow_read_idle_release =
            !(policy.is_enabled && policy.limit_read_bytes_per_sec > 0);
        if (ShouldEmitDelayForPolicy(policy, false, node_read_rate, io_pressure,
                                     node_has_pressured_read_reservation, limits_enabled,
                                     reservations_enabled, io_pressure_threshold,
                                     active_node_rate_threshold_bps)) {
          const uint64_t delay = calculate_delay(
              static_cast<double>(effective_limit(policy, false)), global_read_rate,
              node_read_rate, has_global_read_rate, has_node_read_rate, false,
              allow_read_idle_release, get_delay(previous_config.gid_read_delay(), gid),
              node_id, "gid", std::to_string(gid), "read", io_pressure);
          if (delay > 0) {
            (*gid_read_map)[gid] = delay;
          }
        }
      }

      if (!next_app_delays.global_read.empty() || !next_app_delays.global_write.empty() ||
          !next_app_delays.reservation_read.empty() ||
          !next_app_delays.reservation_write.empty()) {
        next_app_delay_states[node_id] = std::move(next_app_delays);
      }
      fst_io_delay_configs[node_id] = std::move(next_config);
    }
  }

  std::vector<std::pair<std::string, std::string>> encoded_configs;
  encoded_configs.reserve(online_nodes.size());

  for (const auto& node_name : online_nodes) {
    auto& config = fst_io_delay_configs[node_name];
    const size_t entry_count = static_cast<size_t>(config.uid_read_delay_size()) +
                               static_cast<size_t>(config.uid_write_delay_size()) +
                               static_cast<size_t>(config.gid_read_delay_size()) +
                               static_cast<size_t>(config.gid_write_delay_size()) +
                               static_cast<size_t>(config.app_read_delay_size()) +
                               static_cast<size_t>(config.app_write_delay_size());
    const auto has_oversized_app = [](const auto& delays) {
      return std::any_of(delays.begin(), delays.end(), [](const auto& item) {
        return item.first.size() > eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES;
      });
    };
    bool config_within_bounds =
        entry_count <= eos::common::TRAFFIC_SHAPING_FST_CONFIG_MAX_ENTRIES &&
        !has_oversized_app(config.app_read_delay()) &&
        !has_oversized_app(config.app_write_delay());
    std::string serialized;
    if (config_within_bounds) {
      serialized = config.SerializeAsString();
      config_within_bounds =
          serialized.size() <= eos::common::TRAFFIC_SHAPING_FST_CONFIG_MAX_DECODED_BYTES;
    }
    if (!config_within_bounds) {
      eos_static_err("msg=\"Generated FST IO limits config exceeded safety bounds; "
                     "publishing empty fail-open config\" node=\"%s\" entries=%zu "
                     "decoded_bytes=%zu",
                     node_name.c_str(), entry_count, serialized.size());
      config.Clear();
      next_app_delay_states.erase(node_name);
      serialized = config.SerializeAsString();
    }
    std::string encoded;

    if (!eos::common::SymKey::Base64(serialized, encoded) ||
        encoded.size() > eos::common::TRAFFIC_SHAPING_FST_CONFIG_MAX_ENCODED_BYTES) {
      eos_static_err(
          "msg=\"Failed to encode bounded FST IO limits config; publishing "
          "empty fail-open config\" node=\"%s\" encoded_bytes=%zu max_bytes=%zu",
          node_name.c_str(), encoded.size(),
          eos::common::TRAFFIC_SHAPING_FST_CONFIG_MAX_ENCODED_BYTES);
      config.Clear();
      next_app_delay_states.erase(node_name);
      serialized.clear();
      encoded.clear();
      if (!eos::common::SymKey::Base64(serialized, encoded) ||
          encoded.size() > eos::common::TRAFFIC_SHAPING_FST_CONFIG_MAX_ENCODED_BYTES) {
        eos_static_err("msg=\"Failed to encode empty FST IO limits config\" node=\"%s\"",
                       node_name.c_str());
        continue;
      }
    }

    encoded_configs.emplace_back(node_name, std::move(encoded));
  }

  const auto now = std::chrono::steady_clock::now();
  std::vector<std::pair<std::string, std::string>> configs_to_publish;
  uint64_t committed_revision = 0;
  {
    // Serialize only the revision-checked in-memory commit with policy
    // mutations. External configuration publication can block on QDB and must
    // not retain this lock.
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock lock(mMutex);
    if (input_revision != mControllerInputRevision) {
      return;
    }
    std::set<std::string> online_node_set(online_nodes.begin(), online_nodes.end());
    bool removed_node_controller = false;
    for (auto it = mNodeReservationControllers.begin();
         it != mNodeReservationControllers.end();) {
      if (online_node_set.find(it->first) == online_node_set.end()) {
        removed_node_controller |= !it->second.app_limits.empty();
        it = mNodeReservationControllers.erase(it);
      } else {
        ++it;
      }
    }
    if (removed_node_controller) {
      ++mControllerInputRevision;
    }
    committed_revision = mControllerInputRevision;
    mNodeFstIoDelayConfigs = std::move(fst_io_delay_configs);
    mNodeAppDelayStates = std::move(next_app_delay_states);

    for (auto it = mPublishedFstIoDelayConfigs.begin();
         it != mPublishedFstIoDelayConfigs.end();) {
      if (online_node_set.find(it->first) == online_node_set.end()) {
        it = mPublishedFstIoDelayConfigs.erase(it);
      } else {
        ++it;
      }
    }

    for (const auto& [node_name, encoded] : encoded_configs) {
      auto& published = mPublishedFstIoDelayConfigs[node_name];
      const bool config_changed = published.encoded_config != encoded;
      const bool refresh_due =
          published.last_publish_time == std::chrono::steady_clock::time_point{} ||
          now - published.last_publish_time >= kFstIoDelayConfigRefreshInterval;

      if (config_changed || refresh_due) {
        configs_to_publish.emplace_back(node_name, encoded);
      }
    }
  }

  std::vector<std::pair<std::string, std::string>> published_configs;
  if (!configs_to_publish.empty()) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (const auto& [node_name, encoded] : configs_to_publish) {
      auto it = FsView::gFsView.mNodeView.find(node_name);
      if (it != FsView::gFsView.mNodeView.end()) {
        if (it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_IO_LIMITS,
                                        encoded, true)) {
          published_configs.emplace_back(node_name, encoded);
        }
      }
    }
  }

  if (!published_configs.empty()) {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock lock(mMutex);
    if (committed_revision != mControllerInputRevision) {
      return;
    }
    for (const auto& [node_name, encoded] : published_configs) {
      auto& published = mPublishedFstIoDelayConfigs[node_name];
      published.encoded_config = encoded;
      published.last_publish_time = now;
    }
  }
}

std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash>
TrafficShapingManager::GetGlobalStats() const
{
  std::shared_lock lock(mMutex);

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> snapshot_map;
  snapshot_map.reserve(mGlobalStats.size());

  for (const auto& [key, internal_stat] : mGlobalStats) {
    RateSnapshot& snap = snapshot_map[key];

    snap.last_activity_time = internal_stat.last_activity_time;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

std::unordered_map<std::string, RateSnapshot>
TrafficShapingManager::GetNodeStats() const
{
  std::shared_lock lock(mMutex);
  std::unordered_map<std::string, RateSnapshot> snapshot_map;
  snapshot_map.reserve(mNodeStats.size());

  for (const auto& [node_id, internal_stat] : mNodeStats) {
    RateSnapshot& snap = snapshot_map[node_id];
    snap.last_activity_time = internal_stat.last_activity_time;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash>
TrafficShapingManager::GetDiskStats() const
{
  std::shared_lock lock(mMutex);
  std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash> snapshot_map;
  snapshot_map.reserve(mDiskStats.size());

  for (const auto& [disk_key, internal_stat] : mDiskStats) {
    RateSnapshot& snap = snapshot_map[disk_key];
    snap.last_activity_time = internal_stat.last_activity_time;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash>
TrafficShapingManager::GetDetailedStats() const
{
  std::shared_lock lock(mMutex);
  std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash> snapshot_map;
  snapshot_map.reserve(mDetailedStats.size());

  for (const auto& [detailed_key, internal_stat] : mDetailedStats) {
    RateSnapshot& snap = snapshot_map[detailed_key];
    snap.last_activity_time = internal_stat.last_activity_time;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

TrafficShapingManager::GarbageCollectionStats
TrafficShapingManager::GarbageCollect(const int max_idle_seconds)
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  std::unique_lock lock(mMutex);

  const auto now_steady = std::chrono::steady_clock::now();
  const time_t now_unix = time(nullptr);

  GarbageCollectionStats stats = {0, 0, 0, 0, 0};

  for (auto node_it = mNodeStates.begin(); node_it != mNodeStates.end();) {
    NodeStateMap& map = node_it->second.streams;

    for (auto stream_it = map.begin(); stream_it != map.end();) {

      auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(
                             now_steady - stream_it->second.last_update_time)
                             .count();

      if (elapsed_sec > max_idle_seconds) {
        const size_t estimated_stream_bytes =
            EstimateFstStreamStateBytes(node_it->first, stream_it->first.app);
        mFstStreamStateCount -= std::min<size_t>(mFstStreamStateCount, 1);
        mFstStreamStateEstimatedBytes -=
            std::min(mFstStreamStateEstimatedBytes, estimated_stream_bytes);
        stream_it = map.erase(stream_it);
        stats.removed_node_streams++;
      } else {
        ++stream_it;
      }
    }

    // Calculate how long it has been since the Node itself sent a heartbeat
    auto node_idle_sec = std::chrono::duration_cast<std::chrono::seconds>(
                             now_steady - node_it->second.last_report_time)
                             .count();

    // Only erase the node if it has no streams AND the node is actually offline/silent
    if (map.empty() && node_idle_sec > max_idle_seconds) {
      mNodeStats.erase(node_it->first);
      node_it = mNodeStates.erase(node_it);
      stats.removed_nodes++;
    } else {
      ++node_it;
    }
  }

  for (auto it = mGlobalStats.begin(); it != mGlobalStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mGlobalStats.erase(it);
      stats.removed_global_streams++;
    } else {
      ++it;
    }
  }

  for (auto it = mDiskStats.begin(); it != mDiskStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mDiskStats.erase(it);
      stats.removed_disk_stats++;
    } else {
      ++it;
    }
  }

  for (auto it = mDetailedStats.begin(); it != mDetailedStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mDetailedStats.erase(it);
      stats.removed_detailed_stats++;
    } else {
      ++it;
    }
  }

  bool removed_controller_input = false;
  for (auto it = mNodeEntityStats.begin(); it != mNodeEntityStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mNodeEntityStats.erase(it);
      removed_controller_input = true;
    } else {
      ++it;
    }
  }
  auto prune_cumulative_stats = [now_unix, max_idle_seconds](auto& map) {
    for (auto it = map.begin(); it != map.end();) {
      if (now_unix - it->second.last_activity_time > max_idle_seconds) {
        it = map.erase(it);
      } else {
        ++it;
      }
    }
  };

  prune_cumulative_stats(mGlobalCumulativeStats);
  prune_cumulative_stats(mNodeCumulativeStats);
  prune_cumulative_stats(mDiskCumulativeStats);
  prune_cumulative_stats(mDetailedCumulativeStats);
  prune_cumulative_stats(mProjectionCumulativeStats.app);
  prune_cumulative_stats(mProjectionCumulativeStats.uid);
  prune_cumulative_stats(mProjectionCumulativeStats.gid);
  prune_cumulative_stats(mProjectionCumulativeStats.node);

  if (removed_controller_input) {
    // Invalidate any controller calculation that still references the removed
    // samples. Keep unrelated node feedback and limits intact; the next
    // controller tick will release only runtimes whose own inputs disappeared.
    ++mControllerInputRevision;
  }

  return stats;
}

void
TrafficShapingManager::UpdateFstLimitsLoopMicroSec(const uint64_t time_microseconds)
{
  std::unique_lock lock(mMutex);
  if (fst_limits_update_loop_micro_sec) {
    fst_limits_update_loop_micro_sec->Add(time_microseconds);
    fst_limits_update_loop_micro_sec->Tick();
  }
}

void
TrafficShapingManager::UpdateReservationControllerLoopMicroSec(
    const uint64_t time_microseconds)
{
  std::unique_lock lock(mMutex);
  if (reservation_controller_update_loop_micro_sec) {
    reservation_controller_update_loop_micro_sec->Add(time_microseconds);
    reservation_controller_update_loop_micro_sec->Tick();
  }
}

void
TrafficShapingManager::UpdateEstimatorsLoopMicroSec(const uint64_t time_microseconds)
{
  std::unique_lock lock(mMutex);
  if (estimators_update_loop_micro_sec) {
    estimators_update_loop_micro_sec->Add(time_microseconds);
    estimators_update_loop_micro_sec->Tick();
  }
  if (fst_reports_processed_per_second) {
    fst_reports_processed_per_second->Tick();
  }
}

std::tuple<uint64_t, uint64_t, uint64_t>
TrafficShapingManager::GetEstimatorsUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (estimators_update_loop_micro_sec) {
    return {
        estimators_update_loop_micro_sec->GetMedian(true), // Ignore zeroes correctly
        estimators_update_loop_micro_sec->GetMin(true),
        estimators_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0, 0, 0};
}

std::tuple<uint64_t, uint64_t, uint64_t>
TrafficShapingManager::GetFstLimitsUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (fst_limits_update_loop_micro_sec) {
    return {
        fst_limits_update_loop_micro_sec->GetMedian(true),
        fst_limits_update_loop_micro_sec->GetMin(true),
        fst_limits_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0, 0, 0};
}

std::tuple<uint64_t, uint64_t, uint64_t>
TrafficShapingManager::GetReservationControllerUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (reservation_controller_update_loop_micro_sec) {
    return {
        reservation_controller_update_loop_micro_sec->GetMedian(true),
        reservation_controller_update_loop_micro_sec->GetMin(true),
        reservation_controller_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0, 0, 0};
}

MapCardinalityStats
TrafficShapingManager::GetMapCardinalityStats() const
{
  std::shared_lock lock(mMutex);

  MapCardinalityStats stats;
  stats.node_states = static_cast<uint64_t>(mNodeStates.size());
  stats.node_state_streams = static_cast<uint64_t>(mFstStreamStateCount);
  stats.node_state_estimated_bytes = static_cast<uint64_t>(mFstStreamStateEstimatedBytes);
  stats.node_state_rejections_total =
      mFstStreamStatesRejectedTotal.load(std::memory_order_relaxed);
  stats.global_stats = static_cast<uint64_t>(mGlobalStats.size());
  stats.node_stats = static_cast<uint64_t>(mNodeStats.size());
  stats.disk_stats = static_cast<uint64_t>(mDiskStats.size());
  stats.detailed_stats = static_cast<uint64_t>(mDetailedStats.size());
  stats.global_cumulative_stats = static_cast<uint64_t>(mGlobalCumulativeStats.size());
  stats.node_cumulative_stats = static_cast<uint64_t>(mNodeCumulativeStats.size());
  stats.disk_cumulative_stats = static_cast<uint64_t>(mDiskCumulativeStats.size());
  stats.detailed_cumulative_stats =
      static_cast<uint64_t>(mDetailedCumulativeStats.size());
  stats.projection_app_cumulative_stats =
      static_cast<uint64_t>(mProjectionCumulativeStats.app.size());
  stats.projection_uid_cumulative_stats =
      static_cast<uint64_t>(mProjectionCumulativeStats.uid.size());
  stats.projection_gid_cumulative_stats =
      static_cast<uint64_t>(mProjectionCumulativeStats.gid.size());
  stats.projection_node_cumulative_stats =
      static_cast<uint64_t>(mProjectionCumulativeStats.node.size());
  stats.node_entity_stats = static_cast<uint64_t>(mNodeEntityStats.size());
  stats.uid_policies = static_cast<uint64_t>(mUidPolicies.size());
  stats.gid_policies = static_cast<uint64_t>(mGidPolicies.size());
  stats.app_policies = static_cast<uint64_t>(mAppPolicies.size());
  stats.node_fst_io_delay_configs = static_cast<uint64_t>(mNodeFstIoDelayConfigs.size());
  stats.published_fst_io_delay_configs =
      static_cast<uint64_t>(mPublishedFstIoDelayConfigs.size());
  return stats;
}

void
TrafficShapingManager::UpdateFstReportsProcessed(const uint64_t count)
{
  std::unique_lock lock(mMutex);
  if (fst_reports_processed_per_second) {
    fst_reports_processed_per_second->Add(count);
  }
}

double
TrafficShapingManager::GetFstReportsProcessedPerSecondMean() const
{
  std::shared_lock lock(mMutex);
  if (fst_reports_processed_per_second) {
    double multiplier = 1.0;
    if (mEstimatorsTickIntervalSec > 0.0) {
      multiplier = 1.0 / mEstimatorsTickIntervalSec;
    }

    // 0 counts in a tick are valid measurements for processing speed.
    return fst_reports_processed_per_second->GetMean(false) * multiplier;
  }
  return 0.0;
}

void
TrafficShapingManager::UpdateFstReportQueueStats(const uint64_t depth,
                                                 const uint64_t estimated_bytes,
                                                 const uint64_t dropped)
{
  mFstReportQueueDepth.store(depth, std::memory_order_relaxed);
  mFstReportQueueEstimatedBytes.store(estimated_bytes, std::memory_order_relaxed);
  mFstReportsDroppedTotal.fetch_add(dropped, std::memory_order_relaxed);
}

uint64_t
TrafficShapingManager::GetFstReportQueueDepth() const
{
  return mFstReportQueueDepth.load(std::memory_order_relaxed);
}

uint64_t
TrafficShapingManager::GetFstReportQueueEstimatedBytes() const
{
  return mFstReportQueueEstimatedBytes.load(std::memory_order_relaxed);
}

uint64_t
TrafficShapingManager::GetFstReportsDroppedTotal() const
{
  return mFstReportsDroppedTotal.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::Clear()
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  std::unique_lock lock(mMutex);
  mNodeStates.clear();
  mFstStreamStateCount = 0;
  mFstStreamStateEstimatedBytes = 0;
  mGlobalStats.clear();
  mNodeStats.clear();
  mDiskStats.clear();
  mDetailedStats.clear();
  mNodeEntityStats.clear();
  mTotalStats.clear();
  mNodeFstIoDelayConfigs.clear();
  mNodeAppDelayStates.clear();
  mPublishedFstIoDelayConfigs.clear();
  mNodeReservationControllers.clear();
  mGlobalCumulativeStats.clear();
  mNodeCumulativeStats.clear();
  mDiskCumulativeStats.clear();
  mDetailedCumulativeStats.clear();
  mProjectionCumulativeStats = ProjectionCumulativeStats{};
  mCumulativeTotalStats = RateSnapshot{};
  ++mControllerInputRevision;

  estimators_update_loop_micro_sec.reset();
  reservation_controller_update_loop_micro_sec.reset();
  fst_limits_update_loop_micro_sec.reset();
  fst_reports_processed_per_second.reset();
}

void
TrafficShapingManager::ClearRuntimeStats()
{
  std::lock_guard publish_lock(mFstConfigPublishMutex);
  std::unique_lock lock(mMutex);
  mNodeStates.clear();
  mFstStreamStateCount = 0;
  mFstStreamStateEstimatedBytes = 0;
  mGlobalStats.clear();
  mNodeStats.clear();
  mDiskStats.clear();
  mDetailedStats.clear();
  mNodeEntityStats.clear();
  mTotalStats.clear();
  mNodeReservationControllers.clear();
  mNodeAppDelayStates.clear();
  mNodeFstIoDelayConfigs.clear();
  mPublishedFstIoDelayConfigs.clear();
  mGlobalCumulativeStats.clear();
  mNodeCumulativeStats.clear();
  mDiskCumulativeStats.clear();
  mDetailedCumulativeStats.clear();
  mProjectionCumulativeStats = ProjectionCumulativeStats{};
  mCumulativeTotalStats = RateSnapshot{};
  ++mControllerInputRevision;
}

void
TrafficShapingManager::ClearDetailedRuntimeStats()
{
  std::unique_lock lock(mMutex);
  mDiskStats.clear();
  mDetailedStats.clear();
  mDiskCumulativeStats.clear();
  mDetailedCumulativeStats.clear();
}

RateSnapshot
TrafficShapingManager::GetTotalStats() const
{
  std::shared_lock lock(mMutex);
  RateSnapshot snap;
  snap.last_activity_time = mTotalStats.last_activity_time;
  snap.ema = mTotalStats.ema;
  snap.sma = mTotalStats.sma;
  return snap;
}

std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash>
TrafficShapingManager::GetGlobalCumulativeStats() const
{
  std::shared_lock lock(mMutex);
  return mGlobalCumulativeStats;
}

std::unordered_map<std::string, RateSnapshot>
TrafficShapingManager::GetNodeCumulativeStats() const
{
  std::shared_lock lock(mMutex);
  return mNodeCumulativeStats;
}

std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash>
TrafficShapingManager::GetDiskCumulativeStats() const
{
  std::shared_lock lock(mMutex);
  return mDiskCumulativeStats;
}

std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash>
TrafficShapingManager::GetDetailedCumulativeStats() const
{
  std::shared_lock lock(mMutex);
  return mDetailedCumulativeStats;
}

ProjectionCumulativeStats
TrafficShapingManager::GetProjectionCumulativeStats() const
{
  std::shared_lock lock(mMutex);
  return mProjectionCumulativeStats;
}

RateSnapshot
TrafficShapingManager::GetTotalCumulativeStats() const
{
  std::shared_lock lock(mMutex);
  return mCumulativeTotalStats;
}

TrafficShapingEngine::TrafficShapingEngine()
    : mRunning(false)
    , mEstimatorsUpdateThreadPeriodMilliseconds(200)
    , mFstIoPolicyUpdateThreadPeriodMilliseconds(200)
    , mFstIoStatsReportThreadPeriodMilliseconds(
          eos::common::TRAFFIC_SHAPING_FST_IO_STATS_REPORT_PERIOD_DEFAULT_MS)
    , mSystemStatsWindowSeconds(15)
    , mFilesystemDetailEnabled(false)
    , mLimitsEnabled(true)
    , mReservationsEnabled(true)
    , mControllerMinLimitBps(kDefaultControllerMinLimitBps)
    , mIoPressureThreshold(kDefaultIoPressureThreshold)
    , mGarbageCollectionIdleSeconds(kDefaultGarbageCollectionIdleSec)
{
  mManager = std::make_shared<TrafficShapingManager>();
}

TrafficShapingEngine::~TrafficShapingEngine() { Stop(); }

bool
TrafficShapingEngine::ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                                        uint32_t win_s,
                                        bool* applied_successfully) noexcept
try {
  if (applied_successfully != nullptr) {
    *applied_successfully = false;
  }
  std::lock_guard config_lock(mThreadConfigMutex);

  if (est_ms < kMinThreadPeriodMs) {
    est_ms = kMinThreadPeriodMs;
  } else if (est_ms > kMaxThreadPeriodMs) {
    est_ms = kMaxThreadPeriodMs;
  }

  if (pol_ms < kMinThreadPeriodMs) {
    pol_ms = kMinThreadPeriodMs;
  } else if (pol_ms > kMaxThreadPeriodMs) {
    pol_ms = kMaxThreadPeriodMs;
  }

  if (rep_ms < kMinThreadPeriodMs) {
    rep_ms = kMinThreadPeriodMs;
  } else if (rep_ms > kMaxThreadPeriodMs) {
    rep_ms = kMaxThreadPeriodMs;
  }

  if (win_s < kMinSystemStatsWindowSec) {
    win_s = kMinSystemStatsWindowSec;
  } else if (win_s > kMaxSystemStatsWindowSec) {
    win_s = kMaxSystemStatsWindowSec;
  }

  bool changed = false;
  if (mEstimatorsUpdateThreadPeriodMilliseconds.load() != est_ms) {
    changed = true;
  }
  if (mFstIoPolicyUpdateThreadPeriodMilliseconds.load() != pol_ms) {
    changed = true;
  }
  if (mFstIoStatsReportThreadPeriodMilliseconds.load() != rep_ms) {
    changed = true;
  }
  if (mSystemStatsWindowSeconds.load() != win_s) {
    changed = true;
  }

  if (mManager != nullptr &&
      !mManager->ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s)) {
    return false;
  }

  mEstimatorsUpdateThreadPeriodMilliseconds = est_ms;
  mFstIoPolicyUpdateThreadPeriodMilliseconds = pol_ms;
  mFstIoStatsReportThreadPeriodMilliseconds = rep_ms;
  mSystemStatsWindowSeconds = win_s;
  if (applied_successfully != nullptr) {
    *applied_successfully = true;
  }

  return changed;
} catch (...) {
  return false;
}

void
TrafficShapingEngine::SetThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                                      uint32_t win_s)
{
  if (ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s)) {
    StoreThreadConfig();
    SyncTrafficShapingConfigWithFst();
  }
}

void
TrafficShapingEngine::SetEstimatorsUpdateThreadPeriodMilliseconds(
    const uint32_t period_ms)
{
  SetThreadConfig(period_ms, mFstIoPolicyUpdateThreadPeriodMilliseconds,
                  mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds);
}

void
TrafficShapingEngine::SetFstIoPolicyUpdateThreadPeriodMilliseconds(
    const uint32_t period_ms)
{
  SetThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds, period_ms,
                  mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds);
}

void
TrafficShapingEngine::SetFstIoStatsReportThreadPeriodMilliseconds(uint32_t period_ms)
{
  SetThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                  mFstIoPolicyUpdateThreadPeriodMilliseconds, period_ms,
                  mSystemStatsWindowSeconds);
}

void
TrafficShapingEngine::SetSystemStatsWindowSeconds(uint32_t window_seconds)
{
  SetThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                  mFstIoPolicyUpdateThreadPeriodMilliseconds,
                  mFstIoStatsReportThreadPeriodMilliseconds, window_seconds);
}

void
TrafficShapingEngine::SetDetailLevel(const std::string& detail_level)
{
  std::lock_guard detail_lock(mAutomaticDetailLevelMutex);
  const MapCardinalityStats cardinality =
      mManager != nullptr ? mManager->GetMapCardinalityStats() : MapCardinalityStats{};
  const bool changed = ApplyDetailLevelConfig(detail_level);
  StoreDetailLevelConfig(GetDetailLevel());
  if (changed) {
    LogDetailLevelSwitch("manual", GetDetailLevel(), cardinality);
    SyncTrafficShapingConfigWithFst();
  }
}

bool
TrafficShapingEngine::ApplyDetailLevelConfig(const std::string& detail_level)
{
  const bool fs_detail =
      detail_level == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM;
  const bool old_value =
      mFilesystemDetailEnabled.exchange(fs_detail, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetFilesystemDetailEnabled(fs_detail);
    if (old_value != fs_detail) {
      mManager->ClearRuntimeStats();
    } else if (!fs_detail) {
      mManager->ClearDetailedRuntimeStats();
    }
  }

  return old_value != fs_detail;
}

void
TrafficShapingEngine::StoreDetailLevelConfig(const std::string& detail_level)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_LEVEL_CONFIG,
                                  detail_level);
}

std::string
TrafficShapingEngine::GetDetailLevel() const
{
  return mFilesystemDetailEnabled.load(std::memory_order_relaxed)
             ? eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM
             : eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE;
}

void
TrafficShapingEngine::LogDetailLevelSwitch(
    const char* reason, const std::string& detail_level,
    const MapCardinalityStats& cardinality) const noexcept
{
  try {
    eos_static_info(
        "msg=\"Traffic Shaping detail level switch\" reason=\"%s\" "
        "detail_level=\"%s\" auto_enabled=%s auto_low_cardinality=%llu "
        "auto_high_cardinality=%llu node_states=%llu node_state_streams=%llu "
        "global_stats=%llu node_stats=%llu disk_stats=%llu detailed_stats=%llu "
        "global_cumulative_stats=%llu node_cumulative_stats=%llu "
        "disk_cumulative_stats=%llu detailed_cumulative_stats=%llu "
        "node_entity_stats=%llu app_policies=%llu uid_policies=%llu gid_policies=%llu "
        "node_fst_io_delay_configs=%llu published_fst_io_delay_configs=%llu",
        reason, detail_level.c_str(),
        mAutomaticDetailLevelEnabled.load(std::memory_order_relaxed) ? "true" : "false",
        static_cast<unsigned long long>(
            mAutomaticDetailLevelLowCardinality.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(
            mAutomaticDetailLevelHighCardinality.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(cardinality.node_states),
        static_cast<unsigned long long>(cardinality.node_state_streams),
        static_cast<unsigned long long>(cardinality.global_stats),
        static_cast<unsigned long long>(cardinality.node_stats),
        static_cast<unsigned long long>(cardinality.disk_stats),
        static_cast<unsigned long long>(cardinality.detailed_stats),
        static_cast<unsigned long long>(cardinality.global_cumulative_stats),
        static_cast<unsigned long long>(cardinality.node_cumulative_stats),
        static_cast<unsigned long long>(cardinality.disk_cumulative_stats),
        static_cast<unsigned long long>(cardinality.detailed_cumulative_stats),
        static_cast<unsigned long long>(cardinality.node_entity_stats),
        static_cast<unsigned long long>(cardinality.app_policies),
        static_cast<unsigned long long>(cardinality.uid_policies),
        static_cast<unsigned long long>(cardinality.gid_policies),
        static_cast<unsigned long long>(cardinality.node_fst_io_delay_configs),
        static_cast<unsigned long long>(cardinality.published_fst_io_delay_configs));
  } catch (...) {
  }
}

void
TrafficShapingEngine::SetAutomaticDetailLevelEnabled(const bool enabled)
{
  ApplyAutomaticDetailLevelEnabledConfig(enabled);
  StoreAutomaticDetailLevelEnabledConfig(GetAutomaticDetailLevelEnabled());

  if (enabled) {
    ApplyAutomaticDetailLevel();
  }
}

bool
TrafficShapingEngine::GetAutomaticDetailLevelEnabled() const
{
  return mAutomaticDetailLevelEnabled.load(std::memory_order_relaxed);
}

void
TrafficShapingEngine::SetAutomaticDetailLevelCardinality(const uint64_t low_cardinality,
                                                         const uint64_t high_cardinality)
{
  ApplyAutomaticDetailLevelCardinalityConfig(low_cardinality, high_cardinality);
  StoreAutomaticDetailLevelCardinalityConfig(GetAutomaticDetailLevelLowCardinality(),
                                             GetAutomaticDetailLevelHighCardinality());

  if (GetAutomaticDetailLevelEnabled()) {
    ApplyAutomaticDetailLevel();
  }
}

bool
TrafficShapingEngine::ApplyAutomaticDetailLevelEnabledConfig(const bool enabled)
{
  const bool old_value =
      mAutomaticDetailLevelEnabled.exchange(enabled, std::memory_order_relaxed);
  return old_value != enabled;
}

void
TrafficShapingEngine::StoreAutomaticDetailLevelEnabledConfig(const bool enabled)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_AUTO_CONFIG, enabled);
}

bool
TrafficShapingEngine::ApplyAutomaticDetailLevelCardinalityConfig(
    uint64_t low_cardinality, uint64_t high_cardinality)
{
  if (high_cardinality < low_cardinality) {
    high_cardinality = low_cardinality;
  }

  const uint64_t old_low = mAutomaticDetailLevelLowCardinality.exchange(
      low_cardinality, std::memory_order_relaxed);
  const uint64_t old_high = mAutomaticDetailLevelHighCardinality.exchange(
      high_cardinality, std::memory_order_relaxed);
  return old_low != low_cardinality || old_high != high_cardinality;
}

void
TrafficShapingEngine::StoreAutomaticDetailLevelCardinalityConfig(
    const uint64_t low_cardinality, const uint64_t high_cardinality)
{
  FsView::gFsView.SetGlobalConfig(
      common::TRAFFIC_SHAPING_DETAIL_AUTO_LOW_CARDINALITY_CONFIG,
      std::to_string(low_cardinality));
  FsView::gFsView.SetGlobalConfig(
      common::TRAFFIC_SHAPING_DETAIL_AUTO_HIGH_CARDINALITY_CONFIG,
      std::to_string(high_cardinality));
}

void
TrafficShapingEngine::ApplyAutomaticDetailLevel()
{
  std::lock_guard detail_lock(mAutomaticDetailLevelMutex);
  if (!mAutomaticDetailLevelEnabled.load(std::memory_order_relaxed) ||
      mManager == nullptr) {
    return;
  }

  const auto cardinality = mManager->GetMapCardinalityStats();
  const uint64_t streams = cardinality.node_state_streams;
  const bool fs_detail = mFilesystemDetailEnabled.load(std::memory_order_relaxed);
  const uint64_t low_cardinality =
      mAutomaticDetailLevelLowCardinality.load(std::memory_order_relaxed);
  const uint64_t high_cardinality =
      mAutomaticDetailLevelHighCardinality.load(std::memory_order_relaxed);
  const auto now = std::chrono::steady_clock::now();
  const auto aggregate_holdoff =
      std::chrono::seconds(mGarbageCollectionIdleSeconds.load(std::memory_order_relaxed));

  if (fs_detail && streams > high_cardinality) {
    if (ApplyDetailLevelConfig(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE)) {
      mLastAutomaticDetailLevelChange = now;
      LogDetailLevelSwitch("automatic_high_cardinality",
                           eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE,
                           cardinality);
      SyncTrafficShapingConfigWithFst();
    }
  } else if (!fs_detail && streams <= low_cardinality) {
    if (mLastAutomaticDetailLevelChange != std::chrono::steady_clock::time_point{} &&
        now - mLastAutomaticDetailLevelChange < aggregate_holdoff) {
      return;
    }

    if (ApplyDetailLevelConfig(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM)) {
      mLastAutomaticDetailLevelChange = now;
      LogDetailLevelSwitch("automatic_low_cardinality",
                           eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM,
                           cardinality);
      SyncTrafficShapingConfigWithFst();
    }
  }
}

void
TrafficShapingEngine::SetLimitsEnabled(const bool enabled)
{
  ApplyLimitsEnabledConfig(enabled);
  StoreLimitsEnabledConfig(GetLimitsEnabled());
}

bool
TrafficShapingEngine::GetLimitsEnabled() const
{
  return mLimitsEnabled.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyLimitsEnabledConfig(const bool enabled)
{
  const bool old_value = mLimitsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetLimitsEnabled(enabled);
  }

  return old_value != enabled;
}

void
TrafficShapingEngine::StoreLimitsEnabledConfig(const bool enabled)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_LIMITS_ENABLED_CONFIG, enabled);
}

void
TrafficShapingEngine::SetReservationsEnabled(const bool enabled)
{
  ApplyReservationsEnabledConfig(enabled);
  StoreReservationsEnabledConfig(GetReservationsEnabled());
}

bool
TrafficShapingEngine::GetReservationsEnabled() const
{
  return mReservationsEnabled.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyReservationsEnabledConfig(const bool enabled)
{
  const bool old_value =
      mReservationsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetReservationsEnabled(enabled);
  }

  return old_value != enabled;
}

void
TrafficShapingEngine::StoreReservationsEnabledConfig(const bool enabled)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_RESERVATIONS_ENABLED_CONFIG,
                                  enabled);
}

void
TrafficShapingEngine::SetControllerMinLimit(const uint64_t limit_bps)
{
  ApplyControllerMinLimitConfig(limit_bps);
  StoreControllerMinLimitConfig(GetControllerMinLimit());
}

uint64_t
TrafficShapingEngine::GetControllerMinLimit() const
{
  return mControllerMinLimitBps.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyControllerMinLimitConfig(const uint64_t limit_bps)
{
  const uint64_t old_value =
      mControllerMinLimitBps.exchange(limit_bps, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetControllerMinLimit(limit_bps);
  }

  return old_value != limit_bps;
}

void
TrafficShapingEngine::StoreControllerMinLimitConfig(const uint64_t limit_bps)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_CONTROLLER_MIN_LIMIT_CONFIG,
                                  std::to_string(limit_bps));
}

void
TrafficShapingEngine::SetActiveNodeRateThreshold(const uint64_t threshold_bps)
{
  ApplyActiveNodeRateThresholdConfig(threshold_bps);
  StoreActiveNodeRateThresholdConfig(GetActiveNodeRateThreshold());
}

uint64_t
TrafficShapingEngine::GetActiveNodeRateThreshold() const
{
  return mActiveNodeRateThresholdBps.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyActiveNodeRateThresholdConfig(const uint64_t threshold_bps)
{
  const uint64_t old_value =
      mActiveNodeRateThresholdBps.exchange(threshold_bps, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetActiveNodeRateThreshold(threshold_bps);
  }

  return old_value != threshold_bps;
}

void
TrafficShapingEngine::StoreActiveNodeRateThresholdConfig(const uint64_t threshold_bps)
{
  FsView::gFsView.SetGlobalConfig(
      common::TRAFFIC_SHAPING_ACTIVE_NODE_RATE_THRESHOLD_CONFIG,
      std::to_string(threshold_bps));
}

void
TrafficShapingEngine::SetIoPressureThreshold(const double threshold)
{
  ApplyIoPressureThresholdConfig(threshold);
  StoreIoPressureThresholdConfig(GetIoPressureThreshold());
}

double
TrafficShapingEngine::GetIoPressureThreshold() const
{
  return mIoPressureThreshold.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyIoPressureThresholdConfig(const double threshold)
{
  const double clamped_threshold = std::max(0.0, std::min(1.0, threshold));
  const double old_value =
      mIoPressureThreshold.exchange(clamped_threshold, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetIoPressureThreshold(clamped_threshold);
  }

  return old_value != clamped_threshold;
}

void
TrafficShapingEngine::StoreIoPressureThresholdConfig(const double threshold)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_IO_PRESSURE_THRESHOLD_CONFIG,
                                  std::to_string(threshold));
}

void
TrafficShapingEngine::SetGarbageCollectionIdleSeconds(const uint32_t idle_seconds)
{
  ApplyGarbageCollectionIdleSecondsConfig(idle_seconds);
  StoreGarbageCollectionIdleSecondsConfig(GetGarbageCollectionIdleSeconds());
}

bool
TrafficShapingEngine::ApplyGarbageCollectionIdleSecondsConfig(uint32_t idle_seconds)
{
  if (idle_seconds < kMinGarbageCollectionIdleSec) {
    idle_seconds = kMinGarbageCollectionIdleSec;
  } else if (idle_seconds > kMaxGarbageCollectionIdleSec) {
    idle_seconds = kMaxGarbageCollectionIdleSec;
  }

  const uint32_t old_value =
      mGarbageCollectionIdleSeconds.exchange(idle_seconds, std::memory_order_relaxed);
  return old_value != idle_seconds;
}

void
TrafficShapingEngine::StoreGarbageCollectionIdleSecondsConfig(const uint32_t idle_seconds)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_GARBAGE_COLLECTION_IDLE_CONFIG,
                                  std::to_string(idle_seconds));
}

void
TrafficShapingEngine::StoreThreadConfig()
{
  eos::traffic_shaping::ThreadConfig thread_loop_stats;

  thread_loop_stats.set_update_estimators_period_millis(
      mEstimatorsUpdateThreadPeriodMilliseconds);
  thread_loop_stats.set_fst_policy_update_period_millis(
      mFstIoPolicyUpdateThreadPeriodMilliseconds);
  thread_loop_stats.set_fst_io_stats_report_period_millis(
      mFstIoStatsReportThreadPeriodMilliseconds);
  thread_loop_stats.set_system_stats_time_window_seconds(mSystemStatsWindowSeconds);

  std::string serialized = thread_loop_stats.SerializeAsString();
  std::string encoded;

  if (!eos::common::SymKey::Base64(serialized, encoded)) {
    eos_static_warning("%s", "msg=\"failed to base64-encode thread periods config\"");
    return;
  }

  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_THREAD_PERIODS, encoded);
}

void
TrafficShapingEngine::ApplyConfig()
{
  const bool is_enabled =
      FsView::gFsView.GetBoolGlobalConfig(common::TRAFFIC_SHAPING_ENABLE_CONFIG);
  eos_static_info("msg=\"Applying Traffic Shaping Config\" enabled=%s",
                  is_enabled ? "true" : "false");
  if (!ApplyEnabledConfig(is_enabled)) {
    eos_static_err("msg=\"Failed to apply Traffic Shaping enabled config\" enabled=%s",
                   is_enabled ? "true" : "false");
  }

  const std::string config =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG);

  if (const auto manager = GetManager(); manager != nullptr) {
    const bool result = manager->LoadPoliciesFromString(config);
    if (!result) {
      eos_static_err("%s", "msg=\"Failed to load Traffic Shaping policies from config\"");
    }
  }

  uint32_t est_ms = mEstimatorsUpdateThreadPeriodMilliseconds;
  uint32_t pol_ms = mFstIoPolicyUpdateThreadPeriodMilliseconds;
  uint32_t rep_ms = mFstIoStatsReportThreadPeriodMilliseconds;
  uint32_t win_s = mSystemStatsWindowSeconds;

  const std::string thread_periods =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_THREAD_PERIODS);
  if (!thread_periods.empty()) {
    try {
      std::string serialized_thread_periods;

      if (!eos::common::SymKey::DeBase64(thread_periods, serialized_thread_periods)) {
        eos_static_err("%s", "msg=\"failed to base64-decode thread periods config\"");
        serialized_thread_periods.clear();
      }

      eos::traffic_shaping::ThreadConfig thread_config;

      if (!serialized_thread_periods.empty() &&
          thread_config.ParseFromString(serialized_thread_periods)) {
        est_ms = thread_config.update_estimators_period_millis();
        pol_ms = thread_config.fst_policy_update_period_millis();
        rep_ms = thread_config.fst_io_stats_report_period_millis();
        win_s = thread_config.system_stats_time_window_seconds();
      } else if (!serialized_thread_periods.empty()) {
        eos_static_err("%s", "msg=\"failed to parse thread periods config\"");
      }
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse thread periods config\" error=%s", e.what());
    }
  }

  ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s);

  const std::string detail_level =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_LEVEL_CONFIG);

  if (detail_level.empty()) {
    ApplyDetailLevelConfig(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE);
  } else {
    ApplyDetailLevelConfig(detail_level);
  }

  const std::string detail_auto =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_AUTO_CONFIG);
  ApplyAutomaticDetailLevelEnabledConfig(detail_auto.empty() || detail_auto == "true");

  uint64_t detail_auto_low = kDefaultAutomaticFilesystemDetailLowCardinality;
  uint64_t detail_auto_high = kDefaultAutomaticFilesystemDetailHighCardinality;

  const std::string configured_detail_auto_low = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_DETAIL_AUTO_LOW_CARDINALITY_CONFIG);
  if (!configured_detail_auto_low.empty()) {
    try {
      detail_auto_low = std::stoull(configured_detail_auto_low);
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping automatic detail low "
                     "cardinality\" value=\"%s\" error=\"%s\"",
                     configured_detail_auto_low.c_str(), e.what());
    }
  }

  const std::string configured_detail_auto_high = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_DETAIL_AUTO_HIGH_CARDINALITY_CONFIG);
  if (!configured_detail_auto_high.empty()) {
    try {
      detail_auto_high = std::stoull(configured_detail_auto_high);
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping automatic detail high "
                     "cardinality\" value=\"%s\" error=\"%s\"",
                     configured_detail_auto_high.c_str(), e.what());
    }
  }

  ApplyAutomaticDetailLevelCardinalityConfig(detail_auto_low, detail_auto_high);

  const std::string limits_enabled =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_LIMITS_ENABLED_CONFIG);
  ApplyLimitsEnabledConfig(limits_enabled.empty() || limits_enabled == "true");

  const std::string reservations_enabled = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_RESERVATIONS_ENABLED_CONFIG);
  ApplyReservationsEnabledConfig(reservations_enabled.empty() ||
                                 reservations_enabled == "true");

  const std::string controller_min_limit = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_CONTROLLER_MIN_LIMIT_CONFIG);
  if (controller_min_limit.empty()) {
    ApplyControllerMinLimitConfig(kDefaultControllerMinLimitBps);
  } else {
    try {
      ApplyControllerMinLimitConfig(std::stoull(controller_min_limit));
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping controller minimum limit\" "
                     "value=\"%s\" error=\"%s\"",
                     controller_min_limit.c_str(), e.what());
      ApplyControllerMinLimitConfig(kDefaultControllerMinLimitBps);
    }
  }

  const std::string active_node_rate_threshold = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_ACTIVE_NODE_RATE_THRESHOLD_CONFIG);
  if (active_node_rate_threshold.empty()) {
    ApplyActiveNodeRateThresholdConfig(kDefaultActiveNodeRateThresholdBps);
  } else {
    try {
      ApplyActiveNodeRateThresholdConfig(std::stoull(active_node_rate_threshold));
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping active node rate "
                     "threshold\" value=\"%s\" error=\"%s\"",
                     active_node_rate_threshold.c_str(), e.what());
      ApplyActiveNodeRateThresholdConfig(kDefaultActiveNodeRateThresholdBps);
    }
  }

  const std::string io_pressure_threshold = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_IO_PRESSURE_THRESHOLD_CONFIG);
  if (io_pressure_threshold.empty()) {
    ApplyIoPressureThresholdConfig(kDefaultIoPressureThreshold);
  } else {
    try {
      ApplyIoPressureThresholdConfig(std::stod(io_pressure_threshold));
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping IO pressure threshold\" "
                     "value=\"%s\" error=\"%s\"",
                     io_pressure_threshold.c_str(), e.what());
      ApplyIoPressureThresholdConfig(kDefaultIoPressureThreshold);
    }
  }

  const std::string garbage_collection_idle = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_GARBAGE_COLLECTION_IDLE_CONFIG);
  if (garbage_collection_idle.empty()) {
    ApplyGarbageCollectionIdleSecondsConfig(kDefaultGarbageCollectionIdleSec);
  } else {
    try {
      ApplyGarbageCollectionIdleSecondsConfig(std::stoul(garbage_collection_idle));
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping garbage collection idle "
                     "seconds\" value=\"%s\" error=\"%s\"",
                     garbage_collection_idle.c_str(), e.what());
      ApplyGarbageCollectionIdleSecondsConfig(kDefaultGarbageCollectionIdleSec);
    }
  }

  ApplyAutomaticDetailLevel();

  if (!EnsureFstEnabledSyncThread()) {
    eos_static_err("%s", "msg=\"Failed to start Traffic Shaping FST sync thread\"");
  }
}

bool
TrafficShapingEngine::Start() noexcept
{
  try {
    std::unique_lock lifecycle_lock(mRuntimeLifecycleMutex);
    if (mRunning.load(std::memory_order_acquire)) {
      return true;
    }
    if (mEstimatorsUpdateThread != nullptr) {
      mEstimatorsUpdateThread->join();
      mEstimatorsUpdateThread.reset();
    }
    if (mFstIoPolicyUpdateThread != nullptr) {
      mFstIoPolicyUpdateThread->join();
      mFstIoPolicyUpdateThread.reset();
    }

    try {
      auto estimators_thread =
          std::make_unique<AssistedThread>(&TrafficShapingEngine::EstimatorsUpdate, this);
      estimators_thread->setName("Traffic Shaping Estimators Update");

      auto policy_thread = std::make_unique<AssistedThread>(
          &TrafficShapingEngine::FstIoPolicyUpdate, this);
      policy_thread->setName("Traffic Shaping FST Policy Update");

      bool config_applied = false;
      ApplyThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                        mFstIoPolicyUpdateThreadPeriodMilliseconds,
                        mFstIoStatsReportThreadPeriodMilliseconds,
                        mSystemStatsWindowSeconds, &config_applied);
      if (!config_applied) {
        throw std::runtime_error("traffic shaping thread configuration failed");
      }
      mEstimatorsUpdateThread = std::move(estimators_thread);
      mFstIoPolicyUpdateThread = std::move(policy_thread);
      mRunning.store(true, std::memory_order_release);
    } catch (...) {
      mRunning.store(false, std::memory_order_release);
      mEstimatorsUpdateThread.reset();
      mFstIoPolicyUpdateThread.reset();
      throw;
    }

    // NOTE: Do NOT call SyncTrafficShapingEnabledWithFst() here. Start() can be
    // invoked while config replay is in progress; the background sync thread
    // publishes the enabled state after the FsView write lock is released.
    try {
      eos_static_info("msg=\"Traffic Shaping Engine Started\"");
    } catch (...) {
    }
    return true;
  } catch (const std::exception& error) {
    try {
      eos_static_err("msg=\"Traffic Shaping Engine failed to start\" error=\"%s\"",
                     error.what());
    } catch (...) {
    }
    return false;
  } catch (...) {
    try {
      eos_static_err("%s", "msg=\"Traffic Shaping Engine failed to start due to "
                           "unknown exception\"");
    } catch (...) {
    }
    return false;
  }
}

void
TrafficShapingEngine::Stop() noexcept
{
  StopRuntime();
  try {
    StopFstEnabledSyncThread();
  } catch (const std::exception& error) {
    try {
      eos_static_err("msg=\"Traffic Shaping FST sync thread failed to stop\" "
                     "error=\"%s\"",
                     error.what());
    } catch (...) {
    }
  } catch (...) {
    try {
      eos_static_err("%s", "msg=\"Traffic Shaping FST sync thread failed to stop due "
                           "to unknown exception\"");
    } catch (...) {
    }
  }
}

bool
TrafficShapingEngine::StopRuntime() noexcept
{
  try {
    std::unique_lock lifecycle_lock(mRuntimeLifecycleMutex);
    if (!mRunning.load(std::memory_order_acquire) && mEstimatorsUpdateThread == nullptr &&
        mFstIoPolicyUpdateThread == nullptr) {
      return true;
    }
    mRunning.store(false, std::memory_order_release);

    bool threads_stopped = true;
    try {
      if (mEstimatorsUpdateThread != nullptr) {
        mEstimatorsUpdateThread->join();
      }
    } catch (...) {
      threads_stopped = false;
    }
    try {
      if (mFstIoPolicyUpdateThread != nullptr) {
        mFstIoPolicyUpdateThread->join();
      }
    } catch (...) {
      threads_stopped = false;
    }
    if (!threads_stopped) {
      throw std::runtime_error("one or more traffic shaping threads failed to join");
    }
    mEstimatorsUpdateThread.reset();
    mFstIoPolicyUpdateThread.reset();

    {
      std::lock_guard lock(mReportQueueMutex);
      mReportQueue.clear();
      mReportQueueEstimatedBytes = 0;
    }
    mReportProcessingInProgress.store(false, std::memory_order_relaxed);
    mLastReportQueueWarningMonotonicNs.store(0, std::memory_order_relaxed);

    if (mManager != nullptr) {
      mManager->UpdateFstReportQueueStats(0, 0, 0);
      mManager->Clear();
    }

    // NOTE: Do NOT call SyncTrafficShapingEnabledWithFst() here for the same
    // reason as in Start(): config replay synchronizes after releasing ViewMutex.
    try {
      eos_static_info("msg=\"Traffic Shaping Engine Stopped\"");
    } catch (...) {
    }
    return true;
  } catch (const std::exception& error) {
    try {
      eos_static_err("msg=\"Traffic Shaping Engine stop was incomplete\" error=\"%s\"",
                     error.what());
    } catch (...) {
    }
    return false;
  } catch (...) {
    try {
      eos_static_err("%s", "msg=\"Traffic Shaping Engine stop was incomplete due to "
                           "unknown exception\"");
    } catch (...) {
    }
    return false;
  }
}

std::shared_ptr<TrafficShapingManager>
TrafficShapingEngine::GetManager() const
{
  return mManager;
}

void
TrafficShapingEngine::ProcessSerializedFstIoReportNonBlocking(
    const std::string& serialized_report) noexcept
try {
  if (!mRunning) {
    return;
  }

  if (serialized_report.size() > kMaxSerializedFstIoReportBytes) {
    RecordRejectedFstReport();
    if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
      eos_static_warning(
          "msg=\"Rejecting oversized Traffic Shaping FST report\" bytes=%zu "
          "max_bytes=%zu",
          serialized_report.size(), kMaxSerializedFstIoReportBytes);
    }
    return;
  }

  if (mReportProcessingInProgress.load(std::memory_order_acquire) ||
      (mManager != nullptr &&
       (mManager->GetFstReportQueueDepth() >= kReportQueueHighWater ||
        mManager->GetFstReportQueueEstimatedBytes() >=
            kReportQueueEstimatedBytesHighWater))) {
    RecordRejectedFstReport();
    if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
      eos_static_warning("%s", "msg=\"Dropping Traffic Shaping FST report before "
                               "parse while report processing is overloaded\"");
    }
    return;
  }

  if (!ValidateFstIoReportWire(serialized_report)) {
    RecordRejectedFstReport();
    if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
      eos_static_warning("%s", "msg=\"Rejecting Traffic Shaping FST report during "
                               "allocation-free wire preflight\"");
    }
    return;
  }

  eos::traffic_shaping::FstIoReport report;
  if (!report.ParseFromString(serialized_report)) {
    RecordRejectedFstReport();
    if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
      eos_static_warning("%s", "msg=\"failed to parse FstIoReport from string\"");
    }
    return;
  }
  report.DiscardUnknownFields();
  AddReportToQueue(std::move(report));
} catch (const std::exception& error) {
  RecordRejectedFstReport();
  if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
    try {
      eos_static_err("msg=\"Traffic Shaping FST report parsing aborted by exception\" "
                     "error=\"%s\"",
                     error.what());
    } catch (...) {
    }
  }
} catch (...) {
  RecordRejectedFstReport();
  if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
    try {
      eos_static_err("%s", "msg=\"Traffic Shaping FST report parsing aborted by "
                           "unknown exception\"");
    } catch (...) {
    }
  }
}

void
TrafficShapingEngine::RejectFstIoReportNonBlocking(const char* reason) noexcept
{
  RecordRejectedFstReport();
  if (!ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
    return;
  }
  try {
    eos_static_warning("msg=\"Rejecting Traffic Shaping FST report before parsing\" "
                       "reason=\"%s\"",
                       reason == nullptr ? "unspecified" : reason);
  } catch (...) {
    // Rejection accounting must never depend on telemetry allocation.
  }
}

void
TrafficShapingEngine::RecordRejectedFstReport() noexcept
{
  try {
    std::lock_guard lock(mReportQueueMutex);
    if (mManager != nullptr) {
      mManager->UpdateFstReportQueueStats(mReportQueue.size(), mReportQueueEstimatedBytes,
                                          1);
    }
  } catch (...) {
    // Queue accounting is atomic and best effort if even mutex acquisition
    // fails.
    if (mManager != nullptr) {
      mManager->UpdateFstReportQueueStats(0, 0, 1);
    }
  }
}

void
TrafficShapingEngine::AddReportToQueue(eos::traffic_shaping::FstIoReport report) noexcept
try {
  bool report_valid = report.node_id().size() <= kMaxFstReportIdentityBytes &&
                      static_cast<size_t>(report.entries_size()) <= kMaxFstReportEntries;
  if (report_valid) {
    for (const auto& entry : report.entries()) {
      if (entry.app_name().size() > kMaxFstReportIdentityBytes) {
        report_valid = false;
        break;
      }
    }
  }
  const size_t serialized_bytes = report_valid ? report.ByteSizeLong() : 0;
  report_valid &= serialized_bytes <= kMaxSerializedFstIoReportBytes;
  const size_t estimated_bytes =
      report_valid ? serialized_bytes + static_cast<size_t>(report.entries_size()) *
                                            kEstimatedFstReportEntryOverheadBytes
                   : 0;
  report_valid &= estimated_bytes <= kMaxQueuedReportEstimatedBytes;

  uint64_t dropped = 0;
  bool emit_warning = false;
  size_t queue_depth = 0;
  size_t queue_bytes = 0;
  {
    std::lock_guard lock(mReportQueueMutex);
    // StopRuntime clears the queue under this same lock. Recheck the runtime
    // state here so a report parsed concurrently with shutdown cannot enqueue
    // after that clear and leave stale work or queue metrics behind.
    if (!mRunning.load(std::memory_order_acquire) ||
        mReportProcessingInProgress.load(std::memory_order_acquire)) {
      report_valid = false;
    }
    if (!report_valid) {
      dropped = 1;
    } else {
      // Enqueue before eviction so an allocation failure cannot discard
      // already-accepted reports and then lose their drop accounting.
      mReportQueue.push_back({std::move(report), estimated_bytes});
      mReportQueueEstimatedBytes += estimated_bytes;
      while (mReportQueue.size() > kMaxQueuedReports ||
             mReportQueueEstimatedBytes > kMaxQueuedReportEstimatedBytes) {
        mReportQueueEstimatedBytes -=
            std::min(mReportQueueEstimatedBytes, mReportQueue.front().estimated_bytes);
        mReportQueue.pop_front();
        ++dropped;
      }
    }
    const bool queue_overloaded =
        dropped > 0 || mReportQueue.size() >= kReportQueueHighWater ||
        mReportQueueEstimatedBytes >= kReportQueueEstimatedBytesHighWater;
    const uint64_t now_ns = queue_overloaded ? MonotonicNowNs() : 0;
    if (dropped > 0) {
      uint64_t previous_warning_ns =
          mLastReportQueueWarningMonotonicNs.load(std::memory_order_relaxed);
      const uint64_t warning_interval_ns =
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    kReportQueueWarningInterval)
                                    .count());
      if ((previous_warning_ns == 0 ||
           (now_ns >= previous_warning_ns &&
            now_ns - previous_warning_ns >= warning_interval_ns)) &&
          mLastReportQueueWarningMonotonicNs.compare_exchange_strong(
              previous_warning_ns, now_ns, std::memory_order_relaxed)) {
        emit_warning = true;
      }
    }
    queue_depth = mReportQueue.size();
    queue_bytes = mReportQueueEstimatedBytes;
    if (mManager != nullptr) {
      mManager->UpdateFstReportQueueStats(queue_depth, queue_bytes, dropped);
    }
  }
  if (emit_warning) {
    eos_static_warning("msg=\"Traffic Shaping FST report rejected or evicted by queue "
                       "safety bounds\" dropped=%llu size=%zu estimated_bytes=%zu",
                       static_cast<unsigned long long>(dropped), queue_depth,
                       queue_bytes);
  }
} catch (const std::exception& error) {
  RecordRejectedFstReport();
  if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
    try {
      eos_static_err(
          "msg=\"Traffic Shaping FST report enqueue aborted by exception; report "
          "dropped\" error=\"%s\"",
          error.what());
    } catch (...) {
    }
  }
} catch (...) {
  RecordRejectedFstReport();
  if (ShouldEmitRateLimitedWarning(mLastReportQueueWarningMonotonicNs)) {
    try {
      eos_static_err("%s", "msg=\"Traffic Shaping FST report enqueue aborted by "
                           "unknown exception; report dropped\"");
    } catch (...) {
    }
  }
}

void
TrafficShapingEngine::ProcessAllQueuedReports()
{
  std::deque<QueuedFstIoReport> local_queue;
  {
    std::lock_guard lock(mReportQueueMutex);
    std::swap(mReportQueue, local_queue);
    mReportQueueEstimatedBytes = 0;
    if (local_queue.empty()) {
      if (mManager != nullptr) {
        mManager->UpdateFstReportQueueStats(0, 0, 0);
      }
      return;
    }
    mReportProcessingInProgress.store(true, std::memory_order_release);
    if (mManager != nullptr) {
      mManager->UpdateFstReportQueueStats(0, 0, 0);
    }
  }

  struct ProcessingGuard {
    std::shared_ptr<TrafficShapingManager> manager;
    std::mutex* queue_mutex;
    std::deque<QueuedFstIoReport>* queue;
    size_t* queue_estimated_bytes;
    std::atomic<bool>* processing;
    ~ProcessingGuard() noexcept
    {
      try {
        std::lock_guard lock(*queue_mutex);
        if (manager != nullptr) {
          manager->UpdateFstReportQueueStats(queue->size(), *queue_estimated_bytes, 0);
        }
      } catch (...) {
      }
      processing->store(false, std::memory_order_release);
    }
  } processing_guard{mManager, &mReportQueueMutex, &mReportQueue,
                     &mReportQueueEstimatedBytes, &mReportProcessingInProgress};

  if (mManager == nullptr) {
    return;
  }

  for (const auto& queued_report : local_queue) {
    mManager->ProcessReport(queued_report.report);
  }

  mManager->UpdateFstReportsProcessed(local_queue.size());
}

void
TrafficShapingEngine::EstimatorsUpdate(ThreadAssistant& assistant)
try {
  try {
    eos_static_info("%s", "msg=\"Starting Traffic Shaping estimators update thread\"");
  } catch (...) {
  }

  auto last_run = std::chrono::steady_clock::now();
  auto last_garbage_collection = last_run;
  constexpr auto garbage_collection_period = std::chrono::seconds(20);

  while (!assistant.terminationRequested()) {
    assistant.wait_for(
        std::chrono::milliseconds(mEstimatorsUpdateThreadPeriodMilliseconds));

    if (assistant.terminationRequested()) {
      break;
    }

    try {
      auto work_start = std::chrono::steady_clock::now();
      ProcessAllQueuedReports();
      auto reports_done = std::chrono::steady_clock::now();

      const std::chrono::duration<double> elapsed = reports_done - last_run;
      const double time_delta_seconds = elapsed.count();
      last_run = reports_done;

      mManager->UpdateEstimators(time_delta_seconds);
      auto estimators_done = std::chrono::steady_clock::now();

      ApplyAutomaticDetailLevel();

      if (estimators_done - last_garbage_collection >= garbage_collection_period) {
        last_garbage_collection = estimators_done;
        const uint32_t garbage_collection_idle_seconds =
            mGarbageCollectionIdleSeconds.load(std::memory_order_relaxed);
        const auto [removed_nodes, removed_node_streams, removed_global_streams,
                    removed_disk_stats, removed_detailed_stats] =
            mManager->GarbageCollect(garbage_collection_idle_seconds);

        if (removed_node_streams > 0 || removed_global_streams > 0 ||
            removed_disk_stats > 0 || removed_detailed_stats > 0) {
          eos_static_debug("msg=\"Traffic Shaping Garbage Collection\" removed_nodes=%lu "
                           "removed_node_streams=%lu "
                           "removed_global_streams=%lu removed_disk_stats=%lu "
                           "removed_detailed_stats=%lu",
                           removed_nodes, removed_node_streams, removed_global_streams,
                           removed_disk_stats, removed_detailed_stats);
        }
      }

      auto work_done = std::chrono::steady_clock::now();
      const auto work_duration_micro_sec =
          std::chrono::duration_cast<std::chrono::microseconds>(work_done - work_start)
              .count();
      const auto report_duration_micro_sec =
          std::chrono::duration_cast<std::chrono::microseconds>(reports_done - work_start)
              .count();
      const auto estimator_duration_micro_sec =
          std::chrono::duration_cast<std::chrono::microseconds>(estimators_done -
                                                                reports_done)
              .count();
      const auto gc_duration_micro_sec =
          std::chrono::duration_cast<std::chrono::microseconds>(work_done -
                                                                estimators_done)
              .count();

      if (static_cast<double>(work_duration_micro_sec) >
          static_cast<double>(mEstimatorsUpdateThreadPeriodMilliseconds) * 0.5 * 1000.0) {
        eos_static_debug(
            "msg=\"Traffic Shaping Estimators Update loop is slow\" total_ms=%.2f "
            "reports_ms=%.2f estimators_ms=%.2f gc_ms=%.2f",
            static_cast<double>(work_duration_micro_sec) / 1000.0,
            static_cast<double>(report_duration_micro_sec) / 1000.0,
            static_cast<double>(estimator_duration_micro_sec) / 1000.0,
            static_cast<double>(gc_duration_micro_sec) / 1000.0);
      }

      mManager->UpdateEstimatorsLoopMicroSec(work_duration_micro_sec);
    } catch (const std::exception& error) {
      try {
        eos_static_err("msg=\"Traffic Shaping estimators iteration aborted by exception; "
                       "continuing\" error=\"%s\"",
                       error.what());
      } catch (...) {
      }
    } catch (...) {
      try {
        eos_static_err("%s", "msg=\"Traffic Shaping estimators iteration aborted by "
                             "unknown exception; continuing\"");
      } catch (...) {
      }
    }
  }
} catch (const std::exception& error) {
  try {
    eos_static_err("msg=\"Traffic Shaping estimators thread exited after exception\" "
                   "error=\"%s\"",
                   error.what());
  } catch (...) {
  }
} catch (...) {
  try {
    eos_static_err("%s", "msg=\"Traffic Shaping estimators thread exited after unknown "
                         "exception\"");
  } catch (...) {
  }
}

void
TrafficShapingEngine::FstIoPolicyUpdate(ThreadAssistant& assistant) const
try {
  try {
    eos_static_info("%s", "msg=\"Starting FstIoPolicyUpdate thread\"");
  } catch (...) {
  }
  auto last_controller_update = std::chrono::steady_clock::time_point{};

  while (!assistant.terminationRequested()) {
    assistant.wait_for(
        std::chrono::milliseconds(mFstIoPolicyUpdateThreadPeriodMilliseconds));

    if (assistant.terminationRequested()) {
      break;
    }

    const auto iteration_start_time = std::chrono::steady_clock::now();
    const bool controller_due =
        last_controller_update == std::chrono::steady_clock::time_point{} ||
        iteration_start_time - last_controller_update >= kControllerUpdateInterval;

    try {
      const auto work_start_time = iteration_start_time;
      const bool run_controller = controller_due;
      std::vector<std::string> online_nodes;
      const auto io_pressure = CollectIoPressure(&online_nodes, false);
      const auto pressure_done_time = std::chrono::steady_clock::now();

      if (run_controller) {
        mManager->UpdateTrafficShapingController(io_pressure.nodes);
      }
      const auto controller_done_time = std::chrono::steady_clock::now();
      if (run_controller) {
        last_controller_update = controller_done_time;
      }

      mManager->UpdateLimits(io_pressure.nodes, online_nodes);

      auto work_end_time = std::chrono::steady_clock::now();
      const auto compute_duration_us =
          std::chrono::duration_cast<std::chrono::microseconds>(work_end_time -
                                                                work_start_time)
              .count();
      const auto pressure_duration_us =
          std::chrono::duration_cast<std::chrono::microseconds>(pressure_done_time -
                                                                work_start_time)
              .count();
      const auto controller_duration_us =
          std::chrono::duration_cast<std::chrono::microseconds>(controller_done_time -
                                                                pressure_done_time)
              .count();
      const auto limits_duration_us =
          std::chrono::duration_cast<std::chrono::microseconds>(work_end_time -
                                                                controller_done_time)
              .count();

      if (static_cast<double>(compute_duration_us) >
          static_cast<double>(mFstIoPolicyUpdateThreadPeriodMilliseconds) * 0.5 *
              1000.0) {
        eos_static_warning(
            "msg=\"Traffic Shaping FST policy update loop is slow\" total_ms=%.2f "
            "pressure_ms=%.2f controller_ms=%.2f limits_ms=%.2f controller_ran=%d",
            static_cast<double>(compute_duration_us) / 1000.0,
            static_cast<double>(pressure_duration_us) / 1000.0,
            static_cast<double>(controller_duration_us) / 1000.0,
            static_cast<double>(limits_duration_us) / 1000.0, run_controller);
      }

      mManager->UpdateReservationControllerLoopMicroSec(controller_duration_us);
      mManager->UpdateFstLimitsLoopMicroSec(limits_duration_us);
    } catch (const std::exception& error) {
      if (controller_due) {
        // Avoid hammering a failing allocation/collection path every policy
        // tick. The normal controller cadence will retry it.
        last_controller_update = std::chrono::steady_clock::now();
      }
      try {
        eos_static_err("msg=\"Traffic Shaping FST policy iteration aborted by exception; "
                       "continuing\" error=\"%s\"",
                       error.what());
      } catch (...) {
      }
    } catch (...) {
      if (controller_due) {
        last_controller_update = std::chrono::steady_clock::now();
      }
      try {
        eos_static_err("%s", "msg=\"Traffic Shaping FST policy iteration aborted by "
                             "unknown exception; continuing\"");
      } catch (...) {
      }
    }
  }
} catch (const std::exception& error) {
  try {
    eos_static_err("msg=\"Traffic Shaping FST policy thread exited after exception\" "
                   "error=\"%s\"",
                   error.what());
  } catch (...) {
  }
} catch (...) {
  try {
    eos_static_err("%s", "msg=\"Traffic Shaping FST policy thread exited after unknown "
                         "exception\"");
  } catch (...) {
  }
}

void
TrafficShapingEngine::FstTrafficShapingEnabledUpdate(ThreadAssistant& assistant)
try {
  while (!assistant.terminationRequested()) {
    try {
      SyncTrafficShapingEnabledWithFst();
      SyncTrafficShapingConfigWithFst();
    } catch (const std::exception& error) {
      try {
        eos_static_err(
            "msg=\"Traffic Shaping FST configuration sync aborted by exception; "
            "continuing\" error=\"%s\"",
            error.what());
      } catch (...) {
      }
    } catch (...) {
      try {
        eos_static_err("%s", "msg=\"Traffic Shaping FST configuration sync aborted by "
                             "unknown exception; continuing\"");
      } catch (...) {
      }
    }

    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      break;
    }
  }
} catch (const std::exception& error) {
  try {
    eos_static_err("msg=\"Traffic Shaping FST sync thread exited after exception\" "
                   "error=\"%s\"",
                   error.what());
  } catch (...) {
  }
} catch (...) {
  try {
    eos_static_err("%s", "msg=\"Traffic Shaping FST sync thread exited after unknown "
                         "exception\"");
  } catch (...) {
  }
}

bool
TrafficShapingEngine::Enable() noexcept
{
  return SetEnabled(true);
}

bool
TrafficShapingEngine::Disable() noexcept
{
  return SetEnabled(false);
}

bool
TrafficShapingEngine::SetEnabled(bool enabled) noexcept
try {
  std::lock_guard operation_lock(mEnabledOperationMutex);
  const bool previous_enabled = mRunning.load(std::memory_order_acquire);
  const bool runtime_changed = previous_enabled != enabled;
  const auto rollback_runtime = [&]() noexcept {
    if (!runtime_changed || ApplyEnabledConfig(previous_enabled)) {
      return;
    }
    try {
      eos_static_err("msg=\"Failed to roll back Traffic Shaping runtime state\" "
                     "expected_enabled=%d",
                     previous_enabled);
    } catch (...) {
    }
  };

  if (runtime_changed && !ApplyEnabledConfig(enabled)) {
    rollback_runtime();
    return false;
  }
  if (!EnsureFstEnabledSyncThread()) {
    rollback_runtime();
    return false;
  }

  try {
    StoreEnabledConfig(enabled);
    return true;
  } catch (const std::exception& error) {
    try {
      eos_static_err("msg=\"Failed to persist Traffic Shaping enabled state\" "
                     "enabled=%d error=\"%s\"",
                     enabled, error.what());
    } catch (...) {
    }
  } catch (...) {
    try {
      eos_static_err("msg=\"Failed to persist Traffic Shaping enabled state\" "
                     "enabled=%d error=\"unknown exception\"",
                     enabled);
    } catch (...) {
    }
  }
  rollback_runtime();
  return false;
} catch (const std::exception& error) {
  try {
    eos_static_err("msg=\"Traffic Shaping enabled-state operation failed\" "
                   "enabled=%d error=\"%s\"",
                   enabled, error.what());
  } catch (...) {
  }
  return false;
} catch (...) {
  try {
    eos_static_err("msg=\"Traffic Shaping enabled-state operation failed\" "
                   "enabled=%d error=\"unknown exception\"",
                   enabled);
  } catch (...) {
  }
  return false;
}

bool
TrafficShapingEngine::ApplyEnabledConfig(bool enabled) noexcept
{
  if (enabled) {
    return Start();
  }
  return StopRuntime();
}

void
TrafficShapingEngine::StoreEnabledConfig(bool enabled)
{
  if (mFailNextEnabledConfigStore.exchange(false, std::memory_order_relaxed)) {
    throw std::runtime_error("injected Traffic Shaping enabled config store failure");
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_ENABLE_CONFIG, enabled);
}

bool
TrafficShapingEngine::EnsureFstEnabledSyncThread() noexcept
try {
  std::lock_guard lock(mFstEnabledSyncThreadMutex);

  if (mFstTrafficShapingEnabledUpdateThread != nullptr) {
    return true;
  }

  if (mFailNextFstEnabledSyncThreadStart.exchange(false, std::memory_order_relaxed)) {
    throw std::runtime_error("injected Traffic Shaping FST sync thread failure");
  }
  auto sync_thread = std::make_unique<AssistedThread>(
      &TrafficShapingEngine::FstTrafficShapingEnabledUpdate, this);
  try {
    sync_thread->setName("Traffic Shaping FST Enabled Update");
  } catch (...) {
    // Naming is diagnostic only; the sync worker is already running.
  }
  mFstTrafficShapingEnabledUpdateThread = std::move(sync_thread);
  return true;
} catch (const std::exception& error) {
  try {
    eos_static_err("msg=\"Traffic Shaping FST sync thread failed to start\" "
                   "error=\"%s\"",
                   error.what());
  } catch (...) {
  }
  return false;
} catch (...) {
  try {
    eos_static_err("%s", "msg=\"Traffic Shaping FST sync thread failed to start due "
                         "to unknown exception\"");
  } catch (...) {
  }
  return false;
}

void
TrafficShapingEngine::StopFstEnabledSyncThread()
{
  std::lock_guard lock(mFstEnabledSyncThreadMutex);

  if (mFstTrafficShapingEnabledUpdateThread == nullptr) {
    return;
  }

  mFstTrafficShapingEnabledUpdateThread->join();
  mFstTrafficShapingEnabledUpdateThread.reset();
}

std::vector<std::string>
TrafficShapingEngine::GetOnlineFstNodeNames() const
{
  std::vector<std::string> online_nodes;
  {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
      if (node_view->GetStatus() == "online") {
        online_nodes.push_back(node_name);
      }
    }
  }
  return online_nodes;
}

void
TrafficShapingEngine::SyncTrafficShapingEnabledWithFst()
{
  const bool enabled = mRunning;
  const std::string enabled_str = enabled ? "true" : "false";

  for (const auto& node_name : GetOnlineFstNodeNames()) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_name);
    if (it != FsView::gFsView.mNodeView.end()) {
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_ENABLE_TOGGLE,
                                  enabled_str, true);
    }
  }
}

void
TrafficShapingEngine::SyncTrafficShapingConfigWithFst()
{
  const std::string period_str =
      std::to_string(mFstIoStatsReportThreadPeriodMilliseconds);
  const std::string detail_level = GetDetailLevel();

  for (const auto& node_name : GetOnlineFstNodeNames()) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_name);
    if (it != FsView::gFsView.mNodeView.end()) {
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_STATS_THREAD_PERIOD,
                                  period_str, true);
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_DETAIL_LEVEL,
                                  detail_level, true);
    }
  }
}

void
TrafficShapingManager::SetUidPolicy(const uint32_t uid,
                                    const TrafficShapingPolicy& policy)
{
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);
  bool config_changed = false;
  std::string serialized;
  {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock state_lock(mMutex);
    const auto it = mUidPolicies.find(uid);
    TrafficShapingPolicy next_policy =
        PreparePolicyForSet(policy, it != mUidPolicies.end() ? &it->second : nullptr);
    const std::string next_policy_string =
        next_policy.IsEmpty() ? std::string{} : next_policy.ToString();
    const size_t policy_count =
        mUidPolicies.size() + mGidPolicies.size() + mAppPolicies.size();
    if (!next_policy.IsEmpty() && it == mUidPolicies.end() &&
        policy_count >= eos::common::TRAFFIC_SHAPING_POLICY_MAX_ENTITIES) {
      throw std::length_error("traffic shaping policy cardinality limit reached");
    }

    if (next_policy.IsEmpty()) {
      if (it != mUidPolicies.end()) {
        mUidPolicies.erase(it);
        ++mControllerInputRevision;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_uid_write_delay()->erase(uid);
          node_config.second.mutable_uid_read_delay()->erase(uid);
        }
        config_changed = true;
        eos_static_info("msg=\"Removed empty UID Traffic Shaping Policy\" uid=%u", uid);
      }
    } else {
      if (it == mUidPolicies.end()) {
        mUidPolicies[uid] = next_policy;
        ++mControllerInputRevision;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_uid_write_delay()->erase(uid);
          node_config.second.mutable_uid_read_delay()->erase(uid);
        }
        config_changed = true;
        eos_static_info("msg=\"Set UID Traffic Shaping Policy\" uid=%u policy=%s", uid,
                        next_policy_string.c_str());
      } else {
        const uint64_t old_write_limit = it->second.GetEffectiveWriteLimit();
        const uint64_t new_write_limit = next_policy.GetEffectiveWriteLimit();
        const uint64_t old_read_limit = it->second.GetEffectiveReadLimit();
        const uint64_t new_read_limit = next_policy.GetEffectiveReadLimit();
        const bool write_limit_changed = old_write_limit != new_write_limit;
        const bool read_limit_changed = old_read_limit != new_read_limit;
        if (PolicyRuntimeStateChanged(it->second, next_policy)) {
          ++mControllerInputRevision;
        }
        // operator!= ignores controller limits, so it only flags true user config changes
        if (it->second != next_policy) {
          config_changed = true;
        }
        // Always update in-memory to reflect any potential ephemeral controller limit
        // changes
        it->second = next_policy;
        if (write_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_uid_write_delay(), uid,
                                     old_write_limit, new_write_limit);
          }
        }
        if (read_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_uid_read_delay(), uid,
                                     old_read_limit, new_read_limit);
          }
        }
        eos_static_info("msg=\"Updated UID Traffic Shaping Policy\" uid=%u policy=%s "
                        "persistent_changed=%d",
                        uid, next_policy_string.c_str(), config_changed);
      }
    }

    if (config_changed) {
      serialized = SerializePoliciesUnlocked();
    }
  }

  if (config_changed) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  }
}

void
TrafficShapingManager::SetGidPolicy(const uint32_t gid,
                                    const TrafficShapingPolicy& policy)
{
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);
  bool config_changed = false;
  std::string serialized;
  {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock state_lock(mMutex);
    auto it = mGidPolicies.find(gid);
    TrafficShapingPolicy next_policy =
        PreparePolicyForSet(policy, it != mGidPolicies.end() ? &it->second : nullptr);
    const std::string next_policy_string =
        next_policy.IsEmpty() ? std::string{} : next_policy.ToString();
    const size_t policy_count =
        mUidPolicies.size() + mGidPolicies.size() + mAppPolicies.size();
    if (!next_policy.IsEmpty() && it == mGidPolicies.end() &&
        policy_count >= eos::common::TRAFFIC_SHAPING_POLICY_MAX_ENTITIES) {
      throw std::length_error("traffic shaping policy cardinality limit reached");
    }

    if (next_policy.IsEmpty()) {
      if (it != mGidPolicies.end()) {
        mGidPolicies.erase(it);
        ++mControllerInputRevision;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_gid_write_delay()->erase(gid);
          node_config.second.mutable_gid_read_delay()->erase(gid);
        }
        config_changed = true;
        eos_static_info("msg=\"Removed empty GID Traffic Shaping Policy\" gid=%u", gid);
      }
    } else {
      if (it == mGidPolicies.end()) {
        mGidPolicies[gid] = next_policy;
        ++mControllerInputRevision;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_gid_write_delay()->erase(gid);
          node_config.second.mutable_gid_read_delay()->erase(gid);
        }
        config_changed = true;
        eos_static_info("msg=\"Set GID Traffic Shaping Policy\" gid=%u policy=%s", gid,
                        next_policy_string.c_str());
      } else {
        const uint64_t old_write_limit = it->second.GetEffectiveWriteLimit();
        const uint64_t new_write_limit = next_policy.GetEffectiveWriteLimit();
        const uint64_t old_read_limit = it->second.GetEffectiveReadLimit();
        const uint64_t new_read_limit = next_policy.GetEffectiveReadLimit();
        const bool write_limit_changed = old_write_limit != new_write_limit;
        const bool read_limit_changed = old_read_limit != new_read_limit;
        if (PolicyRuntimeStateChanged(it->second, next_policy)) {
          ++mControllerInputRevision;
        }
        if (it->second != next_policy) {
          config_changed = true;
        }
        it->second = next_policy;
        if (write_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_gid_write_delay(), gid,
                                     old_write_limit, new_write_limit);
          }
        }
        if (read_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_gid_read_delay(), gid,
                                     old_read_limit, new_read_limit);
          }
        }
        eos_static_info("msg=\"Updated GID Traffic Shaping Policy\" gid=%u policy=%s "
                        "persistent_changed=%d",
                        gid, next_policy_string.c_str(), config_changed);
      }
    }

    if (config_changed) {
      serialized = SerializePoliciesUnlocked();
    }
  }

  if (config_changed) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  }
}

void
TrafficShapingManager::SetAppPolicy(const std::string& app,
                                    const TrafficShapingPolicy& policy)
{
  if (app.size() > eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES) {
    throw std::length_error("traffic shaping application identity is too long");
  }
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);
  bool config_changed = false;
  std::string serialized;
  {
    bool policy_state_changed = false;
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock state_lock(mMutex);
    const auto it = mAppPolicies.find(app);
    TrafficShapingPolicy next_policy =
        PreparePolicyForSet(policy, it != mAppPolicies.end() ? &it->second : nullptr);
    const std::string next_policy_string =
        next_policy.IsEmpty() ? std::string{} : next_policy.ToString();
    const size_t policy_count =
        mUidPolicies.size() + mGidPolicies.size() + mAppPolicies.size();
    if (!next_policy.IsEmpty() && it == mAppPolicies.end() &&
        policy_count >= eos::common::TRAFFIC_SHAPING_POLICY_MAX_ENTITIES) {
      throw std::length_error("traffic shaping policy cardinality limit reached");
    }

    if (next_policy.IsEmpty()) {
      if (it != mAppPolicies.end()) {
        mAppPolicies.erase(it);
        ++mControllerInputRevision;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_app_write_delay()->erase(app);
          node_config.second.mutable_app_read_delay()->erase(app);
        }
        for (auto& [_, delays] : mNodeAppDelayStates) {
          delays.global_write.erase(app);
          delays.global_read.erase(app);
        }
        config_changed = true;
        policy_state_changed = true;
        eos_static_info("msg=\"Removed empty App Traffic Shaping Policy\" app=%s",
                        app.c_str());
      }
    } else {
      if (it == mAppPolicies.end()) {
        mAppPolicies[app] = next_policy;
        ++mControllerInputRevision;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_app_write_delay()->erase(app);
          node_config.second.mutable_app_read_delay()->erase(app);
        }
        for (auto& [_, delays] : mNodeAppDelayStates) {
          delays.global_write.erase(app);
          delays.global_read.erase(app);
        }
        config_changed = true;
        policy_state_changed = true;
        eos_static_info("msg=\"Set App Traffic Shaping Policy\" app=%s policy=%s",
                        app.c_str(), next_policy_string.c_str());
      } else {
        const uint64_t old_write_limit = it->second.GetEffectiveWriteLimit();
        const uint64_t new_write_limit = next_policy.GetEffectiveWriteLimit();
        const uint64_t old_read_limit = it->second.GetEffectiveReadLimit();
        const uint64_t new_read_limit = next_policy.GetEffectiveReadLimit();
        const bool write_limit_changed = old_write_limit != new_write_limit;
        const bool read_limit_changed = old_read_limit != new_read_limit;
        policy_state_changed = PolicyRuntimeStateChanged(it->second, next_policy);
        if (policy_state_changed) {
          ++mControllerInputRevision;
        }
        if (it->second != next_policy) {
          config_changed = true;
        }
        it->second = next_policy;
        if (write_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_app_write_delay(), app,
                                     old_write_limit, new_write_limit);
          }
          for (auto& [_, delays] : mNodeAppDelayStates) {
            ScaleDelayForLimitChange(&delays.global_write, app, old_write_limit,
                                     new_write_limit);
          }
        }
        if (read_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_app_read_delay(), app,
                                     old_read_limit, new_read_limit);
          }
          for (auto& [_, delays] : mNodeAppDelayStates) {
            ScaleDelayForLimitChange(&delays.global_read, app, old_read_limit,
                                     new_read_limit);
          }
        }
        eos_static_info("msg=\"Updated App Traffic Shaping Policy\" app=%s policy=%s "
                        "persistent_changed=%d",
                        app.c_str(), next_policy_string.c_str(), config_changed);
      }
    }

    if (policy_state_changed) {
      for (auto& [_, runtime] : mNodeReservationControllers) {
        runtime.feedback = {};
        runtime.app_limits.erase(app);
      }
      for (auto& [_, delays] : mNodeAppDelayStates) {
        delays.reservation_write.erase(app);
        delays.reservation_read.erase(app);
      }
    }
    if (config_changed) {
      serialized = SerializePoliciesUnlocked();
    }
  }

  if (config_changed) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  }
}

void
TrafficShapingManager::RemoveUidPolicy(const uint32_t uid)
{
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);
  std::string serialized;
  {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock state_lock(mMutex);
    if (!mUidPolicies.erase(uid)) {
      return;
    }
    for (auto& [_, config] : mNodeFstIoDelayConfigs) {
      config.mutable_uid_write_delay()->erase(uid);
      config.mutable_uid_read_delay()->erase(uid);
    }
    ++mControllerInputRevision;
    eos_static_info("msg=\"Removed UID Traffic Shaping Policy\" uid=%u", uid);
    serialized = SerializePoliciesUnlocked();
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
}

void
TrafficShapingManager::RemoveGidPolicy(const uint32_t gid)
{
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);
  std::string serialized;
  {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock state_lock(mMutex);
    if (!mGidPolicies.erase(gid)) {
      return;
    }
    for (auto& [_, config] : mNodeFstIoDelayConfigs) {
      config.mutable_gid_write_delay()->erase(gid);
      config.mutable_gid_read_delay()->erase(gid);
    }
    ++mControllerInputRevision;
    eos_static_info("msg=\"Removed GID Traffic Shaping Policy\" gid=%u", gid);
    serialized = SerializePoliciesUnlocked();
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
}

void
TrafficShapingManager::RemoveAppPolicy(const std::string& app)
{
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);
  std::string serialized;
  {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock state_lock(mMutex);
    if (!mAppPolicies.erase(app)) {
      return;
    }
    for (auto& [_, config] : mNodeFstIoDelayConfigs) {
      config.mutable_app_write_delay()->erase(app);
      config.mutable_app_read_delay()->erase(app);
    }
    for (auto& [_, delays] : mNodeAppDelayStates) {
      delays.global_write.erase(app);
      delays.global_read.erase(app);
      delays.reservation_write.erase(app);
      delays.reservation_read.erase(app);
    }
    ++mControllerInputRevision;
    for (auto& [_, runtime] : mNodeReservationControllers) {
      runtime.feedback = {};
      runtime.app_limits.erase(app);
    }
    eos_static_info("msg=\"Removed App Traffic Shaping Policy\" app=%s", app.c_str());
    serialized = SerializePoliciesUnlocked();
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShapingManager::GetUidPolicies() const
{
  std::shared_lock lock(mMutex);
  return mUidPolicies;
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShapingManager::GetGidPolicies() const
{
  std::shared_lock lock(mMutex);
  return mGidPolicies;
}

std::unordered_map<std::string, TrafficShapingPolicy>
TrafficShapingManager::GetAppPolicies() const
{
  std::shared_lock lock(mMutex);
  return mAppPolicies;
}

std::unordered_map<std::string, AppIoPressureSnapshot>
TrafficShapingManager::GetReservedAppIoPressure() const
{
  const auto node_io_pressure = CollectIoPressure().nodes;
  const uint64_t active_node_rate_threshold_bps =
      mActiveNodeRateThresholdBps.load(std::memory_order_relaxed);
  std::unordered_map<std::string, AppIoPressureSnapshot> pressure_by_app;

  std::shared_lock lock(mMutex);

  for (const auto& [app, policy] : mAppPolicies) {
    if (policy.is_enabled && (policy.reservation_read_bytes_per_sec > 0 ||
                              policy.reservation_write_bytes_per_sec > 0)) {
      pressure_by_app[app] = {};
    }
  }

  if (pressure_by_app.empty()) {
    return pressure_by_app;
  }

  for (const auto& [node_entity_key, stats] : mNodeEntityStats) {
    const auto app_it = pressure_by_app.find(node_entity_key.stream.app);

    if (app_it == pressure_by_app.end()) {
      continue;
    }

    const auto pressure_it = node_io_pressure.find(node_entity_key.node_id);
    if (pressure_it == node_io_pressure.end()) {
      continue;
    }
    UpdateAppIoPressure(app_it->second, stats.ema, pressure_it->second,
                        active_node_rate_threshold_bps);
  }

  return pressure_by_app;
}

std::vector<AppNodeIoPressureSnapshot>
TrafficShapingManager::GetReservedAppNodeIoPressure(
    std::vector<std::string>* online_nodes_out) const
{
  std::vector<std::string> online_nodes;
  const auto node_io_pressure = CollectIoPressure(&online_nodes).nodes;
  auto snapshots = BuildReservedAppNodeIoPressure(node_io_pressure, online_nodes);
  if (online_nodes_out != nullptr) {
    *online_nodes_out = std::move(online_nodes);
  }
  return snapshots;
}

std::vector<AppNodeIoPressureSnapshot>
TrafficShapingManager::BuildReservedAppNodeIoPressure(
    const std::unordered_map<std::string, double>& node_io_pressure,
    const std::vector<std::string>& online_nodes) const
{
  struct GlobalRates {
    double fast_read_bps = 0.0;
    double fast_write_bps = 0.0;
    double stable_read_bps = 0.0;
    double stable_write_bps = 0.0;
  };
  struct ReservedAppInput {
    TrafficShapingPolicy policy;
    GlobalRates global;
  };
  struct NodeRates {
    double read_bps = 0.0;
    double write_bps = 0.0;
    uint64_t controller_read_limit_bps = 0;
    uint64_t controller_write_limit_bps = 0;
  };

  const std::unordered_set<std::string> online(online_nodes.begin(), online_nodes.end());
  std::vector<AppNodeIoPressureSnapshot> snapshots;
  auto meaningful_deficit = [](const uint64_t reservation_bps,
                               const double current_rate_bps) {
    const double deficit_bps =
        std::max(0.0, static_cast<double>(reservation_bps) - current_rate_bps);
    const double min_deficit_bps =
        std::max(kMinReservationDeficitBps,
                 static_cast<double>(reservation_bps) * kMinReservationDeficitFraction);

    return std::pair{deficit_bps, deficit_bps >= min_deficit_bps};
  };
  std::unordered_map<std::string, ReservedAppInput> reserved_apps;
  std::unordered_map<std::string, std::unordered_map<std::string, NodeRates>>
      sparse_node_rates;
  std::unordered_map<std::string, std::pair<bool, bool>> node_has_pressured_reservation;
  const bool reservations_enabled = mReservationsEnabled.load(std::memory_order_relaxed);
  const double io_pressure_threshold =
      mIoPressureThreshold.load(std::memory_order_relaxed);
  const uint64_t active_node_rate_threshold_bps =
      mActiveNodeRateThresholdBps.load(std::memory_order_relaxed);
  const time_t node_entity_snapshot_time = time(nullptr);
  int node_entity_activity_max_age_seconds = 0;
  {
    std::shared_lock lock(mMutex);
    node_entity_activity_max_age_seconds =
        NodeEntityActivityMaxAgeSeconds(mFstReportTickIntervalSec);
    for (const auto& [app, policy] : mAppPolicies) {
      if (policy.is_enabled && (policy.reservation_read_bytes_per_sec > 0 ||
                                policy.reservation_write_bytes_per_sec > 0)) {
        reserved_apps.emplace(app, ReservedAppInput{policy, {}});
      }
    }
    if (reserved_apps.empty() || online.empty()) {
      return snapshots;
    }

    for (const auto& [key, stats] : mGlobalStats) {
      const auto app_it = reserved_apps.find(key.app);
      if (app_it == reserved_apps.end()) {
        continue;
      }
      auto& rates = app_it->second.global;
      rates.fast_read_bps += SanitizeRate(stats.ema[Ema1s].read_rate_bps);
      rates.fast_write_bps += SanitizeRate(stats.ema[Ema1s].write_rate_bps);
      rates.stable_read_bps += SanitizeRate(stats.ema[Ema5s].read_rate_bps);
      rates.stable_write_bps += SanitizeRate(stats.ema[Ema5s].write_rate_bps);
    }

    for (const auto& [key, stats] : mNodeEntityStats) {
      if (online.find(key.node_id) == online.end() ||
          reserved_apps.find(key.stream.app) == reserved_apps.end()) {
        continue;
      }
      const bool recently_active =
          stats.last_activity_time > 0 &&
          (node_entity_snapshot_time < stats.last_activity_time ||
           node_entity_snapshot_time - stats.last_activity_time <=
               node_entity_activity_max_age_seconds);
      if (!recently_active) {
        continue;
      }
      auto& rates = sparse_node_rates[key.node_id][key.stream.app];
      rates.read_bps += SanitizeRate(stats.ema.read_rate_bps);
      rates.write_bps += SanitizeRate(stats.ema.write_rate_bps);
    }

    for (auto& [node_id, rates_by_app] : sparse_node_rates) {
      const auto runtime_it = mNodeReservationControllers.find(node_id);
      if (runtime_it == mNodeReservationControllers.end()) {
        continue;
      }
      for (auto& [app, rates] : rates_by_app) {
        const auto limit_it = runtime_it->second.app_limits.find(app);
        if (limit_it == runtime_it->second.app_limits.end()) {
          continue;
        }
        rates.controller_read_limit_bps = limit_it->second.read_bps;
        rates.controller_write_limit_bps = limit_it->second.write_bps;
      }
    }
  }

  for (const auto& [node_id, rates_by_app] : sparse_node_rates) {
    for (const auto& [app, rates] : rates_by_app) {
      const auto& input = reserved_apps.at(app);
      const auto& policy = input.policy;
      const double global_read_rate_bps =
          std::max(input.global.fast_read_bps, input.global.stable_read_bps);
      const double global_write_rate_bps =
          std::max(input.global.fast_write_bps, input.global.stable_write_bps);
      const uint64_t effective_read_reservation = EffectiveReservation(policy, false);
      const uint64_t effective_write_reservation = EffectiveReservation(policy, true);
      const auto [read_deficit_bps, read_deficit_active] =
          meaningful_deficit(effective_read_reservation, global_read_rate_bps);
      const auto [write_deficit_bps, write_deficit_active] =
          meaningful_deficit(effective_write_reservation, global_write_rate_bps);

      AppNodeIoPressureSnapshot snapshot;
      snapshot.app = app;
      snapshot.node_id = node_id;
      snapshot.global_read_rate_bps = global_read_rate_bps;
      snapshot.global_write_rate_bps = global_write_rate_bps;
      snapshot.read_reservation_deficit_bps = read_deficit_bps;
      snapshot.write_reservation_deficit_bps = write_deficit_bps;
      snapshot.reservation_read_bytes_per_sec = policy.reservation_read_bytes_per_sec;
      snapshot.reservation_write_bytes_per_sec = policy.reservation_write_bytes_per_sec;
      snapshot.effective_reservation_read_bytes_per_sec = effective_read_reservation;
      snapshot.effective_reservation_write_bytes_per_sec = effective_write_reservation;
      snapshot.read_reservation_deficit_active =
          effective_read_reservation > 0 && read_deficit_active;
      snapshot.write_reservation_deficit_active =
          effective_write_reservation > 0 && write_deficit_active;
      snapshot.node_controller_read_limit_bytes_per_sec = rates.controller_read_limit_bps;
      snapshot.node_controller_write_limit_bytes_per_sec =
          rates.controller_write_limit_bps;

      if (auto pressure_it = node_io_pressure.find(node_id);
          pressure_it != node_io_pressure.end()) {
        snapshot.node_io_pressure = pressure_it->second;
        snapshot.has_node_io_pressure = true;
      }
      snapshot.read_rate_bps = rates.read_bps;
      snapshot.write_rate_bps = rates.write_bps;
      snapshot.has_read_io_pressure = snapshot.has_node_io_pressure &&
                                      rates.read_bps >= active_node_rate_threshold_bps;
      snapshot.has_write_io_pressure = snapshot.has_node_io_pressure &&
                                       rates.write_bps >= active_node_rate_threshold_bps;

      snapshot.read_pressure_active = snapshot.has_read_io_pressure &&
                                      snapshot.has_node_io_pressure &&
                                      snapshot.node_io_pressure >= io_pressure_threshold;
      snapshot.write_pressure_active = snapshot.has_write_io_pressure &&
                                       snapshot.has_node_io_pressure &&
                                       snapshot.node_io_pressure >= io_pressure_threshold;
      snapshot.read_triggers_competitor_throttling =
          reservations_enabled && snapshot.read_pressure_active &&
          snapshot.read_reservation_deficit_active;
      snapshot.write_triggers_competitor_throttling =
          reservations_enabled && snapshot.write_pressure_active &&
          snapshot.write_reservation_deficit_active;

      auto& node_flags = node_has_pressured_reservation[node_id];
      node_flags.first = node_flags.first || snapshot.read_triggers_competitor_throttling;
      node_flags.second =
          node_flags.second || snapshot.write_triggers_competitor_throttling;

      snapshots.push_back(std::move(snapshot));
    }
  }

  for (auto& snapshot : snapshots) {
    if (auto node_it = node_has_pressured_reservation.find(snapshot.node_id);
        node_it != node_has_pressured_reservation.end()) {
      snapshot.node_has_pressured_read_reservation = node_it->second.first;
      snapshot.node_has_pressured_write_reservation = node_it->second.second;
    }
  }

  std::sort(snapshots.begin(), snapshots.end(), [](const auto& lhs, const auto& rhs) {
    return std::tie(lhs.app, lhs.node_id) < std::tie(rhs.app, rhs.node_id);
  });

  return snapshots;
}

NodeReservationControllerSnapshot
TrafficShapingManager::GetNodeReservationControllerSnapshot(
    std::chrono::steady_clock::time_point now) const
{
  if (now == std::chrono::steady_clock::time_point{}) {
    now = std::chrono::steady_clock::now();
  }

  NodeReservationControllerSnapshot snapshot;
  {
    std::shared_lock lock(mMutex);

    for (const auto& [node_id, runtime] : mNodeReservationControllers) {
      bool has_read_limit = false;
      bool has_write_limit = false;
      for (const auto& [app, limit] : runtime.app_limits) {
        if (limit.read_bps == 0 && limit.write_bps == 0) {
          continue;
        }

        snapshot.limits.push_back({node_id, app, limit.read_bps, limit.write_bps});
        has_read_limit = has_read_limit || limit.read_bps > 0;
        has_write_limit = has_write_limit || limit.write_bps > 0;
      }

      const std::string& feedback_node_id = node_id;
      auto append_feedback = [&](const ReservationControllerState::Direction& state,
                                 const bool is_write, const bool has_direction_limit) {
        if (!has_direction_limit && !HasControllerFeedback(state)) {
          return;
        }

        NodeReservationControllerFeedbackSnapshot feedback;
        feedback.node_id = feedback_node_id;
        feedback.is_write = is_write;
        feedback.consecutive_deficit_samples = state.consecutive_deficit_samples;
        feedback.protected_app_count = state.protected_apps.size();
        feedback.applied_reduction_bps = state.applied_reduction_bps;
        feedback.ineffective_probe_count = state.ineffective_probe_count;
        feedback.failed_protected_app_count = state.failed_protected_apps.size();
        feedback.observed_protected_gain_bps = state.last_observed_protected_gain_bps;
        feedback.response_ratio = state.last_response_ratio;
        feedback.awaiting_response =
            state.last_adjustment_time != std::chrono::steady_clock::time_point{};
        feedback.suppressed =
            state.suppressed_until != std::chrono::steady_clock::time_point{} &&
            now < state.suppressed_until;
        if (feedback.suppressed) {
          feedback.suppression_remaining_seconds =
              std::chrono::duration<double>(state.suppressed_until - now).count();
        }
        snapshot.feedback.push_back(std::move(feedback));
      };

      append_feedback(runtime.feedback.read, false, has_read_limit);
      append_feedback(runtime.feedback.write, true, has_write_limit);

      auto append_cohort = [&](const ReservationControllerState::Direction& state,
                               const bool is_write) {
        for (const auto& [app, action] : state.protected_apps) {
          snapshot.cohort_apps.push_back({feedback_node_id, app, is_write, false,
                                          action.target_bps, action.baseline_rate_bps,
                                          action.assigned_reduction_bps, 0.0});
        }
        for (const auto& [app, failed] : state.failed_protected_apps) {
          snapshot.cohort_apps.push_back({feedback_node_id, app, is_write, true,
                                          failed.target_bps, failed.baseline_rate_bps,
                                          failed.assigned_reduction_bps,
                                          failed.rate_at_failure_bps});
        }
      };

      append_cohort(runtime.feedback.read, false);
      append_cohort(runtime.feedback.write, true);
    }
  }

  std::sort(snapshot.limits.begin(), snapshot.limits.end(),
            [](const auto& lhs, const auto& rhs) {
              return std::tie(lhs.node_id, lhs.app) < std::tie(rhs.node_id, rhs.app);
            });
  std::sort(snapshot.feedback.begin(), snapshot.feedback.end(),
            [](const auto& lhs, const auto& rhs) {
              return std::tie(lhs.node_id, lhs.is_write) <
                     std::tie(rhs.node_id, rhs.is_write);
            });
  std::sort(snapshot.cohort_apps.begin(), snapshot.cohort_apps.end(),
            [](const auto& lhs, const auto& rhs) {
              return std::tie(lhs.node_id, lhs.is_write, lhs.failed, lhs.app) <
                     std::tie(rhs.node_id, rhs.is_write, rhs.failed, rhs.app);
            });
  return snapshot;
}

std::optional<TrafficShapingPolicy>
TrafficShapingManager::GetUidPolicy(const uint32_t uid) const
{
  std::shared_lock lock(mMutex);
  if (const auto it = mUidPolicies.find(uid); it != mUidPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShapingManager::GetGidPolicy(const uint32_t gid) const
{
  std::shared_lock lock(mMutex);
  if (const auto it = mGidPolicies.find(gid); it != mGidPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShapingManager::GetAppPolicy(const std::string& app) const
{
  std::shared_lock lock(mMutex);
  if (const auto it = mAppPolicies.find(app); it != mAppPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::string
TrafficShapingManager::SerializePoliciesUnlocked() const
{
  eos::traffic_shaping::TrafficShapingPolicyConfig proto_config;

  auto copy_to_proto = [](const TrafficShapingPolicy& cpp_pol,
                          eos::traffic_shaping::TrafficShapingPolicy& proto_pol) {
    proto_pol.set_limit_write_bytes_per_sec(cpp_pol.limit_write_bytes_per_sec);
    proto_pol.set_limit_read_bytes_per_sec(cpp_pol.limit_read_bytes_per_sec);
    proto_pol.set_reservation_write_bytes_per_sec(
        cpp_pol.reservation_write_bytes_per_sec);
    proto_pol.set_reservation_read_bytes_per_sec(cpp_pol.reservation_read_bytes_per_sec);
    proto_pol.set_is_enabled(cpp_pol.is_enabled);
  };
  const TrafficShapingPolicy empty_user_policy;

  for (const auto& [uid, pol] : mUidPolicies) {
    if (pol == empty_user_policy) {
      continue;
    }
    copy_to_proto(pol, (*proto_config.mutable_uid_policies())[uid]);
  }
  for (const auto& [gid, pol] : mGidPolicies) {
    if (pol == empty_user_policy) {
      continue;
    }
    copy_to_proto(pol, (*proto_config.mutable_gid_policies())[gid]);
  }
  for (const auto& [app, pol] : mAppPolicies) {
    if (pol == empty_user_policy) {
      continue;
    }
    copy_to_proto(pol, (*proto_config.mutable_app_policies())[app]);
  }

  std::string json_data;
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = false;

  if (const auto status =
          google::protobuf::util::MessageToJsonString(proto_config, &json_data, options);
      !status.ok()) {
    eos_static_err("msg=\"Failed to serialize policies to JSON\"");
  }

  return json_data;
}

bool
TrafficShapingManager::LoadPoliciesFromString(const std::string& serialized_policies)
{
  if (serialized_policies.empty()) {
    return true;
  }
  if (serialized_policies.size() > eos::common::TRAFFIC_SHAPING_POLICY_CONFIG_MAX_BYTES) {
    eos_static_err("msg=\"Rejecting oversized Traffic Shaping policy config\" bytes=%zu "
                   "max_bytes=%zu",
                   serialized_policies.size(),
                   eos::common::TRAFFIC_SHAPING_POLICY_CONFIG_MAX_BYTES);
    return false;
  }
  std::lock_guard persistence_lock(mPolicyPersistenceMutex);

  std::lock_guard publish_lock(mFstConfigPublishMutex);

  std::unordered_map<uint32_t, TrafficShapingPolicy> new_uid_policies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> new_gid_policies;
  std::unordered_map<std::string, TrafficShapingPolicy> new_app_policies;

  std::unique_lock lock(mMutex);

  // Parse under mMutex so that concurrent Set/RemovePolicy calls cannot land
  // between parsing and map replacement, which would cause their updates to be lost.
  eos::traffic_shaping::TrafficShapingPolicyConfig proto_config;

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = true;

  const auto status = google::protobuf::util::JsonStringToMessage(serialized_policies,
                                                                  &proto_config, options);
  if (!status.ok()) {
    eos_static_err("msg=\"Failed to parse policies from JSON string\"");
    return false;
  }
  const size_t policy_count = static_cast<size_t>(proto_config.uid_policies_size()) +
                              static_cast<size_t>(proto_config.gid_policies_size()) +
                              static_cast<size_t>(proto_config.app_policies_size());
  const bool has_oversized_app = std::any_of(
      proto_config.app_policies().begin(), proto_config.app_policies().end(),
      [](const auto& item) {
        return item.first.size() > eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES;
      });
  if (policy_count > eos::common::TRAFFIC_SHAPING_POLICY_MAX_ENTITIES ||
      has_oversized_app) {
    eos_static_err(
        "msg=\"Rejecting Traffic Shaping policy config outside safety bounds\" "
        "entities=%zu max_entities=%zu oversized_app=%d",
        policy_count, eos::common::TRAFFIC_SHAPING_POLICY_MAX_ENTITIES,
        has_oversized_app);
    return false;
  }

  auto merge_policies = [](auto& current_map, const auto& proto_map, auto& new_map) {
    auto copy_to_cpp = [](const auto& proto_pol) -> TrafficShapingPolicy {
      TrafficShapingPolicy cpp_pol;
      cpp_pol.limit_write_bytes_per_sec = proto_pol.limit_write_bytes_per_sec();
      cpp_pol.limit_read_bytes_per_sec = proto_pol.limit_read_bytes_per_sec();
      cpp_pol.reservation_write_bytes_per_sec =
          proto_pol.reservation_write_bytes_per_sec();
      cpp_pol.reservation_read_bytes_per_sec = proto_pol.reservation_read_bytes_per_sec();
      cpp_pol.is_enabled = proto_pol.has_is_enabled() ? proto_pol.is_enabled() : true;
      return cpp_pol;
    };

    for (const auto& [id, pol] : proto_map) {
      auto cpp_pol = copy_to_cpp(pol);
      if (auto it = current_map.find(id); cpp_pol.is_enabled && it != current_map.end()) {
        cpp_pol.controller_limit_read_bytes_per_sec =
            it->second.controller_limit_read_bytes_per_sec;
        cpp_pol.controller_limit_write_bytes_per_sec =
            it->second.controller_limit_write_bytes_per_sec;
        cpp_pol.controller_limit_read_update_time =
            it->second.controller_limit_read_update_time;
        cpp_pol.controller_limit_write_update_time =
            it->second.controller_limit_write_update_time;
      }
      new_map[id] = cpp_pol;
    }

    // Retain controller-only ephemeral policies that are intentionally omitted
    // from persistent serialization.
    TrafficShapingPolicy empty_user_pol; // All user fields are 0/disabled
    for (const auto& [id, pol] : current_map) {
      if (new_map.find(id) == new_map.end()) {
        // We use operator== because it deliberately ignores controller fields.
        // If this evaluates to true, it means NO user settings exist for this policy.
        if (pol == empty_user_pol && (pol.controller_limit_read_bytes_per_sec > 0 ||
                                      pol.controller_limit_write_bytes_per_sec > 0)) {
          new_map[id] = pol;
        }
      }
    }
  };

  merge_policies(mUidPolicies, proto_config.uid_policies(), new_uid_policies);
  merge_policies(mGidPolicies, proto_config.gid_policies(), new_gid_policies);
  merge_policies(mAppPolicies, proto_config.app_policies(), new_app_policies);

  const bool app_config_changed = mAppPolicies != new_app_policies;
  const bool policy_config_changed = app_config_changed ||
                                     mUidPolicies != new_uid_policies ||
                                     mGidPolicies != new_gid_policies;

  mUidPolicies = std::move(new_uid_policies);
  mGidPolicies = std::move(new_gid_policies);
  mAppPolicies = std::move(new_app_policies);
  if (policy_config_changed) {
    mNodeAppDelayStates.clear();
    mNodeFstIoDelayConfigs.clear();
    ++mControllerInputRevision;
  }
  if (app_config_changed) {
    mNodeReservationControllers.clear();
  }

  return true;
}


} // namespace eos::mgm::traffic_shaping
