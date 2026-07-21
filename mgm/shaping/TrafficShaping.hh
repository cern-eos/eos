#pragma once

#include "common/AssistedThread.hh"
#include "common/Constants.hh"
#include "common/shaping/IoStatsKey.hh"
#include "common/shaping/SlidingWindowStats.hh"
#include "proto/TrafficShaping.pb.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace eos::mgm::traffic_shaping {

#ifdef IN_TEST_HARNESS
std::optional<double>
ParseFreshFilesystemIoPressureForTest(const std::string& load_value,
                                      const std::string& publish_timestamp_value,
                                      int64_t now_ms);
#endif

// Inputs and outputs for one application in the built-in reservation controller.
struct AppState {
  std::string app_name;

  double current_read_bps;
  double current_write_bps;
  // Max FST disk-load pressure on nodes where this app is currently active.
  // The corresponding has_* flag is false when the app has no active IO sample,
  // so reservation controllers should not create limits on its behalf.
  double current_read_io_pressure;
  double current_write_io_pressure;
  uint64_t reservation_write_bps;
  uint64_t reservation_read_bps;
  uint64_t controller_limit_write_bps;
  uint64_t controller_limit_read_bps;
  bool has_read_io_pressure;
  bool has_write_io_pressure;
  bool has_read_reservation_competition;
  bool has_write_reservation_competition;

  uint64_t new_controller_limit_write_bps;
  uint64_t new_controller_limit_read_bps;
  bool update_write;
  bool update_read;
};

struct ReservationControllerState {
  struct ProtectedAppAction {
    double target_bps = 0.0;
    double baseline_rate_bps = 0.0;
    double assigned_reduction_bps = 0.0;
  };

  struct FailedProtectedApp {
    double target_bps = 0.0;
    double baseline_rate_bps = 0.0;
    double assigned_reduction_bps = 0.0;
    double rate_at_failure_bps = 0.0;
  };

  struct Direction {
    uint32_t consecutive_deficit_samples = 0;
    std::unordered_map<std::string, ProtectedAppAction> protected_apps;
    double applied_reduction_bps = 0.0;
    uint32_t ineffective_probe_count = 0;
    std::unordered_map<std::string, FailedProtectedApp> failed_protected_apps;
    double last_observed_protected_gain_bps = 0.0;
    double last_response_ratio = 0.0;
    std::chrono::steady_clock::time_point last_adjustment_time{};
    std::chrono::steady_clock::time_point healthy_since{};
    std::chrono::steady_clock::time_point suppressed_until{};
  };

  Direction read;
  Direction write;
};

struct NodeAppControllerLimit {
  uint64_t read_bps = 0;
  uint64_t write_bps = 0;
  std::chrono::steady_clock::time_point read_update_time{};
  std::chrono::steady_clock::time_point write_update_time{};
};

struct NodeReservationControllerRuntime {
  ReservationControllerState feedback;
  std::unordered_map<std::string, NodeAppControllerLimit> app_limits;
};

// App delays from global policies and from the built-in node-local controller
// use different rate domains. Keep their feedback state separate and publish
// only the stricter result.
struct NodeAppDelayState {
  std::unordered_map<std::string, uint64_t> global_read;
  std::unordered_map<std::string, uint64_t> global_write;
  std::unordered_map<std::string, uint64_t> reservation_read;
  std::unordered_map<std::string, uint64_t> reservation_write;
};

struct StreamState {
  uint64_t last_bytes_read = 0;
  uint64_t last_bytes_written = 0;
  uint64_t last_iops_read = 0;
  uint64_t last_iops_write = 0;

  uint64_t generation_id = 0;

  // Source timestamp of the newest accepted cumulative snapshot for this stream.
  int64_t last_report_timestamp_ms = 0;

  std::chrono::steady_clock::time_point last_update_time{};
};

struct RateMetrics {
  double read_rate_bps = 0.0;
  double write_rate_bps = 0.0;
  double read_iops = 0.0;
  double write_iops = 0.0;
};

struct RateCounters {
  double bytes_read = 0.0;
  double bytes_written = 0.0;
  double read_ops = 0.0;
  double write_ops = 0.0;
};

// Stores cumulative checkpoints so fixed-window rates take O(log N) time instead
// of rescanning every bucket for every metric and window.
class CumulativeRateHistory {
public:
  CumulativeRateHistory(double max_history_seconds, double tick_interval_seconds);

  void Reset(double max_history_seconds, double tick_interval_seconds);

  void Push(const RateCounters& counters, double elapsed_seconds);

  RateMetrics GetRate(double seconds) const;

private:
  struct Sample {
    RateCounters counters;
    double elapsed_seconds = 0.0;
  };

  struct HistoryRing {
    std::vector<Sample> samples;
    size_t capacity = 0;
    size_t head = 0;
    size_t size = 0;
    Sample base;

    void Reset(size_t capacity);
    void Push(const Sample& sample);
    const Sample& GetLogicalSample(size_t index) const;
    Sample FindBoundary(double target_elapsed) const;
  };

  static constexpr double kFineHistorySeconds = 15.0;
  static constexpr double kMediumHistorySeconds = 60.0;
  double mTickIntervalSec = 0.0;
  double mMediumIntervalSec = 1.0;
  double mCoarseIntervalSec = 5.0;
  double mLastMediumCheckpointSec = 0.0;
  double mLastCoarseCheckpointSec = 0.0;
  HistoryRing mFineHistory;
  HistoryRing mMediumHistory;
  HistoryRing mCoarseHistory;
  Sample mCumulativeSample;
};

struct AppIoPressureSnapshot {
  double read = 0.0;
  double write = 0.0;
  bool has_read = false;
  bool has_write = false;
};

struct AppNodeIoPressureSnapshot {
  std::string app;
  std::string node_id;
  double node_io_pressure = 0.0;
  double read_rate_bps = 0.0;
  double write_rate_bps = 0.0;
  double global_read_rate_bps = 0.0;
  double global_write_rate_bps = 0.0;
  double read_reservation_deficit_bps = 0.0;
  double write_reservation_deficit_bps = 0.0;
  uint64_t reservation_read_bytes_per_sec = 0;
  uint64_t reservation_write_bytes_per_sec = 0;
  uint64_t effective_reservation_read_bytes_per_sec = 0;
  uint64_t effective_reservation_write_bytes_per_sec = 0;
  uint64_t node_controller_read_limit_bytes_per_sec = 0;
  uint64_t node_controller_write_limit_bytes_per_sec = 0;
  bool has_node_io_pressure = false;
  bool has_read_io_pressure = false;
  bool has_write_io_pressure = false;
  bool read_pressure_active = false;
  bool write_pressure_active = false;
  bool read_reservation_deficit_active = false;
  bool write_reservation_deficit_active = false;
  bool read_triggers_competitor_throttling = false;
  bool write_triggers_competitor_throttling = false;
  bool node_has_pressured_read_reservation = false;
  bool node_has_pressured_write_reservation = false;
};

struct NodeReservationControllerLimitSnapshot {
  std::string node_id;
  std::string app;
  uint64_t read_bytes_per_sec = 0;
  uint64_t write_bytes_per_sec = 0;
};

struct NodeReservationControllerFeedbackSnapshot {
  std::string node_id;
  bool is_write = false;
  uint32_t consecutive_deficit_samples = 0;
  size_t protected_app_count = 0;
  double applied_reduction_bps = 0.0;
  uint32_t ineffective_probe_count = 0;
  size_t failed_protected_app_count = 0;
  double observed_protected_gain_bps = 0.0;
  double response_ratio = 0.0;
  bool awaiting_response = false;
  bool suppressed = false;
  double suppression_remaining_seconds = 0.0;
  std::string phase;
};

struct NodeReservationControllerCohortAppSnapshot {
  std::string node_id;
  std::string app;
  bool is_write = false;
  bool failed = false;
  double target_bps = 0.0;
  double baseline_rate_bps = 0.0;
  double assigned_reduction_bps = 0.0;
  double rate_at_failure_bps = 0.0;
};

struct NodeReservationControllerSnapshot {
  std::vector<NodeReservationControllerLimitSnapshot> limits;
  std::vector<NodeReservationControllerFeedbackSnapshot> feedback;
  std::vector<NodeReservationControllerCohortAppSnapshot> cohort_apps;
};

struct MapCardinalityStats {
  uint64_t node_states = 0;
  uint64_t node_state_streams = 0;
  uint64_t node_state_estimated_bytes = 0;
  uint64_t node_state_rejections_total = 0;
  uint64_t global_stats = 0;
  uint64_t node_stats = 0;
  uint64_t disk_stats = 0;
  uint64_t detailed_stats = 0;
  uint64_t global_cumulative_stats = 0;
  uint64_t node_cumulative_stats = 0;
  uint64_t disk_cumulative_stats = 0;
  uint64_t detailed_cumulative_stats = 0;
  uint64_t projection_app_cumulative_stats = 0;
  uint64_t projection_uid_cumulative_stats = 0;
  uint64_t projection_gid_cumulative_stats = 0;
  uint64_t projection_node_cumulative_stats = 0;
  uint64_t node_entity_stats = 0;
  uint64_t uid_policies = 0;
  uint64_t gid_policies = 0;
  uint64_t app_policies = 0;
  uint64_t node_fst_io_delay_configs = 0;
  uint64_t published_fst_io_delay_configs = 0;
};

constexpr std::array<int, 2> EmaWindowSec = {1, 5};
constexpr std::array<int, 5> SmaWindowSec = {1, 5, 15, 60, 300};

enum EmaIdx : size_t { Ema1s = 0, Ema5s = 1 };

enum SmaIdx : size_t { Sma1s = 0, Sma5s = 1, Sma15s = 2, Sma1m = 3, Sma5m = 4 };

constexpr uint32_t kMinThreadPeriodMs = eos::common::TRAFFIC_SHAPING_THREAD_PERIOD_MIN_MS;
constexpr uint32_t kMaxThreadPeriodMs = eos::common::TRAFFIC_SHAPING_THREAD_PERIOD_MAX_MS;
constexpr uint32_t kMinSystemStatsWindowSec = 5;
constexpr uint32_t kMaxSystemStatsWindowSec = 300;
constexpr uint32_t kMinGarbageCollectionIdleSec = 1;
constexpr uint32_t kMaxGarbageCollectionIdleSec = 24 * 60 * 60;
constexpr uint32_t kDefaultGarbageCollectionIdleSec = 5 * 60;
// Leave headroom below the conservative 256 MiB stream-state admission budget
// (roughly 2,000 worst-case streams at the minimum estimator period).
constexpr uint64_t kDefaultAutomaticFilesystemDetailLowCardinality = 1000;
constexpr uint64_t kDefaultAutomaticFilesystemDetailHighCardinality = 1500;
constexpr size_t kMaxSerializedFstIoReportBytes =
    eos::common::TRAFFIC_SHAPING_FST_REPORT_MAX_SERIALIZED_BYTES;
constexpr size_t kMaxBase64EncodedFstIoReportBytes =
    7 + ((kMaxSerializedFstIoReportBytes + 2) / 3) * 4;
constexpr uint64_t kDefaultControllerMinLimitBps = 100ULL * 1000ULL * 1000ULL;
constexpr uint64_t kDefaultActiveNodeRateThresholdBps = 1024ULL * 1024ULL;
constexpr double kDefaultIoPressureThreshold = 0.1;

struct MultiWindowRate {
  double tick_interval_seconds;
  static constexpr double sma_max_history_seconds = SmaWindowSec.back();

  // TrafficShapingManager serializes all access with mMutex, so atomics here only
  // added cache-coherency traffic without providing additional safety.
  double bytes_read_accumulator = 0.0;
  double bytes_written_accumulator = 0.0;
  double read_iops_accumulator = 0.0;
  double write_iops_accumulator = 0.0;

  std::array<RateMetrics, EmaWindowSec.size()> ema{};
  std::array<RateMetrics, SmaWindowSec.size()> sma{};

  CumulativeRateHistory rate_history;

  time_t last_activity_time = 0;

  explicit MultiWindowRate(const double initial_tick_interval)
      : tick_interval_seconds(initial_tick_interval)
      , rate_history(sma_max_history_seconds, initial_tick_interval)
  {
  }

  void
  clear()
  {
    bytes_read_accumulator = 0.0;
    bytes_written_accumulator = 0.0;
    read_iops_accumulator = 0.0;
    write_iops_accumulator = 0.0;

    for (auto& m : ema) {
      m = RateMetrics{};
    }

    for (auto& m : sma) {
      m = RateMetrics{};
    }

    ResetWindows(tick_interval_seconds);

    last_activity_time = 0;
  }

  void
  ResetWindows(double new_tick_interval)
  {
    tick_interval_seconds = new_tick_interval;
    rate_history.Reset(sma_max_history_seconds, tick_interval_seconds);

    for (auto& s : sma) {
      s = RateMetrics{};
    }
  }
};

// The controller only needs a one-second EMA for per-node entities. Keeping the
// full five-minute SMA history here duplicated the most expensive state in the
// manager without any consumer.
struct EmaRate {
  double bytes_read_accumulator = 0.0;
  double bytes_written_accumulator = 0.0;
  double read_iops_accumulator = 0.0;
  double write_iops_accumulator = 0.0;
  RateMetrics ema;
  time_t last_activity_time = 0;
};

struct RateSnapshot {
  uint64_t bytes_read_total = 0;
  uint64_t bytes_written_total = 0;
  uint64_t read_ops_total = 0;
  uint64_t write_ops_total = 0;

  std::array<RateMetrics, EmaWindowSec.size()> ema{};
  std::array<RateMetrics, SmaWindowSec.size()> sma{};

  time_t last_activity_time = 0;
};

struct ProjectionCumulativeStats {
  std::unordered_map<std::string, RateSnapshot> app;
  std::unordered_map<uint32_t, RateSnapshot> uid;
  std::unordered_map<uint32_t, RateSnapshot> gid;
  std::unordered_map<std::string, RateSnapshot> node;
};

using StreamKey = eos::common::traffic_shaping::IoStatsKey;
using StreamKeyHash = eos::common::traffic_shaping::IoStatsKeyHash;

struct DiskKey {
  std::string node_id;
  uint64_t fsid = 0;

  bool
  operator==(const DiskKey& other) const
  {
    return fsid == other.fsid && node_id == other.node_id;
  }

  bool
  operator<(const DiskKey& other) const
  {
    return std::tie(node_id, fsid) < std::tie(other.node_id, other.fsid);
  }
};

struct DiskKeyHash {
  std::size_t
  operator()(const DiskKey& k) const
  {
    std::size_t h = std::hash<std::string>{}(k.node_id);
    return eos::common::traffic_shaping::HashCombine(h, std::hash<uint64_t>{}(k.fsid));
  }
};

struct DetailedKey {
  std::string node_id;
  StreamKey stream;

  bool
  operator==(const DetailedKey& other) const
  {
    return node_id == other.node_id && stream == other.stream;
  }

  bool
  operator<(const DetailedKey& other) const
  {
    return std::tie(node_id, stream) < std::tie(other.node_id, other.stream);
  }
};

struct DetailedKeyHash {
  std::size_t
  operator()(const DetailedKey& k) const
  {
    const std::size_t h = std::hash<std::string>{}(k.node_id);
    return eos::common::traffic_shaping::HashCombine(h, StreamKeyHash{}(k.stream));
  }
};

struct TrafficShapingPolicy {
  // --- Persistent User Configuration ---
  uint64_t limit_write_bytes_per_sec = 0;
  uint64_t limit_read_bytes_per_sec = 0;
  uint64_t reservation_write_bytes_per_sec = 0;
  uint64_t reservation_read_bytes_per_sec = 0;

  bool is_enabled = true;

  // --- Ephemeral Controller Configuration ---
  uint64_t controller_limit_write_bytes_per_sec = 0;
  uint64_t controller_limit_read_bytes_per_sec = 0;
  std::chrono::steady_clock::time_point controller_limit_write_update_time{};
  std::chrono::steady_clock::time_point controller_limit_read_update_time{};

  uint64_t GetEffectiveWriteLimit() const;

  uint64_t GetEffectiveReadLimit() const;

  bool IsReservationConfigurationFeasible() const;

  bool IsEmpty() const;

  bool IsActive() const;

  bool operator==(const TrafficShapingPolicy& policy) const;

  bool operator!=(const TrafficShapingPolicy& policy) const;

  std::string ToString() const;
};

class TrafficShapingManager {
public:
  TrafficShapingManager();

  ~TrafficShapingManager();

  void ProcessReport(const eos::traffic_shaping::FstIoReport& report);

  void UpdateEstimators(double time_delta_seconds);

  void UpdateLimits(const std::unordered_map<std::string, double>& node_io_pressure,
                    const std::vector<std::string>& online_nodes);

  void UpdateTrafficShapingController(
      const std::unordered_map<std::string, double>& node_io_pressure) noexcept;

  void SetLimitsEnabled(bool enabled);

  bool GetLimitsEnabled() const;

  void SetReservationsEnabled(bool enabled);

  bool GetReservationsEnabled() const;

  void SetControllerMinLimit(uint64_t limit_bps);

  uint64_t GetControllerMinLimit() const;

  void SetActiveNodeRateThreshold(uint64_t threshold_bps);

  uint64_t GetActiveNodeRateThreshold() const;

  void SetIoPressureThreshold(double threshold);

  double GetIoPressureThreshold() const;

  size_t ClearControllerLimits();

  size_t ExpireControllerLimits(std::chrono::steady_clock::time_point now);

  bool ApplyThreadConfig(uint32_t estimators_period_ms, uint32_t fst_policy_period_ms,
                         uint32_t fst_report_period_ms, uint32_t window_seconds) noexcept;

  void SetFilesystemDetailEnabled(bool enabled);

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> GetGlobalStats() const;

  std::unordered_map<std::string, RateSnapshot> GetNodeStats() const;

  std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash> GetDiskStats() const;

  std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash> GetDetailedStats() const;

  RateSnapshot GetTotalStats() const;

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash>
  GetGlobalCumulativeStats() const;

  std::unordered_map<std::string, RateSnapshot> GetNodeCumulativeStats() const;

  std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash> GetDiskCumulativeStats() const;

  std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash>
  GetDetailedCumulativeStats() const;

  ProjectionCumulativeStats GetProjectionCumulativeStats() const;

  RateSnapshot GetTotalCumulativeStats() const;

  struct GarbageCollectionStats {
    size_t removed_nodes;
    size_t removed_node_streams;
    size_t removed_global_streams;
    size_t removed_disk_stats;
    size_t removed_detailed_stats;
  };

  GarbageCollectionStats GarbageCollect(int max_idle_seconds);

  void SetUidPolicy(uint32_t uid, const TrafficShapingPolicy& policy);

  void SetGidPolicy(uint32_t gid, const TrafficShapingPolicy& policy);

  void SetAppPolicy(const std::string& app, const TrafficShapingPolicy& policy);

  void RemoveUidPolicy(uint32_t uid);

  void RemoveGidPolicy(uint32_t gid);

  void RemoveAppPolicy(const std::string& app);

  std::string SerializePoliciesUnlocked() const;

  bool LoadPoliciesFromString(const std::string& serialized_policies);

  std::unordered_map<uint32_t, TrafficShapingPolicy> GetUidPolicies() const;

  std::unordered_map<uint32_t, TrafficShapingPolicy> GetGidPolicies() const;

  std::unordered_map<std::string, TrafficShapingPolicy> GetAppPolicies() const;

  std::optional<TrafficShapingPolicy> GetUidPolicy(uint32_t uid) const;

  std::optional<TrafficShapingPolicy> GetGidPolicy(uint32_t gid) const;

  std::optional<TrafficShapingPolicy> GetAppPolicy(const std::string& app) const;

  void UpdateFstLimitsLoopMicroSec(uint64_t time_microseconds);

  void UpdateReservationControllerLoopMicroSec(uint64_t time_microseconds);

  void UpdateEstimatorsLoopMicroSec(uint64_t time_microseconds);

  void UpdateFsViewLockMicroSec(uint64_t wait_microseconds, uint64_t hold_microseconds);

  void UpdateFstReportsProcessed(uint64_t count);

  double GetFstReportsProcessedPerSecondMean() const;

  void UpdateFstReportQueueStats(uint64_t depth, uint64_t estimated_bytes,
                                 uint64_t dropped);

  uint64_t GetFstReportQueueDepth() const;

  uint64_t GetFstReportQueueEstimatedBytes() const;

  uint64_t GetFstReportsDroppedTotal() const;

  std::tuple<uint64_t, uint64_t, uint64_t> GetEstimatorsUpdateLoopMicroSecStats() const;

  std::tuple<uint64_t, uint64_t, uint64_t> GetFstLimitsUpdateLoopMicroSecStats() const;

  std::tuple<uint64_t, uint64_t, uint64_t>
  GetReservationControllerUpdateLoopMicroSecStats() const;

  std::tuple<uint64_t, uint64_t, uint64_t> GetFsViewLockWaitMicroSecStats() const;

  std::tuple<uint64_t, uint64_t, uint64_t> GetFsViewLockHoldMicroSecStats() const;

  MapCardinalityStats GetMapCardinalityStats() const;

  uint32_t
  GetSystemStatsWindowSeconds() const
  {
    std::shared_lock lock(mMutex);
    return mSystemStatsWindowSeconds;
  }

  std::unordered_map<std::string, AppIoPressureSnapshot> GetReservedAppIoPressure() const;

  std::vector<AppNodeIoPressureSnapshot> GetReservedAppNodeIoPressure(
      std::vector<std::string>* online_nodes_out = nullptr) const;

  NodeReservationControllerSnapshot GetNodeReservationControllerSnapshot(
      std::chrono::steady_clock::time_point now = {}) const;

  void Clear();

  void ClearRuntimeStats();

  void ClearDetailedRuntimeStats();

private:
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;

  struct NodeData {
    std::chrono::steady_clock::time_point last_report_time{};
    int64_t node_start_time_ms = 0;
    std::array<int64_t, 4> retired_node_start_times{};
    size_t next_retired_node_start_time = 0;
    // Newest node heartbeat timestamp. Empty reports advance this anchor so
    // changed streams are normalized over the report cadence, not their idle age.
    int64_t last_source_report_timestamp_ms = 0;
    NodeStateMap streams;
  };

  std::unordered_map<std::string, NodeData> mNodeStates;
  size_t mFstStreamStateCount = 0;
  size_t mFstStreamStateEstimatedBytes = 0;
  std::atomic<uint64_t> mFstStreamStatesRejectedTotal{0};
  std::atomic<uint64_t> mLastFstReportStateWarningMonotonicNs{0};
  std::unordered_map<StreamKey, MultiWindowRate, StreamKeyHash> mGlobalStats;
  std::unordered_map<std::string, MultiWindowRate> mNodeStats;
  std::unordered_map<DiskKey, MultiWindowRate, DiskKeyHash> mDiskStats;
  std::unordered_map<DetailedKey, MultiWindowRate, DetailedKeyHash> mDetailedStats;
  std::unordered_map<DetailedKey, EmaRate, DetailedKeyHash> mNodeEntityStats;
  // We provide an initial tick interval but this will be refreshed on initialization
  MultiWindowRate mTotalStats{0.5};

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> mGlobalCumulativeStats;
  std::unordered_map<std::string, RateSnapshot> mNodeCumulativeStats;
  std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash> mDiskCumulativeStats;
  std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash> mDetailedCumulativeStats;
  ProjectionCumulativeStats mProjectionCumulativeStats;
  RateSnapshot mCumulativeTotalStats;

  std::unordered_map<uint32_t, TrafficShapingPolicy> mUidPolicies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> mGidPolicies;
  std::unordered_map<std::string, TrafficShapingPolicy> mAppPolicies;
  // Built-in reservation limits are node-local. Manually configured controller
  // limits remain global application policy.
  std::unordered_map<std::string, NodeReservationControllerRuntime>
      mNodeReservationControllers;
  std::unordered_map<std::string, NodeAppDelayState> mNodeAppDelayStates;

  std::unordered_map<std::string, eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      mNodeFstIoDelayConfigs;

  struct PublishedFstIoDelayConfig {
    std::string encoded_config;
    std::chrono::steady_clock::time_point last_publish_time{};
  };
  std::unordered_map<std::string, PublishedFstIoDelayConfig> mPublishedFstIoDelayConfigs;

  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      estimators_update_loop_micro_sec;
  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      reservation_controller_update_loop_micro_sec;
  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      fst_limits_update_loop_micro_sec;
  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      fsview_lock_wait_micro_sec;
  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      fsview_lock_hold_micro_sec;
  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      fst_reports_processed_per_second;

  double mEstimatorsTickIntervalSec{0.5};
  double mFstPolicyTickIntervalSec{0.5};
  double mFstReportTickIntervalSec{0.5};
  uint32_t mSystemStatsWindowSeconds{15};
  std::atomic<bool> mFilesystemDetailEnabled{false};
  std::atomic<bool> mFailNextThreadConfigPreparation{false};
  std::atomic<bool> mFailNextControllerPublication{false};
  std::atomic<bool> mPauseControllerBeforePublication{false};
  std::atomic<bool> mControllerPublicationPaused{false};

  mutable std::shared_mutex mMutex;
  // Serializes controller-input mutations with external FST config publication.
  // Recursive because the controller update invokes expiry and policy-apply helpers.
  mutable std::recursive_mutex mFstConfigPublishMutex;
  // Orders durable policy snapshots without retaining controller/runtime locks
  // across the potentially blocking QDB persistence wait.
  mutable std::mutex mPolicyPersistenceMutex;

#ifdef IN_TEST_HARNESS
public:
  void
  SetNodeReservationControllerRuntimeForTest(
      const std::string& node, const NodeReservationControllerRuntime& runtime)
  {
    std::lock_guard publish_lock(mFstConfigPublishMutex);
    std::unique_lock lock(mMutex);
    mNodeReservationControllers[node] = runtime;
  }

  std::unordered_map<std::string, NodeReservationControllerRuntime>
  GetNodeReservationControllerRuntimes() const
  {
    std::shared_lock lock(mMutex);
    return mNodeReservationControllers;
  }

  void
  SetGlobalEmaRatesForTest(const StreamKey& key, const RateMetrics& fast,
                           const RateMetrics& stable)
  {
    std::unique_lock lock(mMutex);
    auto [it, _] = mGlobalStats.try_emplace(key, mEstimatorsTickIntervalSec);
    it->second.ema[Ema1s] = fast;
    it->second.ema[Ema5s] = stable;
  }

  void
  SetNodeEntityRateForTest(const std::string& node, const StreamKey& stream,
                           const RateMetrics& rate)
  {
    std::unique_lock lock(mMutex);
    auto& stats = mNodeEntityStats[DetailedKey{node, stream}];
    stats.ema = rate;
    stats.last_activity_time = time(nullptr);
  }

  void
  SetNodeEntityLastActivityForTest(const std::string& node, const StreamKey& stream,
                                   const time_t last_activity_time)
  {
    std::unique_lock lock(mMutex);
    mNodeEntityStats[DetailedKey{node, stream}].last_activity_time = last_activity_time;
  }

  std::pair<size_t, size_t>
  GetFstStreamStateAccountingForTest() const
  {
    std::shared_lock lock(mMutex);
    return {mFstStreamStateCount, mFstStreamStateEstimatedBytes};
  }

  std::unique_lock<std::shared_mutex>
  LockStateForTest()
  {
    return std::unique_lock<std::shared_mutex>(mMutex);
  }

  std::vector<AppNodeIoPressureSnapshot>
  GetReservedAppNodeIoPressureForTest(
      const std::unordered_map<std::string, double>& node_io_pressure,
      const std::vector<std::string>& online_nodes) const
  {
    return BuildReservedAppNodeIoPressure(node_io_pressure, online_nodes);
  }

  void
  FailNextThreadConfigPreparationForTest()
  {
    mFailNextThreadConfigPreparation.store(true, std::memory_order_relaxed);
  }

  void
  FailNextControllerPublicationForTest()
  {
    mFailNextControllerPublication.store(true, std::memory_order_relaxed);
  }

  void
  PauseControllerBeforePublicationForTest()
  {
    mPauseControllerBeforePublication.store(true, std::memory_order_release);
  }

  bool
  IsControllerPublicationPausedForTest() const
  {
    return mControllerPublicationPaused.load(std::memory_order_acquire);
  }

  void
  ResumeControllerPublicationForTest()
  {
    mPauseControllerBeforePublication.store(false, std::memory_order_release);
  }
#endif
  static double CalculateEma(double current_val, double prev_ema, double alpha);

  // Calculates the new FST delay microsecond value given the current rate and limit
  static uint64_t
  CalculateDelayUs(double limit_bps, double current_rate_bps, uint64_t current_delay_us,
                   double io_pressure, bool has_rate_sample, bool allow_idle_release,
                   double delay_reference_bps = 0.0,
                   double io_pressure_threshold = kDefaultIoPressureThreshold);

  static void ApplyDefaultReservationController(
      std::vector<AppState>& apps, bool reservations_enabled = true,
      uint64_t controller_min_limit_bps = kDefaultControllerMinLimitBps,
      double io_pressure_threshold = kDefaultIoPressureThreshold,
      uint64_t active_node_rate_threshold_bps = kDefaultActiveNodeRateThresholdBps,
      ReservationControllerState* controller_state = nullptr,
      std::chrono::steady_clock::time_point now = {},
      const std::unordered_map<std::string, double>* competition_write_rates = nullptr,
      const std::unordered_map<std::string, double>* competition_read_rates = nullptr,
      bool deficits_prequalified = false);

  static bool ShouldEmitDelayForPolicy(
      const TrafficShapingPolicy& policy, bool is_write, double node_rate_bps,
      double io_pressure, bool node_has_pressured_reservation, bool limits_enabled,
      bool reservations_enabled,
      double io_pressure_threshold = kDefaultIoPressureThreshold,
      uint64_t active_node_rate_threshold_bps = kDefaultActiveNodeRateThresholdBps);

#ifdef IN_TEST_HARNESS
private:
#endif

  size_t ClearControllerLimitsUnlocked();
  void UpdateTrafficShapingControllerImpl(
      const std::unordered_map<std::string, double>& node_io_pressure);
  std::vector<AppNodeIoPressureSnapshot> BuildReservedAppNodeIoPressure(
      const std::unordered_map<std::string, double>& node_io_pressure,
      const std::vector<std::string>& online_nodes) const;

  std::atomic<uint64_t> mFstReportQueueDepth{0};
  std::atomic<uint64_t> mFstReportQueueEstimatedBytes{0};
  std::atomic<uint64_t> mFstReportsDroppedTotal{0};

  std::atomic<bool> mLimitsEnabled{true};
  std::atomic<bool> mReservationsEnabled{true};
  std::atomic<uint64_t> mControllerMinLimitBps{kDefaultControllerMinLimitBps};
  std::atomic<uint64_t> mActiveNodeRateThresholdBps{kDefaultActiveNodeRateThresholdBps};
  std::atomic<double> mIoPressureThreshold{kDefaultIoPressureThreshold};
  uint64_t mControllerInputRevision = 0;
};

class TrafficShapingEngine {
public:
  TrafficShapingEngine();

  ~TrafficShapingEngine();

  void ApplyConfig();

  bool Start() noexcept;

  void Stop() noexcept;

  bool Enable() noexcept;

  bool Disable() noexcept;

  bool SetEnabled(bool enabled) noexcept;

  bool
  IsEnabled() const
  {
    return mRunning;
  }

  void SyncTrafficShapingEnabledWithFst();

  void SyncTrafficShapingConfigWithFst();

  std::shared_ptr<TrafficShapingManager> GetManager() const;

  void
  ProcessSerializedFstIoReportNonBlocking(const std::string& serialized_report) noexcept;

  void RejectFstIoReportNonBlocking(const char* reason) noexcept;

  uint32_t
  GetEstimatorsUpdateThreadPeriodMilliseconds() const
  {
    return mEstimatorsUpdateThreadPeriodMilliseconds.load();
  }

  uint32_t
  GetFstIoPolicyUpdateThreadPeriodMilliseconds() const
  {
    return mFstIoPolicyUpdateThreadPeriodMilliseconds.load();
  }

  uint32_t
  GetFstIoStatsReportThreadPeriodMilliseconds() const
  {
    return mFstIoStatsReportThreadPeriodMilliseconds.load();
  }

  uint32_t
  GetSystemStatsWindowSeconds() const
  {
    return mSystemStatsWindowSeconds.load();
  }

  void SetEstimatorsUpdateThreadPeriodMilliseconds(uint32_t period_ms);

  void SetFstIoPolicyUpdateThreadPeriodMilliseconds(uint32_t period_ms);

  void SetFstIoStatsReportThreadPeriodMilliseconds(uint32_t period_ms);

  void SetSystemStatsWindowSeconds(uint32_t window_seconds);

  void SetDetailLevel(const std::string& detail_level);

  std::string GetDetailLevel() const;

  void SetAutomaticDetailLevelEnabled(bool enabled);

  bool GetAutomaticDetailLevelEnabled() const;

  void SetAutomaticDetailLevelCardinality(uint64_t low_cardinality,
                                          uint64_t high_cardinality);

  uint64_t
  GetAutomaticDetailLevelLowCardinality() const
  {
    return mAutomaticDetailLevelLowCardinality.load(std::memory_order_relaxed);
  }

  uint64_t
  GetAutomaticDetailLevelHighCardinality() const
  {
    return mAutomaticDetailLevelHighCardinality.load(std::memory_order_relaxed);
  }

  void SetLimitsEnabled(bool enabled);

  bool GetLimitsEnabled() const;

  void SetReservationsEnabled(bool enabled);

  bool GetReservationsEnabled() const;

  void SetControllerMinLimit(uint64_t limit_bps);

  uint64_t GetControllerMinLimit() const;

  void SetActiveNodeRateThreshold(uint64_t threshold_bps);

  uint64_t GetActiveNodeRateThreshold() const;

  void SetIoPressureThreshold(double threshold);

  double GetIoPressureThreshold() const;

  void SetGarbageCollectionIdleSeconds(uint32_t idle_seconds);

  uint32_t
  GetGarbageCollectionIdleSeconds() const
  {
    return mGarbageCollectionIdleSeconds.load(std::memory_order_relaxed);
  }

#ifdef IN_TEST_HARNESS
  void
  FailNextFstEnabledSyncThreadStartForTest()
  {
    mFailNextFstEnabledSyncThreadStart.store(true, std::memory_order_relaxed);
  }

  void
  FailNextEnabledConfigStoreForTest()
  {
    mFailNextEnabledConfigStore.store(true, std::memory_order_relaxed);
  }
#endif

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif
  bool ApplyEnabledConfig(bool enabled) noexcept;

  void StoreEnabledConfig(bool enabled);

  bool StopRuntime() noexcept;

  bool EnsureFstEnabledSyncThread() noexcept;

  void StopFstEnabledSyncThread();

  std::vector<std::string> GetOnlineFstNodeNames() const;

  bool ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                         uint32_t win_s, bool* applied_successfully = nullptr) noexcept;

  void SetThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms, uint32_t win_s);

  void StoreThreadConfig();

  bool ApplyDetailLevelConfig(const std::string& detail_level);

  void LogDetailLevelSwitch(const char* reason, const std::string& detail_level,
                            const MapCardinalityStats& cardinality) const noexcept;

  static void StoreDetailLevelConfig(const std::string& detail_level);

  bool ApplyAutomaticDetailLevelEnabledConfig(bool enabled);

  static void StoreAutomaticDetailLevelEnabledConfig(bool enabled);

  bool ApplyAutomaticDetailLevelCardinalityConfig(uint64_t low_cardinality,
                                                  uint64_t high_cardinality);

  static void StoreAutomaticDetailLevelCardinalityConfig(uint64_t low_cardinality,
                                                         uint64_t high_cardinality);

  void ApplyAutomaticDetailLevel();

  bool ApplyLimitsEnabledConfig(bool enabled);

  static void StoreLimitsEnabledConfig(bool enabled);

  bool ApplyReservationsEnabledConfig(bool enabled);

  static void StoreReservationsEnabledConfig(bool enabled);

  bool ApplyControllerMinLimitConfig(uint64_t limit_bps);

  static void StoreControllerMinLimitConfig(uint64_t limit_bps);

  bool ApplyActiveNodeRateThresholdConfig(uint64_t threshold_bps);

  static void StoreActiveNodeRateThresholdConfig(uint64_t threshold_bps);

  bool ApplyIoPressureThresholdConfig(double threshold);

  static void StoreIoPressureThresholdConfig(double threshold);

  bool ApplyGarbageCollectionIdleSecondsConfig(uint32_t idle_seconds);

  static void StoreGarbageCollectionIdleSecondsConfig(uint32_t idle_seconds);

  void EstimatorsUpdate(ThreadAssistant&);

  void FstIoPolicyUpdate(ThreadAssistant&) const;

  void FstTrafficShapingEnabledUpdate(ThreadAssistant&);

  void AddReportToQueue(eos::traffic_shaping::FstIoReport report) noexcept;

  void RecordRejectedFstReport() noexcept;

  void ProcessAllQueuedReports();

  std::shared_ptr<TrafficShapingManager> mManager{};

  std::unique_ptr<AssistedThread> mEstimatorsUpdateThread;
  std::unique_ptr<AssistedThread> mFstIoPolicyUpdateThread;
  std::unique_ptr<AssistedThread> mFstTrafficShapingEnabledUpdateThread;

  std::mutex mFstEnabledSyncThreadMutex;
  std::mutex mRuntimeLifecycleMutex;
  std::mutex mEnabledOperationMutex;

  std::atomic<bool> mRunning{};
  std::atomic<bool> mFailNextFstEnabledSyncThreadStart{false};
  std::atomic<bool> mFailNextEnabledConfigStore{false};

  std::atomic<uint32_t> mEstimatorsUpdateThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mFstIoPolicyUpdateThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mFstIoStatsReportThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mSystemStatsWindowSeconds{};
  std::mutex mThreadConfigMutex;
  std::atomic<bool> mFilesystemDetailEnabled{};
  std::atomic<bool> mAutomaticDetailLevelEnabled{true};
  std::atomic<uint64_t> mAutomaticDetailLevelLowCardinality{
      kDefaultAutomaticFilesystemDetailLowCardinality};
  std::atomic<uint64_t> mAutomaticDetailLevelHighCardinality{
      kDefaultAutomaticFilesystemDetailHighCardinality};
  std::mutex mAutomaticDetailLevelMutex;
  std::chrono::steady_clock::time_point mLastAutomaticDetailLevelChange{};
  std::atomic<bool> mLimitsEnabled{true};
  std::atomic<bool> mReservationsEnabled{true};
  std::atomic<uint64_t> mControllerMinLimitBps{kDefaultControllerMinLimitBps};
  std::atomic<uint64_t> mActiveNodeRateThresholdBps{kDefaultActiveNodeRateThresholdBps};
  std::atomic<double> mIoPressureThreshold{kDefaultIoPressureThreshold};
  std::atomic<uint32_t> mGarbageCollectionIdleSeconds{kDefaultGarbageCollectionIdleSec};

  struct QueuedFstIoReport {
    eos::traffic_shaping::FstIoReport report;
    size_t estimated_bytes = 0;
  };
  std::deque<QueuedFstIoReport> mReportQueue{};
  size_t mReportQueueEstimatedBytes = 0;
  std::mutex mReportQueueMutex{};
  std::atomic<uint64_t> mLastReportQueueWarningMonotonicNs{0};
};

} // namespace eos::mgm::traffic_shaping
