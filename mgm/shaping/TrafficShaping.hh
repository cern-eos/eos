#pragma once

#include "common/AssistedThread.hh"
#include "fst/storage/TrafficShaping.hh"
#include "proto/TrafficShaping.pb.h"

#include <atomic>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace eos::mgm::traffic_shaping {

struct StreamState {
  uint64_t last_bytes_read = 0;
  uint64_t last_bytes_written = 0;
  uint64_t last_iops_read = 0;
  uint64_t last_iops_write = 0;

  uint64_t generation_id = 0;

  std::chrono::steady_clock::time_point last_update_time{};
};

struct RateMetrics {
  double read_rate_bps = 0.0;
  double write_rate_bps = 0.0;
  double read_iops = 0.0;
  double write_iops = 0.0;
};

constexpr std::array<int, 2> EmaWindowSec = {1, 5};
constexpr std::array<int, 5> SmaWindowSec = {1, 5, 15, 60, 300};

enum EmaIdx : size_t { Ema1s = 0, Ema5s = 1 };

enum SmaIdx : size_t { Sma1s = 0, Sma5s = 1, Sma15s = 2, Sma1m = 3, Sma5m = 4 };

constexpr uint32_t kMinThreadPeriodMs = 50;
constexpr uint32_t kMaxThreadPeriodMs = 3000;
constexpr uint32_t kMinSystemStatsWindowSec = 5;
constexpr uint32_t kMaxSystemStatsWindowSec = 300;

struct MultiWindowRate {
  double tick_interval_seconds;
  const double sma_max_history_seconds =
      *std::max_element(SmaWindowSec.begin(), SmaWindowSec.end());

  std::atomic<uint64_t> bytes_read_accumulator{0};
  std::atomic<uint64_t> bytes_written_accumulator{0};
  std::atomic<uint64_t> read_iops_accumulator{0};
  std::atomic<uint64_t> write_iops_accumulator{0};

  std::array<RateMetrics, EmaWindowSec.size()> ema{};
  std::array<RateMetrics, SmaWindowSec.size()> sma{};

  eos::fst::traffic_shaping::SlidingWindowStats bytes_read_window;
  eos::fst::traffic_shaping::SlidingWindowStats bytes_written_window;
  eos::fst::traffic_shaping::SlidingWindowStats iops_read_window;
  eos::fst::traffic_shaping::SlidingWindowStats iops_write_window;

  uint32_t active_stream_count = 0;
  time_t last_activity_time = 0;

  explicit MultiWindowRate(const double initial_tick_interval)
      : tick_interval_seconds(initial_tick_interval)
      , bytes_read_window(sma_max_history_seconds, initial_tick_interval)
      , bytes_written_window(sma_max_history_seconds, initial_tick_interval)
      , iops_read_window(sma_max_history_seconds, initial_tick_interval)
      , iops_write_window(sma_max_history_seconds, initial_tick_interval)
  {
  }

  void
  clear()
  {
    bytes_read_accumulator.store(0, std::memory_order_relaxed);
    bytes_written_accumulator.store(0, std::memory_order_relaxed);
    read_iops_accumulator.store(0, std::memory_order_relaxed);
    write_iops_accumulator.store(0, std::memory_order_relaxed);

    for (auto& m : ema) {
      m = RateMetrics{};
    }

    for (auto& m : sma) {
      m = RateMetrics{};
    }

    ResetWindows(tick_interval_seconds);

    active_stream_count = 0;
    last_activity_time = 0;
  }

  void
  ResetWindows(double new_tick_interval)
  {
    tick_interval_seconds = new_tick_interval;
    bytes_read_window = eos::fst::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);
    bytes_written_window = eos::fst::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);
    iops_read_window = eos::fst::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);
    iops_write_window = eos::fst::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);

    for (auto& s : sma) {
      s = RateMetrics{};
    }
  }
};

struct RateSnapshot {
  uint64_t bytes_read_accumulator = 0;
  uint64_t bytes_written_accumulator = 0;

  std::array<RateMetrics, EmaWindowSec.size()> ema{};
  std::array<RateMetrics, SmaWindowSec.size()> sma{};

  uint32_t active_stream_count = 0;
  time_t last_activity_time = 0;
};

struct StreamKey {
  std::string app;
  uint32_t uid;
  uint32_t gid;

  bool
  operator==(const StreamKey& other) const
  {
    return uid == other.uid && gid == other.gid && app == other.app;
  }
};

struct StreamKeyHash {
  std::size_t
  operator()(const StreamKey& k) const
  {
    return std::hash<std::string>{}(k.app) ^ (std::hash<uint32_t>{}(k.uid) << 1) ^
           (std::hash<uint32_t>{}(k.gid) << 2);
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

  uint64_t
  GetEffectiveWriteLimit() const
  {
    // If the policy is disabled, the user limit drops to 0.
    uint64_t active_user_limit = is_enabled ? limit_write_bytes_per_sec : 0;

    if (active_user_limit > 0 && controller_limit_write_bytes_per_sec > 0) {
      return std::min(active_user_limit, controller_limit_write_bytes_per_sec);
    }
    return active_user_limit > 0 ? active_user_limit
                                 : controller_limit_write_bytes_per_sec;
  }

  uint64_t
  GetEffectiveReadLimit() const
  {
    // If the policy is disabled, the user limit drops to 0.
    uint64_t active_user_limit = is_enabled ? limit_read_bytes_per_sec : 0;

    if (active_user_limit > 0 && controller_limit_read_bytes_per_sec > 0) {
      return std::min(active_user_limit, controller_limit_read_bytes_per_sec);
    }
    return active_user_limit > 0 ? active_user_limit
                                 : controller_limit_read_bytes_per_sec;
  }

  bool
  IsEmpty() const
  {
    return limit_write_bytes_per_sec == 0 && limit_read_bytes_per_sec == 0 &&
           reservation_write_bytes_per_sec == 0 && reservation_read_bytes_per_sec == 0 &&
           controller_limit_write_bytes_per_sec == 0 &&
           controller_limit_read_bytes_per_sec == 0;
  }

  bool
  IsActive() const
  {
    const bool has_user_rules =
        limit_write_bytes_per_sec > 0 || limit_read_bytes_per_sec > 0 ||
        reservation_write_bytes_per_sec > 0 || reservation_read_bytes_per_sec > 0;

    const bool has_controller_rules = controller_limit_write_bytes_per_sec > 0 ||
                                      controller_limit_read_bytes_per_sec > 0;

    // The policy is active if the controller has set a limit,
    // OR if the user has rules configured AND the policy is enabled.
    return has_controller_rules || (is_enabled && has_user_rules);
  }

  bool
  operator!=(const TrafficShapingPolicy& policy) const
  {
    return limit_write_bytes_per_sec != policy.limit_write_bytes_per_sec ||
           limit_read_bytes_per_sec != policy.limit_read_bytes_per_sec ||
           reservation_write_bytes_per_sec != policy.reservation_write_bytes_per_sec ||
           reservation_read_bytes_per_sec != policy.reservation_read_bytes_per_sec ||
           is_enabled != policy.is_enabled;
  }

  std::string
  ToString() const
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
};

class TrafficShapingManager {
public:
  TrafficShapingManager();

  ~TrafficShapingManager();

  void ProcessReport(const eos::traffic_shaping::FstIoReport& report);

  void UpdateEstimators(double time_delta_seconds);

  void UpdateLimits();

  void ApplyThreadConfig(uint32_t estimators_period_ms, uint32_t fst_policy_period_ms,
                         uint32_t window_seconds);

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> GetGlobalStats() const;

  std::unordered_map<std::string, RateSnapshot> GetNodeStats() const;

  RateSnapshot GetTotalStats() const;

  struct GarbageCollectionStats {
    size_t removed_nodes;
    size_t removed_node_streams;
    size_t removed_global_streams;
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

  void UpdateEstimatorsLoopMicroSec(uint64_t time_microseconds);

  void UpdateFstReportsProcessed(uint64_t count);

  double GetFstReportsProcessedPerSecondMean() const;

  std::tuple<uint64_t, uint64_t, uint64_t> GetEstimatorsUpdateLoopMicroSecStats() const;

  std::tuple<uint64_t, uint64_t, uint64_t> GetFstLimitsUpdateLoopMicroSecStats() const;

  uint32_t
  GetSystemStatsWindowSeconds() const
  {
    std::shared_lock lock(mMutex);
    return mSystemStatsWindowSeconds;
  }

  void Clear();

private:
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;

  struct NodeData {
    std::chrono::steady_clock::time_point last_report_time{};
    NodeStateMap streams;
  };

  std::unordered_map<std::string, NodeData> mNodeStates;
  std::unordered_map<StreamKey, MultiWindowRate, StreamKeyHash> mGlobalStats;
  std::unordered_map<std::string, MultiWindowRate> mNodeStats;
  // We provide an initial tick interval but this will be refreshed on initialization
  MultiWindowRate mTotalStats{0.5};

  std::unordered_map<uint32_t, TrafficShapingPolicy> mUidPolicies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> mGidPolicies;
  std::unordered_map<std::string, TrafficShapingPolicy> mAppPolicies;

  eos::traffic_shaping::TrafficShapingFstIoDelayConfig mFstIoDelayConfig;

  std::optional<eos::fst::traffic_shaping::SlidingWindowStats>
      estimators_update_loop_micro_sec;
  std::optional<eos::fst::traffic_shaping::SlidingWindowStats>
      fst_limits_update_loop_micro_sec;
  std::optional<eos::fst::traffic_shaping::SlidingWindowStats>
      fst_reports_processed_per_second;

  double mEstimatorsTickIntervalSec{0.5};
  uint32_t mSystemStatsWindowSeconds{15};

  mutable std::shared_mutex mMutex;

  static double CalculateEma(double current_val, double prev_ema, double alpha);

  // Calculates the new FST delay microsecond value given the current rate and limit
  static uint64_t CalculateDelayUs(double limit_bps, double current_rate_bps,
                                   uint64_t current_delay_us);

  std::tuple<std::unordered_map<std::string, double>, // app read
             std::unordered_map<std::string, double>, // app write
             std::unordered_map<uint32_t, double>,    // uid read
             std::unordered_map<uint32_t, double>,    // uid write
             std::unordered_map<uint32_t, double>,    // gid read
             std::unordered_map<uint32_t, double>>    // gid write
  GetCurrentReadAndWriteRates() const;
};

class TrafficShapingEngine {
public:
  TrafficShapingEngine();

  ~TrafficShapingEngine();

  void ApplyConfig();

  void Start();

  void Stop();

  void Enable();

  void Disable();

  bool
  IsEnabled() const
  {
    return mRunning;
  }

  void SyncTrafficShapingEnabledWithFst();

  std::shared_ptr<TrafficShapingManager> GetManager() const;

  void ProcessSerializedFstIoReportNonBlocking(const std::string& serialized_report);

  void ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                         uint32_t win_s, bool save_to_config_engine = true);

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

private:
  void EstimatorsUpdate(ThreadAssistant&);

  void FstIoPolicyUpdate(ThreadAssistant&) const;

  void FstTrafficShapingConfigUpdate(ThreadAssistant&);

  void AddReportToQueue(const eos::traffic_shaping::FstIoReport& report);

  void ProcessAllQueuedReports();

  void UpdateThreadConfigs();

  std::shared_ptr<TrafficShapingManager> mManager{};

  AssistedThread mEstimatorsUpdateThread;
  AssistedThread mFstIoPolicyUpdateThread;
  AssistedThread mFstTrafficShapingConfigUpdateThread;

  std::atomic<bool> mRunning{};

  std::atomic<uint32_t> mEstimatorsUpdateThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mFstIoPolicyUpdateThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mFstIoStatsReportThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mSystemStatsWindowSeconds{};

  std::vector<eos::traffic_shaping::FstIoReport> mReportQueue{};
  std::mutex mReportQueueMutex{};
};

} // namespace eos::mgm::traffic_shaping
