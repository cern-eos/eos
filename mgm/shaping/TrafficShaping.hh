#pragma once

#include "common/AssistedThread.hh"
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
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace eos::mgm::traffic_shaping {

extern "C" {
// Function signatures for the hot-reloaded plugin
struct DelayState {
  double limit_bps;
  double current_rate_bps;
  uint64_t current_delay_us;
  double io_pressure;
  bool has_rate_sample;
  bool allow_idle_release;
  double delay_reference_bps;
  double io_pressure_threshold;
};

typedef uint64_t (*DelayAlgoFunc)(const DelayState* state);

// A flat, simple struct containing all inputs and outputs for ONE app
struct AppState {
  char app_name[128]; // Fixed size so we don't mess with pointers

  // Inputs from MGM -> Plugin
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
  uint64_t controller_min_limit_bps;
  double io_pressure_threshold;
  bool has_read_io_pressure;
  bool has_write_io_pressure;
  bool has_read_reservation_competition;
  bool has_write_reservation_competition;

  // Outputs from Plugin -> MGM
  uint64_t new_controller_limit_write_bps;
  uint64_t new_controller_limit_read_bps;
  bool update_write; // Set to true if the plugin wants to apply the new write limit
  bool update_read;  // Set to true if the plugin wants to apply the new read limit
  uint64_t new_reservation_write_bps;
  uint64_t new_reservation_read_bps;
  bool update_reservation_write;
  bool update_reservation_read;
};

// Pass the pure data array to avoid C++ ABI name mangling and linking issues
typedef void (*ControllerAlgoFunc)(AppState* apps, size_t num_apps);
}

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

constexpr std::array<int, 2> EmaWindowSec = {1, 5};
constexpr std::array<int, 5> SmaWindowSec = {1, 5, 15, 60, 300};

enum EmaIdx : size_t { Ema1s = 0, Ema5s = 1 };

enum SmaIdx : size_t { Sma1s = 0, Sma5s = 1, Sma15s = 2, Sma1m = 3, Sma5m = 4 };

constexpr uint32_t kMinThreadPeriodMs = 50;
constexpr uint32_t kMaxThreadPeriodMs = 3000;
constexpr uint32_t kMinSystemStatsWindowSec = 5;
constexpr uint32_t kMaxSystemStatsWindowSec = 300;
constexpr uint64_t kDefaultControllerMinLimitBps = 100ULL * 1000ULL * 1000ULL;
constexpr double kDefaultIoPressureThreshold = 0.1;

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

  eos::common::traffic_shaping::SlidingWindowStats bytes_read_window;
  eos::common::traffic_shaping::SlidingWindowStats bytes_written_window;
  eos::common::traffic_shaping::SlidingWindowStats iops_read_window;
  eos::common::traffic_shaping::SlidingWindowStats iops_write_window;

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
    bytes_read_window = eos::common::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);
    bytes_written_window = eos::common::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);
    iops_read_window = eos::common::traffic_shaping::SlidingWindowStats(
        sma_max_history_seconds, tick_interval_seconds);
    iops_write_window = eos::common::traffic_shaping::SlidingWindowStats(
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
      const std::unordered_map<std::string, double>& node_io_pressure);

  void SetLimitsEnabled(bool enabled);

  bool GetLimitsEnabled() const;

  void SetReservationsEnabled(bool enabled);

  bool GetReservationsEnabled() const;

  void SetControllerMinLimit(uint64_t limit_bps);

  uint64_t GetControllerMinLimit() const;

  void SetIoPressureThreshold(double threshold);

  double GetIoPressureThreshold() const;

  size_t ClearControllerLimits();

  size_t ExpireControllerLimits(std::chrono::steady_clock::time_point now);

  void ApplyThreadConfig(uint32_t estimators_period_ms, uint32_t fst_policy_period_ms,
                         uint32_t window_seconds);

  void SetFilesystemDetailEnabled(bool enabled);

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> GetGlobalStats() const;

  std::unordered_map<std::string, RateSnapshot> GetNodeStats() const;

  std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash> GetDiskStats() const;

  std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash> GetDetailedStats() const;

  RateSnapshot GetTotalStats() const;

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

  std::tuple<std::unordered_map<std::string, double>, // app read
             std::unordered_map<std::string, double>, // app write
             std::unordered_map<uint32_t, double>,    // uid read
             std::unordered_map<uint32_t, double>,    // uid write
             std::unordered_map<uint32_t, double>,    // gid read
             std::unordered_map<uint32_t, double>>    // gid write
  GetCurrentReadAndWriteRates() const;

  std::unordered_map<std::string, AppIoPressureSnapshot> GetReservedAppIoPressure() const;

  std::vector<AppNodeIoPressureSnapshot> GetReservedAppNodeIoPressure() const;

  void Clear();

  void ClearRuntimeStats();

  void ClearDetailedRuntimeStats();

private:
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;

  struct NodeData {
    std::chrono::steady_clock::time_point last_report_time{};
    NodeStateMap streams;
  };

  std::unordered_map<std::string, NodeData> mNodeStates;
  std::unordered_map<StreamKey, MultiWindowRate, StreamKeyHash> mGlobalStats;
  std::unordered_map<std::string, MultiWindowRate> mNodeStats;
  std::unordered_map<DiskKey, MultiWindowRate, DiskKeyHash> mDiskStats;
  std::unordered_map<DetailedKey, MultiWindowRate, DetailedKeyHash> mDetailedStats;
  std::unordered_map<DetailedKey, MultiWindowRate, DetailedKeyHash> mNodeEntityStats;
  // We provide an initial tick interval but this will be refreshed on initialization
  MultiWindowRate mTotalStats{0.5};

  std::unordered_map<uint32_t, TrafficShapingPolicy> mUidPolicies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> mGidPolicies;
  std::unordered_map<std::string, TrafficShapingPolicy> mAppPolicies;

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
      fst_limits_update_loop_micro_sec;
  std::optional<eos::common::traffic_shaping::SlidingWindowStats>
      fst_reports_processed_per_second;

  double mEstimatorsTickIntervalSec{0.5};
  uint32_t mSystemStatsWindowSeconds{15};
  std::atomic<bool> mFilesystemDetailEnabled{false};

  mutable std::shared_mutex mMutex;

#ifdef IN_TEST_HARNESS
public:
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
      double io_pressure_threshold = kDefaultIoPressureThreshold);

  static bool
  ShouldEmitDelayForPolicy(const TrafficShapingPolicy& policy, bool is_write,
                           double node_rate_bps, double io_pressure,
                           bool node_has_pressured_reservation, bool limits_enabled,
                           bool reservations_enabled,
                           double io_pressure_threshold = kDefaultIoPressureThreshold);
#ifdef IN_TEST_HARNESS
private:
#endif

  size_t ClearControllerLimitsUnlocked();

  // --- Plugin Hot-Reload State ---
  void* mPluginHandle = nullptr;
  DelayAlgoFunc mCustomAlgo = nullptr;
  ControllerAlgoFunc mCustomControllerAlgo = nullptr;
  time_t mPluginLastModified = 0;
  std::chrono::steady_clock::time_point mNextPluginCheckTime{};
  std::shared_mutex mPluginMutex;

  void LoadPluginIfModified();

  std::atomic<bool> mLimitsEnabled{true};
  std::atomic<bool> mReservationsEnabled{true};
  std::atomic<uint64_t> mControllerMinLimitBps{kDefaultControllerMinLimitBps};
  std::atomic<double> mIoPressureThreshold{kDefaultIoPressureThreshold};
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

  void SetEnabled(bool enabled);

  bool
  IsEnabled() const
  {
    return mRunning;
  }

  void SyncTrafficShapingEnabledWithFst();

  void SyncTrafficShapingConfigWithFst();

  std::shared_ptr<TrafficShapingManager> GetManager() const;

  void ProcessSerializedFstIoReportNonBlocking(const std::string& serialized_report);

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

  void SetLimitsEnabled(bool enabled);

  bool GetLimitsEnabled() const;

  void SetReservationsEnabled(bool enabled);

  bool GetReservationsEnabled() const;

  void SetControllerMinLimit(uint64_t limit_bps);

  uint64_t GetControllerMinLimit() const;

  void SetIoPressureThreshold(double threshold);

  double GetIoPressureThreshold() const;

#ifdef IN_TEST_HARNESS
public:
#else
private:
#endif
  void ApplyEnabledConfig(bool enabled);

  void StoreEnabledConfig(bool enabled);

  void StopRuntime();

  void EnsureFstEnabledSyncThread();

  void StopFstEnabledSyncThread();

  std::vector<std::string> GetOnlineFstNodeNames() const;

  bool ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                         uint32_t win_s);

  void SetThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms, uint32_t win_s);

  void StoreThreadConfig();

  bool ApplyDetailLevelConfig(const std::string& detail_level);

  static void StoreDetailLevelConfig(const std::string& detail_level);

  bool ApplyLimitsEnabledConfig(bool enabled);

  static void StoreLimitsEnabledConfig(bool enabled);

  bool ApplyReservationsEnabledConfig(bool enabled);

  static void StoreReservationsEnabledConfig(bool enabled);

  bool ApplyControllerMinLimitConfig(uint64_t limit_bps);

  static void StoreControllerMinLimitConfig(uint64_t limit_bps);

  bool ApplyIoPressureThresholdConfig(double threshold);

  static void StoreIoPressureThresholdConfig(double threshold);

  void EstimatorsUpdate(ThreadAssistant&);

  void FstIoPolicyUpdate(ThreadAssistant&) const;

  void FstTrafficShapingEnabledUpdate(ThreadAssistant&);

  void AddReportToQueue(const eos::traffic_shaping::FstIoReport& report);

  void ProcessAllQueuedReports();

  std::shared_ptr<TrafficShapingManager> mManager{};

  AssistedThread mEstimatorsUpdateThread;
  AssistedThread mFstIoPolicyUpdateThread;
  AssistedThread mFstTrafficShapingEnabledUpdateThread;
  bool mFstEnabledSyncThreadStarted = false;
  std::mutex mFstEnabledSyncThreadMutex;

  std::atomic<bool> mRunning{};

  std::atomic<uint32_t> mEstimatorsUpdateThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mFstIoPolicyUpdateThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mFstIoStatsReportThreadPeriodMilliseconds{};
  std::atomic<uint32_t> mSystemStatsWindowSeconds{};
  std::atomic<bool> mFilesystemDetailEnabled{};
  std::atomic<bool> mLimitsEnabled{true};
  std::atomic<bool> mReservationsEnabled{true};
  std::atomic<uint64_t> mControllerMinLimitBps{kDefaultControllerMinLimitBps};
  std::atomic<double> mIoPressureThreshold{kDefaultIoPressureThreshold};

  std::vector<eos::traffic_shaping::FstIoReport> mReportQueue{};
  std::mutex mReportQueueMutex{};
};

} // namespace eos::mgm::traffic_shaping
