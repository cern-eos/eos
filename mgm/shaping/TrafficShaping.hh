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
  time_t last_update_time = 0;
};

struct RateMetrics {
  double read_rate_bps = 0.0;
  double write_rate_bps = 0.0;
  double read_iops = 0.0;
  double write_iops = 0.0;
};

constexpr std::array<int, 2> EmaWindowSec = {1, 5};
constexpr std::array<int, 4> SmaWindowSec = {1, 5, 60, 300};

enum EmaIdx : size_t { Ema1s = 0, Ema5s = 1 };
enum SmaIdx : size_t { Sma1s = 0, Sma5s = 1, Sma1m = 2, Sma5m = 3 };

inline uint32_t estimatorsUpdateThreadPeriodMilliseconds = 200;

struct MultiWindowRate {
  inline static double tick_interval_seconds =
      estimatorsUpdateThreadPeriodMilliseconds * 0.001;
  inline static double sma_max_history_seconds =
      *std::max_element(SmaWindowSec.begin(), SmaWindowSec.end());

  std::atomic<uint64_t> bytes_read_accumulator{0};
  std::atomic<uint64_t> bytes_written_accumulator{0};
  std::atomic<uint64_t> read_iops_accumulator{0};
  std::atomic<uint64_t> write_iops_accumulator{0};

  std::array<RateMetrics, EmaWindowSec.size()> ema{};
  std::array<RateMetrics, SmaWindowSec.size()> sma{};

  eos::fst::SlidingWindowStats bytes_read_window{sma_max_history_seconds,
                                                 tick_interval_seconds};
  eos::fst::SlidingWindowStats bytes_written_window{sma_max_history_seconds,
                                                    tick_interval_seconds};
  eos::fst::SlidingWindowStats iops_read_window{sma_max_history_seconds,
                                                tick_interval_seconds};
  eos::fst::SlidingWindowStats iops_write_window{sma_max_history_seconds,
                                                 tick_interval_seconds};

  uint32_t active_stream_count = 0;
  time_t last_activity_time = 0;
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
  uint64_t limit_write_bytes_per_sec = 0;
  uint64_t limit_read_bytes_per_sec = 0;
  uint64_t reservation_write_bytes_per_sec = 0;
  uint64_t reservation_read_bytes_per_sec = 0;

  bool is_enabled = true;

  bool
  IsEmpty() const
  {
    return limit_write_bytes_per_sec == 0 && limit_read_bytes_per_sec == 0 &&
           reservation_write_bytes_per_sec == 0 && reservation_read_bytes_per_sec == 0;
  }

  bool
  IsActive() const
  {
    return is_enabled && !IsEmpty();
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
};

class TrafficShapingManager {
public:
  TrafficShapingManager();

  ~TrafficShapingManager();

  void ProcessReport(const eos::traffic_shaping::FstIoReport& report);

  void UpdateEstimators(double time_delta_seconds);

  void ComputeLimitsAndReservations();

  // Returns a snapshot of the calculated rates for dashboards.
  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> GetGlobalStats() const;

  struct GarbageCollectionStats {
    size_t removed_nodes;
    size_t removed_node_streams;
    size_t removed_global_streams;
  };

  GarbageCollectionStats GarbageCollect(int max_idle_seconds = 300);

  void SetUidPolicy(uint32_t uid, const TrafficShapingPolicy& policy);

  void SetGidPolicy(uint32_t gid, const TrafficShapingPolicy& policy);

  void SetAppPolicy(const std::string& app, const TrafficShapingPolicy& policy);

  void RemoveUidPolicy(uint32_t uid);

  void RemoveGidPolicy(uint32_t gid);

  void RemoveAppPolicy(const std::string& app);

  std::unordered_map<uint32_t, TrafficShapingPolicy> GetUidPolicies() const;

  std::unordered_map<uint32_t, TrafficShapingPolicy> GetGidPolicies() const;

  std::unordered_map<std::string, TrafficShapingPolicy> GetAppPolicies() const;

  std::optional<TrafficShapingPolicy> GetUidPolicy(uint32_t uid) const;

  std::optional<TrafficShapingPolicy> GetGidPolicy(uint32_t gid) const;

  std::optional<TrafficShapingPolicy> GetAppPolicy(const std::string& app) const;

  // used to store the max loop time in the past 5 seconds for both loops, to help with
  // tuning the tick interval and ensuring we don't have bottlenecks in the processing
  // loop these sliding windows will be refreshed whenever the respective loop updates its
  // tick time
  eos::fst::SlidingWindowStats estimators_update_loop_micro_sec{5.0, 1.0};
  eos::fst::SlidingWindowStats fst_limits_update_loop_micro_sec{5.0, 1.0};

  void
  UpdateFstLimitsLoopMicroSec(const uint64_t time_microseconds)
  {
    std::unique_lock lock(mMutex);
    fst_limits_update_loop_micro_sec.Add(time_microseconds);
    fst_limits_update_loop_micro_sec.Tick();
  }

  void
  UpdateEstimatorsLoopMicroSec(const uint64_t time_microseconds)
  {
    std::unique_lock lock(mMutex);
    estimators_update_loop_micro_sec.Add(time_microseconds);
    estimators_update_loop_micro_sec.Tick();
  }

  std::tuple<double, uint64_t, uint64_t>
  GetEstimatorsUpdateLoopMicroSecStats() const
  {
    std::shared_lock lock(mMutex);
    return {
        estimators_update_loop_micro_sec.GetMean(),
        estimators_update_loop_micro_sec.GetMin(),
        estimators_update_loop_micro_sec.GetMax(),
    };
  }

  std::tuple<double, uint64_t, uint64_t>
  GetFstLimitsUpdateLoopMicroSecStats() const
  {
    std::shared_lock lock(mMutex);
    return {
        fst_limits_update_loop_micro_sec.GetMean(),
        fst_limits_update_loop_micro_sec.GetMin(),
        fst_limits_update_loop_micro_sec.GetMax(),
    };
  }

private:
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;
  std::unordered_map<std::string, NodeStateMap> mNodeStates;

  std::unordered_map<StreamKey, MultiWindowRate, StreamKeyHash> mGlobalStats;

  std::unordered_map<uint32_t, TrafficShapingPolicy> mUidPolicies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> mGidPolicies;
  std::unordered_map<std::string, TrafficShapingPolicy> mAppPolicies;

  eos::traffic_shaping::TrafficShapingFstIoDelayConfig mFstIoDelayConfig;

  mutable std::shared_mutex mMutex;

  static double CalculateEma(double current_val, double prev_ema, double alpha);

  std::pair<std::unordered_map<std::string, double>,
            std::unordered_map<std::string, double>>
  GetCurrentReadAndWriteRateForApps() const;
};

class TrafficShapingEngine {
public:
  TrafficShapingEngine();

  ~TrafficShapingEngine();

  void Start();

  void Stop();

  std::shared_ptr<TrafficShapingManager> GetBrain() const;

  void ProcessSerializedFstIoReportNonBlocking(const std::string& serialized_report);

  uint32_t
  GetEstimatorsUpdateThreadPeriodMilliseconds() const
  {
    return mEstimatorsUpdateThreadPeriodMilliseconds;
  }

  uint32_t
  GetFstIoPolicyUpdateThreadPeriodMilliseconds() const
  {
    return mFstIoPolicyUpdateThreadPeriodMilliseconds;
  }

  void
  SetEstimatorsUpdateThreadPeriodMilliseconds(const uint32_t period_ms)
  {
    // Updating the period has significant consequences in the estimators, this is not
    // trivial, we need to completely reset the stats
    mEstimatorsUpdateThreadPeriodMilliseconds = period_ms;
    mBrain->estimators_update_loop_micro_sec =
        eos::fst::SlidingWindowStats(5.0, mEstimatorsUpdateThreadPeriodMilliseconds);
  }

  void
  SetFstIoPolicyUpdateThreadPeriodMilliseconds(const uint32_t period_ms)
  {
    // Updating the period has significant consequences in the estimators, this is not
    // trivial, we need to completely reset the stats
    mFstIoPolicyUpdateThreadPeriodMilliseconds = period_ms;
    mBrain->fst_limits_update_loop_micro_sec =
        eos::fst::SlidingWindowStats(5.0, mFstIoPolicyUpdateThreadPeriodMilliseconds);
  }

private:
  void EstimatorsUpdate(ThreadAssistant&);

  void FstIoPolicyUpdate(ThreadAssistant&);

  void AddReportToQueue(const eos::traffic_shaping::FstIoReport& report);

  void ProcessAllQueuedReports();

  std::shared_ptr<TrafficShapingManager> mBrain;

  AssistedThread mEstimatorsUpdateThread;
  AssistedThread mFstIoPolicyUpdateThread;

  std::atomic<bool> mRunning;

  std::atomic<uint32_t> mEstimatorsUpdateThreadPeriodMilliseconds =
      estimatorsUpdateThreadPeriodMilliseconds;
  std::atomic<uint32_t> mFstIoPolicyUpdateThreadPeriodMilliseconds = 500;

  // queue for incoming io reports from FST. We don't process these in the message handler
  // to avoid blocking This is used as a double buffering queue for minimum blocking since
  // the lock takes place in the message handler
  std::vector<eos::traffic_shaping::FstIoReport> mReportQueue;
  std::mutex mReportQueueMutex;
};

} // namespace eos::mgm::traffic_shaping
