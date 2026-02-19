#pragma once

#include "common/AssistedThread.hh"
#include "fst/storage/TrafficShapingStats.hh"
#include "proto/Shaping.pb.h"
#include <atomic>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace eos::mgm {
// -----------------------------------------------------------------------------
// 1. Per-Node State (For Delta Calculation)
// -----------------------------------------------------------------------------
// Tracks the last raw counter received from a specific FST.
// Used solely to calculate: Delta = Current_Counter - Last_Counter
struct StreamState {
  uint64_t last_bytes_read = 0;
  uint64_t last_bytes_written = 0;
  uint64_t last_iops_read = 0;
  uint64_t last_iops_write = 0;

  uint64_t generation_id = 0;
  time_t last_update_time = 0;
};

// -----------------------------------------------------------------------------
// 2. Global Aggregated State (For Rate Calculation)
// -----------------------------------------------------------------------------
// Tracks the aggregated speed of a User/App across the entire cluster.
// 1. Internal State (Keep as is)
struct MultiWindowRate {
  inline static double tick_interval_seconds = 0.1;
  inline static double sma_max_history_seconds = 300.0;

  std::atomic<uint64_t> bytes_read_accumulator{0};
  std::atomic<uint64_t> bytes_written_accumulator{0};
  std::atomic<uint64_t> read_iops_accumulator{0};  // Added
  std::atomic<uint64_t> write_iops_accumulator{0}; // Added

  // --- EMA Storage ---
  double read_rate_ema_5s = 0;
  double read_iops_ema_5s = 0;
  double write_rate_ema_5s = 0;
  double write_iops_ema_5s = 0;

  double read_rate_ema_1m = 0;
  double read_iops_ema_1m = 0;
  double write_rate_ema_1m = 0;
  double write_iops_ema_1m = 0;

  double read_rate_ema_5m = 0;
  double read_iops_ema_5m = 0;
  double write_rate_ema_5m = 0;
  double write_iops_ema_5m = 0;
  // ... (1m and 5m fields) ...

  // --- SMA Storage (The Circular Buffers) ---
  // We need one buffer per metric type
  eos::fst::SlidingWindowStats bytes_read_window{sma_max_history_seconds, tick_interval_seconds};
  eos::fst::SlidingWindowStats bytes_written_window{sma_max_history_seconds, tick_interval_seconds};
  eos::fst::SlidingWindowStats iops_read_window{sma_max_history_seconds, tick_interval_seconds};
  eos::fst::SlidingWindowStats iops_write_window{sma_max_history_seconds, tick_interval_seconds};

  // --- SMA Calculated Values (Cached for Snapshot) ---
  double read_rate_sma_5s = 0;
  double write_rate_sma_5s = 0;
  double read_iops_sma_5s = 0;
  double write_iops_sma_5s = 0;

  double read_rate_sma_1m = 0;
  double write_rate_sma_1m = 0;
  double read_iops_sma_1m = 0;
  double write_iops_sma_1m = 0;

  double read_rate_sma_5m = 0;
  double write_rate_sma_5m = 0;
  double read_iops_sma_5m = 0;
  double write_iops_sma_5m = 0;

  uint32_t active_stream_count = 0;
  time_t last_activity_time = 0;
};

// 2. NEW: Snapshot State (For Return/Copying)
// This is exactly the same as above but WITHOUT std::atomic
struct RateSnapshot {
  uint64_t bytes_read_accumulator = 0;
  uint64_t bytes_written_accumulator = 0;

  double read_rate_ema_5s = 0;
  double read_iops_ema_5s = 0;
  double write_rate_ema_5s = 0;
  double write_iops_ema_5s = 0;

  double read_rate_ema_1m = 0;
  double read_iops_ema_1m = 0;
  double write_rate_ema_1m = 0;
  double write_iops_ema_1m = 0;

  double read_rate_ema_5m = 0;
  double read_iops_ema_5m = 0;
  double write_rate_ema_5m = 0;
  double write_iops_ema_5m = 0;

  double read_rate_sma_5s = 0;
  double write_rate_sma_5s = 0;
  double read_iops_sma_5s = 0;
  double write_iops_sma_5s = 0;

  double read_rate_sma_1m = 0;
  double write_rate_sma_1m = 0;
  double read_iops_sma_1m = 0;
  double write_iops_sma_1m = 0;

  double read_rate_sma_5m = 0;
  double write_rate_sma_5m = 0;
  double read_iops_sma_5m = 0;
  double write_iops_sma_5m = 0;

  uint32_t active_stream_count = 0;
  time_t last_activity_time = 0;
};

// -----------------------------------------------------------------------------
// Keys & Hashes
// -----------------------------------------------------------------------------
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
    // Combine hashes efficiently
    return std::hash<std::string>{}(k.app) ^ (std::hash<uint32_t>{}(k.uid) << 1) ^ (std::hash<uint32_t>{}(k.gid) << 2);
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
    return limit_write_bytes_per_sec == 0 && limit_read_bytes_per_sec == 0 && reservation_write_bytes_per_sec == 0 &&
           reservation_read_bytes_per_sec == 0;
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
           reservation_read_bytes_per_sec != policy.reservation_read_bytes_per_sec || is_enabled != policy.is_enabled;
  }
};

// -----------------------------------------------------------------------------
// Class: TrafficShapingManager
// -----------------------------------------------------------------------------
class TrafficShapingManager {
public:
  TrafficShapingManager();

  ~TrafficShapingManager();

  // --- Fast Path (RPC Threads) ---
  // 1. Looks up NodeState to calculate Delta.
  // 2. Adds Delta to GlobalStats Accumulator.
  void process_report(const Shaping::FstIoReport& report);

  // --- Slow Path (Background Timer) ---
  // Called once per second.
  // 1. Reads Accumulators.
  // 2. Calculates EMAs (1s, 1m, 5m).
  // 3. Resets Accumulators.
  void UpdateTimeWindows(double time_delta_seconds);

  void ComputeLimitsAndReservations();

  // --- Monitoring API ---
  // Returns a snapshot of the calculated rates for dashboards.
  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> GetGlobalStats() const;

  // Cleanup old streams

  struct GarbageCollectionStats {
    size_t removed_nodes;
    size_t removed_node_streams;
    size_t removed_global_streams;
  };

  GarbageCollectionStats garbage_collect(int max_idle_seconds = 300);

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

private:
  // A. The Per-Node Map (NodeID -> StreamKey -> RawCounters)
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;
  std::unordered_map<std::string, NodeStateMap> mNodeStates;

  // B. The Global Map (StreamKey -> EMAs)
  std::unordered_map<StreamKey, MultiWindowRate, StreamKeyHash> mGlobalStats;

  // Policy map (limits / reservations)
  std::unordered_map<uint32_t, TrafficShapingPolicy> mUidPolicies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> mGidPolicies;
  std::unordered_map<std::string, TrafficShapingPolicy> mAppPolicies;

  eos::traffic_shaping::TrafficShapingFstIoDelayConfig mFstIoDelayConfig;

  // Synchronization
  mutable std::shared_mutex mMutex;

  // Internal Helper
  static double CalculateEma(double current_val, double prev_ema, double alpha);

  std::pair<std::unordered_map<std::string, double>, std::unordered_map<std::string, double>>
  GetCurrentReadAndWriteRateForApps() const;
};

class TrafficShapingEngine {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  TrafficShapingEngine();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~TrafficShapingEngine();

  //----------------------------------------------------------------------------
  //! Start the background ticker thread
  //----------------------------------------------------------------------------
  void Start();

  //----------------------------------------------------------------------------
  //! Stop the background ticker thread
  //----------------------------------------------------------------------------
  void Stop();

  //----------------------------------------------------------------------------
  //! Get the Logic Engine (Brain)
  //!
  //! This shared pointer should be passed to the gRPC Service so it can
  //! ingest reports into the same memory this engine is updating.
  //----------------------------------------------------------------------------
  std::shared_ptr<eos::mgm::TrafficShapingManager> GetBrain() const;

  void ProcessSerializedFstIoReportNonBlocking(const std::string& serialized_report);

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

private:
  //----------------------------------------------------------------------------
  //! The main loop running at 1Hz
  //! Uses sleep_until to ensure drift-free timing.
  //----------------------------------------------------------------------------
  void TickerLoop(ThreadAssistant&);

  void FstIoPolicyUpdate(ThreadAssistant&);

  void AddReportToQueue(const Shaping::FstIoReport& report);

  void ProcessAllQueuedReports();

  // --- Members ---
  std::shared_ptr<eos::mgm::TrafficShapingManager> mBrain;
  AssistedThread mTickerThread;
  AssistedThread mFstIoPolicyUpdateThread;

  std::atomic<bool> mRunning;

  // fifo queue for pair of (id, Shaping::FstIoReport)

  // queue for incoming io reports from FST. We don't process these in the message handler to avoid blocking
  // This is used as a double buffering queue for minimum blocking since the lock takes place in the message handler
  std::vector<Shaping::FstIoReport> mReportQueue;
  std::mutex mReportQueueMutex;
};
} // namespace eos::mgm
