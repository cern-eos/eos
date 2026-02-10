#pragma once

#include "IoStatsCollector.hh"
#include "proto/TrafficShaping.pb.h"
#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace eos::common {
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
  SlidingWindowStats bytes_read_window;
  SlidingWindowStats bytes_written_window;
  SlidingWindowStats iops_read_window;
  SlidingWindowStats iops_write_window;

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

  bool operator==(const StreamKey& other) const {
    return uid == other.uid && gid == other.gid && app == other.app;
  }
};

struct StreamKeyHash {
  std::size_t operator()(const StreamKey& k) const {
    // Combine hashes efficiently
    return std::hash<std::string>{}(k.app) ^ (std::hash<uint32_t>{}(k.uid) << 1) ^ (std::hash<uint32_t>{}(k.gid) << 2);
  }
};

// -----------------------------------------------------------------------------
// Class: BrainIoIngestor
// -----------------------------------------------------------------------------
class BrainIoIngestor {
public:
  BrainIoIngestor();

  ~BrainIoIngestor();

  // --- Fast Path (RPC Threads) ---
  // 1. Looks up NodeState to calculate Delta.
  // 2. Adds Delta to GlobalStats Accumulator.
  void process_report(const eos::traffic_shaping::FstIoReport& report);

  // --- Slow Path (Background Timer) ---
  // Called once per second.
  // 1. Reads Accumulators.
  // 2. Calculates EMAs (1s, 1m, 5m).
  // 3. Resets Accumulators.
  void UpdateTimeWindows(double time_delta_seconds);

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

private:
  // A. The Per-Node Map (NodeID -> StreamKey -> RawCounters)
  using NodeStateMap = std::unordered_map<StreamKey, StreamState, StreamKeyHash>;
  std::unordered_map<std::string, NodeStateMap> mNodeStates;

  // B. The Global Map (StreamKey -> EMAs)
  std::unordered_map<StreamKey, MultiWindowRate, StreamKeyHash> mGlobalStats;

  // Synchronization
  mutable std::shared_mutex mMutex;

  // Internal Helper
  static double CalculateEma(double current_val, double prev_ema, double alpha);
};
} // namespace eos::common
