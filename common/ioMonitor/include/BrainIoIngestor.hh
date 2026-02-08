#pragma once

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

  double read_rate_5s = 0;
  double read_rate_1m = 0;
  double read_rate_5m = 0;

  double write_rate_5s = 0;
  double write_rate_1m = 0;
  double write_rate_5m = 0;

  // Metadata
  uint32_t active_stream_count = 0;
  time_t last_activity_time = 0;
};

// 2. NEW: Snapshot State (For Return/Copying)
// This is exactly the same as above but WITHOUT std::atomic
struct RateSnapshot {
  uint64_t bytes_read_accumulator = 0;
  uint64_t bytes_written_accumulator = 0;

  double read_rate_5s = 0;
  double read_rate_1m = 0;
  double read_rate_5m = 0;

  double write_rate_5s = 0;
  double write_rate_1m = 0;
  double write_rate_5m = 0;

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
  void process_report(const eos::ioshapping::FstIoReport& report);

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
  void garbage_collect(int max_idle_seconds = 300);

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
