#include "common/ioMonitor/include/BrainIoIngestor.hh"
#include "common/Logging.hh"

namespace eos::common {
// -----------------------------------------------------------------------------
// Constructor / Destructor
// -----------------------------------------------------------------------------
BrainIoIngestor::BrainIoIngestor() {}

BrainIoIngestor::~BrainIoIngestor() {}

// -----------------------------------------------------------------------------
// Helper: Exponential Moving Average Calculation
// -----------------------------------------------------------------------------
double BrainIoIngestor::CalculateEma(double current_val, double prev_ema, double alpha) {
  // Standard EMA formula:
  // EMA_today = (Value_today * alpha) + (EMA_yesterday * (1 - alpha))
  return (alpha * current_val) + ((1.0 - alpha) * prev_ema);
}

// -----------------------------------------------------------------------------
// Fast Path: Process Report from FST
// -----------------------------------------------------------------------------
void BrainIoIngestor::process_report(const eos::ioshapping::FstIoReport& report) {
  std::string node_id = report.node_id();
  time_t now = time(nullptr);

  // Global Write Lock (Simpler for now, we will shard this later)
  // This protects both mNodeStates and mGlobalStats map insertions
  std::unique_lock lock(mMutex);

  // Get or create the state map for this node
  NodeStateMap& node_map = mNodeStates[node_id];

  for (const auto& entry : report.entries()) {
    StreamKey key{entry.app_name(), entry.uid(), entry.gid()};
    eos_static_info("msg=\"Processing IoReport entry\" node=%s app=%s uid=%u gid=%u bytes_read=%lu bytes_written=%lu "
                    "read_ops=%lu write_ops=%lu gen_id=%lu",
                    node_id.c_str(), key.app.c_str(), key.uid, key.gid, entry.total_bytes_read(),
                    entry.total_bytes_written(), entry.total_read_ops(), entry.total_write_ops(),
                    entry.generation_id());

    // --- 1. Fetch Previous Node State ---
    StreamState& state = node_map[key];

    // --- 2. Calculate Deltas ---
    uint64_t delta_bytes_read = 0;
    uint64_t delta_bytes_written = 0;
    uint64_t delta_iops_read = 0;
    uint64_t delta_iops_write = 0;

    // Check Generation ID (Detect Restarts)
    if (state.generation_id != entry.generation_id()) {
      // New Session / Restart detected
      if (state.generation_id != 0) {
        eos_static_debug("msg=\"Stream reset\" node=%s app=%s uid=%u old_gen=%lu new_gen=%lu", node_id.c_str(),
                         key.app.c_str(), key.uid, state.generation_id, entry.generation_id());
      }

      state.generation_id = entry.generation_id();

      // Assume entire value is new traffic (started from 0)
      delta_bytes_read = entry.total_bytes_read();
      delta_bytes_written = entry.total_bytes_written();
      delta_iops_read = entry.total_read_ops();
      delta_iops_write = entry.total_write_ops();
    } else {
      // Standard Monotonic Increase
      if (entry.total_bytes_read() >= state.last_bytes_read) {
        delta_bytes_read = entry.total_bytes_read() - state.last_bytes_read;
      }

      if (entry.total_bytes_written() >= state.last_bytes_written) {
        delta_bytes_written = entry.total_bytes_written() - state.last_bytes_written;
      }

      if (entry.total_read_ops() >= state.last_iops_read) {
        delta_iops_read = entry.total_read_ops() - state.last_iops_read;
      }

      if (entry.total_write_ops() >= state.last_iops_write) {
        delta_iops_write = entry.total_write_ops() - state.last_iops_write;
      }
    }

    // --- 3. Update Node State (Memory) ---
    state.last_bytes_read = entry.total_bytes_read();
    state.last_bytes_written = entry.total_bytes_written();
    state.last_iops_read = entry.total_read_ops();
    state.last_iops_write = entry.total_write_ops();
    state.last_update_time = now;

    // --- 4. Update Global Aggregates ---
    if (delta_bytes_read > 0 || delta_bytes_written > 0 || delta_iops_read > 0 || delta_iops_write > 0) {
      eos_static_info("msg=\"Updating global stats\" node=%s app=%s uid=%u gid=%u read_delta=%lu write_delta=%lu "
                      "iops_read_delta=%lu iops_write_delta=%lu",
                      node_id.c_str(), key.app.c_str(), key.uid, key.gid, delta_bytes_read, delta_bytes_written,
                      delta_iops_read, delta_iops_write);

      // Get global entry (creates it if missing)
      MultiWindowRate& global = mGlobalStats[key];

      // Add to accumulators (Atomic add is safe here, but we are under lock anyway)
      global.bytes_read_accumulator += delta_bytes_read;
      global.bytes_written_accumulator += delta_bytes_written;
      // Note: We need to add IOPS accumulators to MultiWindowRate struct in header if we want to track them
      // For now, assuming you might add them or we skip accurate IOPS global aggregation for this step.
      // Let's assume you simply add them if the struct supports it, otherwise skip.

      global.last_activity_time = now;

      // Debug logging for high traffic
      if (delta_bytes_read > 10 * 1024 * 1024) {
        // > 10 MB spike
        eos_static_debug("msg=\"Heavy IO detected\" node=%s app=%s read_delta=%lu", node_id.c_str(), key.app.c_str(),
                         delta_bytes_read);
      }

      // print size of mGlobalStats
      eos_static_info("msg=\"Current number of entries in global stats\" count=%zu", mGlobalStats.size());
    }
  }
}

// -----------------------------------------------------------------------------
// Slow Path: Update Time Windows (Called every 1 second)
// -----------------------------------------------------------------------------
void BrainIoIngestor::UpdateTimeWindows() {
  // Write lock needed because we modify the rate values in the map
  std::unique_lock lock(mMutex);

  // --- Configuration: EMA Alphas ---
  // Alpha = 2 / (N + 1)
  constexpr double kAlpha5s = 0.3333; // ~5 seconds (Instant/Spiky)
  constexpr double kAlpha1m = 0.0328; // ~1 minute (Stable)
  constexpr double kAlpha5m = 0.0066; // ~5 minutes (Trend)

  // print how many items in global stats
  eos_static_info("msg=\"Updating IoStats time windows\" mGlobalStats.size=%zu", mGlobalStats.size());
  for (auto& [key, stats] : mGlobalStats) {
    // 1. Snapshot and Reset Accumulators
    // exchange(0) atomically reads the value and sets it to 0 for the next cycle
    uint64_t bytes_read_now = stats.bytes_read_accumulator.exchange(0);
    uint64_t bytes_written_now = stats.bytes_written_accumulator.exchange(0);

    // 2. Calculate Instant Rate (Bytes/Sec for this last second)
    double current_read_bps = (double)bytes_read_now;
    double current_write_bps = (double)bytes_written_now;

    // 3. Update Moving Averages (EMA)

    // Helper macro or lambda to update a specific set of rates
    auto update_rate_set = [&](double current, double& r5s, double& r1m, double& r5m) {
      if (r5s == 0.0 && current > 0) {
        // Cold start: jump directly to current value to avoid long ramp-up
        r5s = current;
        r1m = current;
        r5m = current;
      } else {
        r5s = CalculateEma(current, r5s, kAlpha5s);
        r1m = CalculateEma(current, r1m, kAlpha1m);
        r5m = CalculateEma(current, r5m, kAlpha5m);
      }
    };

    update_rate_set(current_read_bps, stats.read_rate_5s, stats.read_rate_1m, stats.read_rate_5m);
    update_rate_set(current_write_bps, stats.write_rate_5s, stats.write_rate_1m, stats.write_rate_5m);

    // print info using eos_static_info
    eos_static_info("msg=\"Updated IoStats rates\" app=%s uid=%u gid=%u read_bps=%.2f write_bps=%.2f "
                    "read_rate_5s=%.2f read_rate_1m=%.2f read_rate_5m=%.2f "
                    "write_rate_5s=%.2f write_rate_1m=%.2f write_rate_5m=%.2f",
                    key.app.c_str(), key.uid, key.gid, current_read_bps, current_write_bps, stats.read_rate_5s,
                    stats.read_rate_1m, stats.read_rate_5m, stats.write_rate_5s, stats.write_rate_1m,
                    stats.write_rate_5m);
    // (Repeat for IOPS if you add accumulators for them)
  }
}

// -----------------------------------------------------------------------------
// Monitoring API
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// Monitoring API (Fix for std::atomic copy error)
// -----------------------------------------------------------------------------
std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> BrainIoIngestor::GetGlobalStats() const {
  std::shared_lock lock(mMutex);

  // Create a new map for the result
  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> snapshot_map;

  // Iterate over internal state and convert to snapshot
  for (const auto& [key, internal_stat] : mGlobalStats) {
    RateSnapshot& snap = snapshot_map[key];

    // Atomic Load -> Plain Int
    snap.bytes_read_accumulator = internal_stat.bytes_read_accumulator.load();
    snap.bytes_written_accumulator = internal_stat.bytes_written_accumulator.load();

    // Doubles copy normally
    snap.read_rate_5s = internal_stat.read_rate_5s;
    snap.read_rate_1m = internal_stat.read_rate_1m;
    snap.read_rate_5m = internal_stat.read_rate_5m;

    snap.write_rate_5s = internal_stat.write_rate_5s;
    snap.write_rate_1m = internal_stat.write_rate_1m;
    snap.write_rate_5m = internal_stat.write_rate_5m;

    snap.active_stream_count = internal_stat.active_stream_count;
    snap.last_activity_time = internal_stat.last_activity_time;
  }

  return snapshot_map; // This is now copyable!
}

// -----------------------------------------------------------------------------
// Garbage Collection
// -----------------------------------------------------------------------------
void BrainIoIngestor::garbage_collect(int max_idle_seconds) {
  std::unique_lock lock(mMutex);
  time_t now = time(nullptr);
  size_t removed_nodes = 0;
  size_t removed_globals = 0;

  // 1. Clean Per-Node States
  for (auto node_it = mNodeStates.begin(); node_it != mNodeStates.end();) {
    NodeStateMap& map = node_it->second;
    for (auto stream_it = map.begin(); stream_it != map.end();) {
      if (now - stream_it->second.last_update_time > max_idle_seconds) {
        stream_it = map.erase(stream_it);
      } else {
        ++stream_it;
      }
    }
    if (map.empty()) {
      node_it = mNodeStates.erase(node_it);
      removed_nodes++;
    } else {
      ++node_it;
    }
  }

  // 2. Clean Global Stats
  // We remove global entries if they haven't been active recently.
  // Note: last_activity_time should be updated in process_report
  for (auto it = mGlobalStats.begin(); it != mGlobalStats.end();) {
    if (now - it->second.last_activity_time > max_idle_seconds) {
      it = mGlobalStats.erase(it);
      removed_globals++;
    } else {
      ++it;
    }
  }

  if (removed_nodes > 0 || removed_globals > 0) {
    eos_static_info("msg=\"IoStats GC\" removed_nodes=%lu removed_global_streams=%lu", removed_nodes, removed_globals);
  }
}
} // namespace eos::common
