#include "common/ioMonitor/include/BrainIoIngestor.hh"
#include "common/Logging.hh"

namespace eos::common {
// -----------------------------------------------------------------------------
// Constructor / Destructor
// -----------------------------------------------------------------------------
BrainIoIngestor::BrainIoIngestor() = default;

BrainIoIngestor::~BrainIoIngestor() = default;

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
  const std::string& node_id = report.node_id();
  const time_t now = time(nullptr);

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
void BrainIoIngestor::UpdateTimeWindows(double time_delta_seconds) {
  if (time_delta_seconds <= 0.000001) {
    // time_delta_seconds should be around 1.0 second, if it's too small, we might have a problem with the ticker or
    // system clock.
    eos_static_err("msg=\"Invalid time_delta_seconds for UpdateTimeWindows\" time_delta_seconds=%f",
                   time_delta_seconds);
    return;
  }

  // raise warning if time_delta_seconds is significantly different from 1 second (e.g., >1.5s or <0.5s)
  // The values computed by this algorithm do not make sense if the time_delta_seconds is too far from 1 second, so we
  // want to be alerted if that happens. Given the recursive nature of the calculations, errors in the past will be
  // eventually corrected over time.
  constexpr double expected_time_delta_seconds = 1.0;
  constexpr double tolerance = 0.10; // 10% tolerance
  if (time_delta_seconds < expected_time_delta_seconds * (1.0 - tolerance) ||
      time_delta_seconds > expected_time_delta_seconds * (1.0 + tolerance)) {
    eos_static_warning("msg=\"Ticker time_delta_seconds out of expected range\" time_delta_seconds=%f",
                       time_delta_seconds);
  }

  // Write lock needed because we modify the rate values in the map
  std::unique_lock lock(mMutex);

  // --- Configuration: EMA Alphas ---
  // These values are valid for time delta of around 1 second. They should be updated if the ticker interval changes.
  // Alpha = 2 / (Seconds + 1)
  constexpr double kAlpha5s = 0.33333333; // 5 seconds
  constexpr double kAlpha1m = 0.03278688; // 60 seconds
  constexpr double kAlpha5m = 0.00664452; // 300 seconds

  // print how many items in global stats
  for (auto& [key, stats] : mGlobalStats) {
    // 1. Snapshot and Reset Accumulators
    // exchange(0) atomically reads the value and sets it to 0 for the next cycle
    const uint64_t bytes_read_now = stats.bytes_read_accumulator.exchange(0);
    const uint64_t bytes_written_now = stats.bytes_written_accumulator.exchange(0);

    // 2. Calculate Instant Rate (Bytes/Sec for this last second)
    const double current_read_bps = static_cast<double>(bytes_read_now) / time_delta_seconds;
    const double current_write_bps = static_cast<double>(bytes_written_now) / time_delta_seconds;

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
    /*
    eos_static_info("msg=\"Updated IoStats rates\" app=%s uid=%u gid=%u read_bps=%.2f write_bps=%.2f "
                    "read_rate_5s=%.2f read_rate_1m=%.2f read_rate_5m=%.2f "
                    "write_rate_5s=%.2f write_rate_1m=%.2f write_rate_5m=%.2f",
                    key.app.c_str(), key.uid, key.gid, current_read_bps, current_write_bps, stats.read_rate_5s,
                    stats.read_rate_1m, stats.read_rate_5m, stats.write_rate_5s, stats.write_rate_1m,
                    stats.write_rate_5m);
                    */
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

BrainIoIngestor::GarbageCollectionStats BrainIoIngestor::garbage_collect(int max_idle_seconds) {
  std::unique_lock lock(mMutex);
  time_t now = time(nullptr);

  GarbageCollectionStats stats = {0, 0, 0};

  // ---------------------------------------------------------------------------
  // 1. Clean Per-Node States (mNodeStates)
  // ---------------------------------------------------------------------------
  // A "Node Stream" is: App 'python' running specifically on Node 'fst01'.
  // If fst01 hasn't sent an update for 'python' in 60s, remove it.
  for (auto node_it = mNodeStates.begin(); node_it != mNodeStates.end();) {
    NodeStateMap& map = node_it->second;

    // Iterate over streams (App/UID/GID) within this Node
    for (auto stream_it = map.begin(); stream_it != map.end();) {
      if (now - stream_it->second.last_update_time > max_idle_seconds) {
        // Erasure returns iterator to the next element
        stream_it = map.erase(stream_it);
        stats.removed_node_streams++;
      } else {
        ++stream_it;
      }
    }

    // If the Node is now empty (no active streams left), remove the Node entry entirely
    if (map.empty()) {
      node_it = mNodeStates.erase(node_it);
      stats.removed_nodes++;
    } else {
      ++node_it;
    }
  }

  // ---------------------------------------------------------------------------
  // 2. Clean Global Stats (mGlobalStats)
  // ---------------------------------------------------------------------------
  // A "Global Stream" is: App 'python' aggregated across ALL nodes.
  // If NO node has reported activity for 'python' in 60s, this entry is stale.
  for (auto it = mGlobalStats.begin(); it != mGlobalStats.end();) {
    // Note: Ensure 'last_activity_time' is updated in process_report() whenever
    // an update arrives for this key.
    if (now - it->second.last_activity_time > max_idle_seconds) {
      it = mGlobalStats.erase(it);
      stats.removed_global_streams++;
    } else {
      ++it;
    }
  }

  return stats;
}
} // namespace eos::common
