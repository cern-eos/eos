#include "mgm/shaping/TrafficShapingManager.hh"

#include "common/Logging.hh"
#include "proto/Shaping.pb.h"

namespace eos::mgm {
// -----------------------------------------------------------------------------
// Constructor / Destructor
// -----------------------------------------------------------------------------
TrafficShapingManager::TrafficShapingManager() = default;

TrafficShapingManager::~TrafficShapingManager() = default;

// -----------------------------------------------------------------------------
// Helper: Exponential Moving Average Calculation
// -----------------------------------------------------------------------------
double
TrafficShapingManager::CalculateEma(double current_val, double prev_ema, double alpha)
{
  return (alpha * current_val) + ((1.0 - alpha) * prev_ema);
}

// -----------------------------------------------------------------------------
// Fast Path: Process Report from FST
// -----------------------------------------------------------------------------
void
TrafficShapingManager::process_report(const Shaping::FstIoReport& report)
{
  const std::string& node_id = report.node_id();
  const time_t now = time(nullptr);

  std::unique_lock lock(mMutex);

  // Get or create the state map for this node
  NodeStateMap& node_map = mNodeStates[node_id];

  for (const auto& entry : report.entries()) {
    StreamKey key{entry.app_name(), entry.uid(), entry.gid()};

    // --- 1. Fetch Previous Node State ---
    StreamState& state = node_map[key];

    // --- 2. Calculate Deltas ---
    uint64_t delta_bytes_read = 0;
    uint64_t delta_bytes_written = 0;
    uint64_t delta_read_iops = 0;
    uint64_t delta_write_iops = 0;

    // Check Generation ID (Detect Restarts)
    if (state.generation_id != entry.generation_id()) {
      // New Session: Assume entire value is new traffic
      state.generation_id = entry.generation_id();
      delta_bytes_read = entry.total_bytes_read();
      delta_bytes_written = entry.total_bytes_written();
      delta_read_iops = entry.total_read_ops();
      delta_write_iops = entry.total_write_ops();
    } else {
      // Standard Monotonic Increase
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

    // --- 3. Update Node State ---
    state.last_bytes_read = entry.total_bytes_read();
    state.last_bytes_written = entry.total_bytes_written();
    state.last_iops_read = entry.total_read_ops();
    state.last_iops_write = entry.total_write_ops();
    state.last_update_time = now;

    // --- 4. Update Global Aggregates ---
    if (delta_bytes_read > 0 || delta_bytes_written > 0 || delta_read_iops > 0 || delta_write_iops > 0) {
      // Get global entry
      MultiWindowRate& global = mGlobalStats[key];

      // Accumulate
      global.bytes_read_accumulator += delta_bytes_read;
      global.bytes_written_accumulator += delta_bytes_written;
      global.read_iops_accumulator += delta_read_iops;
      global.write_iops_accumulator += delta_write_iops;

      // Update Activity Time (Critical for GC)
      global.last_activity_time = now;

      eos_static_info("msg=\"updated global stats\" app=\"%s\" uid=%u gid=%u "
                      "delta_bytes_read=%lu delta_bytes_written=%lu delta_read_iops=%lu delta_write_iops=%lu",
                      key.app.c_str(),
                      key.uid,
                      key.gid,
                      delta_bytes_read,
                      delta_bytes_written,
                      delta_read_iops,
                      delta_write_iops);
    }
  }
}

// -----------------------------------------------------------------------------
// Slow Path: Update Time Windows (Called every 1 second)
// -----------------------------------------------------------------------------
void
TrafficShapingManager::UpdateTimeWindows(const double time_delta_seconds)
{
  if (time_delta_seconds <= 0.000001) {
    return;
  }

  // Write lock needed
  std::unique_lock lock(mMutex);

  // Constants for 1s ticker
  constexpr double kAlpha5s = 0.33333333; // ~5 seconds
  constexpr double kAlpha1m = 0.03278688; // ~60 seconds
  constexpr double kAlpha5m = 0.00664452; // ~300 seconds

  // --- Helper Lambda for Zero-Snapping ---
  // If current_rate is 0, we snap the 5s window to 0 to avoid "ghosting".
  // The 1m and 5m windows decay naturally.
  auto update_rate_set = [&](double current_rate, double& r5s, double& r1m, double& r5m) {
    /*
    if (current_rate == 0.0) {
      r5s = 0.0; // Hard Snap
    } else {
      r5s = CalculateEma(current_rate, r5s, kAlpha5s);
    }
    */
    r5s = CalculateEma(current_rate, r5s, kAlpha5s);
    r1m = CalculateEma(current_rate, r1m, kAlpha1m);
    r5m = CalculateEma(current_rate, r5m, kAlpha5m);
  };

  for (auto& [key, stats] : mGlobalStats) {
    // 1. Snapshot and Reset Accumulators
    const uint64_t bytes_read_now = stats.bytes_read_accumulator.exchange(0);
    const uint64_t bytes_written_now = stats.bytes_written_accumulator.exchange(0);
    const uint64_t read_iops_now = stats.read_iops_accumulator.exchange(0);
    const uint64_t write_iops_now = stats.write_iops_accumulator.exchange(0);

    // 2. Calculate Instant Rate (Units/Sec)
    const double current_read_bps = static_cast<double>(bytes_read_now) / time_delta_seconds;
    const double current_write_bps = static_cast<double>(bytes_written_now) / time_delta_seconds;
    const double current_read_iops = static_cast<double>(read_iops_now) / time_delta_seconds;
    const double current_write_iops = static_cast<double>(write_iops_now) / time_delta_seconds;

    update_rate_set(current_read_bps, stats.read_rate_ema_5s, stats.read_rate_ema_1m, stats.read_rate_ema_5m);
    update_rate_set(current_write_bps, stats.write_rate_ema_5s, stats.write_rate_ema_1m, stats.write_rate_ema_5m);
    update_rate_set(current_read_iops, stats.read_iops_ema_5s, stats.read_iops_ema_1m, stats.read_iops_ema_5m);
    update_rate_set(current_write_iops, stats.write_iops_ema_5s, stats.write_iops_ema_1m, stats.write_iops_ema_5m);

    // -------------------------------------------------------------------------
    // SMA Calculation (Uses Raw Counts + Sliding Window)
    // -------------------------------------------------------------------------

    // A. Add current second's raw data to the current bucket
    // Note: We add the raw count, not the rate.
    stats.bytes_read_window.Add(bytes_read_now);
    stats.bytes_written_window.Add(bytes_written_now);
    stats.iops_read_window.Add(read_iops_now);
    stats.iops_write_window.Add(write_iops_now);

    // B. Tick (Move head forward, clear next bucket)
    stats.bytes_read_window.Tick();
    stats.bytes_written_window.Tick();
    stats.iops_read_window.Tick();
    stats.iops_write_window.Tick();

    // C. Compute and Cache SMA Rates
    // 5s Window
    stats.read_rate_sma_5s = stats.bytes_read_window.GetRate(5);
    stats.write_rate_sma_5s = stats.bytes_written_window.GetRate(5);
    stats.read_iops_sma_5s = stats.iops_read_window.GetRate(5);
    stats.write_iops_sma_5s = stats.iops_write_window.GetRate(5);

    // 1m Window (60s)
    stats.read_rate_sma_1m = stats.bytes_read_window.GetRate(60);
    stats.write_rate_sma_1m = stats.bytes_written_window.GetRate(60);
    stats.read_iops_sma_1m = stats.iops_read_window.GetRate(60);
    stats.write_iops_sma_1m = stats.iops_write_window.GetRate(60);

    // 5m Window (300s)
    stats.read_rate_sma_5m = stats.bytes_read_window.GetRate(300);
    stats.write_rate_sma_5m = stats.bytes_written_window.GetRate(300);
    stats.read_iops_sma_5m = stats.iops_read_window.GetRate(300);
    stats.write_iops_sma_5m = stats.iops_write_window.GetRate(300);
  }
}

// -----------------------------------------------------------------------------
// Monitoring API
// -----------------------------------------------------------------------------
std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash>
TrafficShapingManager::GetGlobalStats() const
{
  std::shared_lock lock(mMutex);

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> snapshot_map;

  for (const auto& [key, internal_stat] : mGlobalStats) {
    RateSnapshot& snap = snapshot_map[key];

    snap.last_activity_time = internal_stat.last_activity_time;

    // EMA
    snap.read_rate_ema_5s = internal_stat.read_rate_ema_5s;
    snap.read_rate_ema_1m = internal_stat.read_rate_ema_1m;
    snap.read_rate_ema_5m = internal_stat.read_rate_ema_5m;

    snap.write_rate_ema_5s = internal_stat.write_rate_ema_5s;
    snap.write_rate_ema_1m = internal_stat.write_rate_ema_1m;
    snap.write_rate_ema_5m = internal_stat.write_rate_ema_5m;

    snap.read_iops_ema_5s = internal_stat.read_iops_ema_5s;
    snap.read_iops_ema_1m = internal_stat.read_iops_ema_1m;
    snap.read_iops_ema_5m = internal_stat.read_iops_ema_5m;

    // SMA
    snap.read_rate_sma_5s = internal_stat.read_rate_sma_5s;
    snap.read_rate_sma_1m = internal_stat.read_rate_sma_1m;
    snap.read_rate_sma_5m = internal_stat.read_rate_sma_5m;

    snap.write_rate_sma_5s = internal_stat.write_rate_sma_5s;
    snap.write_rate_sma_1m = internal_stat.write_rate_sma_1m;
    snap.write_rate_sma_5m = internal_stat.write_rate_sma_5m;

    snap.read_iops_sma_5s = internal_stat.read_iops_sma_5s;
    snap.read_iops_sma_1m = internal_stat.read_iops_sma_1m;
    snap.read_iops_sma_5m = internal_stat.read_iops_sma_5m;

    snap.write_iops_sma_5s = internal_stat.write_iops_sma_5s;
    snap.write_iops_sma_1m = internal_stat.write_iops_sma_1m;
    snap.write_iops_sma_5m = internal_stat.write_iops_sma_5m;
  }

  return snapshot_map;
}

TrafficShapingManager::GarbageCollectionStats
TrafficShapingManager::garbage_collect(int max_idle_seconds)
{
  std::unique_lock lock(mMutex);
  const time_t now = time(nullptr);

  GarbageCollectionStats stats = {0, 0, 0};

  for (auto node_it = mNodeStates.begin(); node_it != mNodeStates.end();) {
    NodeStateMap& map = node_it->second;
    for (auto stream_it = map.begin(); stream_it != map.end();) {
      if (now - stream_it->second.last_update_time > max_idle_seconds) {
        stream_it = map.erase(stream_it);
        stats.removed_node_streams++;
      } else {
        ++stream_it;
      }
    }

    if (map.empty()) {
      node_it = mNodeStates.erase(node_it);
      stats.removed_nodes++;
    } else {
      ++node_it;
    }
  }

  for (auto it = mGlobalStats.begin(); it != mGlobalStats.end();) {
    if (now - it->second.last_activity_time > max_idle_seconds) {
      it = mGlobalStats.erase(it);
      stats.removed_global_streams++;
    } else {
      ++it;
    }
  }

  return stats;
}

// IoStatsEngine

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TrafficShapingEngine::TrafficShapingEngine()
    : mRunning(false)
{
  // Initialize the logic engine
  mBrain = std::make_shared<eos::mgm::TrafficShapingManager>();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TrafficShapingEngine::~TrafficShapingEngine() { Stop(); }

//------------------------------------------------------------------------------
// Start
//------------------------------------------------------------------------------
void
TrafficShapingEngine::Start()
{
  if (mRunning) {
    return;
  }

  mRunning = true;

  // Launch the thread
  // mTickerThread = std::thread(&TrafficShapingEngine::TickerLoop, this);
  mTickerThread.reset(&TrafficShapingEngine::TickerLoop, this);

  eos_static_info("msg=\"IoStatsEngine started\"");
}

//------------------------------------------------------------------------------
// Stop
//------------------------------------------------------------------------------
void
TrafficShapingEngine::Stop()
{
  if (!mRunning) {
    return;
  }

  mRunning = false;

  // Wait for thread to finish
  mTickerThread.join();

  eos_static_info("msg=\"IoStatsEngine stopped\"");
}

//------------------------------------------------------------------------------
// GetBrain
//------------------------------------------------------------------------------
std::shared_ptr<eos::mgm::TrafficShapingManager>
TrafficShapingEngine::GetBrain() const
{
  return mBrain;
}

void
TrafficShapingEngine::ProcessSerializedFstIoReportNonBlocking(const std::string& serialized_report)
{
  Shaping::FstIoReport report;
  if (!report.ParseFromString(serialized_report)) {
    eos_static_warning("%s", "msg=\"failed to parse FstIoReport from string\"");
    return;
  }
  AddReportToQueue(report);
}

void
TrafficShapingEngine::AddReportToQueue(const Shaping::FstIoReport& report)
{
  std::lock_guard lock(mReportQueueMutex);
  mReportQueue.emplace_back(report);
  // if over 100 reports, warning
  if (mReportQueue.size() > 100) {
    eos_static_warning("msg=\"IoStatsEngine report queue size is large\" size=%zu", mReportQueue.size());
  }
  // if over 1000, delete the oldest report until 1000 remain
  while (mReportQueue.size() > 1000) {
    mReportQueue.emplace_back();
    eos_static_warning("msg=\"IoStatsEngine report queue size exceeded limit, dropping oldest report\" size=%zu",
                       mReportQueue.size());
  }
}

void
TrafficShapingEngine::ProcessAllQueuedReports()
{
  // We copy the queue to a local variable and clear the main queue under lock, then process the local copy without
  // holding the lock. This minimizes the time we hold the lock and allows incoming reports to be added to the main
  // queue while we are processing.
  std::vector<Shaping::FstIoReport> local_queue;
  {
    std::lock_guard lock(mReportQueueMutex);
    std::swap(mReportQueue, local_queue);
  }
  for (const auto& report : local_queue) {
    mBrain->process_report(report);
  }
}

//------------------------------------------------------------------------------
// TickerLoop (The Heartbeat)
//------------------------------------------------------------------------------
void
TrafficShapingEngine::TickerLoop(ThreadAssistant& assistant)
{
  ThreadAssistant::setSelfThreadName("TrafficShaping TickerLoop");
  eos_static_info("%s", "msg=\"starting IoStatsEngine ticker thread\"");

  // 1. Anchor the timeline
  auto next_tick = std::chrono::steady_clock::now();

  // Initialize the delta tracker
  auto last_run = std::chrono::steady_clock::now();

  int gc_counter = 0;
  // TODO: measure how expensive garbage collection is and tune this parameter accordingly. We want to run GC often
  // enough to prevent memory bloat but not so often that it impacts performance. Since GC runs in the same thread, it
  // will delay the next tick if it takes too long. We could also consider running GC in a separate thread if it becomes
  // a bottleneck, but for now we will keep it simple and run it in the ticker thread at a reasonable interval.
  constexpr int gc_counter_limit = 1000;

  // TODO: process incoming messages, otherwise nothing will show up here!
  while (!assistant.terminationRequested()) {
    constexpr auto tick_interval_millis = std::chrono::milliseconds(100);
    next_tick += tick_interval_millis;

    // 3. Sleep precisely until that moment (Handles drift)
    std::this_thread::sleep_until(next_tick);

    ProcessAllQueuedReports();

    // 4. Measure actual elapsed time (dt)
    // Even with sleep_until, we might be woken up slightly late by the OS.
    // We measure this to pass the exact 'dt' to the EMA calculator.
    auto now = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = now - last_run;
    const double time_delta_seconds = elapsed.count();
    last_run = now;

    mBrain->UpdateTimeWindows(time_delta_seconds);

    // print total bytes stats and rates for debugging in info level

    const auto global_stats = mBrain->GetGlobalStats();
    for (const auto& [key, snap] : global_stats) {
      eos_static_info("msg=\"global stat entry\" app=\"%s\" uid=%u gid=%u "
                      "read_rate_ema_5s=%.2f read_rate_ema_1m=%.2f read_rate_ema_5m=%.2f "
                      "write_rate_ema_5s=%.2f write_rate_ema_1m=%.2f write_rate_ema_5m=%.2f "
                      "read_iops_ema_5s=%.2f read_iops_ema_1m=%.2f read_iops_ema_5m=%.2f "
                      "write_iops_ema_5s=%.2f write_iops_ema_1m=%.2f write_iops_ema_5m=%.2f "
                      "read_rate_sma_5s=%.2f read_rate_sma_1m=%.2f read_rate_sma_5m=%.2f "
                      "write_rate_sma_5s=%.2f write_rate_sma_1m=%.2f write_rate_sma_5m=%.2f "
                      "read_iops_sma_5s=%.2f read_iops_sma_1m=%.2f read_iops_sma_5m=%.2f "
                      "write_iops_sma_5s=%.2f write_iops_sma_1m=%.2f write_iops_sma_5m=%.2f",
                      key.app.c_str(),
                      key.uid,
                      key.gid,
                      snap.read_rate_ema_5s,
                      snap.read_rate_ema_1m,
                      snap.read_rate_ema_5m,
                      snap.write_rate_ema_5s,
                      snap.write_rate_ema_1m,
                      snap.write_rate_ema_5m,
                      snap.read_iops_ema_5s,
                      snap.read_iops_ema_1m,
                      snap.read_iops_ema_5m,
                      snap.write_iops_ema_5s,
                      snap.write_iops_ema_1m,
                      snap.write_iops_ema_5m,
                      snap.read_rate_sma_5s,
                      snap.read_rate_sma_1m,
                      snap.read_rate_sma_5m,
                      snap.write_rate_sma_5s,
                      snap.write_rate_sma_1m,
                      snap.write_rate_sma_5m,
                      snap.read_iops_sma_5s,
                      snap.read_iops_sma_1m,
                      snap.read_iops_sma_5m,
                      snap.write_iops_sma_5s,
                      snap.write_iops_sma_1m,
                      snap.write_iops_sma_5m);
    }

    if (++gc_counter >= gc_counter_limit) {
      eos_static_info("msg=\"IoStats GC triggered\" gc_counter=%d", gc_counter);
      gc_counter = 0;
      // Remove streams that haven't been active for a while
      const auto [removed_nodes, removed_node_streams, removed_global_streams] = mBrain->garbage_collect(900);
      // 15 minutes or 3 times longer than the biggest EMA (5m)

      if (removed_node_streams > 0 || removed_global_streams > 0) {
        eos_static_info("msg=\"IoStats GC\" removed_nodes=%lu removed_node_streams=%lu removed_global_streams=%lu",
                        removed_nodes,
                        removed_node_streams,
                        removed_global_streams);
      }
    }

    auto work_done = std::chrono::steady_clock::now();
    std::chrono::duration<double> work_duration = work_done - now;
    double work_ms = work_duration.count() * 1000.0;

    eos_static_info("msg=\"IoStats Ticker tick\" duration_ms=%.3f", work_ms);

    // TODO: expose this as a metric in prometheus
    // Warn if we are using too much of our time budget
    if (work_ms > tick_interval_millis.count() * 0.1) {
      eos_static_warning("msg=\"IoStats Ticker is slow\" work_duration_ms=%.3f threshold=200.0", work_ms);
    }
  }
}

} // namespace eos::mgm
