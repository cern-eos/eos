#include "mgm/shaping/TrafficShaping.hh"

#include "Constants.hh"
#include "common/Logging.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "proto/TrafficShaping.pb.h"

namespace eos::mgm {

TrafficShaping::TrafficShaping() = default;

TrafficShaping::~TrafficShaping() = default;

// -----------------------------------------------------------------------------
// Helper: Exponential Moving Average Calculation
// -----------------------------------------------------------------------------
double
TrafficShaping::CalculateEma(double current_val, double prev_ema, double alpha)
{
  return (alpha * current_val) + ((1.0 - alpha) * prev_ema);
}

std::pair<std::unordered_map<std::string, double>,
          std::unordered_map<std::string, double>>
TrafficShaping::GetCurrentReadAndWriteRateForApps() const
{
  std::shared_lock lock(mMutex);

  std::unordered_map<std::string, double> app_read_rates;
  std::unordered_map<std::string, double> app_write_rates;

  for (const auto& [key, stats] : mGlobalStats) {
    app_read_rates[key.app] += stats.ema[eos::mgm::Ema5s].read_rate_bps;
    app_write_rates[key.app] += stats.ema[eos::mgm::Ema5s].write_rate_bps;
  }

  return {app_read_rates, app_write_rates};
}

// -----------------------------------------------------------------------------
// Fast Path: Process Report from FST
// -----------------------------------------------------------------------------
void
TrafficShaping::process_report(const eos::traffic_shaping::FstIoReport& report)
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
    if (delta_bytes_read > 0 || delta_bytes_written > 0 || delta_read_iops > 0 ||
        delta_write_iops > 0) {
      // Get global entry
      MultiWindowRate& global = mGlobalStats[key];

      // Accumulate
      global.bytes_read_accumulator += delta_bytes_read;
      global.bytes_written_accumulator += delta_bytes_written;
      global.read_iops_accumulator += delta_read_iops;
      global.write_iops_accumulator += delta_write_iops;
      // Used by Garbage Collector
      global.last_activity_time = now;
    }
  }
}

double
ComputeEmaAlpha(double window_seconds, double time_delta_seconds)
{
  if (time_delta_seconds <= 0.0 || window_seconds <= 0.0) {
    return 1.0; // Instantly apply new values if time is zero or negative
  }
  return (2.0 * time_delta_seconds) / (window_seconds + time_delta_seconds);
}

// -----------------------------------------------------------------------------
// Slow Path: Update Time Windows (Called every 1 second)
// -----------------------------------------------------------------------------

void
TrafficShaping::UpdateTimeWindows(const double time_delta_seconds)
{
  if (time_delta_seconds <= 0.000001) {
    return;
  }

  // Write lock needed
  std::unique_lock lock(mMutex);

  // Pre-compute alphas for all configured EMA windows to save CPU cycles
  // instead of recalculating them for every single stream.
  std::array<double, EmaWindowSec.size()> ema_alphas;
  for (size_t i = 0; i < EmaWindowSec.size(); ++i) {
    ema_alphas[i] =
        ComputeEmaAlpha(static_cast<double>(EmaWindowSec[i]), time_delta_seconds);
  }

  for (auto& [key, stats] : mGlobalStats) {
    // 1. Snapshot and Reset Accumulators
    const uint64_t bytes_read_now = stats.bytes_read_accumulator.exchange(0);
    const uint64_t bytes_written_now = stats.bytes_written_accumulator.exchange(0);
    const uint64_t read_iops_now = stats.read_iops_accumulator.exchange(0);
    const uint64_t write_iops_now = stats.write_iops_accumulator.exchange(0);

    // 2. Calculate Instant Rate (Units/Sec)
    const double current_read_bps =
        static_cast<double>(bytes_read_now) / time_delta_seconds;
    const double current_write_bps =
        static_cast<double>(bytes_written_now) / time_delta_seconds;
    const double current_read_iops =
        static_cast<double>(read_iops_now) / time_delta_seconds;
    const double current_write_iops =
        static_cast<double>(write_iops_now) / time_delta_seconds;

    // 3. Update EMAs
    for (size_t i = 0; i < EmaWindowSec.size(); ++i) {
      stats.ema[i].read_rate_bps =
          CalculateEma(current_read_bps, stats.ema[i].read_rate_bps, ema_alphas[i]);
      stats.ema[i].write_rate_bps =
          CalculateEma(current_write_bps, stats.ema[i].write_rate_bps, ema_alphas[i]);
      stats.ema[i].read_iops =
          CalculateEma(current_read_iops, stats.ema[i].read_iops, ema_alphas[i]);
      stats.ema[i].write_iops =
          CalculateEma(current_write_iops, stats.ema[i].write_iops, ema_alphas[i]);
    }

    // -------------------------------------------------------------------------
    // 4. SMA Calculation (Uses Raw Counts + Sliding Window)
    // -------------------------------------------------------------------------

    // A. Add current second's raw data to the current bucket
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
    for (size_t i = 0; i < SmaWindowSec.size(); ++i) {
      const int window_sec = SmaWindowSec[i];
      stats.sma[i].read_rate_bps = stats.bytes_read_window.GetRate(window_sec);
      stats.sma[i].write_rate_bps = stats.bytes_written_window.GetRate(window_sec);
      stats.sma[i].read_iops = stats.iops_read_window.GetRate(window_sec);
      stats.sma[i].write_iops = stats.iops_write_window.GetRate(window_sec);
    }
  }
}

void
TrafficShaping::ComputeLimitsAndReservations()
{
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig fst_io_delay_config;
  auto* app_write_map = fst_io_delay_config.mutable_app_write_delay();
  auto* app_read_map = fst_io_delay_config.mutable_app_read_delay();

  constexpr uint64_t kMaxDelayUs = 1000000;
  constexpr int64_t MAX_STEP_US = kMaxDelayUs / 20;
  // This is sensitive to the thread period, we should recompute.

  {
    std::shared_lock lock(mMutex);
    const auto [app_read_rates, app_write_rates] = GetCurrentReadAndWriteRateForApps();
    // iterate over all apps limits
    for (const auto& [app, policy] : mAppPolicies) {
      if (!policy.IsActive()) {
        continue;
      }

      if (policy.limit_write_bytes_per_sec > 0) {
        const double current_rate =
            app_write_rates.count(app) > 0 ? app_write_rates.at(app) : 0.0;
        const auto limit = static_cast<double>(policy.limit_write_bytes_per_sec);
        const double ratio = current_rate / limit;

        uint64_t& delay_us = (*mFstIoDelayConfig.mutable_app_write_delay())[app];
        if (delay_us == 0 && ratio > 1.0) {
          delay_us = 100;
        } else {
          double kp = (ratio > 1.0) ? 0.15 : 0.05;
          double damped_ratio = 1.0 + ((ratio - 1.0) * kp);

          auto current_delay = static_cast<int64_t>(delay_us);
          auto target_delay =
              static_cast<int64_t>(static_cast<double>(current_delay) * damped_ratio);
          int64_t delta_us = target_delay - current_delay;

          if (delta_us > MAX_STEP_US) {
            delta_us = MAX_STEP_US;
          } else if (delta_us < -MAX_STEP_US) {
            delta_us = -MAX_STEP_US;
          }

          delay_us = static_cast<uint64_t>(current_delay + delta_us);
        }

        delay_us = std::min<uint64_t>(kMaxDelayUs, delay_us);
        if (delay_us < 10 && ratio < 1.0) {
          delay_us = 0;
        }
        if (delay_us > 0) {
          (*app_write_map)[app] = delay_us;
        }
      }

      if (policy.limit_read_bytes_per_sec > 0) {
        const double current_rate =
            app_read_rates.count(app) > 0 ? app_read_rates.at(app) : 0.0;
        const auto limit = static_cast<double>(policy.limit_read_bytes_per_sec);
        const double ratio = current_rate / limit;

        uint64_t& delay_us = (*mFstIoDelayConfig.mutable_app_read_delay())[app];
        if (delay_us == 0 && ratio > 1.0) {
          delay_us = 100;
        } else {
          double kp = (ratio > 1.0) ? 0.15 : 0.05;
          double damped_ratio = 1.0 + ((ratio - 1.0) * kp);

          auto current_delay = static_cast<int64_t>(delay_us);
          auto target_delay =
              static_cast<int64_t>(static_cast<double>(current_delay) * damped_ratio);
          int64_t delta_us = target_delay - current_delay;

          if (delta_us > MAX_STEP_US) {
            delta_us = MAX_STEP_US;
          } else if (delta_us < -MAX_STEP_US) {
            delta_us = -MAX_STEP_US;
          }

          delay_us = static_cast<uint64_t>(current_delay + delta_us);
        }

        delay_us = std::min<uint64_t>(kMaxDelayUs, delay_us);
        if (delay_us < 10 && ratio < 1.0) {
          delay_us = 0;
        }
        if (delay_us > 0) {
          (*app_read_map)[app] = delay_us;
        }
      }
    }
  }

  for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
    if (node_view->GetStatus() == "online") {
      node_view->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_IO_LIMITS,
                                 fst_io_delay_config.SerializeAsString(), true);
    }
  }
}

std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash>
TrafficShaping::GetGlobalStats() const
{
  std::shared_lock lock(mMutex);

  std::unordered_map<StreamKey, RateSnapshot, StreamKeyHash> snapshot_map;
  snapshot_map.reserve(mGlobalStats.size());

  for (const auto& [key, internal_stat] : mGlobalStats) {
    RateSnapshot& snap = snapshot_map[key];

    snap.last_activity_time = internal_stat.last_activity_time;
    snap.active_stream_count = internal_stat.active_stream_count;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

TrafficShaping::GarbageCollectionStats
TrafficShaping::garbage_collect(int max_idle_seconds)
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
  mBrain = std::make_shared<eos::mgm::TrafficShaping>();

  mBrain->estimators_update_loop_micro_sec = eos::fst::SlidingWindowStats(
      5.0, mEstimatorsUpdateThreadPeriodMilliseconds * 0.001);
  mBrain->fst_limits_update_loop_micro_sec = eos::fst::SlidingWindowStats(
      5.0, mFstIoPolicyUpdateThreadPeriodMilliseconds * 0.001);
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

  mEstimatorsUpdateThread.reset(&TrafficShapingEngine::EstimatorsUpdate, this);
  mFstIoPolicyUpdateThread.reset(&TrafficShapingEngine::FstIoPolicyUpdate, this);

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

  mEstimatorsUpdateThread.join();
  mFstIoPolicyUpdateThread.join();

  eos_static_info("msg=\"IoStatsEngine stopped\"");
}

//------------------------------------------------------------------------------
// GetBrain
//------------------------------------------------------------------------------
std::shared_ptr<eos::mgm::TrafficShaping>
TrafficShapingEngine::GetBrain() const
{
  return mBrain;
}

void
TrafficShapingEngine::ProcessSerializedFstIoReportNonBlocking(
    const std::string& serialized_report)
{
  eos::traffic_shaping::FstIoReport report;
  if (!report.ParseFromString(serialized_report)) {
    eos_static_warning("%s", "msg=\"failed to parse FstIoReport from string\"");
    return;
  }
  AddReportToQueue(report);
}

void
TrafficShapingEngine::AddReportToQueue(const eos::traffic_shaping::FstIoReport& report)
{
  std::lock_guard lock(mReportQueueMutex);
  mReportQueue.emplace_back(report);
  // if over 100 reports, warning
  if (mReportQueue.size() > 100) {
    eos_static_warning("msg=\"IoStatsEngine report queue size is large\" size=%zu",
                       mReportQueue.size());
  }
  // if over 1000, delete the oldest report until 1000 remain
  while (mReportQueue.size() > 1000) {
    mReportQueue.emplace_back();
    eos_static_warning("msg=\"IoStatsEngine report queue size exceeded limit, dropping "
                       "oldest report\" size=%zu",
                       mReportQueue.size());
  }
}

void
TrafficShapingEngine::ProcessAllQueuedReports()
{
  // We copy the queue to a local variable and clear the main queue under lock, then
  // process the local copy without holding the lock. This minimizes the time we hold the
  // lock and allows incoming reports to be added to the main queue while we are
  // processing.
  std::vector<eos::traffic_shaping::FstIoReport> local_queue; //
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
TrafficShapingEngine::EstimatorsUpdate(ThreadAssistant& assistant)
{
  ThreadAssistant::setSelfThreadName("TrafficShaping TickerLoop");
  eos_static_info("%s", "msg=\"starting IoStatsEngine ticker thread\"");

  // 1. Anchor the timeline
  auto next_tick = std::chrono::steady_clock::now();

  // Initialize the delta tracker
  auto last_run = std::chrono::steady_clock::now();

  int gc_counter = 0;
  // TODO: measure how expensive garbage collection is and tune this parameter
  // accordingly. We want to run GC often enough to prevent memory bloat but not so often
  // that it impacts performance. Since GC runs in the same thread, it will delay the next
  // tick if it takes too long. We could also consider running GC in a separate thread if
  // it becomes a bottleneck, but for now we will keep it simple and run it in the ticker
  // thread at a reasonable interval.
  constexpr int gc_counter_limit = 50;

  while (!assistant.terminationRequested()) {
    next_tick += std::chrono::milliseconds(mEstimatorsUpdateThreadPeriodMilliseconds);
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

    if (++gc_counter >= gc_counter_limit) {
      gc_counter = 0;
      // Remove streams that haven't been active for a while
      const auto [removed_nodes, removed_node_streams, removed_global_streams] =
          mBrain->garbage_collect(900);
      // 15 minutes or 3 times longer than the biggest EMA (5m)

      if (removed_node_streams > 0 || removed_global_streams > 0) {
        eos_static_info("msg=\"IoStats GC\" removed_nodes=%lu removed_node_streams=%lu "
                        "removed_global_streams=%lu",
                        removed_nodes, removed_node_streams, removed_global_streams);
      }
    }

    auto work_done = std::chrono::steady_clock::now();
    const auto work_duration_micro_sec =
        std::chrono::duration_cast<std::chrono::microseconds>(work_done - now).count();

    if (static_cast<double>(work_duration_micro_sec) >
        static_cast<double>(mEstimatorsUpdateThreadPeriodMilliseconds) * 0.1 * 1000.0) {
      eos_static_warning("msg=\"IoStats Ticker is slow\" work_duration_ms=%.2f",
                         static_cast<double>(work_duration_micro_sec) / 1000.0);
    }

    mBrain->update_estimators_update_loop_micro_sec(work_duration_micro_sec);
  }
}

// policies for traffic shaping manager
// -----------------------------------------------------------------------------
// Shaping Policy API (Configuration)
// -----------------------------------------------------------------------------

void
TrafficShapingEngine::FstIoPolicyUpdate(ThreadAssistant& assistant)
{
  ThreadAssistant::setSelfThreadName("TrafficShaping FstIoPolicyUpdate");
  eos_static_info("%s", "msg=\"starting FstIoPolicyUpdate thread\"");

  auto next_wakeup_time = std::chrono::steady_clock::now();

  while (!assistant.terminationRequested()) {
    const auto current_period =
        std::chrono::milliseconds(mFstIoPolicyUpdateThreadPeriodMilliseconds);

    next_wakeup_time += current_period;

    if (auto now = std::chrono::steady_clock::now(); next_wakeup_time < now) {
      next_wakeup_time = now;
    }

    std::this_thread::sleep_until(next_wakeup_time);

    auto work_start_time = std::chrono::steady_clock::now();

    mBrain->ComputeLimitsAndReservations();

    auto work_end_time = std::chrono::steady_clock::now();
    const auto compute_duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(work_end_time -
                                                              work_start_time)
            .count();

    mBrain->update_fst_limits_update_loop_micro_sec(compute_duration_us);
  }

  eos_static_info("%s", "msg=\"stopping FstIoPolicyUpdate thread\"");
}

void
TrafficShaping::SetUidPolicy(uint32_t uid, const TrafficShapingPolicy& policy)
{
  // Use unique_lock for writing
  std::unique_lock lock(mMutex);
  mUidPolicies[uid] = policy;
}

void
TrafficShaping::SetGidPolicy(uint32_t gid, const TrafficShapingPolicy& policy)
{
  std::unique_lock lock(mMutex);
  mGidPolicies[gid] = policy;
}

void
TrafficShaping::SetAppPolicy(const std::string& app, const TrafficShapingPolicy& policy)
{
  std::unique_lock lock(mMutex);
  mAppPolicies[app] = policy;

  eos_static_info("msg=\"Set App Traffic Shaping policy\" app=%s is_enabled=%d "
                  "limit_read_bps=%lu limit_write_bps=%lu "
                  "reservation_read_bps=%lu reservation_write_bps=%lu",
                  app.c_str(), policy.is_enabled, policy.limit_read_bytes_per_sec,
                  policy.limit_write_bytes_per_sec, policy.reservation_read_bytes_per_sec,
                  policy.reservation_write_bytes_per_sec);
}

void
TrafficShaping::RemoveUidPolicy(uint32_t uid)
{
  std::unique_lock lock(mMutex);
  if (mUidPolicies.erase(uid)) {
    eos_static_info("msg=\"Removed UID shaping policy\" uid=%u", uid);
  }
}

void
TrafficShaping::RemoveGidPolicy(uint32_t gid)
{
  std::unique_lock lock(mMutex);
  if (mGidPolicies.erase(gid)) {
    eos_static_info("msg=\"Removed GID shaping policy\" gid=%u", gid);
  }
}

void
TrafficShaping::RemoveAppPolicy(const std::string& app)
{
  std::unique_lock lock(mMutex);
  if (mAppPolicies.erase(app)) {
    eos_static_info("msg=\"Removed App shaping policy\" app=%s", app.c_str());
  }
}

// -----------------------------------------------------------------------------
// Getters (Return copies for thread safety)
// -----------------------------------------------------------------------------

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShaping::GetUidPolicies() const
{
  // Use shared_lock for reading
  std::shared_lock lock(mMutex);
  return mUidPolicies;
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShaping::GetGidPolicies() const
{
  std::shared_lock lock(mMutex);
  return mGidPolicies;
}

std::unordered_map<std::string, TrafficShapingPolicy>
TrafficShaping::GetAppPolicies() const
{
  std::shared_lock lock(mMutex);
  return mAppPolicies;
}

std::optional<TrafficShapingPolicy>
TrafficShaping::GetUidPolicy(uint32_t uid) const
{
  std::shared_lock lock(mMutex);
  auto it = mUidPolicies.find(uid);
  if (it != mUidPolicies.end()) {
    return it->second; // Returns a copy of the single struct
  }
  return std::nullopt; // Doesn't exist yet
}

std::optional<TrafficShapingPolicy>
TrafficShaping::GetGidPolicy(uint32_t gid) const
{
  std::shared_lock lock(mMutex);
  auto it = mGidPolicies.find(gid);
  if (it != mGidPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShaping::GetAppPolicy(const std::string& app) const
{
  std::shared_lock lock(mMutex);
  auto it = mAppPolicies.find(app);
  if (it != mAppPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

// policies for traffic shaping engine

void
TrafficShapingEngine::SetUidPolicy(uint32_t uid, const TrafficShapingPolicy& policy)
{
  if (mBrain) {
    mBrain->SetUidPolicy(uid, policy);
  }
}

void
TrafficShapingEngine::SetGidPolicy(uint32_t gid, const TrafficShapingPolicy& policy)
{
  if (mBrain) {
    mBrain->SetGidPolicy(gid, policy);
  }
}

void
TrafficShapingEngine::SetAppPolicy(const std::string& app,
                                   const TrafficShapingPolicy& policy)
{
  if (mBrain) {
    mBrain->SetAppPolicy(app, policy);
  }
}

void
TrafficShapingEngine::RemoveUidPolicy(uint32_t uid)
{
  if (mBrain) {
    mBrain->RemoveUidPolicy(uid);
  }
}

void
TrafficShapingEngine::RemoveGidPolicy(uint32_t gid)
{
  if (mBrain) {
    mBrain->RemoveGidPolicy(gid);
  }
}

void
TrafficShapingEngine::RemoveAppPolicy(const std::string& app)
{
  if (mBrain) {
    mBrain->RemoveAppPolicy(app);
  }
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShapingEngine::GetUidPolicies() const
{
  return mBrain ? mBrain->GetUidPolicies()
                : std::unordered_map<uint32_t, TrafficShapingPolicy>{};
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShapingEngine::GetGidPolicies() const
{
  return mBrain ? mBrain->GetGidPolicies()
                : std::unordered_map<uint32_t, TrafficShapingPolicy>{};
}

std::unordered_map<std::string, TrafficShapingPolicy>
TrafficShapingEngine::GetAppPolicies() const
{
  return mBrain ? mBrain->GetAppPolicies()
                : std::unordered_map<std::string, TrafficShapingPolicy>{};
}

std::optional<TrafficShapingPolicy>
TrafficShapingEngine::GetUidPolicy(uint32_t uid) const
{
  return mBrain ? mBrain->GetUidPolicy(uid) : std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShapingEngine::GetGidPolicy(uint32_t gid) const
{
  return mBrain ? mBrain->GetGidPolicy(gid) : std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShapingEngine::GetAppPolicy(const std::string& app) const
{
  return mBrain ? mBrain->GetAppPolicy(app) : std::nullopt;
}

} // namespace eos::mgm
