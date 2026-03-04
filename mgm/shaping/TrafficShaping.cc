#include "mgm/shaping/TrafficShaping.hh"
#include "Constants.hh"
#include "common/Logging.hh"
#include "config/IConfigEngine.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "namespace/interface/IFsView.hh"

#include "proto/TrafficShaping.pb.h"

#include <google/protobuf/util/json_util.h>

namespace eos::mgm::traffic_shaping {

TrafficShapingManager::TrafficShapingManager() = default;

TrafficShapingManager::~TrafficShapingManager() = default;

void
TrafficShapingManager::ApplyThreadConfig(uint32_t estimators_period_ms,
                                         uint32_t fst_policy_period_ms,
                                         uint32_t window_seconds)
{
  std::unique_lock lock(mMutex);

  mSystemStatsWindowSeconds = window_seconds;
  mEstimatorsTickIntervalSec = estimators_period_ms * 0.001;

  for (auto& [key, stats] : mGlobalStats) {
    stats.ResetWindows(mEstimatorsTickIntervalSec);
  }

  estimators_update_loop_micro_sec.emplace(mSystemStatsWindowSeconds,
                                           mEstimatorsTickIntervalSec);
  fst_limits_update_loop_micro_sec.emplace(mSystemStatsWindowSeconds,
                                           fst_policy_period_ms * 0.001);
}

double
TrafficShapingManager::CalculateEma(double current_val, double prev_ema, double alpha)
{
  return (alpha * current_val) + ((1.0 - alpha) * prev_ema);
}

std::pair<std::unordered_map<std::string, double>,
          std::unordered_map<std::string, double>>
TrafficShapingManager::GetCurrentReadAndWriteRateForApps() const
{
  std::shared_lock lock(mMutex);

  std::unordered_map<std::string, double> app_read_rates;
  std::unordered_map<std::string, double> app_write_rates;

  for (const auto& [key, stats] : mGlobalStats) {
    app_read_rates[key.app] += stats.ema[Ema5s].read_rate_bps;
    app_write_rates[key.app] += stats.ema[Ema5s].write_rate_bps;
  }

  return {app_read_rates, app_write_rates};
}

void
TrafficShapingManager::ProcessReport(const eos::traffic_shaping::FstIoReport& report)
{
  const std::string& node_id = report.node_id();

  const auto now_steady = std::chrono::steady_clock::now();
  const time_t now_unix = time(nullptr);

  std::unique_lock lock(mMutex);

  NodeData& node_data = mNodeStates[node_id];
  NodeStateMap& node_map = node_data.streams;

  bool is_first_node_contact =
      (node_data.last_report_time == std::chrono::steady_clock::time_point{});

  double node_elapsed_sec = 0.0;
  if (!is_first_node_contact) {
    node_elapsed_sec =
        std::chrono::duration<double>(now_steady - node_data.last_report_time).count();
  }
  node_data.last_report_time = now_steady;

  bool is_node_delayed =
      (!is_first_node_contact && node_elapsed_sec > kMaxThreadPeriodMs * 0.001);

  if (is_first_node_contact && !report.entries().empty()) {
    eos_static_info(
        "Received first FST report from node '%s'. We will treat this as a baseline and "
        "not calculate deltas to prevent spikes. node_elapsed_sec=%.3f",
        node_id.c_str(), node_elapsed_sec);
  }

  if (is_node_delayed && !report.entries().empty()) {
    eos_static_warning(
        "msg=\"Large delay in FST report, dropping deltas to prevent rate spike\" "
        "node=%s node_elapsed_sec=%.3f",
        node_id.c_str(), node_elapsed_sec);
  }

  if (!report.entries().empty()) {
    eos_static_debug("Received FST IO Report from node '%s'. Report: "
                     "%s",
                     node_id.c_str(), report.DebugString().c_str());
  }

  for (const auto& entry : report.entries()) {
    StreamKey key{entry.app_name(), entry.uid(), entry.gid()};

    StreamState& state = node_map[key];

    uint64_t delta_bytes_read = 0;
    uint64_t delta_bytes_written = 0;
    uint64_t delta_read_iops = 0;
    uint64_t delta_write_iops = 0;
    bool is_first_stream_contact =
        (state.last_update_time == std::chrono::steady_clock::time_point{});

    // Handle New Streams, MGM Restarts, and FST Restarts
    if (is_first_stream_contact || state.generation_id != entry.generation_id()) {
      state.generation_id = entry.generation_id();

      const uint64_t now_sys_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count();

      // If the stream was created more than 3 seconds ago, it is a ghost from the past
      bool is_old_stream = false;
      if (now_sys_ms > entry.generation_id() &&
          (now_sys_ms - entry.generation_id() > 3000)) {
        is_old_stream = true;
      }

      if (is_first_node_contact || (is_first_stream_contact && is_old_stream)) {
        delta_bytes_read = 0;
        delta_bytes_written = 0;
        delta_read_iops = 0;
        delta_write_iops = 0;

        eos_static_info("msg=\"Issue detected with IO Stats report, we will not "
                        "calculate deltas for this report.\" "
                        "node=%s app=%s uid=%u gid=%u is_old_stream=%d "
                        "is_first_stream_contact=%d is_first_node_contact=%d",
                        node_id.c_str(), entry.app_name().c_str(), entry.uid(),
                        entry.gid(), is_old_stream, is_first_stream_contact,
                        is_first_node_contact);

      } else {
        delta_bytes_read = entry.total_bytes_read();
        delta_bytes_written = entry.total_bytes_written();
        delta_read_iops = entry.total_read_ops();
        delta_write_iops = entry.total_write_ops();
      }
    } else {
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

    // Prevent huge spikes due to delayed reports or restarts by zeroing out deltas if we
    // detect a delay
    if (is_node_delayed) {
      delta_bytes_read = 0;
      delta_bytes_written = 0;
      delta_read_iops = 0;
      delta_write_iops = 0;
    }

    state.last_bytes_read = entry.total_bytes_read();
    state.last_bytes_written = entry.total_bytes_written();
    state.last_iops_read = entry.total_read_ops();
    state.last_iops_write = entry.total_write_ops();
    state.last_update_time = now_steady;

    if (delta_bytes_read > 0 || delta_bytes_written > 0 || delta_read_iops > 0 ||
        delta_write_iops > 0) {

      auto [it, inserted] = mGlobalStats.try_emplace(key, mEstimatorsTickIntervalSec);
      MultiWindowRate& global = it->second;

      global.bytes_read_accumulator += delta_bytes_read;
      global.bytes_written_accumulator += delta_bytes_written;
      global.read_iops_accumulator += delta_read_iops;
      global.write_iops_accumulator += delta_write_iops;
      global.last_activity_time = now_unix;
    }
  }
}

double
ComputeEmaAlpha(const double window_seconds, const double time_delta_seconds)
{
  if (time_delta_seconds <= 0.0 || window_seconds <= 0.0) {
    return 1.0;
  }
  return (2.0 * time_delta_seconds) / (window_seconds + time_delta_seconds);
}

void
TrafficShapingManager::UpdateEstimators(const double time_delta_seconds)
{
  if (time_delta_seconds <= 0.001 /* 1 ms */) {
    eos_static_err(
        "msg=\"Skipping estimator update due to problem time delta seconds: %f\"",
        time_delta_seconds);
    return;
  }

  std::unique_lock lock(mMutex);

  std::array<double, EmaWindowSec.size()> ema_alphas{};
  for (size_t i = 0; i < EmaWindowSec.size(); ++i) {
    ema_alphas[i] = ComputeEmaAlpha(EmaWindowSec[i], time_delta_seconds);
  }

  for (auto& [key, stats] : mGlobalStats) {
    const uint64_t bytes_read_now = stats.bytes_read_accumulator.exchange(0);
    const uint64_t bytes_written_now = stats.bytes_written_accumulator.exchange(0);
    const uint64_t read_iops_now = stats.read_iops_accumulator.exchange(0);
    const uint64_t write_iops_now = stats.write_iops_accumulator.exchange(0);

    const double current_read_bps =
        static_cast<double>(bytes_read_now) / time_delta_seconds;
    const double current_write_bps =
        static_cast<double>(bytes_written_now) / time_delta_seconds;
    const double current_read_iops =
        static_cast<double>(read_iops_now) / time_delta_seconds;
    const double current_write_iops =
        static_cast<double>(write_iops_now) / time_delta_seconds;

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

    stats.bytes_read_window.Add(bytes_read_now);
    stats.bytes_written_window.Add(bytes_written_now);
    stats.iops_read_window.Add(read_iops_now);
    stats.iops_write_window.Add(write_iops_now);

    stats.bytes_read_window.Tick();
    stats.bytes_written_window.Tick();
    stats.iops_read_window.Tick();
    stats.iops_write_window.Tick();

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
TrafficShapingManager::ComputeLimitsAndReservations()
{
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig fst_io_delay_config;
  auto* app_write_map = fst_io_delay_config.mutable_app_write_delay();
  auto* app_read_map = fst_io_delay_config.mutable_app_read_delay();

  constexpr uint64_t kMaxDelayUs = 1000000;
  constexpr int64_t kMaxStepUs = kMaxDelayUs / 20;

  auto adjust_delay = [&](const double limit_bps, const double current_rate,
                          uint64_t& delay_us, auto* output_map, const std::string& app) {
    if (limit_bps <= 0) {
      return;
    }

    const double ratio = current_rate / limit_bps;

    if (delay_us == 0 && ratio > 1.0) {
      delay_us = 100;
    } else {
      const double kp = (ratio > 1.0) ? 0.15 : 0.05;
      const double damped_ratio = 1.0 + ((ratio - 1.0) * kp);

      const auto current_delay = static_cast<int64_t>(delay_us);
      const auto target_delay =
          static_cast<int64_t>(static_cast<double>(current_delay) * damped_ratio);

      int64_t delta_us = target_delay - current_delay;
      if (delta_us > kMaxStepUs) {
        delta_us = kMaxStepUs;
      } else if (delta_us < -kMaxStepUs) {
        delta_us = -kMaxStepUs;
      }

      delay_us = static_cast<uint64_t>(current_delay + delta_us);
    }

    delay_us = std::min<uint64_t>(kMaxDelayUs, delay_us);

    if (delay_us < 10 && ratio < 1.0) {
      delay_us = 0;
    }

    if (delay_us > 0) {
      (*output_map)[app] = delay_us;
    }
  };
  //
  {
    std::shared_lock lock(mMutex);
    const auto [app_read_rates, app_write_rates] = GetCurrentReadAndWriteRateForApps();

    for (const auto& [app, policy] : mAppPolicies) {
      if (!policy.IsActive()) {
        continue;
      }

      adjust_delay(static_cast<double>(policy.limit_write_bytes_per_sec),
                   app_write_rates.count(app) ? app_write_rates.at(app) : 0.0,
                   (*mFstIoDelayConfig.mutable_app_write_delay())[app], app_write_map,
                   app);

      adjust_delay(static_cast<double>(policy.limit_read_bytes_per_sec),
                   app_read_rates.count(app) ? app_read_rates.at(app) : 0.0,
                   (*mFstIoDelayConfig.mutable_app_read_delay())[app], app_read_map, app);
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
TrafficShapingManager::GetGlobalStats() const
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

TrafficShapingManager::GarbageCollectionStats
TrafficShapingManager::GarbageCollect(const int max_idle_seconds)
{
  std::unique_lock lock(mMutex);

  const auto now_steady = std::chrono::steady_clock::now();
  const time_t now_unix = time(nullptr);

  GarbageCollectionStats stats = {0, 0, 0};

  for (auto node_it = mNodeStates.begin(); node_it != mNodeStates.end();) {
    NodeStateMap& map = node_it->second.streams;

    for (auto stream_it = map.begin(); stream_it != map.end();) {

      auto elapsed_sec = std::chrono::duration_cast<std::chrono::seconds>(
                             now_steady - stream_it->second.last_update_time)
                             .count();

      if (elapsed_sec > max_idle_seconds) {
        stream_it = map.erase(stream_it);
        stats.removed_node_streams++;
      } else {
        ++stream_it;
      }
    }

    // Calculate how long it has been since the Node itself sent a heartbeat
    auto node_idle_sec = std::chrono::duration_cast<std::chrono::seconds>(
                             now_steady - node_it->second.last_report_time)
                             .count();

    // Only erase the node if it has no streams AND the node is actually offline/silent
    if (map.empty() && node_idle_sec > max_idle_seconds) {
      node_it = mNodeStates.erase(node_it);
      stats.removed_nodes++;
    } else {
      ++node_it;
    }
  }

  for (auto it = mGlobalStats.begin(); it != mGlobalStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mGlobalStats.erase(it);
      stats.removed_global_streams++;
    } else {
      ++it;
    }
  }

  return stats;
}

void
TrafficShapingManager::UpdateFstLimitsLoopMicroSec(const uint64_t time_microseconds)
{
  std::unique_lock lock(mMutex);
  if (fst_limits_update_loop_micro_sec) {
    fst_limits_update_loop_micro_sec->Add(time_microseconds);
    fst_limits_update_loop_micro_sec->Tick();
  }
}

void
TrafficShapingManager::UpdateEstimatorsLoopMicroSec(const uint64_t time_microseconds)
{
  std::unique_lock lock(mMutex);
  if (estimators_update_loop_micro_sec) {
    estimators_update_loop_micro_sec->Add(time_microseconds);
    estimators_update_loop_micro_sec->Tick();
  }
}

std::tuple<double, uint64_t, uint64_t>
TrafficShapingManager::GetEstimatorsUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (estimators_update_loop_micro_sec) {
    return {
        estimators_update_loop_micro_sec->GetMean(true),
        estimators_update_loop_micro_sec->GetMin(true),
        estimators_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0.0, 0, 0};
}

std::tuple<double, uint64_t, uint64_t>
TrafficShapingManager::GetFstLimitsUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (fst_limits_update_loop_micro_sec) {
    return {
        fst_limits_update_loop_micro_sec->GetMean(true),
        fst_limits_update_loop_micro_sec->GetMin(true),
        fst_limits_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0.0, 0, 0};
}

// --------------------------------------------------------------------------------------
// ENGINE IMPLEMENTATION
// --------------------------------------------------------------------------------------

TrafficShapingEngine::TrafficShapingEngine()
    : mRunning(false)
    , mEstimatorsUpdateThreadPeriodMilliseconds(500)
    , mFstIoPolicyUpdateThreadPeriodMilliseconds(500)
    , mFstIoStatsReportThreadPeriodMilliseconds(500)
    , mSystemStatsWindowSeconds(15)
{
  mManager = std::make_shared<TrafficShapingManager>();
}

TrafficShapingEngine::~TrafficShapingEngine() { Stop(); }

void
TrafficShapingEngine::ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                                        uint32_t win_s, bool save_to_config_engine)
{
  if (est_ms < kMinThreadPeriodMs) {
    est_ms = kMinThreadPeriodMs;
  } else if (est_ms > kMaxThreadPeriodMs) {
    est_ms = kMaxThreadPeriodMs;
  }

  if (pol_ms < kMinThreadPeriodMs) {
    pol_ms = kMinThreadPeriodMs;
  } else if (pol_ms > kMaxThreadPeriodMs) {
    pol_ms = kMaxThreadPeriodMs;
  }

  if (rep_ms < kMinThreadPeriodMs) {
    rep_ms = kMinThreadPeriodMs;
  } else if (rep_ms > kMaxThreadPeriodMs) {
    rep_ms = kMaxThreadPeriodMs;
  }

  if (win_s < kMinSystemStatsWindowSec) {
    win_s = kMinSystemStatsWindowSec;
  } else if (win_s > kMaxSystemStatsWindowSec) {
    win_s = kMaxSystemStatsWindowSec;
  }

  bool changed = false;
  if (mEstimatorsUpdateThreadPeriodMilliseconds.load() != est_ms) {
    changed = true;
  }
  if (mFstIoPolicyUpdateThreadPeriodMilliseconds.load() != pol_ms) {
    changed = true;
  }
  if (mFstIoStatsReportThreadPeriodMilliseconds.load() != rep_ms) {
    changed = true;
  }
  if (mSystemStatsWindowSeconds.load() != win_s) {
    changed = true;
  }

  mEstimatorsUpdateThreadPeriodMilliseconds = est_ms;
  mFstIoPolicyUpdateThreadPeriodMilliseconds = pol_ms;
  mFstIoStatsReportThreadPeriodMilliseconds = rep_ms;
  mSystemStatsWindowSeconds = win_s;

  if (mManager != nullptr) {
    mManager->ApplyThreadConfig(est_ms, pol_ms, win_s);
  }

  if (save_to_config_engine && changed) {
    UpdateThreadConfigs();
    SyncTrafficShapingEnabledWithFst();
  }
}

void
TrafficShapingEngine::SetEstimatorsUpdateThreadPeriodMilliseconds(uint32_t period_ms)
{
  ApplyThreadConfig(period_ms, mFstIoPolicyUpdateThreadPeriodMilliseconds,
                    mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds,
                    true);
}

void
TrafficShapingEngine::SetFstIoPolicyUpdateThreadPeriodMilliseconds(uint32_t period_ms)
{
  ApplyThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds, period_ms,
                    mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds,
                    true);
}

void
TrafficShapingEngine::SetFstIoStatsReportThreadPeriodMilliseconds(uint32_t period_ms)
{
  ApplyThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                    mFstIoPolicyUpdateThreadPeriodMilliseconds, period_ms,
                    mSystemStatsWindowSeconds, true);
}

void
TrafficShapingEngine::SetSystemStatsWindowSeconds(uint32_t window_seconds)
{
  ApplyThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                    mFstIoPolicyUpdateThreadPeriodMilliseconds,
                    mFstIoStatsReportThreadPeriodMilliseconds, window_seconds, true);
}

void
TrafficShapingEngine::UpdateThreadConfigs()
{
  eos::traffic_shaping::ThreadConfig thread_loop_stats;

  thread_loop_stats.set_update_estimators_period_millis(
      mEstimatorsUpdateThreadPeriodMilliseconds);
  thread_loop_stats.set_fst_policy_update_period_millis(
      mFstIoPolicyUpdateThreadPeriodMilliseconds);
  thread_loop_stats.set_fst_io_stats_report_period_millis(
      mFstIoStatsReportThreadPeriodMilliseconds);
  thread_loop_stats.set_system_stats_time_window_seconds(mSystemStatsWindowSeconds);

  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_THREAD_PERIODS,
                                  thread_loop_stats.SerializeAsString());
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingEngine::ApplyConfig()
{
  const bool is_enabled =
      FsView::gFsView.GetBoolGlobalConfig(common::TRAFFIC_SHAPING_ENABLE_CONFIG);
  eos_static_info("msg=\"Applying Traffic Shaping Config\" enabled=%s",
                  is_enabled ? "true" : "false");
  if (is_enabled) {
    Enable();
  } else {
    Disable();
  }

  const std::string config =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG);

  if (const auto manager = GetManager(); manager != nullptr) {
    const bool result = manager->LoadPoliciesFromString(config);
    if (!result) {
      eos_static_err("%s", "msg=\"Failed to load Traffic Shaping policies from config\"");
    }
  }

  uint32_t est_ms = mEstimatorsUpdateThreadPeriodMilliseconds;
  uint32_t pol_ms = mFstIoPolicyUpdateThreadPeriodMilliseconds;
  uint32_t rep_ms = mFstIoStatsReportThreadPeriodMilliseconds;
  uint32_t win_s = mSystemStatsWindowSeconds;

  const std::string thread_periods =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_THREAD_PERIODS);
  if (!thread_periods.empty()) {
    eos::traffic_shaping::ThreadConfig thread_config;
    try {
      thread_config.ParseFromString(thread_periods);
      est_ms = thread_config.update_estimators_period_millis();
      pol_ms = thread_config.fst_policy_update_period_millis();
      rep_ms = thread_config.fst_io_stats_report_period_millis();
      win_s = thread_config.system_stats_time_window_seconds();
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse thread periods config\" error=%s", e.what());
    }
  }

  ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s, false);

  SyncTrafficShapingEnabledWithFst();
}

void
TrafficShapingEngine::Start()
{
  if (mRunning) {
    return;
  }
  mRunning = true;

  mEstimatorsUpdateThread.reset(&TrafficShapingEngine::EstimatorsUpdate, this);
  mEstimatorsUpdateThread.setName("Traffic Shaping Estimators Update");

  mFstIoPolicyUpdateThread.reset(&TrafficShapingEngine::FstIoPolicyUpdate, this);
  mFstIoPolicyUpdateThread.setName("Traffic Shaping FST Policy Update");

  mFstTrafficShapingConfigUpdateThread.reset(
      &TrafficShapingEngine::FstTrafficShapingConfigUpdate, this);
  mFstTrafficShapingConfigUpdateThread.setName("Traffic Shaping FST Config Update");

  ApplyThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                    mFstIoPolicyUpdateThreadPeriodMilliseconds,
                    mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds,
                    false);

  eos_static_info("msg=\"Traffic Shaping Engine Started\"");
}

void
TrafficShapingEngine::Stop()
{
  if (!mRunning) {
    return;
  }
  mRunning = false;

  mEstimatorsUpdateThread.join();
  mFstIoPolicyUpdateThread.join();
  {
    std::lock_guard lock(mReportQueueMutex);
    mReportQueue.clear();
  }

  if (mManager != nullptr) {
    mManager->Clear();
  }

  eos_static_info("msg=\"Traffic Shaping Engine Stopped\"");
}

std::shared_ptr<TrafficShapingManager>
TrafficShapingEngine::GetManager() const
{
  return mManager;
}

void
TrafficShapingEngine::ProcessSerializedFstIoReportNonBlocking(
    const std::string& serialized_report)
{
  if (!mRunning) {
    return;
  }

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
  if (mReportQueue.size() > 500) {
    eos_static_warning("msg=\"Traffic Shaping report queue size is too large\" size=%zu",
                       mReportQueue.size());
    mReportQueue.clear();
  } else {
    mReportQueue.emplace_back(report);
  }
}

void
TrafficShapingEngine::ProcessAllQueuedReports()
{
  std::vector<eos::traffic_shaping::FstIoReport> local_queue;
  {
    std::lock_guard lock(mReportQueueMutex);
    std::swap(mReportQueue, local_queue);
  }

  for (const auto& report : local_queue) {
    mManager->ProcessReport(report);
  }
}

void
TrafficShapingEngine::EstimatorsUpdate(ThreadAssistant& assistant)
{
  eos_static_info("%s", "msg=\"Starting Traffic Shaping estimators update thread\"");

  auto next_tick = std::chrono::steady_clock::now();
  auto last_run = std::chrono::steady_clock::now();

  int infrequent_action_counter = 0;
  constexpr int infrequent_action_threshold = 100;

  while (!assistant.terminationRequested()) {
    next_tick += std::chrono::milliseconds(mEstimatorsUpdateThreadPeriodMilliseconds);
    std::this_thread::sleep_until(next_tick);

    ProcessAllQueuedReports();

    auto now = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed = now - last_run;
    const double time_delta_seconds = elapsed.count();
    last_run = now;

    mManager->UpdateEstimators(time_delta_seconds);

    if (++infrequent_action_counter >= infrequent_action_threshold) {
      infrequent_action_counter = 0;
      const auto [removed_nodes, removed_node_streams, removed_global_streams] =
          mManager->GarbageCollect(900);

      if (removed_node_streams > 0 || removed_global_streams > 0) {
        eos_static_info("msg=\"Traffic Shaping Garbage Collection\" removed_nodes=%lu "
                        "removed_node_streams=%lu "
                        "removed_global_streams=%lu",
                        removed_nodes, removed_node_streams, removed_global_streams);
      }
    }

    auto work_done = std::chrono::steady_clock::now();
    const auto work_duration_micro_sec =
        std::chrono::duration_cast<std::chrono::microseconds>(work_done - now).count();

    if (static_cast<double>(work_duration_micro_sec) >
        static_cast<double>(mEstimatorsUpdateThreadPeriodMilliseconds) * 0.1 * 1000.0) {
      eos_static_warning(
          "msg=\"Traffic Shaping Estimators Update loop is slow\" work_duration_ms=%.2f",
          static_cast<double>(work_duration_micro_sec) / 1000.0);
    }

    mManager->UpdateEstimatorsLoopMicroSec(work_duration_micro_sec);
  }
}

void
TrafficShapingEngine::FstIoPolicyUpdate(ThreadAssistant& assistant) const
{
  eos_static_info("%s", "msg=\"Starting FstIoPolicyUpdate thread\"");

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

    mManager->ComputeLimitsAndReservations();

    auto work_end_time = std::chrono::steady_clock::now();
    const auto compute_duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(work_end_time -
                                                              work_start_time)
            .count();

    mManager->UpdateFstLimitsLoopMicroSec(compute_duration_us);
  }
}

void
TrafficShapingEngine::FstTrafficShapingConfigUpdate(ThreadAssistant& assistant)
{
  auto next_wakeup_time = std::chrono::steady_clock::now();
  const auto period = std::chrono::seconds(5);

  while (!assistant.terminationRequested()) {
    next_wakeup_time += period;

    if (auto now = std::chrono::steady_clock::now(); next_wakeup_time < now) {
      next_wakeup_time = now;
    }

    std::this_thread::sleep_until(next_wakeup_time);

    SyncTrafficShapingEnabledWithFst();
  }
}

void
TrafficShapingEngine::Enable()
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_ENABLE_CONFIG, true);
  gOFS->mConfigEngine->AutoSave();
  Start();
}

void
TrafficShapingEngine::Disable()
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_ENABLE_CONFIG, false);
  gOFS->mConfigEngine->AutoSave();
  Stop();
}

void
TrafficShapingEngine::SyncTrafficShapingEnabledWithFst()
{
  const bool enabled = mRunning;
  for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
    if (node_view->GetStatus() == "online") {
      node_view->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_ENABLE_TOGGLE,
                                 enabled ? "true" : "false", true);
      node_view->SetConfigMember(
          eos::common::FST_TRAFFIC_SHAPING_STATS_THREAD_PERIOD,
          std::to_string(mFstIoStatsReportThreadPeriodMilliseconds), true);
    }
  }
}

void
TrafficShapingManager::SetUidPolicy(const uint32_t uid,
                                    const TrafficShapingPolicy& policy)
{
  std::unique_lock lock(mMutex);
  mUidPolicies[uid] = policy;

  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG,
                                  SerializePoliciesUnlocked());
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingManager::SetGidPolicy(const uint32_t gid,
                                    const TrafficShapingPolicy& policy)
{
  std::unique_lock lock(mMutex);
  mGidPolicies[gid] = policy;

  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG,
                                  SerializePoliciesUnlocked());
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingManager::SetAppPolicy(const std::string& app,
                                    const TrafficShapingPolicy& policy)
{
  std::unique_lock lock(mMutex);
  mAppPolicies[app] = policy;

  const std::string serialized = SerializePoliciesUnlocked();

  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingManager::RemoveUidPolicy(const uint32_t uid)
{
  std::unique_lock lock(mMutex);
  if (mUidPolicies.erase(uid)) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG,
                                    SerializePoliciesUnlocked());
    gOFS->mConfigEngine->AutoSave();
  }
}

void
TrafficShapingManager::RemoveGidPolicy(const uint32_t gid)
{
  std::unique_lock lock(mMutex);
  if (mGidPolicies.erase(gid)) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG,
                                    SerializePoliciesUnlocked());
    gOFS->mConfigEngine->AutoSave();
  }
}

void
TrafficShapingManager::RemoveAppPolicy(const std::string& app)
{
  std::unique_lock lock(mMutex);
  if (mAppPolicies.erase(app)) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG,
                                    SerializePoliciesUnlocked());
    gOFS->mConfigEngine->AutoSave();
  }
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShapingManager::GetUidPolicies() const
{
  std::shared_lock lock(mMutex);
  return mUidPolicies;
}

std::unordered_map<uint32_t, TrafficShapingPolicy>
TrafficShapingManager::GetGidPolicies() const
{
  std::shared_lock lock(mMutex);
  return mGidPolicies;
}

std::unordered_map<std::string, TrafficShapingPolicy>
TrafficShapingManager::GetAppPolicies() const
{
  std::shared_lock lock(mMutex);
  return mAppPolicies;
}

std::optional<TrafficShapingPolicy>
TrafficShapingManager::GetUidPolicy(const uint32_t uid) const
{
  std::shared_lock lock(mMutex);
  if (const auto it = mUidPolicies.find(uid); it != mUidPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShapingManager::GetGidPolicy(const uint32_t gid) const
{
  std::shared_lock lock(mMutex);
  if (const auto it = mGidPolicies.find(gid); it != mGidPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<TrafficShapingPolicy>
TrafficShapingManager::GetAppPolicy(const std::string& app) const
{
  std::shared_lock lock(mMutex);
  if (const auto it = mAppPolicies.find(app); it != mAppPolicies.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::string
TrafficShapingManager::SerializePoliciesUnlocked() const
{
  eos::traffic_shaping::TrafficShapingPolicyConfig proto_config;

  auto copy_to_proto = [](const TrafficShapingPolicy& cpp_pol,
                          eos::traffic_shaping::TrafficShapingPolicy& proto_pol) {
    proto_pol.set_limit_write_bytes_per_sec(cpp_pol.limit_write_bytes_per_sec);
    proto_pol.set_limit_read_bytes_per_sec(cpp_pol.limit_read_bytes_per_sec);
    proto_pol.set_reservation_write_bytes_per_sec(
        cpp_pol.reservation_write_bytes_per_sec);
    proto_pol.set_reservation_read_bytes_per_sec(cpp_pol.reservation_read_bytes_per_sec);
    proto_pol.set_is_enabled(cpp_pol.is_enabled);
  };

  for (const auto& [uid, pol] : mUidPolicies) {
    copy_to_proto(pol, (*proto_config.mutable_uid_policies())[uid]);
  }
  for (const auto& [gid, pol] : mGidPolicies) {
    copy_to_proto(pol, (*proto_config.mutable_gid_policies())[gid]);
  }
  for (const auto& [app, pol] : mAppPolicies) {
    copy_to_proto(pol, (*proto_config.mutable_app_policies())[app]);
  }

  std::string json_data;
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = false;

  if (auto status =
          google::protobuf::util::MessageToJsonString(proto_config, &json_data, options);
      !status.ok()) {
    eos_static_err("msg=\"Failed to serialize policies to JSON\"");
  }

  return json_data;
}

bool
TrafficShapingManager::LoadPoliciesFromString(const std::string& serialized_data)
{
  if (serialized_data.empty()) {
    return true;
  }

  eos::traffic_shaping::TrafficShapingPolicyConfig proto_config;

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = true;

  auto status = google::protobuf::util::JsonStringToMessage(serialized_data,
                                                            &proto_config, options);
  if (!status.ok()) {
    eos_static_err("msg=\"Failed to parse policies from JSON string\"");
    return false;
  }

  mUidPolicies.clear();
  mGidPolicies.clear();
  mAppPolicies.clear();

  auto copy_to_cpp = [](const eos::traffic_shaping::TrafficShapingPolicy& proto_pol)
      -> TrafficShapingPolicy {
    TrafficShapingPolicy cpp_pol;
    cpp_pol.limit_write_bytes_per_sec = proto_pol.limit_write_bytes_per_sec();
    cpp_pol.limit_read_bytes_per_sec = proto_pol.limit_read_bytes_per_sec();
    cpp_pol.reservation_write_bytes_per_sec = proto_pol.reservation_write_bytes_per_sec();
    cpp_pol.reservation_read_bytes_per_sec = proto_pol.reservation_read_bytes_per_sec();
    cpp_pol.is_enabled = proto_pol.is_enabled();
    return cpp_pol;
  };

  for (const auto& [uid, pol] : proto_config.uid_policies()) {
    mUidPolicies[uid] = copy_to_cpp(pol);
  }
  for (const auto& [gid, pol] : proto_config.gid_policies()) {
    mGidPolicies[gid] = copy_to_cpp(pol);
  }
  for (const auto& [app, pol] : proto_config.app_policies()) {
    mAppPolicies[app] = copy_to_cpp(pol);
  }

  return true;
}

} // namespace eos::mgm::traffic_shaping
