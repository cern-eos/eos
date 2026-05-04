#include "mgm/shaping/TrafficShaping.hh"
#include "Constants.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "config/IConfigEngine.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "namespace/interface/IFsView.hh"

#include "proto/TrafficShaping.pb.h"

#include <dlfcn.h>
#include <google/protobuf/util/json_util.h>
#include <set>
#include <sstream>
#include <sys/stat.h>

namespace eos::mgm::traffic_shaping {

uint64_t
TrafficShapingPolicy::GetEffectiveWriteLimit() const
{
  const uint64_t active_user_limit = is_enabled ? limit_write_bytes_per_sec : 0;
  if (active_user_limit > 0 && controller_limit_write_bytes_per_sec > 0) {
    return std::min(active_user_limit, controller_limit_write_bytes_per_sec);
  }
  return active_user_limit > 0 ? active_user_limit : controller_limit_write_bytes_per_sec;
}

uint64_t
TrafficShapingPolicy::GetEffectiveReadLimit() const
{
  const uint64_t active_user_limit = is_enabled ? limit_read_bytes_per_sec : 0;
  if (active_user_limit > 0 && controller_limit_read_bytes_per_sec > 0) {
    return std::min(active_user_limit, controller_limit_read_bytes_per_sec);
  }
  return active_user_limit > 0 ? active_user_limit : controller_limit_read_bytes_per_sec;
}

bool
TrafficShapingPolicy::IsEmpty() const
{
  return limit_write_bytes_per_sec == 0 && limit_read_bytes_per_sec == 0 &&
         reservation_write_bytes_per_sec == 0 && reservation_read_bytes_per_sec == 0 &&
         controller_limit_write_bytes_per_sec == 0 &&
         controller_limit_read_bytes_per_sec == 0;
}

bool
TrafficShapingPolicy::IsActive() const
{
  const bool has_user_rules =
      limit_write_bytes_per_sec > 0 || limit_read_bytes_per_sec > 0 ||
      reservation_write_bytes_per_sec > 0 || reservation_read_bytes_per_sec > 0;
  const bool has_controller_rules =
      controller_limit_write_bytes_per_sec > 0 || controller_limit_read_bytes_per_sec > 0;
  return has_controller_rules || (is_enabled && has_user_rules);
}

bool
TrafficShapingPolicy::operator==(const TrafficShapingPolicy& policy) const
{
  return limit_write_bytes_per_sec == policy.limit_write_bytes_per_sec &&
         limit_read_bytes_per_sec == policy.limit_read_bytes_per_sec &&
         reservation_write_bytes_per_sec == policy.reservation_write_bytes_per_sec &&
         reservation_read_bytes_per_sec == policy.reservation_read_bytes_per_sec &&
         is_enabled == policy.is_enabled;
}

bool
TrafficShapingPolicy::operator!=(const TrafficShapingPolicy& policy) const
{
  return !(*this == policy);
}

std::string
TrafficShapingPolicy::ToString() const
{
  std::ostringstream oss;
  oss << (is_enabled ? "Enabled" : "Disabled") << ", "
      << "Read Limit: " << limit_read_bytes_per_sec << " Bps, "
      << "Write Limit: " << limit_write_bytes_per_sec << " Bps, "
      << "Read Reservation: " << reservation_read_bytes_per_sec << " Bps, "
      << "Write Reservation: " << reservation_write_bytes_per_sec << " Bps, "
      << "Controller Read Limit: " << controller_limit_read_bytes_per_sec << " Bps, "
      << "Controller Write Limit: " << controller_limit_write_bytes_per_sec << " Bps";
  return oss.str();
}

TrafficShapingManager::TrafficShapingManager() = default;

TrafficShapingManager::~TrafficShapingManager() { Clear(); }

void
TrafficShapingManager::ApplyThreadConfig(const uint32_t estimators_period_ms,
                                         const uint32_t fst_policy_period_ms,
                                         const uint32_t window_seconds)
{
  std::unique_lock lock(mMutex);

  mSystemStatsWindowSeconds = window_seconds;
  mEstimatorsTickIntervalSec = estimators_period_ms * 0.001;

  for (auto& [key, stats] : mGlobalStats) {
    stats.ResetWindows(mEstimatorsTickIntervalSec);
  }
  for (auto& [node, stats] : mNodeStats) {
    stats.ResetWindows(mEstimatorsTickIntervalSec);
  }
  for (auto& [disk_key, stats] : mDiskStats) {
    stats.ResetWindows(mEstimatorsTickIntervalSec);
  }
  for (auto& [detailed_key, stats] : mDetailedStats) {
    stats.ResetWindows(mEstimatorsTickIntervalSec);
  }
  mTotalStats.ResetWindows(mEstimatorsTickIntervalSec);

  estimators_update_loop_micro_sec.emplace(mSystemStatsWindowSeconds,
                                           mEstimatorsTickIntervalSec);
  fst_reports_processed_per_second.emplace(mSystemStatsWindowSeconds,
                                           mEstimatorsTickIntervalSec);
  fst_limits_update_loop_micro_sec.emplace(mSystemStatsWindowSeconds,
                                           fst_policy_period_ms * 0.001);
}

double
TrafficShapingManager::CalculateEma(const double current_val, const double prev_ema,
                                    const double alpha)
{
  return (alpha * current_val) + ((1.0 - alpha) * prev_ema);
}

std::tuple<std::unordered_map<std::string, double>,
           std::unordered_map<std::string, double>, std::unordered_map<uint32_t, double>,
           std::unordered_map<uint32_t, double>, std::unordered_map<uint32_t, double>,
           std::unordered_map<uint32_t, double>>
TrafficShapingManager::GetCurrentReadAndWriteRates() const
{
  std::shared_lock lock(mMutex);

  std::unordered_map<std::string, double> app_read_rates, app_write_rates;
  std::unordered_map<uint32_t, double> uid_read_rates, uid_write_rates;
  std::unordered_map<uint32_t, double> gid_read_rates, gid_write_rates;

  for (const auto& [key, stats] : mGlobalStats) {
    const double read_bps = stats.ema[Ema5s].read_rate_bps;
    const double write_bps = stats.ema[Ema5s].write_rate_bps;

    app_read_rates[key.app] += read_bps;
    app_write_rates[key.app] += write_bps;

    uid_read_rates[key.uid] += read_bps;
    uid_write_rates[key.uid] += write_bps;

    gid_read_rates[key.gid] += read_bps;
    gid_write_rates[key.gid] += write_bps;
  }

  return {app_read_rates,  app_write_rates, uid_read_rates,
          uid_write_rates, gid_read_rates,  gid_write_rates};
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

  const bool is_first_node_contact =
      (node_data.last_report_time == std::chrono::steady_clock::time_point{});

  double node_elapsed_sec = 0.0;
  if (!is_first_node_contact) {
    node_elapsed_sec =
        std::chrono::duration<double>(now_steady - node_data.last_report_time).count();
  }
  node_data.last_report_time = now_steady;

  const bool is_node_delayed =
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

  uint64_t total_node_delta_bytes_read = 0;
  uint64_t total_node_delta_bytes_written = 0;
  uint64_t total_node_delta_read_iops = 0;
  uint64_t total_node_delta_write_iops = 0;

  for (const auto& entry : report.entries()) {
    StreamKey key{entry.app_name(), entry.uid(), entry.gid(), entry.fsid()};

    StreamState& state = node_map[key];

    uint64_t delta_bytes_read = 0;
    uint64_t delta_bytes_written = 0;
    uint64_t delta_read_iops = 0;
    uint64_t delta_write_iops = 0;
    const bool is_first_stream_contact =
        state.last_update_time == std::chrono::steady_clock::time_point{};

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

    total_node_delta_bytes_read += delta_bytes_read;
    total_node_delta_bytes_written += delta_bytes_written;
    total_node_delta_read_iops += delta_read_iops;
    total_node_delta_write_iops += delta_write_iops;

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

      if (entry.fsid() != 0) {
        DetailedKey detailed_key{node_id, key};
        auto [detailed_it, detailed_inserted] =
            mDetailedStats.try_emplace(detailed_key, mEstimatorsTickIntervalSec);
        MultiWindowRate& detailed = detailed_it->second;

        detailed.bytes_read_accumulator += delta_bytes_read;
        detailed.bytes_written_accumulator += delta_bytes_written;
        detailed.read_iops_accumulator += delta_read_iops;
        detailed.write_iops_accumulator += delta_write_iops;
        detailed.last_activity_time = now_unix;

        DiskKey disk_key{node_id, entry.fsid()};
        auto [disk_it, disk_inserted] =
            mDiskStats.try_emplace(disk_key, mEstimatorsTickIntervalSec);
        MultiWindowRate& disk = disk_it->second;

        disk.bytes_read_accumulator += delta_bytes_read;
        disk.bytes_written_accumulator += delta_bytes_written;
        disk.read_iops_accumulator += delta_read_iops;
        disk.write_iops_accumulator += delta_write_iops;
        disk.last_activity_time = now_unix;
      }
    }
  }

  if (total_node_delta_bytes_read > 0 || total_node_delta_bytes_written > 0 ||
      total_node_delta_read_iops > 0 || total_node_delta_write_iops > 0) {

    auto [it, inserted] = mNodeStats.try_emplace(node_id, mEstimatorsTickIntervalSec);
    MultiWindowRate& node_stat = it->second;

    node_stat.bytes_read_accumulator += total_node_delta_bytes_read;
    node_stat.bytes_written_accumulator += total_node_delta_bytes_written;
    node_stat.read_iops_accumulator += total_node_delta_read_iops;
    node_stat.write_iops_accumulator += total_node_delta_write_iops;
    node_stat.last_activity_time = now_unix;

    mTotalStats.bytes_read_accumulator += total_node_delta_bytes_read;
    mTotalStats.bytes_written_accumulator += total_node_delta_bytes_written;
    mTotalStats.read_iops_accumulator += total_node_delta_read_iops;
    mTotalStats.write_iops_accumulator += total_node_delta_write_iops;
    mTotalStats.last_activity_time = now_unix;
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

  auto process_rate = [&](MultiWindowRate& stats) {
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
  };

  for (auto& [key, stats] : mGlobalStats) {
    process_rate(stats);
  }
  for (auto& [node_id, stats] : mNodeStats) {
    process_rate(stats);
  }
  for (auto& [disk_key, stats] : mDiskStats) {
    process_rate(stats);
  }
  for (auto& [detailed_key, stats] : mDetailedStats) {
    process_rate(stats);
  }
  process_rate(mTotalStats);
}

uint64_t
TrafficShapingManager::CalculateDelayUs(const double limit_bps,
                                        const double current_rate_bps,
                                        const uint64_t current_delay_us)
{
  if (limit_bps <= 0.0) {
    return 0;
  }

  // --- Tuning Constants ---
  constexpr uint64_t kMaxDelayUs = 2000000;          // 2 seconds max artificial delay
  constexpr int64_t kMaxStepUpUs = kMaxDelayUs / 25; // Max delta to ADD per tick (80ms)
  // Allow faster recovery: shed delay 2.5x faster than we add it.
  constexpr int64_t kMaxStepDownUs =
      kMaxDelayUs / 10;                   // Max delta to REMOVE per tick (200ms)
  constexpr double kIdleThreshold = 0.01; // < 1% of limit => no meaningful traffic

  const double ratio = current_rate_bps / limit_bps;
  uint64_t delay_us = current_delay_us;

  // 0. Idle freeze: preserve the current delay when traffic is essentially absent.
  if (current_rate_bps < limit_bps * kIdleThreshold) {
    return delay_us;
  }

  // 1. Kickstart the delay if we breach the limit for the first time
  if (delay_us == 0 && ratio > 1.0) {
    delay_us = 100;
  }
  // 2. Adjust the delay using a proportional feedback loop
  else if (delay_us > 0) {
    // Asymmetric Proportional Gain (kp):
    // Bumped the recovery gain from 0.05 to 0.10 so it bounces back from undershoots
    // faster.
    const double kp = (ratio > 1.0) ? 0.15 : 0.10;
    const double damped_ratio = 1.0 + ((ratio - 1.0) * kp);

    const auto current_delay_signed = static_cast<int64_t>(delay_us);
    const auto target_delay =
        static_cast<int64_t>(static_cast<double>(current_delay_signed) * damped_ratio);

    // Apply the asymmetric step limits
    int64_t delta_us = target_delay - current_delay_signed;
    if (delta_us > kMaxStepUpUs) {
      delta_us = kMaxStepUpUs;
    } else if (delta_us < -kMaxStepDownUs) {
      delta_us = -kMaxStepDownUs;
    }

    delay_us =
        static_cast<uint64_t>(std::max(int64_t{0}, current_delay_signed + delta_us));
  }

  // 3. Clamp to absolute maximum
  delay_us = std::min<uint64_t>(kMaxDelayUs, delay_us);

  // 4. Snap to 0 if the delay drops too low, and we are under the limit
  if (delay_us < 10 && ratio < 1.0) {
    delay_us = 0;
  }

  return delay_us;
}

void
TrafficShapingManager::UpdateTrafficShapingController()
{
  std::shared_lock read_lock(mPluginMutex);

  if (!mCustomControllerAlgo) {
    return;
  }

  // 1. Gather all unique active apps and defined policies.
  // Snapshot mAppPolicies under mMutex to avoid racing with Set/RemoveAppPolicy.
  std::set<std::string> unique_apps;
  auto rates = GetCurrentReadAndWriteRates();
  auto& app_read_map = std::get<0>(rates);
  auto& app_write_map = std::get<1>(rates);

  for (const auto& [app, _] : app_read_map) {
    unique_apps.insert(app);
  }
  for (const auto& [app, _] : app_write_map) {
    unique_apps.insert(app);
  }

  std::unordered_map<std::string, TrafficShapingPolicy> policies_snapshot;
  {
    std::shared_lock policies_lock(mMutex);
    policies_snapshot = mAppPolicies;
  }
  for (const auto& [app, _] : policies_snapshot) {
    unique_apps.insert(app);
  }

  if (unique_apps.empty()) {
    return;
  }

  // 2. Prepare the flat array for the plugin
  std::vector<AppState> app_array;
  app_array.reserve(unique_apps.size());

  for (const auto& app : unique_apps) {
    AppState st{}; // Zero-initialize
    strncpy(st.app_name, app.c_str(), sizeof(st.app_name) - 1);

    // Telemetry
    if (auto it = app_read_map.find(app); it != app_read_map.end()) {
      st.current_read_bps = it->second;
    }
    if (auto it = app_write_map.find(app); it != app_write_map.end()) {
      st.current_write_bps = it->second;
    }

    // Policy (from snapshot)
    if (auto pol_it = policies_snapshot.find(app); pol_it != policies_snapshot.end()) {
      st.reservation_write_bps = pol_it->second.reservation_write_bytes_per_sec;
      st.reservation_read_bps = pol_it->second.reservation_read_bytes_per_sec;
      st.controller_limit_write_bps = pol_it->second.controller_limit_write_bytes_per_sec;
      st.controller_limit_read_bps = pol_it->second.controller_limit_read_bytes_per_sec;
    }

    app_array.push_back(st);
  }

  // 3. INJECT THE DATA INTO THE PLUGIN
  mCustomControllerAlgo(app_array.data(), app_array.size());

  // 4. Read the results back and apply them
  for (const auto& st : app_array) {
    if (st.update_write || st.update_read) {
      TrafficShapingPolicy policy;

      // Preserve existing user configuration from snapshot.
      // SetAppPolicy acquires mMutex internally; do not hold it here.
      if (auto pol_it = policies_snapshot.find(st.app_name);
          pol_it != policies_snapshot.end()) {
        policy = pol_it->second;
      }

      if (st.update_write) {
        policy.controller_limit_write_bytes_per_sec = st.new_controller_limit_write_bps;
      }
      if (st.update_read) {
        policy.controller_limit_read_bytes_per_sec = st.new_controller_limit_read_bps;
      }

      // Automatically syncs to FSTs because SetAppPolicy bumps the config version
      SetAppPolicy(st.app_name, policy);

      eos_static_info("msg=\"Plugin applied dynamic controller limits\" app=\"%s\" "
                      "controller_limit_write_bps=%lu controller_limit_read_bps=%lu",
                      st.app_name, policy.controller_limit_write_bytes_per_sec,
                      policy.controller_limit_read_bytes_per_sec);
    }
  }
}

void
TrafficShapingManager::UpdateLimits()
{
  // 1. Evaluate hot-reload status at the start of every tick
  LoadPluginIfModified();

  eos::traffic_shaping::TrafficShapingFstIoDelayConfig fst_io_delay_config;

  auto* app_write_map = fst_io_delay_config.mutable_app_write_delay();
  auto* app_read_map = fst_io_delay_config.mutable_app_read_delay();
  auto* uid_write_map = fst_io_delay_config.mutable_uid_write_delay();
  auto* uid_read_map = fst_io_delay_config.mutable_uid_read_delay();
  auto* gid_write_map = fst_io_delay_config.mutable_gid_write_delay();
  auto* gid_read_map = fst_io_delay_config.mutable_gid_read_delay();

  auto adjust_delay = [&](const double limit_bps, const double current_rate,
                          uint64_t& delay_us, auto* output_map, const auto& entity_key,
                          const char* entity_type, const std::string& entity_id,
                          const char* op_type) {
    if (limit_bps <= 0) {
      return;
    }

    const uint64_t old_delay = delay_us;
    const double ratio = current_rate / limit_bps;
    {
      std::shared_lock read_lock(mPluginMutex);
      if (mCustomAlgo) {
        delay_us = mCustomAlgo(limit_bps, current_rate, old_delay);
      } else {
        delay_us = CalculateDelayUs(limit_bps, current_rate, old_delay);
      }
    }

    if (delay_us > 0) {
      (*output_map)[entity_key] = delay_us;
    }

    eos_static_debug("msg=\"throttle evaluation\" type=\"%s\" id=\"%s\" op=\"%s\" "
                     "limit_bps=%.0f current_rate_bps=%.0f ratio=%.3f "
                     "old_delay_us=%lu new_delay_us=%lu",
                     entity_type, entity_id.c_str(), op_type, limit_bps, current_rate,
                     ratio, old_delay, delay_us);
  };

  const auto [app_read_rates, app_write_rates, uid_read_rates, uid_write_rates,
              gid_read_rates, gid_write_rates] = GetCurrentReadAndWriteRates();

  // Helper lambda to do a single-pass map lookup
  auto get_rate = [](const auto& map, const auto& key) {
    if (auto it = map.find(key); it != map.end()) {
      return it->second;
    }
    return 0.0;
  };
  {
    std::shared_lock lock(mMutex);

    for (const auto& [app, policy] : mAppPolicies) {
      if (!policy.IsActive()) {
        eos_static_debug("msg=\"skipping inactive policy\" type=\"app\" id=\"%s\"",
                         app.c_str());
        continue;
      }

      adjust_delay(static_cast<double>(policy.GetEffectiveWriteLimit()),
                   get_rate(app_write_rates, app),
                   (*mFstIoDelayConfig.mutable_app_write_delay())[app], app_write_map,
                   app, "app", app, "write");

      adjust_delay(static_cast<double>(policy.GetEffectiveReadLimit()),
                   get_rate(app_read_rates, app),
                   (*mFstIoDelayConfig.mutable_app_read_delay())[app], app_read_map, app,
                   "app", app, "read");
    }

    for (const auto& [uid, policy] : mUidPolicies) {
      if (!policy.IsActive()) {
        eos_static_debug("msg=\"skipping inactive policy\" type=\"uid\" id=\"%u\"", uid);
        continue;
      }

      adjust_delay(static_cast<double>(policy.GetEffectiveWriteLimit()),
                   get_rate(uid_write_rates, uid),
                   (*mFstIoDelayConfig.mutable_uid_write_delay())[uid], uid_write_map,
                   uid, "uid", std::to_string(uid), "write");

      adjust_delay(static_cast<double>(policy.GetEffectiveReadLimit()),
                   get_rate(uid_read_rates, uid),
                   (*mFstIoDelayConfig.mutable_uid_read_delay())[uid], uid_read_map, uid,
                   "uid", std::to_string(uid), "read");
    }

    for (const auto& [gid, policy] : mGidPolicies) {
      if (!policy.IsActive()) {
        eos_static_debug("msg=\"skipping inactive policy\" type=\"gid\" id=\"%u\"", gid);
        continue;
      }

      adjust_delay(static_cast<double>(policy.GetEffectiveWriteLimit()),
                   get_rate(gid_write_rates, gid),
                   (*mFstIoDelayConfig.mutable_gid_write_delay())[gid], gid_write_map,
                   gid, "gid", std::to_string(gid), "write");

      adjust_delay(static_cast<double>(policy.GetEffectiveReadLimit()),
                   get_rate(gid_read_rates, gid),
                   (*mFstIoDelayConfig.mutable_gid_read_delay())[gid], gid_read_map, gid,
                   "gid", std::to_string(gid), "read");
    }
  }

  std::vector<std::string> online_nodes;
  {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
      if (node_view->GetStatus() == "online") {
        online_nodes.push_back(node_name);
      }
    }
  }

  std::string serialized = fst_io_delay_config.SerializeAsString();
  std::string encoded;

  if (!eos::common::SymKey::Base64(serialized, encoded)) {
    eos_static_warning("%s", "msg=\"failed to base64-encode FST IO limits config\"");
    return;
  }

  for (const auto& node_name : online_nodes) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_name);
    if (it != FsView::gFsView.mNodeView.end()) {
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_IO_LIMITS, encoded,
                                  true);
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

std::unordered_map<std::string, RateSnapshot>
TrafficShapingManager::GetNodeStats() const
{
  std::shared_lock lock(mMutex);
  std::unordered_map<std::string, RateSnapshot> snapshot_map;
  snapshot_map.reserve(mNodeStats.size());

  for (const auto& [node_id, internal_stat] : mNodeStats) {
    RateSnapshot& snap = snapshot_map[node_id];
    snap.last_activity_time = internal_stat.last_activity_time;
    snap.active_stream_count = internal_stat.active_stream_count;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash>
TrafficShapingManager::GetDiskStats() const
{
  std::shared_lock lock(mMutex);
  std::unordered_map<DiskKey, RateSnapshot, DiskKeyHash> snapshot_map;
  snapshot_map.reserve(mDiskStats.size());

  for (const auto& [disk_key, internal_stat] : mDiskStats) {
    RateSnapshot& snap = snapshot_map[disk_key];
    snap.last_activity_time = internal_stat.last_activity_time;
    snap.active_stream_count = internal_stat.active_stream_count;
    snap.ema = internal_stat.ema;
    snap.sma = internal_stat.sma;
  }

  return snapshot_map;
}

std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash>
TrafficShapingManager::GetDetailedStats() const
{
  std::shared_lock lock(mMutex);
  std::unordered_map<DetailedKey, RateSnapshot, DetailedKeyHash> snapshot_map;
  snapshot_map.reserve(mDetailedStats.size());

  for (const auto& [detailed_key, internal_stat] : mDetailedStats) {
    RateSnapshot& snap = snapshot_map[detailed_key];
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

  GarbageCollectionStats stats = {0, 0, 0, 0, 0};

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
      mNodeStats.erase(node_it->first);
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

  for (auto it = mDiskStats.begin(); it != mDiskStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mDiskStats.erase(it);
      stats.removed_disk_stats++;
    } else {
      ++it;
    }
  }

  for (auto it = mDetailedStats.begin(); it != mDetailedStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mDetailedStats.erase(it);
      stats.removed_detailed_stats++;
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
  if (fst_reports_processed_per_second) {
    fst_reports_processed_per_second->Tick();
  }
}

std::tuple<uint64_t, uint64_t, uint64_t>
TrafficShapingManager::GetEstimatorsUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (estimators_update_loop_micro_sec) {
    return {
        estimators_update_loop_micro_sec->GetMedian(true), // Ignore zeroes correctly
        estimators_update_loop_micro_sec->GetMin(true),
        estimators_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0, 0, 0};
}

std::tuple<uint64_t, uint64_t, uint64_t>
TrafficShapingManager::GetFstLimitsUpdateLoopMicroSecStats() const
{
  std::shared_lock lock(mMutex);
  if (fst_limits_update_loop_micro_sec) {
    return {
        fst_limits_update_loop_micro_sec->GetMedian(true),
        fst_limits_update_loop_micro_sec->GetMin(true),
        fst_limits_update_loop_micro_sec->GetMax(true),
    };
  }
  return {0, 0, 0};
}

void
TrafficShapingManager::UpdateFstReportsProcessed(const uint64_t count)
{
  std::unique_lock lock(mMutex);
  if (fst_reports_processed_per_second) {
    fst_reports_processed_per_second->Add(count);
  }
}

double
TrafficShapingManager::GetFstReportsProcessedPerSecondMean() const
{
  std::shared_lock lock(mMutex);
  if (fst_reports_processed_per_second) {
    double multiplier = 1.0;
    if (mEstimatorsTickIntervalSec > 0.0) {
      multiplier = 1.0 / mEstimatorsTickIntervalSec;
    }

    // 0 counts in a tick are valid measurements for processing speed.
    return fst_reports_processed_per_second->GetMean(false) * multiplier;
  }
  return 0.0;
}

void
TrafficShapingManager::Clear()
{
  std::unique_lock lock(mMutex);
  mNodeStates.clear();
  mGlobalStats.clear();
  mNodeStats.clear();
  mDiskStats.clear();
  mDetailedStats.clear();
  mTotalStats.clear();

  estimators_update_loop_micro_sec.reset();
  fst_limits_update_loop_micro_sec.reset();
  fst_reports_processed_per_second.reset();

  if (mPluginHandle) {
    dlclose(mPluginHandle);
    mPluginHandle = nullptr;
    mCustomAlgo = nullptr;
    mCustomControllerAlgo = nullptr;
    mPluginLastModified = 0;
  }
}

void
TrafficShapingManager::ClearRuntimeStats()
{
  std::unique_lock lock(mMutex);
  mNodeStates.clear();
  mGlobalStats.clear();
  mNodeStats.clear();
  mDiskStats.clear();
  mDetailedStats.clear();
  mTotalStats.clear();
}

void
TrafficShapingManager::ClearDetailedRuntimeStats()
{
  std::unique_lock lock(mMutex);
  mDiskStats.clear();
  mDetailedStats.clear();
}

RateSnapshot
TrafficShapingManager::GetTotalStats() const
{
  std::shared_lock lock(mMutex);
  RateSnapshot snap;
  snap.last_activity_time = mTotalStats.last_activity_time;
  snap.active_stream_count = mTotalStats.active_stream_count;
  snap.ema = mTotalStats.ema;
  snap.sma = mTotalStats.sma;
  return snap;
}

TrafficShapingEngine::TrafficShapingEngine()
    : mRunning(false)
    , mEstimatorsUpdateThreadPeriodMilliseconds(200)
    , mFstIoPolicyUpdateThreadPeriodMilliseconds(200)
    , mFstIoStatsReportThreadPeriodMilliseconds(200)
    , mSystemStatsWindowSeconds(15)
    , mFilesystemDetailEnabled(false)
{
  mManager = std::make_shared<TrafficShapingManager>();
}

TrafficShapingEngine::~TrafficShapingEngine() { Stop(); }

void
TrafficShapingEngine::ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                                        uint32_t win_s, const bool save_to_config_engine)
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
TrafficShapingEngine::SetEstimatorsUpdateThreadPeriodMilliseconds(
    const uint32_t period_ms)
{
  ApplyThreadConfig(period_ms, mFstIoPolicyUpdateThreadPeriodMilliseconds,
                    mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds,
                    true);
}

void
TrafficShapingEngine::SetFstIoPolicyUpdateThreadPeriodMilliseconds(
    const uint32_t period_ms)
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
TrafficShapingEngine::SetDetailLevel(const std::string& detail_level,
                                     bool save_to_config_engine)
{
  const bool fs_detail =
      detail_level == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM;
  const std::string normalized_detail_level =
      fs_detail ? eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM
                : eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE;
  const bool old_value =
      mFilesystemDetailEnabled.exchange(fs_detail, std::memory_order_relaxed);

  if (mManager != nullptr) {
    if (old_value != fs_detail) {
      mManager->ClearRuntimeStats();
    } else if (!fs_detail) {
      mManager->ClearDetailedRuntimeStats();
    }
  }

  if (save_to_config_engine) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_LEVEL_CONFIG,
                                    normalized_detail_level);
    gOFS->mConfigEngine->AutoSave();
  }

  if (save_to_config_engine || old_value != fs_detail) {
    SyncTrafficShapingEnabledWithFst();
  }
}

std::string
TrafficShapingEngine::GetDetailLevel() const
{
  return mFilesystemDetailEnabled.load(std::memory_order_relaxed)
             ? eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM
             : eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE;
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

  std::string serialized = thread_loop_stats.SerializeAsString();
  std::string encoded;

  if (!eos::common::SymKey::Base64(serialized, encoded)) {
    eos_static_warning("%s", "msg=\"failed to base64-encode thread periods config\"");
    return;
  }

  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_THREAD_PERIODS, encoded);
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
    try {
      std::string serialized_thread_periods;

      if (!eos::common::SymKey::DeBase64(thread_periods, serialized_thread_periods)) {
        eos_static_err("%s", "msg=\"failed to base64-decode thread periods config\"");
        serialized_thread_periods.clear();
      }

      eos::traffic_shaping::ThreadConfig thread_config;

      if (!serialized_thread_periods.empty() &&
          thread_config.ParseFromString(serialized_thread_periods)) {
        est_ms = thread_config.update_estimators_period_millis();
        pol_ms = thread_config.fst_policy_update_period_millis();
        rep_ms = thread_config.fst_io_stats_report_period_millis();
        win_s = thread_config.system_stats_time_window_seconds();
      } else if (!serialized_thread_periods.empty()) {
        eos_static_err("%s", "msg=\"failed to parse thread periods config\"");
      }
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse thread periods config\" error=%s", e.what());
    }
  }

  ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s, false);

  const std::string detail_level =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_LEVEL_CONFIG);

  if (detail_level.empty()) {
    SetDetailLevel(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE);
  } else {
    SetDetailLevel(detail_level, false);
  }

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

  // NOTE: Do NOT call SyncTrafficShapingEnabledWithFst() here.
  // Start() can be invoked from Enable() which is called from
  // FsView::ApplyGlobalConfig() while the ViewMutex WRITE lock is held.
  // SyncTrafficShapingEnabledWithFst() acquires ViewMutex READ lock, which
  // would deadlock the same thread.  The periodic FstTrafficShapingConfigUpdate
  // thread provides the heartbeat sync, and TrafficShapingEngine::ApplyConfig()
  // (called after the write lock is released) provides the immediate sync.
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
  mFstTrafficShapingConfigUpdateThread.join();
  {
    std::lock_guard lock(mReportQueueMutex);
    mReportQueue.clear();
  }

  if (mManager != nullptr) {
    mManager->Clear();
  }

  // NOTE: Do NOT call SyncTrafficShapingEnabledWithFst() here for the same
  // reason as in Start(): Stop() can be called from Disable() which is called
  // from FsView::ApplyGlobalConfig() while ViewMutex write lock is held.
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
  static constexpr std::size_t kMaxReports = 500;

  std::lock_guard lock(mReportQueueMutex);

  if (mReportQueue.size() >= kMaxReports) {
    eos_static_warning(
        "msg=\"Traffic Shaping report queue full, dropping oldest report\" "
        "size=%zu",
        mReportQueue.size());
    mReportQueue.erase(mReportQueue.begin());
  }

  mReportQueue.emplace_back(report);
}

void
TrafficShapingEngine::ProcessAllQueuedReports()
{
  std::vector<eos::traffic_shaping::FstIoReport> local_queue;
  {
    std::lock_guard lock(mReportQueueMutex);
    std::swap(mReportQueue, local_queue);
  }

  if (mManager == nullptr) {
    return;
  }

  for (const auto& report : local_queue) {
    mManager->ProcessReport(report);
  }

  mManager->UpdateFstReportsProcessed(local_queue.size());
}

void
TrafficShapingEngine::EstimatorsUpdate(ThreadAssistant& assistant)
{
  eos_static_info("%s", "msg=\"Starting Traffic Shaping estimators update thread\"");

  auto last_run = std::chrono::steady_clock::now();

  int infrequent_action_counter = 0;
  constexpr int infrequent_action_threshold = 100;

  while (!assistant.terminationRequested()) {
    assistant.wait_for(
        std::chrono::milliseconds(mEstimatorsUpdateThreadPeriodMilliseconds));

    if (assistant.terminationRequested()) {
      break;
    }

    ProcessAllQueuedReports();

    auto now = std::chrono::steady_clock::now();
    const std::chrono::duration<double> elapsed = now - last_run;
    const double time_delta_seconds = elapsed.count();
    last_run = now;

    mManager->UpdateEstimators(time_delta_seconds);

    if (++infrequent_action_counter >= infrequent_action_threshold) {
      infrequent_action_counter = 0;
      const auto [removed_nodes, removed_node_streams, removed_global_streams,
                  removed_disk_stats, removed_detailed_stats] =
          mManager->GarbageCollect(900);

      if (removed_node_streams > 0 || removed_global_streams > 0 ||
          removed_disk_stats > 0 || removed_detailed_stats > 0) {
        eos_static_debug("msg=\"Traffic Shaping Garbage Collection\" removed_nodes=%lu "
                         "removed_node_streams=%lu "
                         "removed_global_streams=%lu removed_disk_stats=%lu "
                         "removed_detailed_stats=%lu",
                         removed_nodes, removed_node_streams, removed_global_streams,
                         removed_disk_stats, removed_detailed_stats);
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

  while (!assistant.terminationRequested()) {
    assistant.wait_for(
        std::chrono::milliseconds(mFstIoPolicyUpdateThreadPeriodMilliseconds));

    if (assistant.terminationRequested()) {
      break;
    }

    auto work_start_time = std::chrono::steady_clock::now();

    mManager->UpdateTrafficShapingController();

    mManager->UpdateLimits();

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
  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      break;
    }

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
  const std::string enabled_str = enabled ? "true" : "false";
  const std::string period_str =
      std::to_string(mFstIoStatsReportThreadPeriodMilliseconds);
  const std::string detail_level = GetDetailLevel();

  std::vector<std::string> online_nodes;
  {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
      if (node_view->GetStatus() == "online") {
        online_nodes.push_back(node_name);
      }
    }
  }

  for (const auto& node_name : online_nodes) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_name);
    if (it != FsView::gFsView.mNodeView.end()) {
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_ENABLE_TOGGLE,
                                  enabled_str, true);
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_STATS_THREAD_PERIOD,
                                  period_str, true);
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_DETAIL_LEVEL,
                                  detail_level, true);
    }
  }
}

void
TrafficShapingManager::SetUidPolicy(const uint32_t uid,
                                    const TrafficShapingPolicy& policy)
{
  bool config_changed = false;
  std::string serialized;
  {
    std::unique_lock lock(mMutex);
    const auto it = mUidPolicies.find(uid);

    if (policy.IsEmpty()) {
      if (it != mUidPolicies.end()) {
        mUidPolicies.erase(it);
        config_changed = true;
        eos_static_info("msg=\"Removed empty UID Traffic Shaping Policy\" uid=%u", uid);
      }
    } else {
      if (it == mUidPolicies.end()) {
        mUidPolicies[uid] = policy;
        config_changed = true;
        eos_static_info("msg=\"Set UID Traffic Shaping Policy\" uid=%u policy=%s", uid,
                        policy.ToString().c_str());
      } else {
        // operator!= ignores controller limits, so it only flags true user config changes
        if (it->second != policy) {
          config_changed = true;
        }
        // Always update in-memory to reflect any potential ephemeral controller limit
        // changes
        it->second = policy;
        eos_static_info("msg=\"Updated UID Traffic Shaping Policy\" uid=%u policy=%s "
                        "persistent_changed=%d",
                        uid, policy.ToString().c_str(), config_changed);
      }
    }

    if (config_changed) {
      serialized = SerializePoliciesUnlocked();
    }
  }

  if (config_changed) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
    gOFS->mConfigEngine->AutoSave();
  }
}

void
TrafficShapingManager::SetGidPolicy(const uint32_t gid,
                                    const TrafficShapingPolicy& policy)
{
  bool config_changed = false;
  std::string serialized;
  {
    std::unique_lock lock(mMutex);
    auto it = mGidPolicies.find(gid);

    if (policy.IsEmpty()) {
      if (it != mGidPolicies.end()) {
        mGidPolicies.erase(it);
        config_changed = true;
        eos_static_info("msg=\"Removed empty GID Traffic Shaping Policy\" gid=%u", gid);
      }
    } else {
      if (it == mGidPolicies.end()) {
        mGidPolicies[gid] = policy;
        config_changed = true;
        eos_static_info("msg=\"Set GID Traffic Shaping Policy\" gid=%u policy=%s", gid,
                        policy.ToString().c_str());
      } else {
        if (it->second != policy) {
          config_changed = true;
        }
        it->second = policy;
        eos_static_info("msg=\"Updated GID Traffic Shaping Policy\" gid=%u policy=%s "
                        "persistent_changed=%d",
                        gid, policy.ToString().c_str(), config_changed);
      }
    }

    if (config_changed) {
      serialized = SerializePoliciesUnlocked();
    }
  }

  if (config_changed) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
    gOFS->mConfigEngine->AutoSave();
  }
}

void
TrafficShapingManager::SetAppPolicy(const std::string& app,
                                    const TrafficShapingPolicy& policy)
{
  bool config_changed = false;
  std::string serialized;
  {
    std::unique_lock lock(mMutex);
    const auto it = mAppPolicies.find(app);

    if (policy.IsEmpty()) {
      if (it != mAppPolicies.end()) {
        mAppPolicies.erase(it);
        config_changed = true;
        eos_static_info("msg=\"Removed empty App Traffic Shaping Policy\" app=%s",
                        app.c_str());
      }
    } else {
      if (it == mAppPolicies.end()) {
        mAppPolicies[app] = policy;
        config_changed = true;
        eos_static_info("msg=\"Set App Traffic Shaping Policy\" app=%s policy=%s",
                        app.c_str(), policy.ToString().c_str());
      } else {
        if (it->second != policy) {
          config_changed = true;
        }
        it->second = policy;
        eos_static_info("msg=\"Updated App Traffic Shaping Policy\" app=%s policy=%s "
                        "persistent_changed=%d",
                        app.c_str(), policy.ToString().c_str(), config_changed);
      }
    }

    if (config_changed) {
      serialized = SerializePoliciesUnlocked();
    }
  }

  if (config_changed) {
    FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
    gOFS->mConfigEngine->AutoSave();
  }
}

void
TrafficShapingManager::RemoveUidPolicy(const uint32_t uid)
{
  std::string serialized;
  {
    std::unique_lock lock(mMutex);
    if (!mUidPolicies.erase(uid)) {
      return;
    }
    eos_static_info("msg=\"Removed UID Traffic Shaping Policy\" uid=%u", uid);
    serialized = SerializePoliciesUnlocked();
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingManager::RemoveGidPolicy(const uint32_t gid)
{
  std::string serialized;
  {
    std::unique_lock lock(mMutex);
    if (!mGidPolicies.erase(gid)) {
      return;
    }
    eos_static_info("msg=\"Removed GID Traffic Shaping Policy\" gid=%u", gid);
    serialized = SerializePoliciesUnlocked();
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingManager::RemoveAppPolicy(const std::string& app)
{
  std::string serialized;
  {
    std::unique_lock lock(mMutex);
    if (!mAppPolicies.erase(app)) {
      return;
    }
    eos_static_info("msg=\"Removed App Traffic Shaping Policy\" app=%s", app.c_str());
    serialized = SerializePoliciesUnlocked();
  }
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_POLICIES_CONFIG, serialized);
  gOFS->mConfigEngine->AutoSave();
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

  if (const auto status =
          google::protobuf::util::MessageToJsonString(proto_config, &json_data, options);
      !status.ok()) {
    eos_static_err("msg=\"Failed to serialize policies to JSON\"");
  }

  return json_data;
}

bool
TrafficShapingManager::LoadPoliciesFromString(const std::string& serialized_policies)
{
  if (serialized_policies.empty()) {
    return true;
  }

  std::unordered_map<uint32_t, TrafficShapingPolicy> new_uid_policies;
  std::unordered_map<uint32_t, TrafficShapingPolicy> new_gid_policies;
  std::unordered_map<std::string, TrafficShapingPolicy> new_app_policies;

  std::unique_lock lock(mMutex);

  // Parse under mMutex so that concurrent Set/RemovePolicy calls cannot land
  // between parsing and map replacement, which would cause their updates to be lost.
  eos::traffic_shaping::TrafficShapingPolicyConfig proto_config;

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = true;

  const auto status = google::protobuf::util::JsonStringToMessage(serialized_policies,
                                                                  &proto_config, options);
  if (!status.ok()) {
    eos_static_err("msg=\"Failed to parse policies from JSON string\"");
    return false;
  }

  auto merge_policies = [](auto& current_map, const auto& proto_map, auto& new_map) {
    auto copy_to_cpp = [](const auto& proto_pol) -> TrafficShapingPolicy {
      TrafficShapingPolicy cpp_pol;
      cpp_pol.limit_write_bytes_per_sec = proto_pol.limit_write_bytes_per_sec();
      cpp_pol.limit_read_bytes_per_sec = proto_pol.limit_read_bytes_per_sec();
      cpp_pol.reservation_write_bytes_per_sec =
          proto_pol.reservation_write_bytes_per_sec();
      cpp_pol.reservation_read_bytes_per_sec = proto_pol.reservation_read_bytes_per_sec();
      cpp_pol.is_enabled = proto_pol.is_enabled();
      return cpp_pol;
    };

    for (const auto& [id, pol] : proto_map) {
      auto cpp_pol = copy_to_cpp(pol);
      if (auto it = current_map.find(id); it != current_map.end()) {
        cpp_pol.controller_limit_read_bytes_per_sec =
            it->second.controller_limit_read_bytes_per_sec;
        cpp_pol.controller_limit_write_bytes_per_sec =
            it->second.controller_limit_write_bytes_per_sec;
      }
      new_map[id] = cpp_pol;
    }

    // 2. Retain Python-generated ephemeral policies (policies with ONLY controller
    // limits) that might have been skipped by the DB serialization
    TrafficShapingPolicy empty_user_pol; // All user fields are 0/disabled
    for (const auto& [id, pol] : current_map) {
      if (new_map.find(id) == new_map.end()) {
        // We use operator== because it deliberately ignores controller fields.
        // If this evaluates to true, it means NO user settings exist for this policy.
        if (pol == empty_user_pol && (pol.controller_limit_read_bytes_per_sec > 0 ||
                                      pol.controller_limit_write_bytes_per_sec > 0)) {
          new_map[id] = pol;
        }
      }
    }
  };

  merge_policies(mUidPolicies, proto_config.uid_policies(), new_uid_policies);
  merge_policies(mGidPolicies, proto_config.gid_policies(), new_gid_policies);
  merge_policies(mAppPolicies, proto_config.app_policies(), new_app_policies);

  mUidPolicies = std::move(new_uid_policies);
  mGidPolicies = std::move(new_gid_policies);
  mAppPolicies = std::move(new_app_policies);

  return true;
}

void
TrafficShapingManager::LoadPluginIfModified()
{
  const char* plugin_path = "/etc/eos/traffic_shaping_plugin.so";
  struct stat file_stat;

  // 1. Check if the hot-reload file exists on disk
  if (stat(plugin_path, &file_stat) == 0) {
    // 2. Check if the file has been updated since our last load
    if (file_stat.st_mtime > mPluginLastModified) {
      eos_static_info("msg=\"Detected new or modified traffic shaping plugin. "
                      "Loading...\" path=\"%s\"",
                      plugin_path);

      // ACQUIRE EXCLUSIVE WRITE LOCK: Wait for any active evaluation threads to finish
      std::unique_lock write_lock(mPluginMutex);

      // Close the old plugin if it exists
      if (mPluginHandle) {
        dlclose(mPluginHandle);
        mPluginHandle = nullptr;
        mCustomAlgo = nullptr;
        mCustomControllerAlgo = nullptr;
      }

      // Open the new plugin
      mPluginHandle = dlopen(plugin_path, RTLD_NOW | RTLD_LOCAL);
      if (mPluginHandle) {
        dlerror(); // Clear any existing errors

        // Find our specific C functions
        mCustomAlgo = (DelayAlgoFunc)dlsym(mPluginHandle, "calculate_delay");
        mCustomControllerAlgo =
            (ControllerAlgoFunc)dlsym(mPluginHandle, "update_controller");

        // We require at least one symbol to be present to consider the load a success
        if (!mCustomAlgo && !mCustomControllerAlgo) {
          eos_static_err("msg=\"Failed to find any matching symbols ('calculate_delay' "
                         "or 'update_controller') in plugin! "
                         "Falling back to built-in.\" error=\"%s\"",
                         dlerror());
          dlclose(mPluginHandle);
          mPluginHandle = nullptr;
          mCustomAlgo = nullptr;
          mCustomControllerAlgo = nullptr;
        } else {
          eos_static_info("msg=\"Successfully loaded and activated custom traffic "
                          "shaping plugin\"");
        }
      } else {
        eos_static_err("msg=\"Failed to load traffic shaping plugin. Falling back to "
                       "built-in.\" error=\"%s\"",
                       dlerror());
      }

      // Update our timestamp so we don't reload it again until it changes
      mPluginLastModified = file_stat.st_mtime;
    }
  } else {
    // 3. File doesn't exist. If we have one loaded, the user deleted it. Revert to
    // built-in.
    if (mPluginHandle) {
      eos_static_info("msg=\"Traffic shaping plugin file removed. Reverting to "
                      "built-in algorithm.\"");

      std::unique_lock write_lock(mPluginMutex);

      dlclose(mPluginHandle);
      mPluginHandle = nullptr;
      mCustomAlgo = nullptr;
      mCustomControllerAlgo = nullptr;
      mPluginLastModified = 0;
    }
  }
}

} // namespace eos::mgm::traffic_shaping
