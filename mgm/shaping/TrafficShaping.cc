#include "mgm/shaping/TrafficShaping.hh"
#include "common/Constants.hh"
#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "mgm/config/IConfigEngine.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"

#include "proto/TrafficShaping.pb.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <google/protobuf/util/json_util.h>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <utility>

namespace eos::mgm::traffic_shaping {

namespace {
constexpr double kUnknownIoPressure = 1.0;
constexpr double kActiveEntityNodeRateBps = 1024.0 * 1024.0;
constexpr double kMinReservationDeficitFraction = 0.05;
constexpr double kMinReservationDeficitBps = 16.0 * 1024.0 * 1024.0;
constexpr auto kControllerLimitTtl = std::chrono::minutes(5);
constexpr auto kControllerUpdateInterval = std::chrono::milliseconds(500);
constexpr auto kFstIoDelayConfigRefreshInterval = std::chrono::seconds(5);
constexpr auto kPluginCheckInterval = std::chrono::seconds(1);

template <typename DelayMap, typename Key>
void
ScaleDelayForLimitChange(DelayMap* delays, const Key& key, const uint64_t old_limit,
                         const uint64_t new_limit)
{
  if (old_limit == new_limit) {
    return;
  }

  if (new_limit == 0) {
    delays->erase(key);
    return;
  }

  if (old_limit == 0) {
    return;
  }

  auto it = delays->find(key);
  if (it == delays->end() || it->second == 0) {
    return;
  }

  const long double scaled_delay =
      static_cast<long double>(it->second) * old_limit / new_limit;
  it->second = std::max<uint64_t>(1, static_cast<uint64_t>(scaled_delay + 0.5L));
}

std::string
NormalizeFstNodeId(const std::string& node_id)
{
  if (node_id.rfind("/eos/", 0) == 0) {
    return node_id;
  }

  return SSTR("/eos/" << node_id << "/fst");
}

TrafficShapingPolicy
PreparePolicyForSet(const TrafficShapingPolicy& policy,
                    const TrafficShapingPolicy* old_policy)
{
  auto next_policy = policy;
  const auto now = std::chrono::steady_clock::now();

  auto refresh_timestamp =
      [now](const uint64_t limit, std::chrono::steady_clock::time_point& update_time,
            const TrafficShapingPolicy* old_policy, const uint64_t old_limit,
            const std::chrono::steady_clock::time_point old_update_time) {
        if (limit == 0) {
          update_time = {};
          return;
        }

        if (old_policy == nullptr || limit != old_limit ||
            update_time == std::chrono::steady_clock::time_point{}) {
          update_time = now;
          return;
        }

        if (update_time == old_update_time) {
          return;
        }
      };

  refresh_timestamp(next_policy.controller_limit_write_bytes_per_sec,
                    next_policy.controller_limit_write_update_time, old_policy,
                    old_policy ? old_policy->controller_limit_write_bytes_per_sec : 0,
                    old_policy ? old_policy->controller_limit_write_update_time
                               : std::chrono::steady_clock::time_point{});
  refresh_timestamp(next_policy.controller_limit_read_bytes_per_sec,
                    next_policy.controller_limit_read_update_time, old_policy,
                    old_policy ? old_policy->controller_limit_read_bytes_per_sec : 0,
                    old_policy ? old_policy->controller_limit_read_update_time
                               : std::chrono::steady_clock::time_point{});

  return next_policy;
}
} // namespace

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
  for (auto& [node_entity_key, stats] : mNodeEntityStats) {
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

void
TrafficShapingManager::SetFilesystemDetailEnabled(const bool enabled)
{
  mFilesystemDetailEnabled.store(enabled, std::memory_order_relaxed);
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

namespace {
struct EntityRateMaps {
  std::unordered_map<std::string, double> app_read;
  std::unordered_map<std::string, double> app_write;
  std::unordered_map<uint32_t, double> uid_read;
  std::unordered_map<uint32_t, double> uid_write;
  std::unordered_map<uint32_t, double> gid_read;
  std::unordered_map<uint32_t, double> gid_write;
};

std::unordered_map<std::string, double>
CollectNodeIoPressure(std::vector<std::string>* online_nodes = nullptr)
{
  std::unordered_map<std::string, double> node_io_pressure;

  eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
  for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
    if (node_view->GetStatus() != "online") {
      continue;
    }

    if (online_nodes) {
      online_nodes->push_back(node_name);
    }

    bool have_pressure_sample = false;
    double max_disk_load = 0.0;

    for (auto fsid_it = node_view->begin(); fsid_it != node_view->end(); ++fsid_it) {
      auto* fs = FsView::gFsView.mIdView.lookupByID(*fsid_it);
      if (!BaseView::ConsiderForStatistics(fs)) {
        continue;
      }

      max_disk_load = std::max(max_disk_load, fs->GetDouble("stat.disk.load"));
      have_pressure_sample = true;
    }

    if (have_pressure_sample) {
      node_io_pressure[node_name] = max_disk_load;
    }
  }

  return node_io_pressure;
}

void
UpdateAppIoPressure(AppIoPressureSnapshot& app_pressure, const RateMetrics& metrics,
                    const double node_pressure)
{
  if (metrics.read_rate_bps >= kActiveEntityNodeRateBps) {
    app_pressure.read = std::max(app_pressure.read, node_pressure);
    app_pressure.has_read = true;
  }

  if (metrics.write_rate_bps >= kActiveEntityNodeRateBps) {
    app_pressure.write = std::max(app_pressure.write, node_pressure);
    app_pressure.has_write = true;
  }
}

void
AddStreamRates(EntityRateMaps& rates, const StreamKey& key, const RateMetrics& metrics)
{
  rates.app_read[key.app] += metrics.read_rate_bps;
  rates.app_write[key.app] += metrics.write_rate_bps;
  rates.uid_read[key.uid] += metrics.read_rate_bps;
  rates.uid_write[key.uid] += metrics.write_rate_bps;
  rates.gid_read[key.gid] += metrics.read_rate_bps;
  rates.gid_write[key.gid] += metrics.write_rate_bps;
}

void
AddDeltas(MultiWindowRate& stats, const uint64_t delta_bytes_read,
          const uint64_t delta_bytes_written, const uint64_t delta_read_iops,
          const uint64_t delta_write_iops, const time_t now_unix)
{
  stats.bytes_read_accumulator += delta_bytes_read;
  stats.bytes_written_accumulator += delta_bytes_written;
  stats.read_iops_accumulator += delta_read_iops;
  stats.write_iops_accumulator += delta_write_iops;
  stats.last_activity_time = now_unix;
}
} // namespace

void
TrafficShapingManager::ProcessReport(const eos::traffic_shaping::FstIoReport& report)
{
  const std::string node_id = NormalizeFstNodeId(report.node_id());

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

      AddDeltas(global, delta_bytes_read, delta_bytes_written, delta_read_iops,
                delta_write_iops, now_unix);

      DetailedKey node_entity_key{node_id, {key.app, key.uid, key.gid, 0}};
      auto [node_entity_it, node_entity_inserted] =
          mNodeEntityStats.try_emplace(node_entity_key, mEstimatorsTickIntervalSec);
      MultiWindowRate& node_entity = node_entity_it->second;

      AddDeltas(node_entity, delta_bytes_read, delta_bytes_written, delta_read_iops,
                delta_write_iops, now_unix);

      if (mFilesystemDetailEnabled.load(std::memory_order_relaxed) && entry.fsid() != 0) {
        DetailedKey detailed_key{node_id, key};
        auto [detailed_it, detailed_inserted] =
            mDetailedStats.try_emplace(detailed_key, mEstimatorsTickIntervalSec);
        MultiWindowRate& detailed = detailed_it->second;

        AddDeltas(detailed, delta_bytes_read, delta_bytes_written, delta_read_iops,
                  delta_write_iops, now_unix);

        DiskKey disk_key{node_id, entry.fsid()};
        auto [disk_it, disk_inserted] =
            mDiskStats.try_emplace(disk_key, mEstimatorsTickIntervalSec);
        MultiWindowRate& disk = disk_it->second;

        AddDeltas(disk, delta_bytes_read, delta_bytes_written, delta_read_iops,
                  delta_write_iops, now_unix);
      }
    }
  }

  if (total_node_delta_bytes_read > 0 || total_node_delta_bytes_written > 0 ||
      total_node_delta_read_iops > 0 || total_node_delta_write_iops > 0) {

    auto [it, inserted] = mNodeStats.try_emplace(node_id, mEstimatorsTickIntervalSec);
    MultiWindowRate& node_stat = it->second;

    AddDeltas(node_stat, total_node_delta_bytes_read, total_node_delta_bytes_written,
              total_node_delta_read_iops, total_node_delta_write_iops, now_unix);

    AddDeltas(mTotalStats, total_node_delta_bytes_read, total_node_delta_bytes_written,
              total_node_delta_read_iops, total_node_delta_write_iops, now_unix);
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
  for (auto& [node_entity_key, stats] : mNodeEntityStats) {
    process_rate(stats);
  }
  process_rate(mTotalStats);
}

uint64_t
TrafficShapingManager::CalculateDelayUs(
    const double limit_bps, const double current_rate_bps,
    const uint64_t current_delay_us, const double io_pressure, const bool has_rate_sample,
    const bool allow_idle_release, const double delay_reference_bps,
    const double io_pressure_threshold)
{
  if (limit_bps <= 0.0) {
    return 0;
  }

  constexpr uint64_t kMaxDelayUs = 2000000;
  constexpr uint64_t kDelayReferenceBytes = 1024 * 1024;
  constexpr uint64_t kIdleReleaseStepDownUs = 250000;
  constexpr uint64_t kSparseSampleStepDownUs = 80000;
  constexpr double kControlBias = 0.78;
  constexpr double kIdleRatio = 0.01;
  constexpr double kLowerDeadbandRatio = 0.94;
  constexpr double kUpperDeadbandRatio = 1.02;

  uint64_t delay_us = current_delay_us;
  const double reference_bps =
      delay_reference_bps > 0.0 ? delay_reference_bps : limit_bps;
  const double biased_limit_bps = limit_bps * kControlBias;
  const double biased_reference_bps = reference_bps * kControlBias;
  const uint64_t seed_delay_us = std::min<uint64_t>(
      kMaxDelayUs,
      static_cast<uint64_t>((kDelayReferenceBytes * 1000000.0) / biased_reference_bps));
  const double ratio = current_rate_bps / biased_limit_bps;

  // 0. Idle seed: when traffic is sparse, use the current limit's baseline
  // delay. This shapes the first transfer and lets raised limits release old
  // delay even before dense samples arrive. If EOS already measures no storage
  // pressure for this FST, this is a controller-only limit, and we have seen
  // this entity on the node, release the idle delay instead of keeping a sparse
  // low-demand app throttled indefinitely. Do not release explicit user limits
  // or a pre-traffic seed, otherwise a hard policy can decay away before or
  // during the first transfer. For explicit limits, shed existing delay slowly
  // while the entity still has a rate sample; a delayed stream can briefly report
  // near zero rate, and resetting straight to the seed creates a burst/collapse
  // cycle.
  if (ratio < kIdleRatio) {
    if (allow_idle_release && has_rate_sample &&
        io_pressure < std::max(0.0, std::min(1.0, io_pressure_threshold))) {
      return delay_us > kIdleReleaseStepDownUs ? delay_us - kIdleReleaseStepDownUs : 0;
    }

    if (has_rate_sample) {
      const uint64_t released_delay_us =
          delay_us > kSparseSampleStepDownUs ? delay_us - kSparseSampleStepDownUs : 0;
      return std::max(released_delay_us, seed_delay_us);
    }

    return seed_delay_us;
  }

  if (delay_us == 0 && ratio > kUpperDeadbandRatio) {
    delay_us = std::max<uint64_t>(1000, seed_delay_us);
  }

  if (ratio > kUpperDeadbandRatio) {
    const double excess = std::min(4.0, ratio - 1.0);
    const uint64_t proportional_step = static_cast<uint64_t>(
        static_cast<double>(std::max(delay_us, seed_delay_us)) * (0.30 * excess));
    const uint64_t seed_step =
        static_cast<uint64_t>(static_cast<double>(seed_delay_us) * (3.5 * excess));
    const uint64_t step = std::clamp<uint64_t>(
        std::max<uint64_t>({proportional_step, seed_step, 1000}), 1000, 160000);
    delay_us = std::min<uint64_t>(kMaxDelayUs, delay_us + step);
  } else if (ratio < kLowerDeadbandRatio) {
    const double deficit = std::min(1.0, 1.0 - ratio);
    const uint64_t proportional_step =
        static_cast<uint64_t>(static_cast<double>(delay_us) * (0.025 + 0.05 * deficit));
    const uint64_t step = std::clamp<uint64_t>(
        std::max<uint64_t>(proportional_step, seed_delay_us / 5), 250, 25000);
    delay_us = delay_us > step ? delay_us - step : 0;

    if (ratio > 0.70) {
      delay_us = std::max(delay_us, seed_delay_us);
    }
  } else if (delay_us < seed_delay_us) {
    delay_us = seed_delay_us;
  }

  if (delay_us < 10 && ratio < kLowerDeadbandRatio) {
    delay_us = 0;
  }

  return delay_us;
}

void
TrafficShapingManager::ApplyDefaultReservationController(
    std::vector<AppState>& apps, const bool reservations_enabled,
    const uint64_t controller_min_limit_bps, const double io_pressure_threshold)
{
  const double pressure_threshold = std::max(0.0, std::min(1.0, io_pressure_threshold));

  auto update_direction = [reservations_enabled, controller_min_limit_bps,
                           pressure_threshold](std::vector<AppState>& apps,
                                               const bool is_write) {
    auto reservation = [is_write](const AppState& app) {
      return is_write ? app.reservation_write_bps : app.reservation_read_bps;
    };
    auto controller_limit = [is_write](const AppState& app) {
      return is_write ? app.controller_limit_write_bps : app.controller_limit_read_bps;
    };
    auto current_rate = [is_write](const AppState& app) {
      return is_write ? app.current_write_bps : app.current_read_bps;
    };
    auto has_pressure = [is_write](const AppState& app) {
      return is_write ? app.has_write_io_pressure : app.has_read_io_pressure;
    };
    auto io_pressure = [is_write](const AppState& app) {
      return is_write ? app.current_write_io_pressure : app.current_read_io_pressure;
    };
    auto has_reservation_competition = [is_write](const AppState& app) {
      return is_write ? app.has_write_reservation_competition
                      : app.has_read_reservation_competition;
    };
    auto set_controller_limit = [is_write](AppState& app, const uint64_t limit) {
      if (is_write) {
        app.new_controller_limit_write_bps = limit;
        app.update_write = true;
      } else {
        app.new_controller_limit_read_bps = limit;
        app.update_read = true;
      }
    };

    auto is_pressure_active_reservation = [&](const AppState& app) {
      return reservations_enabled && reservation(app) > 0 && has_pressure(app) &&
             io_pressure(app) >= pressure_threshold;
    };

    uint64_t reserved_limit_bps = 0;
    double reserved_rate_bps = 0.0;
    double competitor_rate_bps = 0.0;

    for (const auto& app : apps) {
      if (is_pressure_active_reservation(app)) {
        reserved_limit_bps += reservation(app);
        reserved_rate_bps += current_rate(app);
      } else if (reservation(app) == 0 && has_reservation_competition(app) &&
                 current_rate(app) >= kActiveEntityNodeRateBps) {
        competitor_rate_bps += current_rate(app);
      }
    }

    const double reserved_deficit_bps =
        std::max(0.0, static_cast<double>(reserved_limit_bps) - reserved_rate_bps);
    const double min_meaningful_deficit_bps =
        std::max(kMinReservationDeficitBps, static_cast<double>(reserved_limit_bps) *
                                                kMinReservationDeficitFraction);
    const bool should_throttle_competitors =
        reservations_enabled && reserved_deficit_bps >= min_meaningful_deficit_bps &&
        competitor_rate_bps > 0.0;
    double competitor_scale = 1.0;

    if (should_throttle_competitors) {
      constexpr double kMinCompetitorShare = 0.10;
      const double competitor_budget_bps =
          std::max(competitor_rate_bps - reserved_deficit_bps,
                   competitor_rate_bps * kMinCompetitorShare);
      competitor_scale = std::clamp(competitor_budget_bps / competitor_rate_bps,
                                    kMinCompetitorShare, 1.0);
    }

    for (auto& app : apps) {
      uint64_t desired_limit = 0;

      if (should_throttle_competitors && reservation(app) == 0 &&
          has_reservation_competition(app) &&
          current_rate(app) >= kActiveEntityNodeRateBps) {
        desired_limit = static_cast<uint64_t>(current_rate(app) * competitor_scale + 0.5);

        if (controller_min_limit_bps > 0) {
          desired_limit = std::max(desired_limit, controller_min_limit_bps);
        }
      }

      if (desired_limit != controller_limit(app) || desired_limit > 0) {
        set_controller_limit(app, desired_limit);
      }
    }
  };

  update_direction(apps, true);
  update_direction(apps, false);
}

bool
TrafficShapingManager::ShouldEmitDelayForPolicy(
    const TrafficShapingPolicy& policy, const bool is_write, const double node_rate_bps,
    const double io_pressure, const bool node_has_pressured_reservation,
    const bool limits_enabled, const bool reservations_enabled,
    const double io_pressure_threshold)
{
  if (!limits_enabled) {
    return false;
  }

  const bool has_explicit_limit =
      policy.is_enabled && (is_write ? policy.limit_write_bytes_per_sec > 0
                                     : policy.limit_read_bytes_per_sec > 0);
  if (has_explicit_limit) {
    return true;
  }

  if (!reservations_enabled) {
    return false;
  }

  const bool has_controller_limit = is_write
                                        ? policy.controller_limit_write_bytes_per_sec > 0
                                        : policy.controller_limit_read_bytes_per_sec > 0;
  if (!has_controller_limit) {
    return false;
  }

  const bool has_reservation = is_write ? policy.reservation_write_bytes_per_sec > 0
                                        : policy.reservation_read_bytes_per_sec > 0;
  if (!has_reservation) {
    return node_has_pressured_reservation;
  }

  return node_rate_bps >= kActiveEntityNodeRateBps &&
         io_pressure >= std::max(0.0, std::min(1.0, io_pressure_threshold));
}

void
TrafficShapingManager::UpdateTrafficShapingController(
    const std::unordered_map<std::string, double>& node_io_pressure)
{
  const bool limits_enabled = GetLimitsEnabled();
  const bool reservations_enabled = GetReservationsEnabled();
  const uint64_t controller_min_limit_bps = GetControllerMinLimit();
  const double io_pressure_threshold = GetIoPressureThreshold();
  const auto now = std::chrono::steady_clock::now();

  ExpireControllerLimits(now);

  // 1. Gather all unique active apps and defined policies.
  // Snapshot mAppPolicies under mMutex to avoid racing with Set/RemoveAppPolicy.
  std::set<std::string> unique_apps;
  auto rates = GetCurrentReadAndWriteRates();
  auto& app_read_map = std::get<0>(rates);
  auto& app_write_map = std::get<1>(rates);
  std::unordered_map<std::string, AppIoPressureSnapshot> app_io_pressure;

  for (const auto& [app, _] : app_read_map) {
    unique_apps.insert(app);
  }
  for (const auto& [app, _] : app_write_map) {
    unique_apps.insert(app);
  }

  std::unordered_map<std::string, TrafficShapingPolicy> policies_snapshot;
  struct AppReservationCompetition {
    bool read = false;
    bool write = false;
  };
  std::unordered_map<std::string, AppReservationCompetition> app_reservation_competition;
  {
    std::shared_lock policies_lock(mMutex);
    policies_snapshot = mAppPolicies;

    std::unordered_map<std::string, EntityRateMaps> node_rates;
    for (const auto& [node_entity_key, stats] : mNodeEntityStats) {
      AddStreamRates(node_rates[node_entity_key.node_id], node_entity_key.stream,
                     stats.ema[Ema1s]);
      const auto pressure_it = node_io_pressure.find(node_entity_key.node_id);
      const double node_pressure = pressure_it != node_io_pressure.end()
                                       ? pressure_it->second
                                       : kUnknownIoPressure;
      UpdateAppIoPressure(app_io_pressure[node_entity_key.stream.app], stats.ema[Ema1s],
                          node_pressure);
    }

    auto node_has_pressured_reservation = [&](const EntityRateMaps& node_rates,
                                              const bool is_write,
                                              const double io_pressure) {
      if (!reservations_enabled || io_pressure < io_pressure_threshold) {
        return false;
      }

      for (const auto& [app, policy] : policies_snapshot) {
        const uint64_t reservation_bps = is_write ? policy.reservation_write_bytes_per_sec
                                                  : policy.reservation_read_bytes_per_sec;
        if (reservation_bps == 0) {
          continue;
        }

        const auto& global_map = is_write ? app_write_map : app_read_map;
        const auto& node_map = is_write ? node_rates.app_write : node_rates.app_read;
        const auto global_it = global_map.find(app);
        const auto node_it = node_map.find(app);
        const double global_rate_bps =
            global_it != global_map.end() ? global_it->second : 0.0;
        const double node_rate_bps = node_it != node_map.end() ? node_it->second : 0.0;

        if (node_rate_bps < kActiveEntityNodeRateBps) {
          continue;
        }

        const double reserved_deficit_bps =
            std::max(0.0, static_cast<double>(reservation_bps) - global_rate_bps);
        const double min_meaningful_deficit_bps =
            std::max(kMinReservationDeficitBps, static_cast<double>(reservation_bps) *
                                                    kMinReservationDeficitFraction);

        if (reserved_deficit_bps >= min_meaningful_deficit_bps) {
          return true;
        }
      }

      return false;
    };

    for (const auto& [node_id, rates] : node_rates) {
      const auto pressure_it = node_io_pressure.find(node_id);
      const double node_pressure = pressure_it != node_io_pressure.end()
                                       ? pressure_it->second
                                       : kUnknownIoPressure;
      const bool has_write_reservation =
          node_has_pressured_reservation(rates, true, node_pressure);
      const bool has_read_reservation =
          node_has_pressured_reservation(rates, false, node_pressure);

      if (!has_write_reservation && !has_read_reservation) {
        continue;
      }

      if (has_write_reservation) {
        for (const auto& [app, rate_bps] : rates.app_write) {
          if (rate_bps < kActiveEntityNodeRateBps) {
            continue;
          }

          const auto policy_it = policies_snapshot.find(app);
          const bool has_own_reservation =
              policy_it != policies_snapshot.end() &&
              policy_it->second.reservation_write_bytes_per_sec > 0;
          if (!has_own_reservation) {
            app_reservation_competition[app].write = true;
          }
        }
      }

      if (has_read_reservation) {
        for (const auto& [app, rate_bps] : rates.app_read) {
          if (rate_bps < kActiveEntityNodeRateBps) {
            continue;
          }

          const auto policy_it = policies_snapshot.find(app);
          const bool has_own_reservation =
              policy_it != policies_snapshot.end() &&
              policy_it->second.reservation_read_bytes_per_sec > 0;
          if (!has_own_reservation) {
            app_reservation_competition[app].read = true;
          }
        }
      }
    }
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
    if (auto it = app_io_pressure.find(app); it != app_io_pressure.end()) {
      st.current_read_io_pressure = it->second.read;
      st.current_write_io_pressure = it->second.write;
      st.has_read_io_pressure = it->second.has_read;
      st.has_write_io_pressure = it->second.has_write;
    }
    if (auto it = app_reservation_competition.find(app);
        it != app_reservation_competition.end()) {
      st.has_read_reservation_competition = it->second.read;
      st.has_write_reservation_competition = it->second.write;
    }

    // Policy (from snapshot)
    if (auto pol_it = policies_snapshot.find(app); pol_it != policies_snapshot.end()) {
      st.reservation_write_bps = pol_it->second.reservation_write_bytes_per_sec;
      st.reservation_read_bps = pol_it->second.reservation_read_bytes_per_sec;
      st.controller_limit_write_bps = pol_it->second.controller_limit_write_bytes_per_sec;
      st.controller_limit_read_bps = pol_it->second.controller_limit_read_bytes_per_sec;
    }
    st.controller_min_limit_bps = controller_min_limit_bps;
    st.io_pressure_threshold = io_pressure_threshold;

    app_array.push_back(st);
  }

  // 3. Let the hot-reloaded plugin update controller state, or fall back to the
  // built-in reservation controller.
  bool used_custom_controller = false;
  if (!limits_enabled || !reservations_enabled) {
    ApplyDefaultReservationController(app_array, false, controller_min_limit_bps,
                                      io_pressure_threshold);
  } else {
    // Keep the plugin lock scoped to the actual callback. Applying the resulting
    // policy updates can persist config, so it must not run while holding the
    // hot-reload lock.
    {
      std::shared_lock read_lock(mPluginMutex);
      if (mCustomControllerAlgo) {
        mCustomControllerAlgo(app_array.data(), app_array.size());
        used_custom_controller = true;
      }
    }

    if (!used_custom_controller) {
      ApplyDefaultReservationController(app_array, true, controller_min_limit_bps,
                                        io_pressure_threshold);
    }
  }

  // 4. Read the results back and apply them
  for (const auto& st : app_array) {
    if (st.update_write || st.update_read || st.update_reservation_write ||
        st.update_reservation_read) {
      TrafficShapingPolicy policy;

      // Preserve existing user configuration from snapshot.
      // SetAppPolicy acquires mMutex internally; do not hold it here.
      if (auto pol_it = policies_snapshot.find(st.app_name);
          pol_it != policies_snapshot.end()) {
        policy = pol_it->second;
      }

      const bool write_controller_changed =
          st.update_write && policy.controller_limit_write_bytes_per_sec !=
                                 st.new_controller_limit_write_bps;
      const bool read_controller_changed =
          st.update_read &&
          policy.controller_limit_read_bytes_per_sec != st.new_controller_limit_read_bps;

      if (st.update_write) {
        policy.controller_limit_write_bytes_per_sec = st.new_controller_limit_write_bps;
        policy.controller_limit_write_update_time =
            st.new_controller_limit_write_bps > 0
                ? now
                : std::chrono::steady_clock::time_point{};
      }
      if (st.update_read) {
        policy.controller_limit_read_bytes_per_sec = st.new_controller_limit_read_bps;
        policy.controller_limit_read_update_time =
            st.new_controller_limit_read_bps > 0
                ? now
                : std::chrono::steady_clock::time_point{};
      }
      if (st.update_reservation_write) {
        policy.reservation_write_bytes_per_sec = st.new_reservation_write_bps;
      }
      if (st.update_reservation_read) {
        policy.reservation_read_bytes_per_sec = st.new_reservation_read_bps;
      }

      // Automatically syncs to FSTs because SetAppPolicy bumps the config version
      SetAppPolicy(st.app_name, policy);

      if (write_controller_changed || read_controller_changed ||
          st.update_reservation_write || st.update_reservation_read) {
        eos_static_info("msg=\"Dynamic traffic shaping controller applied policy\" "
                        "source=\"%s\" app=\"%s\" "
                        "controller_limit_write_bps=%lu controller_limit_read_bps=%lu "
                        "reservation_write_bps=%lu reservation_read_bps=%lu",
                        used_custom_controller ? "plugin" : "default", st.app_name,
                        policy.controller_limit_write_bytes_per_sec,
                        policy.controller_limit_read_bytes_per_sec,
                        policy.reservation_write_bytes_per_sec,
                        policy.reservation_read_bytes_per_sec);
      }
    }
  }
}

void
TrafficShapingManager::SetLimitsEnabled(const bool enabled)
{
  const bool old_value = mLimitsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (old_value != enabled) {
    std::unique_lock lock(mMutex);
    if (!enabled) {
      ClearControllerLimitsUnlocked();
    }
    mNodeFstIoDelayConfigs.clear();
  }
}

bool
TrafficShapingManager::GetLimitsEnabled() const
{
  return mLimitsEnabled.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetReservationsEnabled(const bool enabled)
{
  const bool old_value =
      mReservationsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (old_value != enabled) {
    std::unique_lock lock(mMutex);
    if (!enabled) {
      ClearControllerLimitsUnlocked();
    }
    mNodeFstIoDelayConfigs.clear();
  }
}

bool
TrafficShapingManager::GetReservationsEnabled() const
{
  return mReservationsEnabled.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetControllerMinLimit(const uint64_t limit_bps)
{
  const uint64_t old_value =
      mControllerMinLimitBps.exchange(limit_bps, std::memory_order_relaxed);

  if (old_value != limit_bps) {
    std::unique_lock lock(mMutex);
    mNodeFstIoDelayConfigs.clear();
  }
}

uint64_t
TrafficShapingManager::GetControllerMinLimit() const
{
  return mControllerMinLimitBps.load(std::memory_order_relaxed);
}

void
TrafficShapingManager::SetIoPressureThreshold(const double threshold)
{
  const double clamped_threshold = std::max(0.0, std::min(1.0, threshold));
  const double old_value =
      mIoPressureThreshold.exchange(clamped_threshold, std::memory_order_relaxed);

  if (old_value != clamped_threshold) {
    std::unique_lock lock(mMutex);
    mNodeFstIoDelayConfigs.clear();
  }
}

double
TrafficShapingManager::GetIoPressureThreshold() const
{
  return mIoPressureThreshold.load(std::memory_order_relaxed);
}

size_t
TrafficShapingManager::ClearControllerLimits()
{
  std::unique_lock lock(mMutex);
  const size_t cleared = ClearControllerLimitsUnlocked();

  if (cleared > 0) {
    mNodeFstIoDelayConfigs.clear();
  }

  return cleared;
}

size_t
TrafficShapingManager::ClearControllerLimitsUnlocked()
{
  size_t cleared = 0;

  auto clear_map = [&cleared](auto& policies) {
    for (auto it = policies.begin(); it != policies.end();) {
      auto& policy = it->second;
      const bool had_controller_limit = policy.controller_limit_write_bytes_per_sec > 0 ||
                                        policy.controller_limit_read_bytes_per_sec > 0;

      if (!had_controller_limit) {
        ++it;
        continue;
      }

      policy.controller_limit_write_bytes_per_sec = 0;
      policy.controller_limit_read_bytes_per_sec = 0;
      policy.controller_limit_write_update_time = {};
      policy.controller_limit_read_update_time = {};
      ++cleared;

      if (policy.IsEmpty()) {
        it = policies.erase(it);
      } else {
        ++it;
      }
    }
  };

  clear_map(mUidPolicies);
  clear_map(mGidPolicies);
  clear_map(mAppPolicies);

  return cleared;
}

size_t
TrafficShapingManager::ExpireControllerLimits(
    const std::chrono::steady_clock::time_point now)
{
  std::unique_lock lock(mMutex);
  size_t expired = 0;

  auto expire_map = [&expired, now](auto& policies) {
    for (auto it = policies.begin(); it != policies.end();) {
      auto& policy = it->second;
      bool policy_expired = false;

      auto expire_limit = [now](uint64_t& limit,
                                std::chrono::steady_clock::time_point& update_time) {
        if (limit == 0) {
          update_time = {};
          return false;
        }

        if (update_time == std::chrono::steady_clock::time_point{} ||
            now - update_time >= kControllerLimitTtl) {
          limit = 0;
          update_time = {};
          return true;
        }

        return false;
      };

      policy_expired |= expire_limit(policy.controller_limit_write_bytes_per_sec,
                                     policy.controller_limit_write_update_time);
      policy_expired |= expire_limit(policy.controller_limit_read_bytes_per_sec,
                                     policy.controller_limit_read_update_time);

      if (policy_expired) {
        ++expired;
      }

      if (policy.IsEmpty()) {
        it = policies.erase(it);
      } else {
        ++it;
      }
    }
  };

  expire_map(mUidPolicies);
  expire_map(mGidPolicies);
  expire_map(mAppPolicies);

  if (expired > 0) {
    mNodeFstIoDelayConfigs.clear();
  }

  return expired;
}

void
TrafficShapingManager::UpdateLimits(
    const std::unordered_map<std::string, double>& node_io_pressure,
    const std::vector<std::string>& online_nodes)
{
  // 1. Evaluate hot-reload status at the start of every tick
  LoadPluginIfModified();

  const bool limits_enabled = GetLimitsEnabled();
  const bool reservations_enabled = GetReservationsEnabled();
  const double io_pressure_threshold = GetIoPressureThreshold();

  auto adjust_delay = [&](const double limit_bps, const double global_rate,
                          const double node_rate, const bool has_global_rate_sample,
                          const bool has_node_rate_sample, const bool allow_idle_release,
                          uint64_t& delay_us, auto* output_map, const auto& entity_key,
                          const std::string& node_id, const char* entity_type,
                          const std::string& entity_id, const char* op_type,
                          const double io_pressure) {
    if (limit_bps <= 0) {
      return;
    }

    const uint64_t old_delay = delay_us;
    const double control_limit_bps = limit_bps;
    const double control_rate_bps = global_rate;
    const bool has_control_rate_sample = has_node_rate_sample || has_global_rate_sample;
    const double ratio = control_rate_bps / control_limit_bps;
    const double delay_reference_bps = control_limit_bps;
    {
      std::shared_lock read_lock(mPluginMutex);
      if (mCustomAlgo) {
        const DelayState state{
            control_limit_bps,   control_rate_bps,        old_delay,
            io_pressure,         has_control_rate_sample, allow_idle_release,
            delay_reference_bps, io_pressure_threshold};
        delay_us = mCustomAlgo(&state);
      } else {
        delay_us =
            CalculateDelayUs(control_limit_bps, control_rate_bps, old_delay, io_pressure,
                             has_control_rate_sample, allow_idle_release,
                             delay_reference_bps, io_pressure_threshold);
      }
    }

    if (delay_us > 0) {
      (*output_map)[entity_key] = delay_us;
    }

    eos_static_debug(
        "msg=\"throttle evaluation\" node=\"%s\" type=\"%s\" id=\"%s\" op=\"%s\" "
        "limit_bps=%.0f global_rate_bps=%.0f node_rate_bps=%.0f ratio=%.3f "
        "active_nodes=%zu control_limit_bps=%.0f control_rate_bps=%.0f "
        "delay_reference_bps=%.0f io_pressure=%.3f "
        "allow_idle_release=%d old_delay_us=%lu new_delay_us=%lu",
        node_id.c_str(), entity_type, entity_id.c_str(), op_type, limit_bps, global_rate,
        node_rate, ratio, size_t{1}, control_limit_bps, control_rate_bps,
        delay_reference_bps, io_pressure, allow_idle_release, old_delay, delay_us);
  };

  // Helper lambda to do a single-pass map lookup
  auto get_rate = [](const auto& map, const auto& key) {
    if (auto it = map.find(key); it != map.end()) {
      return std::pair{it->second, true};
    }
    return std::pair{0.0, false};
  };

  auto effective_limit = [limits_enabled, reservations_enabled](
                             const TrafficShapingPolicy& policy, const bool is_write) {
    if (!limits_enabled) {
      return uint64_t{0};
    }

    const uint64_t user_limit = policy.is_enabled
                                    ? (is_write ? policy.limit_write_bytes_per_sec
                                                : policy.limit_read_bytes_per_sec)
                                    : 0;
    const uint64_t controller_limit =
        reservations_enabled ? (is_write ? policy.controller_limit_write_bytes_per_sec
                                         : policy.controller_limit_read_bytes_per_sec)
                             : 0;

    if (user_limit > 0 && controller_limit > 0) {
      return std::min(user_limit, controller_limit);
    }

    return user_limit > 0 ? user_limit : controller_limit;
  };

  auto get_pressure = [&](const std::string& node_id) {
    if (auto it = node_io_pressure.find(node_id); it != node_io_pressure.end()) {
      return it->second;
    }
    return kUnknownIoPressure;
  };

  std::unordered_map<std::string, eos::traffic_shaping::TrafficShapingFstIoDelayConfig>
      fst_io_delay_configs;

  {
    std::unique_lock lock(mMutex);

    std::set<std::string> online_node_set(online_nodes.begin(), online_nodes.end());
    for (auto it = mNodeFstIoDelayConfigs.begin(); it != mNodeFstIoDelayConfigs.end();) {
      if (online_node_set.find(it->first) == online_node_set.end()) {
        it = mNodeFstIoDelayConfigs.erase(it);
      } else {
        ++it;
      }
    }

    auto has_active_policy = [](const auto& policies) {
      for (const auto& [id, policy] : policies) {
        if (policy.IsActive()) {
          return true;
        }
      }

      return false;
    };

    const bool has_active_policies = has_active_policy(mAppPolicies) ||
                                     has_active_policy(mUidPolicies) ||
                                     has_active_policy(mGidPolicies);

    if (!has_active_policies) {
      for (const auto& node_id : online_nodes) {
        mNodeFstIoDelayConfigs[node_id].Clear();
        fst_io_delay_configs[node_id] =
            eos::traffic_shaping::TrafficShapingFstIoDelayConfig{};
      }
    } else {
      std::unordered_map<std::string, EntityRateMaps> node_rates;
      for (const auto& [node_entity_key, stats] : mNodeEntityStats) {
        AddStreamRates(node_rates[node_entity_key.node_id], node_entity_key.stream,
                       stats.ema[Ema1s]);
      }

      // Policies are global for an app/uid/gid. Each FST follows the aggregate
      // feedback loop and receives the same delay for a matching policy.
      EntityRateMaps global_rates;
      for (const auto& [key, stats] : mGlobalStats) {
        AddStreamRates(global_rates, key, stats.ema[Ema1s]);
      }

      auto has_pressured_app_reservation = [&](const EntityRateMaps& rates,
                                               const bool is_write,
                                               const double io_pressure) {
        if (!reservations_enabled || io_pressure < io_pressure_threshold) {
          return false;
        }

        for (const auto& [app, policy] : mAppPolicies) {
          const uint64_t reservation_bps = is_write
                                               ? policy.reservation_write_bytes_per_sec
                                               : policy.reservation_read_bytes_per_sec;
          if (reservation_bps == 0) {
            continue;
          }

          const auto [global_rate_bps, _] =
              get_rate(is_write ? global_rates.app_write : global_rates.app_read, app);
          const auto [node_rate_bps, has_node_rate_sample] =
              get_rate(is_write ? rates.app_write : rates.app_read, app);

          if (!has_node_rate_sample || node_rate_bps < kActiveEntityNodeRateBps) {
            continue;
          }

          const double reserved_deficit_bps =
              std::max(0.0, static_cast<double>(reservation_bps) - global_rate_bps);
          const double min_meaningful_deficit_bps =
              std::max(kMinReservationDeficitBps, static_cast<double>(reservation_bps) *
                                                      kMinReservationDeficitFraction);

          if (reserved_deficit_bps >= min_meaningful_deficit_bps) {
            return true;
          }
        }

        return false;
      };

      for (const auto& node_id : online_nodes) {
        const EntityRateMaps& rates = node_rates[node_id];
        const double io_pressure = get_pressure(node_id);
        const bool node_has_pressured_write_reservation =
            has_pressured_app_reservation(rates, true, io_pressure);
        const bool node_has_pressured_read_reservation =
            has_pressured_app_reservation(rates, false, io_pressure);
        auto& previous_config = mNodeFstIoDelayConfigs[node_id];
        eos::traffic_shaping::TrafficShapingFstIoDelayConfig next_config;

        auto* app_write_map = next_config.mutable_app_write_delay();
        auto* app_read_map = next_config.mutable_app_read_delay();
        auto* uid_write_map = next_config.mutable_uid_write_delay();
        auto* uid_read_map = next_config.mutable_uid_read_delay();
        auto* gid_write_map = next_config.mutable_gid_write_delay();
        auto* gid_read_map = next_config.mutable_gid_read_delay();

        for (const auto& [app, policy] : mAppPolicies) {
          if (!policy.IsActive()) {
            eos_static_debug(
                "msg=\"skipping inactive policy\" node=\"%s\" type=\"app\" id=\"%s\"",
                node_id.c_str(), app.c_str());
            continue;
          }

          const auto [global_write_rate, has_global_write_rate] =
              get_rate(global_rates.app_write, app);
          const auto [node_write_rate, has_node_write_rate] =
              get_rate(rates.app_write, app);
          const bool allow_write_idle_release =
              !(policy.is_enabled && policy.limit_write_bytes_per_sec > 0);
          if (ShouldEmitDelayForPolicy(policy, true, node_write_rate, io_pressure,
                                       node_has_pressured_write_reservation,
                                       limits_enabled, reservations_enabled,
                                       io_pressure_threshold)) {
            adjust_delay(static_cast<double>(effective_limit(policy, true)),
                         global_write_rate, node_write_rate, has_global_write_rate,
                         has_node_write_rate, allow_write_idle_release,
                         (*previous_config.mutable_app_write_delay())[app], app_write_map,
                         app, node_id, "app", app, "write", io_pressure);
          }

          const auto [global_read_rate, has_global_read_rate] =
              get_rate(global_rates.app_read, app);
          const auto [node_read_rate, has_node_read_rate] = get_rate(rates.app_read, app);
          const bool allow_read_idle_release =
              !(policy.is_enabled && policy.limit_read_bytes_per_sec > 0);
          if (ShouldEmitDelayForPolicy(policy, false, node_read_rate, io_pressure,
                                       node_has_pressured_read_reservation,
                                       limits_enabled, reservations_enabled,
                                       io_pressure_threshold)) {
            adjust_delay(static_cast<double>(effective_limit(policy, false)),
                         global_read_rate, node_read_rate, has_global_read_rate,
                         has_node_read_rate, allow_read_idle_release,
                         (*previous_config.mutable_app_read_delay())[app], app_read_map,
                         app, node_id, "app", app, "read", io_pressure);
          }
        }

        for (const auto& [uid, policy] : mUidPolicies) {
          if (!policy.IsActive()) {
            eos_static_debug(
                "msg=\"skipping inactive policy\" node=\"%s\" type=\"uid\" id=\"%u\"",
                node_id.c_str(), uid);
            continue;
          }

          const auto [global_write_rate, has_global_write_rate] =
              get_rate(global_rates.uid_write, uid);
          const auto [node_write_rate, has_node_write_rate] =
              get_rate(rates.uid_write, uid);
          const bool allow_write_idle_release =
              !(policy.is_enabled && policy.limit_write_bytes_per_sec > 0);
          if (ShouldEmitDelayForPolicy(policy, true, node_write_rate, io_pressure,
                                       node_has_pressured_write_reservation,
                                       limits_enabled, reservations_enabled,
                                       io_pressure_threshold)) {
            adjust_delay(static_cast<double>(effective_limit(policy, true)),
                         global_write_rate, node_write_rate, has_global_write_rate,
                         has_node_write_rate, allow_write_idle_release,
                         (*previous_config.mutable_uid_write_delay())[uid], uid_write_map,
                         uid, node_id, "uid", std::to_string(uid), "write", io_pressure);
          }

          const auto [global_read_rate, has_global_read_rate] =
              get_rate(global_rates.uid_read, uid);
          const auto [node_read_rate, has_node_read_rate] = get_rate(rates.uid_read, uid);
          const bool allow_read_idle_release =
              !(policy.is_enabled && policy.limit_read_bytes_per_sec > 0);
          if (ShouldEmitDelayForPolicy(policy, false, node_read_rate, io_pressure,
                                       node_has_pressured_read_reservation,
                                       limits_enabled, reservations_enabled,
                                       io_pressure_threshold)) {
            adjust_delay(static_cast<double>(effective_limit(policy, false)),
                         global_read_rate, node_read_rate, has_global_read_rate,
                         has_node_read_rate, allow_read_idle_release,
                         (*previous_config.mutable_uid_read_delay())[uid], uid_read_map,
                         uid, node_id, "uid", std::to_string(uid), "read", io_pressure);
          }
        }

        for (const auto& [gid, policy] : mGidPolicies) {
          if (!policy.IsActive()) {
            eos_static_debug(
                "msg=\"skipping inactive policy\" node=\"%s\" type=\"gid\" id=\"%u\"",
                node_id.c_str(), gid);
            continue;
          }

          const auto [global_write_rate, has_global_write_rate] =
              get_rate(global_rates.gid_write, gid);
          const auto [node_write_rate, has_node_write_rate] =
              get_rate(rates.gid_write, gid);
          const bool allow_write_idle_release =
              !(policy.is_enabled && policy.limit_write_bytes_per_sec > 0);
          if (ShouldEmitDelayForPolicy(policy, true, node_write_rate, io_pressure,
                                       node_has_pressured_write_reservation,
                                       limits_enabled, reservations_enabled,
                                       io_pressure_threshold)) {
            adjust_delay(static_cast<double>(effective_limit(policy, true)),
                         global_write_rate, node_write_rate, has_global_write_rate,
                         has_node_write_rate, allow_write_idle_release,
                         (*previous_config.mutable_gid_write_delay())[gid], gid_write_map,
                         gid, node_id, "gid", std::to_string(gid), "write", io_pressure);
          }

          const auto [global_read_rate, has_global_read_rate] =
              get_rate(global_rates.gid_read, gid);
          const auto [node_read_rate, has_node_read_rate] = get_rate(rates.gid_read, gid);
          const bool allow_read_idle_release =
              !(policy.is_enabled && policy.limit_read_bytes_per_sec > 0);
          if (ShouldEmitDelayForPolicy(policy, false, node_read_rate, io_pressure,
                                       node_has_pressured_read_reservation,
                                       limits_enabled, reservations_enabled,
                                       io_pressure_threshold)) {
            adjust_delay(static_cast<double>(effective_limit(policy, false)),
                         global_read_rate, node_read_rate, has_global_read_rate,
                         has_node_read_rate, allow_read_idle_release,
                         (*previous_config.mutable_gid_read_delay())[gid], gid_read_map,
                         gid, node_id, "gid", std::to_string(gid), "read", io_pressure);
          }
        }

        previous_config = next_config;
        fst_io_delay_configs[node_id] = std::move(next_config);
      }
    }
  }

  std::vector<std::pair<std::string, std::string>> encoded_configs;
  encoded_configs.reserve(online_nodes.size());

  for (const auto& node_name : online_nodes) {
    std::string serialized = fst_io_delay_configs[node_name].SerializeAsString();
    std::string encoded;

    if (!eos::common::SymKey::Base64(serialized, encoded)) {
      eos_static_warning(
          "msg=\"failed to base64-encode FST IO limits config\" node=\"%s\"",
          node_name.c_str());
      continue;
    }

    encoded_configs.emplace_back(node_name, std::move(encoded));
  }

  const auto now = std::chrono::steady_clock::now();
  std::vector<std::pair<std::string, std::string>> configs_to_publish;
  {
    std::unique_lock lock(mMutex);
    std::set<std::string> online_node_set(online_nodes.begin(), online_nodes.end());
    for (auto it = mPublishedFstIoDelayConfigs.begin();
         it != mPublishedFstIoDelayConfigs.end();) {
      if (online_node_set.find(it->first) == online_node_set.end()) {
        it = mPublishedFstIoDelayConfigs.erase(it);
      } else {
        ++it;
      }
    }

    for (const auto& [node_name, encoded] : encoded_configs) {
      auto& published = mPublishedFstIoDelayConfigs[node_name];
      const bool config_changed = published.encoded_config != encoded;
      const bool refresh_due =
          published.last_publish_time == std::chrono::steady_clock::time_point{} ||
          now - published.last_publish_time >= kFstIoDelayConfigRefreshInterval;

      if (config_changed || refresh_due) {
        configs_to_publish.emplace_back(node_name, encoded);
      }
    }
  }

  std::vector<std::pair<std::string, std::string>> published_configs;
  if (!configs_to_publish.empty()) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (const auto& [node_name, encoded] : configs_to_publish) {
      auto it = FsView::gFsView.mNodeView.find(node_name);
      if (it != FsView::gFsView.mNodeView.end()) {
        if (it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_IO_LIMITS,
                                        encoded, true)) {
          published_configs.emplace_back(node_name, encoded);
        }
      }
    }
  }

  if (!published_configs.empty()) {
    std::unique_lock lock(mMutex);
    for (const auto& [node_name, encoded] : published_configs) {
      auto& published = mPublishedFstIoDelayConfigs[node_name];
      published.encoded_config = encoded;
      published.last_publish_time = now;
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

  for (auto it = mNodeEntityStats.begin(); it != mNodeEntityStats.end();) {
    if (now_unix - it->second.last_activity_time > max_idle_seconds) {
      it = mNodeEntityStats.erase(it);
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

MapCardinalityStats
TrafficShapingManager::GetMapCardinalityStats() const
{
  std::shared_lock lock(mMutex);

  MapCardinalityStats stats;
  stats.node_states = static_cast<uint64_t>(mNodeStates.size());
  for (const auto& node_entry : mNodeStates) {
    stats.node_state_streams += static_cast<uint64_t>(node_entry.second.streams.size());
  }
  stats.global_stats = static_cast<uint64_t>(mGlobalStats.size());
  stats.node_stats = static_cast<uint64_t>(mNodeStats.size());
  stats.disk_stats = static_cast<uint64_t>(mDiskStats.size());
  stats.detailed_stats = static_cast<uint64_t>(mDetailedStats.size());
  stats.node_entity_stats = static_cast<uint64_t>(mNodeEntityStats.size());
  stats.uid_policies = static_cast<uint64_t>(mUidPolicies.size());
  stats.gid_policies = static_cast<uint64_t>(mGidPolicies.size());
  stats.app_policies = static_cast<uint64_t>(mAppPolicies.size());
  stats.node_fst_io_delay_configs = static_cast<uint64_t>(mNodeFstIoDelayConfigs.size());
  stats.published_fst_io_delay_configs =
      static_cast<uint64_t>(mPublishedFstIoDelayConfigs.size());
  return stats;
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
  mNodeEntityStats.clear();
  mTotalStats.clear();
  mNodeFstIoDelayConfigs.clear();
  mPublishedFstIoDelayConfigs.clear();

  estimators_update_loop_micro_sec.reset();
  fst_limits_update_loop_micro_sec.reset();
  fst_reports_processed_per_second.reset();

  if (mPluginHandle) {
    dlclose(mPluginHandle);
    mPluginHandle = nullptr;
    mCustomAlgo = nullptr;
    mCustomControllerAlgo = nullptr;
    mPluginLastModified = 0;
    mNextPluginCheckTime = {};
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
  mNodeEntityStats.clear();
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
    , mFstIoStatsReportThreadPeriodMilliseconds(
          eos::common::TRAFFIC_SHAPING_FST_IO_STATS_REPORT_PERIOD_DEFAULT_MS)
    , mSystemStatsWindowSeconds(15)
    , mFilesystemDetailEnabled(false)
    , mLimitsEnabled(true)
    , mReservationsEnabled(true)
    , mControllerMinLimitBps(kDefaultControllerMinLimitBps)
    , mIoPressureThreshold(kDefaultIoPressureThreshold)
{
  mManager = std::make_shared<TrafficShapingManager>();
}

TrafficShapingEngine::~TrafficShapingEngine() { Stop(); }

bool
TrafficShapingEngine::ApplyThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                                        uint32_t win_s)
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

  return changed;
}

void
TrafficShapingEngine::SetThreadConfig(uint32_t est_ms, uint32_t pol_ms, uint32_t rep_ms,
                                      uint32_t win_s)
{
  if (ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s)) {
    StoreThreadConfig();
    SyncTrafficShapingConfigWithFst();
  }
}

void
TrafficShapingEngine::SetEstimatorsUpdateThreadPeriodMilliseconds(
    const uint32_t period_ms)
{
  SetThreadConfig(period_ms, mFstIoPolicyUpdateThreadPeriodMilliseconds,
                  mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds);
}

void
TrafficShapingEngine::SetFstIoPolicyUpdateThreadPeriodMilliseconds(
    const uint32_t period_ms)
{
  SetThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds, period_ms,
                  mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds);
}

void
TrafficShapingEngine::SetFstIoStatsReportThreadPeriodMilliseconds(uint32_t period_ms)
{
  SetThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                  mFstIoPolicyUpdateThreadPeriodMilliseconds, period_ms,
                  mSystemStatsWindowSeconds);
}

void
TrafficShapingEngine::SetSystemStatsWindowSeconds(uint32_t window_seconds)
{
  SetThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                  mFstIoPolicyUpdateThreadPeriodMilliseconds,
                  mFstIoStatsReportThreadPeriodMilliseconds, window_seconds);
}

void
TrafficShapingEngine::SetDetailLevel(const std::string& detail_level)
{
  ApplyDetailLevelConfig(detail_level);
  StoreDetailLevelConfig(GetDetailLevel());
}

bool
TrafficShapingEngine::ApplyDetailLevelConfig(const std::string& detail_level)
{
  const bool fs_detail =
      detail_level == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM;
  const bool old_value =
      mFilesystemDetailEnabled.exchange(fs_detail, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetFilesystemDetailEnabled(fs_detail);

    if (old_value != fs_detail) {
      mManager->ClearRuntimeStats();
    } else if (!fs_detail) {
      mManager->ClearDetailedRuntimeStats();
    }
  }

  return old_value != fs_detail;
}

void
TrafficShapingEngine::StoreDetailLevelConfig(const std::string& detail_level)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_LEVEL_CONFIG,
                                  detail_level);
  gOFS->mConfigEngine->AutoSave();
}

std::string
TrafficShapingEngine::GetDetailLevel() const
{
  return mFilesystemDetailEnabled.load(std::memory_order_relaxed)
             ? eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM
             : eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE;
}

void
TrafficShapingEngine::SetLimitsEnabled(const bool enabled)
{
  ApplyLimitsEnabledConfig(enabled);
  StoreLimitsEnabledConfig(GetLimitsEnabled());
}

bool
TrafficShapingEngine::GetLimitsEnabled() const
{
  return mLimitsEnabled.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyLimitsEnabledConfig(const bool enabled)
{
  const bool old_value = mLimitsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetLimitsEnabled(enabled);
  }

  return old_value != enabled;
}

void
TrafficShapingEngine::StoreLimitsEnabledConfig(const bool enabled)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_LIMITS_ENABLED_CONFIG, enabled);
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingEngine::SetReservationsEnabled(const bool enabled)
{
  ApplyReservationsEnabledConfig(enabled);
  StoreReservationsEnabledConfig(GetReservationsEnabled());
}

bool
TrafficShapingEngine::GetReservationsEnabled() const
{
  return mReservationsEnabled.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyReservationsEnabledConfig(const bool enabled)
{
  const bool old_value =
      mReservationsEnabled.exchange(enabled, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetReservationsEnabled(enabled);
  }

  return old_value != enabled;
}

void
TrafficShapingEngine::StoreReservationsEnabledConfig(const bool enabled)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_RESERVATIONS_ENABLED_CONFIG,
                                  enabled);
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingEngine::SetControllerMinLimit(const uint64_t limit_bps)
{
  ApplyControllerMinLimitConfig(limit_bps);
  StoreControllerMinLimitConfig(GetControllerMinLimit());
}

uint64_t
TrafficShapingEngine::GetControllerMinLimit() const
{
  return mControllerMinLimitBps.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyControllerMinLimitConfig(const uint64_t limit_bps)
{
  const uint64_t old_value =
      mControllerMinLimitBps.exchange(limit_bps, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetControllerMinLimit(limit_bps);
  }

  return old_value != limit_bps;
}

void
TrafficShapingEngine::StoreControllerMinLimitConfig(const uint64_t limit_bps)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_CONTROLLER_MIN_LIMIT_CONFIG,
                                  std::to_string(limit_bps));
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingEngine::SetIoPressureThreshold(const double threshold)
{
  ApplyIoPressureThresholdConfig(threshold);
  StoreIoPressureThresholdConfig(GetIoPressureThreshold());
}

double
TrafficShapingEngine::GetIoPressureThreshold() const
{
  return mIoPressureThreshold.load(std::memory_order_relaxed);
}

bool
TrafficShapingEngine::ApplyIoPressureThresholdConfig(const double threshold)
{
  const double clamped_threshold = std::max(0.0, std::min(1.0, threshold));
  const double old_value =
      mIoPressureThreshold.exchange(clamped_threshold, std::memory_order_relaxed);

  if (mManager != nullptr) {
    mManager->SetIoPressureThreshold(clamped_threshold);
  }

  return old_value != clamped_threshold;
}

void
TrafficShapingEngine::StoreIoPressureThresholdConfig(const double threshold)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_IO_PRESSURE_THRESHOLD_CONFIG,
                                  std::to_string(threshold));
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingEngine::StoreThreadConfig()
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
  ApplyEnabledConfig(is_enabled);

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

  ApplyThreadConfig(est_ms, pol_ms, rep_ms, win_s);

  const std::string detail_level =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_DETAIL_LEVEL_CONFIG);

  if (detail_level.empty()) {
    ApplyDetailLevelConfig(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE);
  } else {
    ApplyDetailLevelConfig(detail_level);
  }

  const std::string limits_enabled =
      FsView::gFsView.GetGlobalConfig(common::TRAFFIC_SHAPING_LIMITS_ENABLED_CONFIG);
  ApplyLimitsEnabledConfig(limits_enabled.empty() || limits_enabled == "true");

  const std::string reservations_enabled = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_RESERVATIONS_ENABLED_CONFIG);
  ApplyReservationsEnabledConfig(reservations_enabled.empty() ||
                                 reservations_enabled == "true");

  const std::string controller_min_limit = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_CONTROLLER_MIN_LIMIT_CONFIG);
  if (controller_min_limit.empty()) {
    ApplyControllerMinLimitConfig(kDefaultControllerMinLimitBps);
  } else {
    try {
      ApplyControllerMinLimitConfig(std::stoull(controller_min_limit));
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping controller minimum limit\" "
                     "value=\"%s\" error=\"%s\"",
                     controller_min_limit.c_str(), e.what());
      ApplyControllerMinLimitConfig(kDefaultControllerMinLimitBps);
    }
  }

  const std::string io_pressure_threshold = FsView::gFsView.GetGlobalConfig(
      common::TRAFFIC_SHAPING_IO_PRESSURE_THRESHOLD_CONFIG);
  if (io_pressure_threshold.empty()) {
    ApplyIoPressureThresholdConfig(kDefaultIoPressureThreshold);
  } else {
    try {
      ApplyIoPressureThresholdConfig(std::stod(io_pressure_threshold));
    } catch (const std::exception& e) {
      eos_static_err("msg=\"failed to parse Traffic Shaping IO pressure threshold\" "
                     "value=\"%s\" error=\"%s\"",
                     io_pressure_threshold.c_str(), e.what());
      ApplyIoPressureThresholdConfig(kDefaultIoPressureThreshold);
    }
  }

  EnsureFstEnabledSyncThread();
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

  ApplyThreadConfig(mEstimatorsUpdateThreadPeriodMilliseconds,
                    mFstIoPolicyUpdateThreadPeriodMilliseconds,
                    mFstIoStatsReportThreadPeriodMilliseconds, mSystemStatsWindowSeconds);

  // NOTE: Do NOT call SyncTrafficShapingEnabledWithFst() here. Start() can be
  // invoked while config replay is in progress; TrafficShapingEngine::ApplyConfig()
  // provides the immediate sync after the FsView write lock is released.
  eos_static_info("msg=\"Traffic Shaping Engine Started\"");
}

void
TrafficShapingEngine::Stop()
{
  StopRuntime();
  StopFstEnabledSyncThread();
}

void
TrafficShapingEngine::StopRuntime()
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

  // NOTE: Do NOT call SyncTrafficShapingEnabledWithFst() here for the same
  // reason as in Start(): config replay synchronizes after releasing ViewMutex.
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

    auto work_start = std::chrono::steady_clock::now();
    ProcessAllQueuedReports();
    auto reports_done = std::chrono::steady_clock::now();

    const std::chrono::duration<double> elapsed = reports_done - last_run;
    const double time_delta_seconds = elapsed.count();
    last_run = reports_done;

    mManager->UpdateEstimators(time_delta_seconds);
    auto estimators_done = std::chrono::steady_clock::now();

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
        std::chrono::duration_cast<std::chrono::microseconds>(work_done - work_start)
            .count();
    const auto report_duration_micro_sec =
        std::chrono::duration_cast<std::chrono::microseconds>(reports_done - work_start)
            .count();
    const auto estimator_duration_micro_sec =
        std::chrono::duration_cast<std::chrono::microseconds>(estimators_done -
                                                              reports_done)
            .count();
    const auto gc_duration_micro_sec =
        std::chrono::duration_cast<std::chrono::microseconds>(work_done - estimators_done)
            .count();

    if (static_cast<double>(work_duration_micro_sec) >
        static_cast<double>(mEstimatorsUpdateThreadPeriodMilliseconds) * 0.5 * 1000.0) {
      eos_static_warning(
          "msg=\"Traffic Shaping Estimators Update loop is slow\" total_ms=%.2f "
          "reports_ms=%.2f estimators_ms=%.2f gc_ms=%.2f",
          static_cast<double>(work_duration_micro_sec) / 1000.0,
          static_cast<double>(report_duration_micro_sec) / 1000.0,
          static_cast<double>(estimator_duration_micro_sec) / 1000.0,
          static_cast<double>(gc_duration_micro_sec) / 1000.0);
    }

    mManager->UpdateEstimatorsLoopMicroSec(work_duration_micro_sec);
  }
}

void
TrafficShapingEngine::FstIoPolicyUpdate(ThreadAssistant& assistant) const
{
  eos_static_info("%s", "msg=\"Starting FstIoPolicyUpdate thread\"");
  auto last_controller_update = std::chrono::steady_clock::time_point{};

  while (!assistant.terminationRequested()) {
    assistant.wait_for(
        std::chrono::milliseconds(mFstIoPolicyUpdateThreadPeriodMilliseconds));

    if (assistant.terminationRequested()) {
      break;
    }

    auto work_start_time = std::chrono::steady_clock::now();

    std::vector<std::string> online_nodes;
    const auto node_io_pressure = CollectNodeIoPressure(&online_nodes);
    const auto pressure_done_time = std::chrono::steady_clock::now();

    const bool run_controller =
        last_controller_update == std::chrono::steady_clock::time_point{} ||
        pressure_done_time - last_controller_update >= kControllerUpdateInterval;
    if (run_controller) {
      mManager->UpdateTrafficShapingController(node_io_pressure);
      last_controller_update = pressure_done_time;
    }
    const auto controller_done_time = std::chrono::steady_clock::now();

    mManager->UpdateLimits(node_io_pressure, online_nodes);

    auto work_end_time = std::chrono::steady_clock::now();
    const auto compute_duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(work_end_time -
                                                              work_start_time)
            .count();
    const auto pressure_duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(pressure_done_time -
                                                              work_start_time)
            .count();
    const auto controller_duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(controller_done_time -
                                                              pressure_done_time)
            .count();
    const auto limits_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                        work_end_time - controller_done_time)
                                        .count();

    if (static_cast<double>(compute_duration_us) >
        static_cast<double>(mFstIoPolicyUpdateThreadPeriodMilliseconds) * 0.5 * 1000.0) {
      eos_static_warning(
          "msg=\"Traffic Shaping FST policy update loop is slow\" total_ms=%.2f "
          "pressure_ms=%.2f controller_ms=%.2f limits_ms=%.2f controller_ran=%d",
          static_cast<double>(compute_duration_us) / 1000.0,
          static_cast<double>(pressure_duration_us) / 1000.0,
          static_cast<double>(controller_duration_us) / 1000.0,
          static_cast<double>(limits_duration_us) / 1000.0, run_controller);
    }

    mManager->UpdateFstLimitsLoopMicroSec(compute_duration_us);
  }
}

void
TrafficShapingEngine::FstTrafficShapingEnabledUpdate(ThreadAssistant& assistant)
{
  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(5));

    if (assistant.terminationRequested()) {
      break;
    }

    SyncTrafficShapingEnabledWithFst();
    SyncTrafficShapingConfigWithFst();
  }
}

void
TrafficShapingEngine::Enable()
{
  SetEnabled(true);
}

void
TrafficShapingEngine::Disable()
{
  SetEnabled(false);
}

void
TrafficShapingEngine::SetEnabled(bool enabled)
{
  EnsureFstEnabledSyncThread();
  StoreEnabledConfig(enabled);
  ApplyEnabledConfig(enabled);
  SyncTrafficShapingEnabledWithFst();
}

void
TrafficShapingEngine::ApplyEnabledConfig(bool enabled)
{
  if (enabled) {
    Start();
  } else {
    StopRuntime();
  }
}

void
TrafficShapingEngine::StoreEnabledConfig(bool enabled)
{
  FsView::gFsView.SetGlobalConfig(common::TRAFFIC_SHAPING_ENABLE_CONFIG, enabled);
  gOFS->mConfigEngine->AutoSave();
}

void
TrafficShapingEngine::EnsureFstEnabledSyncThread()
{
  std::lock_guard lock(mFstEnabledSyncThreadMutex);

  if (mFstEnabledSyncThreadStarted) {
    return;
  }

  mFstTrafficShapingEnabledUpdateThread.reset(
      &TrafficShapingEngine::FstTrafficShapingEnabledUpdate, this);
  mFstTrafficShapingEnabledUpdateThread.setName("Traffic Shaping FST Enabled Update");
  mFstEnabledSyncThreadStarted = true;
}

void
TrafficShapingEngine::StopFstEnabledSyncThread()
{
  std::lock_guard lock(mFstEnabledSyncThreadMutex);

  if (!mFstEnabledSyncThreadStarted) {
    return;
  }

  mFstTrafficShapingEnabledUpdateThread.join();
  mFstEnabledSyncThreadStarted = false;
}

std::vector<std::string>
TrafficShapingEngine::GetOnlineFstNodeNames() const
{
  std::vector<std::string> online_nodes;
  {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    for (const auto& [node_name, node_view] : FsView::gFsView.mNodeView) {
      if (node_view->GetStatus() == "online") {
        online_nodes.push_back(node_name);
      }
    }
  }
  return online_nodes;
}

void
TrafficShapingEngine::SyncTrafficShapingEnabledWithFst()
{
  const bool enabled = mRunning;
  const std::string enabled_str = enabled ? "true" : "false";

  for (const auto& node_name : GetOnlineFstNodeNames()) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_name);
    if (it != FsView::gFsView.mNodeView.end()) {
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_ENABLE_TOGGLE,
                                  enabled_str, true);
    }
  }
}

void
TrafficShapingEngine::SyncTrafficShapingConfigWithFst()
{
  const std::string period_str =
      std::to_string(mFstIoStatsReportThreadPeriodMilliseconds);

  for (const auto& node_name : GetOnlineFstNodeNames()) {
    eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mNodeView.find(node_name);
    if (it != FsView::gFsView.mNodeView.end()) {
      it->second->SetConfigMember(eos::common::FST_TRAFFIC_SHAPING_STATS_THREAD_PERIOD,
                                  period_str, true);
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
    TrafficShapingPolicy next_policy =
        PreparePolicyForSet(policy, it != mUidPolicies.end() ? &it->second : nullptr);

    if (next_policy.IsEmpty()) {
      if (it != mUidPolicies.end()) {
        mUidPolicies.erase(it);
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_uid_write_delay()->erase(uid);
          node_config.second.mutable_uid_read_delay()->erase(uid);
        }
        config_changed = true;
        eos_static_info("msg=\"Removed empty UID Traffic Shaping Policy\" uid=%u", uid);
      }
    } else {
      if (it == mUidPolicies.end()) {
        mUidPolicies[uid] = next_policy;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_uid_write_delay()->erase(uid);
          node_config.second.mutable_uid_read_delay()->erase(uid);
        }
        config_changed = true;
        eos_static_info("msg=\"Set UID Traffic Shaping Policy\" uid=%u policy=%s", uid,
                        next_policy.ToString().c_str());
      } else {
        const uint64_t old_write_limit = it->second.GetEffectiveWriteLimit();
        const uint64_t new_write_limit = next_policy.GetEffectiveWriteLimit();
        const uint64_t old_read_limit = it->second.GetEffectiveReadLimit();
        const uint64_t new_read_limit = next_policy.GetEffectiveReadLimit();
        const bool write_limit_changed = old_write_limit != new_write_limit;
        const bool read_limit_changed = old_read_limit != new_read_limit;
        // operator!= ignores controller limits, so it only flags true user config changes
        if (it->second != next_policy) {
          config_changed = true;
        }
        // Always update in-memory to reflect any potential ephemeral controller limit
        // changes
        it->second = next_policy;
        if (write_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_uid_write_delay(), uid,
                                     old_write_limit, new_write_limit);
          }
        }
        if (read_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_uid_read_delay(), uid,
                                     old_read_limit, new_read_limit);
          }
        }
        eos_static_info("msg=\"Updated UID Traffic Shaping Policy\" uid=%u policy=%s "
                        "persistent_changed=%d",
                        uid, next_policy.ToString().c_str(), config_changed);
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
    TrafficShapingPolicy next_policy =
        PreparePolicyForSet(policy, it != mGidPolicies.end() ? &it->second : nullptr);

    if (next_policy.IsEmpty()) {
      if (it != mGidPolicies.end()) {
        mGidPolicies.erase(it);
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_gid_write_delay()->erase(gid);
          node_config.second.mutable_gid_read_delay()->erase(gid);
        }
        config_changed = true;
        eos_static_info("msg=\"Removed empty GID Traffic Shaping Policy\" gid=%u", gid);
      }
    } else {
      if (it == mGidPolicies.end()) {
        mGidPolicies[gid] = next_policy;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_gid_write_delay()->erase(gid);
          node_config.second.mutable_gid_read_delay()->erase(gid);
        }
        config_changed = true;
        eos_static_info("msg=\"Set GID Traffic Shaping Policy\" gid=%u policy=%s", gid,
                        next_policy.ToString().c_str());
      } else {
        const uint64_t old_write_limit = it->second.GetEffectiveWriteLimit();
        const uint64_t new_write_limit = next_policy.GetEffectiveWriteLimit();
        const uint64_t old_read_limit = it->second.GetEffectiveReadLimit();
        const uint64_t new_read_limit = next_policy.GetEffectiveReadLimit();
        const bool write_limit_changed = old_write_limit != new_write_limit;
        const bool read_limit_changed = old_read_limit != new_read_limit;
        if (it->second != next_policy) {
          config_changed = true;
        }
        it->second = next_policy;
        if (write_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_gid_write_delay(), gid,
                                     old_write_limit, new_write_limit);
          }
        }
        if (read_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_gid_read_delay(), gid,
                                     old_read_limit, new_read_limit);
          }
        }
        eos_static_info("msg=\"Updated GID Traffic Shaping Policy\" gid=%u policy=%s "
                        "persistent_changed=%d",
                        gid, next_policy.ToString().c_str(), config_changed);
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
    TrafficShapingPolicy next_policy =
        PreparePolicyForSet(policy, it != mAppPolicies.end() ? &it->second : nullptr);

    if (next_policy.IsEmpty()) {
      if (it != mAppPolicies.end()) {
        mAppPolicies.erase(it);
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_app_write_delay()->erase(app);
          node_config.second.mutable_app_read_delay()->erase(app);
        }
        config_changed = true;
        eos_static_info("msg=\"Removed empty App Traffic Shaping Policy\" app=%s",
                        app.c_str());
      }
    } else {
      if (it == mAppPolicies.end()) {
        mAppPolicies[app] = next_policy;
        for (auto& node_config : mNodeFstIoDelayConfigs) {
          node_config.second.mutable_app_write_delay()->erase(app);
          node_config.second.mutable_app_read_delay()->erase(app);
        }
        config_changed = true;
        eos_static_info("msg=\"Set App Traffic Shaping Policy\" app=%s policy=%s",
                        app.c_str(), next_policy.ToString().c_str());
      } else {
        const uint64_t old_write_limit = it->second.GetEffectiveWriteLimit();
        const uint64_t new_write_limit = next_policy.GetEffectiveWriteLimit();
        const uint64_t old_read_limit = it->second.GetEffectiveReadLimit();
        const uint64_t new_read_limit = next_policy.GetEffectiveReadLimit();
        const bool write_limit_changed = old_write_limit != new_write_limit;
        const bool read_limit_changed = old_read_limit != new_read_limit;
        if (it->second != next_policy) {
          config_changed = true;
        }
        it->second = next_policy;
        if (write_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_app_write_delay(), app,
                                     old_write_limit, new_write_limit);
          }
        }
        if (read_limit_changed) {
          for (auto& node_config : mNodeFstIoDelayConfigs) {
            ScaleDelayForLimitChange(node_config.second.mutable_app_read_delay(), app,
                                     old_read_limit, new_read_limit);
          }
        }
        eos_static_info("msg=\"Updated App Traffic Shaping Policy\" app=%s policy=%s "
                        "persistent_changed=%d",
                        app.c_str(), next_policy.ToString().c_str(), config_changed);
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

std::unordered_map<std::string, AppIoPressureSnapshot>
TrafficShapingManager::GetReservedAppIoPressure() const
{
  const auto node_io_pressure = CollectNodeIoPressure();
  std::unordered_map<std::string, AppIoPressureSnapshot> pressure_by_app;

  std::shared_lock lock(mMutex);

  for (const auto& [app, policy] : mAppPolicies) {
    if (policy.reservation_read_bytes_per_sec > 0 ||
        policy.reservation_write_bytes_per_sec > 0) {
      pressure_by_app[app] = {};
    }
  }

  if (pressure_by_app.empty()) {
    return pressure_by_app;
  }

  for (const auto& [node_entity_key, stats] : mNodeEntityStats) {
    const auto app_it = pressure_by_app.find(node_entity_key.stream.app);

    if (app_it == pressure_by_app.end()) {
      continue;
    }

    const auto pressure_it = node_io_pressure.find(node_entity_key.node_id);
    const double node_pressure =
        pressure_it != node_io_pressure.end() ? pressure_it->second : kUnknownIoPressure;
    UpdateAppIoPressure(app_it->second, stats.ema[Ema1s], node_pressure);
  }

  return pressure_by_app;
}

std::vector<AppNodeIoPressureSnapshot>
TrafficShapingManager::GetReservedAppNodeIoPressure() const
{
  std::vector<std::string> online_nodes;
  const auto node_io_pressure = CollectNodeIoPressure(&online_nodes);
  std::vector<AppNodeIoPressureSnapshot> snapshots;

  auto get_rate = [](const auto& map, const auto& key) {
    if (auto it = map.find(key); it != map.end()) {
      return std::pair{it->second, true};
    }

    return std::pair{0.0, false};
  };

  auto meaningful_deficit = [](const uint64_t reservation_bps,
                               const double current_rate_bps) {
    const double deficit_bps =
        std::max(0.0, static_cast<double>(reservation_bps) - current_rate_bps);
    const double min_deficit_bps =
        std::max(kMinReservationDeficitBps,
                 static_cast<double>(reservation_bps) * kMinReservationDeficitFraction);

    return std::pair{deficit_bps, deficit_bps >= min_deficit_bps};
  };

  std::shared_lock lock(mMutex);

  EntityRateMaps global_rates;
  for (const auto& [key, stats] : mGlobalStats) {
    AddStreamRates(global_rates, key, stats.ema[Ema1s]);
  }

  std::unordered_map<std::string, EntityRateMaps> node_rates;
  for (const auto& [node_entity_key, stats] : mNodeEntityStats) {
    AddStreamRates(node_rates[node_entity_key.node_id], node_entity_key.stream,
                   stats.ema[Ema1s]);
  }

  const bool reservations_enabled = mReservationsEnabled.load(std::memory_order_relaxed);
  const double io_pressure_threshold =
      mIoPressureThreshold.load(std::memory_order_relaxed);
  std::unordered_map<std::string, std::pair<bool, bool>> node_has_pressured_reservation;

  for (const auto& [app, policy] : mAppPolicies) {
    if (policy.reservation_read_bytes_per_sec == 0 &&
        policy.reservation_write_bytes_per_sec == 0) {
      continue;
    }

    const auto [global_read_rate_bps, _read_global_seen] =
        get_rate(global_rates.app_read, app);
    const auto [global_write_rate_bps, _write_global_seen] =
        get_rate(global_rates.app_write, app);
    const auto [read_deficit_bps, read_deficit_active] =
        meaningful_deficit(policy.reservation_read_bytes_per_sec, global_read_rate_bps);
    const auto [write_deficit_bps, write_deficit_active] =
        meaningful_deficit(policy.reservation_write_bytes_per_sec, global_write_rate_bps);

    for (const auto& node_id : online_nodes) {
      AppNodeIoPressureSnapshot snapshot;
      snapshot.app = app;
      snapshot.node_id = node_id;
      snapshot.global_read_rate_bps = global_read_rate_bps;
      snapshot.global_write_rate_bps = global_write_rate_bps;
      snapshot.read_reservation_deficit_bps = read_deficit_bps;
      snapshot.write_reservation_deficit_bps = write_deficit_bps;
      snapshot.reservation_read_bytes_per_sec = policy.reservation_read_bytes_per_sec;
      snapshot.reservation_write_bytes_per_sec = policy.reservation_write_bytes_per_sec;
      snapshot.read_reservation_deficit_active =
          policy.reservation_read_bytes_per_sec > 0 && read_deficit_active;
      snapshot.write_reservation_deficit_active =
          policy.reservation_write_bytes_per_sec > 0 && write_deficit_active;

      if (auto pressure_it = node_io_pressure.find(node_id);
          pressure_it != node_io_pressure.end()) {
        snapshot.node_io_pressure = pressure_it->second;
        snapshot.has_node_io_pressure = true;
      }

      if (auto rates_it = node_rates.find(node_id); rates_it != node_rates.end()) {
        const auto [read_rate_bps, has_read_rate] =
            get_rate(rates_it->second.app_read, app);
        const auto [write_rate_bps, has_write_rate] =
            get_rate(rates_it->second.app_write, app);
        snapshot.read_rate_bps = read_rate_bps;
        snapshot.write_rate_bps = write_rate_bps;
        snapshot.has_read_io_pressure =
            has_read_rate && read_rate_bps >= kActiveEntityNodeRateBps;
        snapshot.has_write_io_pressure =
            has_write_rate && write_rate_bps >= kActiveEntityNodeRateBps;
      }

      snapshot.read_pressure_active = snapshot.has_read_io_pressure &&
                                      snapshot.has_node_io_pressure &&
                                      snapshot.node_io_pressure >= io_pressure_threshold;
      snapshot.write_pressure_active = snapshot.has_write_io_pressure &&
                                       snapshot.has_node_io_pressure &&
                                       snapshot.node_io_pressure >= io_pressure_threshold;
      snapshot.read_triggers_competitor_throttling =
          reservations_enabled && snapshot.read_pressure_active &&
          snapshot.read_reservation_deficit_active;
      snapshot.write_triggers_competitor_throttling =
          reservations_enabled && snapshot.write_pressure_active &&
          snapshot.write_reservation_deficit_active;

      auto& node_flags = node_has_pressured_reservation[node_id];
      node_flags.first = node_flags.first || snapshot.read_triggers_competitor_throttling;
      node_flags.second =
          node_flags.second || snapshot.write_triggers_competitor_throttling;

      snapshots.push_back(std::move(snapshot));
    }
  }

  for (auto& snapshot : snapshots) {
    if (auto node_it = node_has_pressured_reservation.find(snapshot.node_id);
        node_it != node_has_pressured_reservation.end()) {
      snapshot.node_has_pressured_read_reservation = node_it->second.first;
      snapshot.node_has_pressured_write_reservation = node_it->second.second;
    }
  }

  std::sort(snapshots.begin(), snapshots.end(), [](const auto& lhs, const auto& rhs) {
    return std::tie(lhs.app, lhs.node_id) < std::tie(rhs.app, rhs.node_id);
  });

  return snapshots;
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
        cpp_pol.controller_limit_read_update_time =
            it->second.controller_limit_read_update_time;
        cpp_pol.controller_limit_write_update_time =
            it->second.controller_limit_write_update_time;
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
  const auto now = std::chrono::steady_clock::now();
  if (now < mNextPluginCheckTime) {
    return;
  }
  mNextPluginCheckTime = now + kPluginCheckInterval;

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
          eos_static_err("msg=\"Failed to find any matching symbols "
                         "('calculate_delay' or 'update_controller') in plugin! "
                         "Falling back to built-in.\" error=\"%s\"",
                         dlerror());
          dlclose(mPluginHandle);
          mPluginHandle = nullptr;
          mCustomAlgo = nullptr;
          mCustomControllerAlgo = nullptr;
        } else {
          eos_static_info("msg=\"Successfully loaded and activated custom traffic "
                          "shaping plugin\" delay=%d controller=%d",
                          mCustomAlgo != nullptr, mCustomControllerAlgo != nullptr);
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
