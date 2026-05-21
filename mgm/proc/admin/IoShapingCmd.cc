#include "IoCmd.hh"
#include "common/Constants.hh"
#include "common/shaping/Identity.hh"
#include "common/shaping/IoStatsKey.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/shaping/TrafficShaping.hh"

#include "proto/ConsoleReply.pb.h"
#include "proto/TrafficShaping.pb.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <json/json.h>
#include <map>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>

std::string
format_rate(const double bytes_per_sec)
{
  const char* units[] = {"B/s", "kB/s", "MB/s", "GB/s", "TB/s", "PB/s"};
  int unit_idx = 0;
  double val = bytes_per_sec;
  while (val >= 1000.0 && unit_idx < 5) {
    val /= 1000.0;
    unit_idx++;
  }
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(2) << val << " " << units[unit_idx];
  return ss.str();
}

std::string
format_io_pressure(const double pressure)
{
  std::ostringstream ss;
  ss << std::fixed << std::setprecision(3) << pressure;
  return ss.str();
}

std::string
format_duration_us(const uint64_t microseconds)
{
  std::ostringstream ss;

  if (microseconds >= 1000000) {
    ss << std::fixed << std::setprecision(2)
       << static_cast<double>(microseconds) / 1000000.0 << " s";
  } else if (microseconds >= 1000) {
    ss << std::fixed << std::setprecision(2) << static_cast<double>(microseconds) / 1000.0
       << " ms";
  } else {
    ss << microseconds << " us";
  }

  return ss.str();
}

std::string
format_optional_io_pressure(const bool has_pressure, const double pressure)
{
  return has_pressure ? format_io_pressure(pressure) : "-";
}

const char*
format_bool(const bool value)
{
  return value ? "yes" : "no";
}

std::string
format_table_label(const std::string& value, const size_t width)
{
  if (value.size() <= width) {
    return value;
  }

  if (width <= 3) {
    return value.substr(0, width);
  }

  return value.substr(0, width - 3) + "...";
}

namespace {

using eos::common::traffic_shaping::GidLabel;
using eos::common::traffic_shaping::kUnknownId;
using eos::common::traffic_shaping::NodeLabel;
using eos::common::traffic_shaping::UidLabel;

// Replace empty string identifiers with the shared <unknown> placeholder so the
// CLI/JSON output never contains a bare empty value.
const std::string&
LabelOrUnknown(const std::string& value)
{
  static const std::string unknown(kUnknownId);
  return value.empty() ? unknown : value;
}

std::string
CompactJsonString(const Json::Value& value)
{
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "";
  return Json::writeString(builder, value);
}

constexpr const char* kAutomaticDetailCardinalityPrefix = "auto-cardinality:";

bool
ParseAutomaticDetailCardinalityConfig(const std::string& value,
                                      std::optional<uint64_t>& low_cardinality,
                                      std::optional<uint64_t>& high_cardinality,
                                      std::optional<bool>& automatic_enabled)
{
  if (value.rfind(kAutomaticDetailCardinalityPrefix, 0) != 0) {
    return false;
  }

  const std::string payload =
      value.substr(std::strlen(kAutomaticDetailCardinalityPrefix));
  const size_t separator = payload.find(':');

  if (separator == std::string::npos) {
    return false;
  }

  const size_t state_separator = payload.find(':', separator + 1);
  const std::string low = payload.substr(0, separator);
  const std::string high =
      state_separator == std::string::npos
          ? payload.substr(separator + 1)
          : payload.substr(separator + 1, state_separator - separator - 1);
  const std::string state =
      state_separator == std::string::npos ? "" : payload.substr(state_separator + 1);

  try {
    if (!low.empty()) {
      low_cardinality = std::stoull(low);
    }

    if (!high.empty()) {
      high_cardinality = std::stoull(high);
    }
  } catch (const std::exception&) {
    return false;
  }

  if (!state.empty()) {
    if (state == "enabled") {
      automatic_enabled = true;
    } else if (state == "disabled") {
      automatic_enabled = false;
    } else {
      return false;
    }
  }

  return low_cardinality.has_value() || high_cardinality.has_value() ||
         automatic_enabled.has_value();
}

} // namespace

namespace eos::mgm {

namespace {

struct Rates {
  double r_bps = 0;
  double w_bps = 0;
  double r_iops = 0;
  double w_iops = 0;

  double
  total_throughput() const
  {
    return r_bps + w_bps;
  }

  void
  add(const Rates& other)
  {
    r_bps += other.r_bps;
    w_bps += other.w_bps;
    r_iops += other.r_iops;
    w_iops += other.w_iops;
  }
};

Rates
ExtractWindowRates(const traffic_shaping::RateSnapshot& snap,
                   eos::traffic_shaping::TrafficShapingRateRequest::Estimators estimator)
{
  auto unpack = [](const traffic_shaping::RateMetrics& m) -> Rates {
    return {m.read_rate_bps, m.write_rate_bps, m.read_iops, m.write_iops};
  };

  using Request = eos::traffic_shaping::TrafficShapingRateRequest;
  switch (estimator) {
  case Request::SMA_1_SECONDS:
    return unpack(snap.sma[traffic_shaping::Sma1s]);
  case Request::SMA_5_SECONDS:
    return unpack(snap.sma[traffic_shaping::Sma5s]);
  case Request::SMA_15_SECONDS:
    return unpack(snap.sma[traffic_shaping::Sma15s]);
  case Request::SMA_1_MINUTES:
    return unpack(snap.sma[traffic_shaping::Sma1m]);
  case Request::SMA_5_MINUTES:
    return unpack(snap.sma[traffic_shaping::Sma5m]);
  case Request::EMA_1_SECONDS:
    return unpack(snap.ema[traffic_shaping::Ema1s]);
  case Request::EMA_5_SECONDS:
    return unpack(snap.ema[traffic_shaping::Ema5s]);
  case Request::UNSPECIFIED:
  default:
    return unpack(snap.sma[traffic_shaping::Sma1m]);
  }
}

void
SetRateStats(eos::traffic_shaping::RateStats* stats,
             eos::traffic_shaping::TrafficShapingRateRequest::Estimators estimator,
             const Rates& rates)
{
  stats->set_window(estimator);
  stats->set_bytes_read_per_sec(rates.r_bps);
  stats->set_bytes_written_per_sec(rates.w_bps);
  stats->set_iops_read(rates.r_iops);
  stats->set_iops_write(rates.w_iops);
}

void
BuildReport(const std::shared_ptr<traffic_shaping::TrafficShapingManager>& manager,
            const eos::traffic_shaping::TrafficShapingRateRequest& request,
            eos::traffic_shaping::TrafficShapingRateResponse& report)
{
  auto global_stats = manager->GetGlobalStats();

  const auto [estimator_mean, estimator_min, estimator_max] =
      manager->GetEstimatorsUpdateLoopMicroSecStats();
  const auto [fst_limits_mean, fst_limits_min, fst_limits_max] =
      manager->GetFstLimitsUpdateLoopMicroSecStats();
  const auto [reservation_controller_mean, reservation_controller_min,
              reservation_controller_max] =
      manager->GetReservationControllerUpdateLoopMicroSecStats();

  auto* est_stats = report.mutable_estimators_update_thread_loop_stats();
  est_stats->set_mean_elapsed_time_micro_sec(estimator_mean);
  est_stats->set_min_elapsed_time_micro_sec(estimator_min);
  est_stats->set_max_elapsed_time_micro_sec(estimator_max);

  auto* fst_stats = report.mutable_fst_limits_update_thread_loop_stats();
  fst_stats->set_mean_elapsed_time_micro_sec(fst_limits_mean);
  fst_stats->set_min_elapsed_time_micro_sec(fst_limits_min);
  fst_stats->set_max_elapsed_time_micro_sec(fst_limits_max);

  auto* reservation_controller_stats =
      report.mutable_reservation_controller_update_thread_loop_stats();
  reservation_controller_stats->set_mean_elapsed_time_micro_sec(
      reservation_controller_mean);
  reservation_controller_stats->set_min_elapsed_time_micro_sec(
      reservation_controller_min);
  reservation_controller_stats->set_max_elapsed_time_micro_sec(
      reservation_controller_max);

  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  report.set_timestamp_ms(now_ms);

  bool do_uid = false, do_gid = false, do_app = false, do_detailed = false;
  if (request.include_types_size() == 0) {
    do_uid = do_gid = do_app = true;
  } else {
    for (const auto type : request.include_types()) {
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_UID) {
        do_uid = true;
      }
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_GID) {
        do_gid = true;
      }
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_APP) {
        do_app = true;
      }
      if (type == eos::traffic_shaping::TrafficShapingRateRequest::ENTITY_DETAILED) {
        do_detailed = true;
      }
    }
  }

  auto detailed_stats = do_detailed
                            ? manager->GetDetailedStats()
                            : std::unordered_map<traffic_shaping::DetailedKey,
                                                 traffic_shaping::RateSnapshot,
                                                 traffic_shaping::DetailedKeyHash>{};

  std::vector<eos::traffic_shaping::TrafficShapingRateRequest::Estimators> estimators;
  if (request.estimators_size() == 0) {
    estimators.push_back(eos::traffic_shaping::TrafficShapingRateRequest::SMA_5_SECONDS);
  } else {
    for (auto w : request.estimators()) {
      if (w != eos::traffic_shaping::TrafficShapingRateRequest::UNSPECIFIED) {
        estimators.push_back(
            static_cast<eos::traffic_shaping::TrafficShapingRateRequest::Estimators>(w));
      }
    }
  }

  if (estimators.empty()) {
    estimators.push_back(eos::traffic_shaping::TrafficShapingRateRequest::SMA_5_SECONDS);
  }

  eos::traffic_shaping::TrafficShapingRateRequest::Estimators sort_window = estimators[0];
  if (request.has_sort_by_estimator() &&
      request.sort_by_estimator() !=
          eos::traffic_shaping::TrafficShapingRateRequest::UNSPECIFIED) {
    sort_window = request.sort_by_estimator();
  }

  struct AggregatedEntity {
    uint32_t active_streams = 0;
    std::map<eos::traffic_shaping::TrafficShapingRateRequest::Estimators, Rates>
        window_rates{};
  };

  std::map<uint32_t, AggregatedEntity> uid_agg;
  std::map<uint32_t, AggregatedEntity> gid_agg;
  std::map<std::string, AggregatedEntity> app_agg;

  for (const auto& [key, snap] : global_stats) {
    for (auto win : estimators) {
      Rates rates = ExtractWindowRates(snap, win);

      if (do_uid) {
        auto& agg = uid_agg[key.uid];
        agg.window_rates[win].add(rates);
        if (win == estimators[0]) {
          agg.active_streams++;
        }
      }
      if (do_gid) {
        auto& agg = gid_agg[key.gid];
        agg.window_rates[win].add(rates);
        if (win == estimators[0]) {
          agg.active_streams++;
        }
      }
      if (do_app) {
        auto& agg = app_agg[key.app];
        agg.window_rates[win].add(rates);
        if (win == estimators[0]) {
          agg.active_streams++;
        }
      }
    }
  }

  auto process_stats = [&](const auto& source_map, auto add_entry_fn, auto set_id_fn) {
    if (source_map.empty()) {
      return;
    }

    using PairType = typename std::decay_t<decltype(source_map)>::value_type;
    std::vector<const PairType*> vec;
    vec.reserve(source_map.size());
    for (const auto& item : source_map) {
      vec.push_back(&item);
    }

    auto sorter = [&](const PairType* a, const PairType* b) {
      double val_a = 0, val_b = 0;
      if (auto it = a->second.window_rates.find(sort_window);
          it != a->second.window_rates.end()) {
        val_a = it->second.total_throughput();
      }
      if (auto it = b->second.window_rates.find(sort_window);
          it != b->second.window_rates.end()) {
        val_b = it->second.total_throughput();
      }
      return val_a > val_b;
    };

    size_t n = vec.size();
    if (request.has_top_n() && request.top_n() > 0) {
      n = std::min(static_cast<size_t>(request.top_n()), n);
      std::partial_sort(vec.begin(), vec.begin() + n, vec.end(), sorter);
    } else {
      std::sort(vec.begin(), vec.end(), sorter);
    }

    for (size_t i = 0; i < n; ++i) {
      auto* entry = add_entry_fn();
      set_id_fn(entry, vec[i]->first);

      for (const auto& [estimator, rates] : vec[i]->second.window_rates) {
        SetRateStats(entry->add_stats(), estimator, rates);
      }
    }
  };

  if (do_uid) {
    process_stats(
        uid_agg, [&]() { return report.add_user_stats(); },
        [](auto* e, uint32_t id) { e->set_uid(id); });
  }

  if (do_gid) {
    process_stats(
        gid_agg, [&]() { return report.add_group_stats(); },
        [](auto* e, uint32_t id) { e->set_gid(id); });
  }

  if (do_app) {
    process_stats(
        app_agg, [&]() { return report.add_app_stats(); },
        [](auto* e, const std::string& id) { e->set_app_name(id); });
  }

  if (do_detailed && !detailed_stats.empty()) {
    using DetailedItem = decltype(detailed_stats)::value_type;
    std::vector<const DetailedItem*> sorted;
    sorted.reserve(detailed_stats.size());

    for (const auto& item : detailed_stats) {
      sorted.push_back(&item);
    }

    auto sorter = [&](const DetailedItem* a, const DetailedItem* b) {
      return ExtractWindowRates(a->second, sort_window).total_throughput() >
             ExtractWindowRates(b->second, sort_window).total_throughput();
    };

    size_t n = sorted.size();
    if (request.has_top_n() && request.top_n() > 0) {
      n = std::min(static_cast<size_t>(request.top_n()), n);
      std::partial_sort(sorted.begin(), sorted.begin() + n, sorted.end(), sorter);
    } else {
      std::sort(sorted.begin(), sorted.end(), sorter);
    }

    for (size_t i = 0; i < n; ++i) {
      const auto& key = sorted[i]->first;
      auto* entry = report.add_detailed_stats();
      entry->set_node_id(NodeLabel(LabelOrUnknown(key.node_id)));
      entry->set_app_name(key.stream.app);
      entry->set_uid(key.stream.uid);
      entry->set_gid(key.stream.gid);
      entry->set_fsid(key.stream.fsid);

      for (const auto estimator : estimators) {
        SetRateStats(entry->add_stats(), estimator,
                     ExtractWindowRates(sorted[i]->second, estimator));
      }
    }
  }
}

} // namespace

bool
BuildTrafficShapingRateReport(
    const eos::traffic_shaping::TrafficShapingRateRequest& request,
    eos::traffic_shaping::TrafficShapingRateResponse& report, std::string* error)
{
  const auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();

  if (!manager) {
    if (error) {
      *error = "Traffic shaping engine is not initialized";
    }
    return false;
  }

  BuildReport(manager, request, report);
  return true;
}

void
ShapingPolicySet(const eos::console::IoProto_ShapingProto_PolicyAction_SetAction& set_req,
                 eos::console::ReplyProto& reply)
{
  const auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();
  if (!manager) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  traffic_shaping::TrafficShapingPolicy policy;
  std::string target_desc;

  if (set_req.has_app()) {
    target_desc = "App '" + set_req.app() + "'";
    policy = manager->GetAppPolicy(set_req.app())
                 .value_or(traffic_shaping::TrafficShapingPolicy{});
  } else if (set_req.has_uid()) {
    target_desc = "UID " + std::to_string(set_req.uid());
    policy = manager->GetUidPolicy(set_req.uid())
                 .value_or(traffic_shaping::TrafficShapingPolicy{});
  } else if (set_req.has_gid()) {
    target_desc = "GID " + std::to_string(set_req.gid());
    policy = manager->GetGidPolicy(set_req.gid())
                 .value_or(traffic_shaping::TrafficShapingPolicy{});
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: You must specify a target (--app, --uid, or --gid).\n");
    return;
  }

  // --- Parse User Limits ---
  if (set_req.has_limit_read_bytes_per_sec()) {
    policy.limit_read_bytes_per_sec = set_req.limit_read_bytes_per_sec();
  }
  if (set_req.has_limit_write_bytes_per_sec()) {
    policy.limit_write_bytes_per_sec = set_req.limit_write_bytes_per_sec();
  }

  // --- Parse Reservations ---
  if (set_req.has_reservation_read_bytes_per_sec()) {
    policy.reservation_read_bytes_per_sec = set_req.reservation_read_bytes_per_sec();
  }
  if (set_req.has_reservation_write_bytes_per_sec()) {
    policy.reservation_write_bytes_per_sec = set_req.reservation_write_bytes_per_sec();
  }

  // --- Parse Ephemeral Controller Limits ---
  if (set_req.has_controller_limit_read_bytes_per_sec()) {
    policy.controller_limit_read_bytes_per_sec =
        set_req.controller_limit_read_bytes_per_sec();
  }
  if (set_req.has_controller_limit_write_bytes_per_sec()) {
    policy.controller_limit_write_bytes_per_sec =
        set_req.controller_limit_write_bytes_per_sec();
  }

  if (set_req.has_is_enabled()) {
    policy.is_enabled = set_req.is_enabled();
  }

  if (set_req.has_app()) {
    manager->SetAppPolicy(set_req.app(), policy);
  } else if (set_req.has_uid()) {
    manager->SetUidPolicy(set_req.uid(), policy);
  } else if (set_req.has_gid()) {
    manager->SetGidPolicy(set_req.gid(), policy);
  }

  const std::string status_str = policy.is_enabled ? "Enabled" : "Disabled";

  reply.set_retc(0);
  reply.set_std_out("success: Updated shaping policy for " + target_desc +
                    " (Status: " + status_str + ")\n");
}

void
ShapingPolicyDelete(
    const eos::console::IoProto_ShapingProto_PolicyAction_RemoveAction& rm_req,
    eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();
  if (!manager) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  std::string target_desc;

  if (rm_req.has_app()) {
    manager->RemoveAppPolicy(rm_req.app());
    target_desc = "App '" + rm_req.app() + "'";
  } else if (rm_req.has_uid()) {
    manager->RemoveUidPolicy(rm_req.uid());
    target_desc = "UID " + std::to_string(rm_req.uid());
  } else if (rm_req.has_gid()) {
    manager->RemoveGidPolicy(rm_req.gid());
    target_desc = "GID " + std::to_string(rm_req.gid());
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err(
        "error: You must specify a target to delete (--app, --uid, or --gid).\n");
    return;
  }

  reply.set_retc(0);
  reply.set_std_out("success: Deleted shaping policy for " + target_desc + "\n");
}

void
ShapingTrafficEnable(eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  engine.Enable();
  reply.set_retc(0);
  reply.set_std_out("success: Traffic Shaping enabled.\n");
}

void
ShapingTrafficDisable(eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  engine.Disable();
  reply.set_retc(0);
  reply.set_std_out("success: Traffic Shaping disabled.\n");
}

void
ShapingList(const eos::console::IoProto_ShapingProto_ListAction& list_req,
            eos::console::ReplyProto& reply)
{
  const auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();
  if (!manager) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic Shaping Engine is not initialized.\n");
    return;
  }

  if (list_req.show_fs() &&
      engine.GetDetailLevel() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
    reply.set_retc(0);

    if (list_req.json_output()) {
      reply.set_std_out("[]\n");
    } else {
      reply.set_std_out("Filesystem-level traffic stats are disabled. Enable them with "
                        "'eos io shaping config set --detail fs'.\n");
    }

    return;
  }

  const auto total_stats = manager->GetTotalStats();

  struct AggregatedStats {
    double read_rate = 0.0;
    double write_rate = 0.0;
    double read_iops = 0.0;
    double write_iops = 0.0;
  };

  using traffic_shaping::DetailedKey;
  using traffic_shaping::DiskKey;

  std::map<std::string, AggregatedStats> agg_stats;
  std::map<DiskKey, AggregatedStats> fs_agg_stats;
  std::map<DetailedKey, AggregatedStats> detailed_agg_stats;
  const auto reserved_app_io_pressure =
      list_req.show_apps()
          ? manager->GetReservedAppIoPressure()
          : std::unordered_map<std::string, traffic_shaping::AppIoPressureSnapshot>{};
  const bool resolve_ids =
      list_req.has_resolve_ids() ? list_req.resolve_ids() : !list_req.json_output();

  auto uid_label = [resolve_ids](const uint32_t uid) -> std::string {
    if (!resolve_ids) {
      return std::to_string(uid);
    }

    return UidLabel(uid);
  };

  auto gid_label = [resolve_ids](const uint32_t gid) -> std::string {
    if (!resolve_ids) {
      return std::to_string(gid);
    }

    return GidLabel(gid);
  };

  traffic_shaping::SmaIdx sma_idx = traffic_shaping::Sma1m;
  uint32_t window_sec = list_req.time_window_seconds();

  switch (window_sec) {
  case 1:
    sma_idx = traffic_shaping::Sma1s;
    break;
  case 5:
    sma_idx = traffic_shaping::Sma5s;
    break;
  case 15:
    sma_idx = traffic_shaping::Sma15s;
    break;
  case 300:
    sma_idx = traffic_shaping::Sma5m;
    break;
  case 60:
  default:
    sma_idx = traffic_shaping::Sma1m;
    window_sec = 60; // Enforce default for output formatting
    break;
  }

  auto accumulate = [&sma_idx](AggregatedStats& entry,
                               const traffic_shaping::RateSnapshot& snapshot) {
    const auto& sma_metrics = snapshot.sma[sma_idx];
    entry.read_rate += sma_metrics.read_rate_bps;
    entry.write_rate += sma_metrics.write_rate_bps;
    entry.read_iops += sma_metrics.read_iops;
    entry.write_iops += sma_metrics.write_iops;
  };

  if (list_req.show_all()) {
    if (engine.GetDetailLevel() == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
      for (const auto& [detailed_key, snapshot] : manager->GetDetailedStats()) {
        DetailedKey group_key{NodeLabel(LabelOrUnknown(detailed_key.node_id)),
                              {LabelOrUnknown(detailed_key.stream.app),
                               detailed_key.stream.uid, detailed_key.stream.gid,
                               detailed_key.stream.fsid}};
        accumulate(detailed_agg_stats[group_key], snapshot);
      }
    } else {
      // Filesystem detail is off: no node/fsid context, so synthesize one
      // <unknown> bucket per (app, uid, gid) — fsid is omitted (0).
      for (const auto& [key, snapshot] : manager->GetGlobalStats()) {
        DetailedKey group_key{kUnknownId, {LabelOrUnknown(key.app), key.uid, key.gid, 0}};
        accumulate(detailed_agg_stats[group_key], snapshot);
      }
    }
  } else if (list_req.show_fs()) {
    for (const auto& [disk_key, snapshot] : manager->GetDiskStats()) {
      DiskKey group_key{NodeLabel(LabelOrUnknown(disk_key.node_id)), disk_key.fsid};
      accumulate(fs_agg_stats[group_key], snapshot);
    }
  } else if (list_req.show_nodes()) {
    for (const auto& [node_id, snapshot] : manager->GetNodeStats()) {
      accumulate(agg_stats[NodeLabel(LabelOrUnknown(node_id))], snapshot);
    }
  } else {
    for (const auto& [key, snapshot] : manager->GetGlobalStats()) {
      std::string group_key;

      if (list_req.show_apps()) {
        group_key = LabelOrUnknown(key.app);
      } else if (list_req.show_users()) {
        group_key = std::to_string(key.uid);
      } else if (list_req.show_groups()) {
        group_key = std::to_string(key.gid);
      } else {
        group_key = "app:" + key.app; // Default fallback
      }

      accumulate(agg_stats[group_key], snapshot);
    }

    if (list_req.show_apps()) {
      for (const auto& [app, _] : reserved_app_io_pressure) {
        agg_stats.try_emplace(app);
      }
    }
  }

  const auto& total_sma_metrics = total_stats.sma[sma_idx];

  std::ostringstream oss;

  if (list_req.json_output()) {
    std::string type_str = "unknown";
    if (list_req.show_apps()) {
      type_str = "app";
    } else if (list_req.show_users()) {
      type_str = "uid";
    } else if (list_req.show_groups()) {
      type_str = "gid";
    } else if (list_req.show_nodes()) {
      type_str = "node";
    }

    Json::Value json(Json::arrayValue);

    auto add_rate_fields = [window_sec](Json::Value& entry, const AggregatedStats& stat) {
      entry["window_sec"] = static_cast<Json::Value::UInt>(window_sec);
      entry["read_rate_bps"] = stat.read_rate;
      entry["write_rate_bps"] = stat.write_rate;
      entry["read_iops"] = stat.read_iops;
      entry["write_iops"] = stat.write_iops;
    };

    auto add_app_io_pressure_fields =
        [&reserved_app_io_pressure](Json::Value& entry, const std::string& app) {
          const auto pressure_it = reserved_app_io_pressure.find(app);

          if (pressure_it == reserved_app_io_pressure.end()) {
            return;
          }

          entry["has_read_io_pressure"] = pressure_it->second.has_read;
          entry["has_write_io_pressure"] = pressure_it->second.has_write;

          if (pressure_it->second.has_read) {
            entry["read_io_pressure"] = pressure_it->second.read;
          }

          if (pressure_it->second.has_write) {
            entry["write_io_pressure"] = pressure_it->second.write;
          }
        };

    if (list_req.show_all()) {
      for (const auto& [detailed_key, stat] : detailed_agg_stats) {
        Json::Value entry;
        entry["type"] = "all";
        entry["node_id"] = detailed_key.node_id;
        entry["fsid"] = static_cast<Json::Value::UInt64>(detailed_key.stream.fsid);
        entry["app"] = detailed_key.stream.app;
        entry["uid"] = static_cast<Json::Value::UInt>(detailed_key.stream.uid);
        entry["gid"] = static_cast<Json::Value::UInt>(detailed_key.stream.gid);

        if (resolve_ids) {
          entry["user"] = uid_label(detailed_key.stream.uid);
          entry["group"] = gid_label(detailed_key.stream.gid);
        }

        add_rate_fields(entry, stat);
        json.append(entry);
      }
    } else if (list_req.show_fs()) {
      for (const auto& [fs_key, stat] : fs_agg_stats) {
        Json::Value entry;
        entry["type"] = "fs";
        entry["node_id"] = fs_key.node_id;
        entry["fsid"] = static_cast<Json::Value::UInt64>(fs_key.fsid);
        add_rate_fields(entry, stat);
        json.append(entry);
      }
    } else {
      for (const auto& [name, stat] : agg_stats) {
        Json::Value entry;
        entry["id"] = name;
        entry["type"] = type_str;

        if (resolve_ids && (list_req.show_users() || list_req.show_groups())) {
          entry["name"] = list_req.show_users() ? uid_label(std::stoul(name))
                                                : gid_label(std::stoul(name));
        }

        add_rate_fields(entry, stat);
        if (list_req.show_apps()) {
          add_app_io_pressure_fields(entry, name);
        }
        json.append(entry);
      }
    }

    if (list_req.system_stats()) {
      const auto [estimator_median, estimator_min, estimator_max] =
          manager->GetEstimatorsUpdateLoopMicroSecStats();
      const auto [fst_limits_median, fst_limits_min, fst_limits_max] =
          manager->GetFstLimitsUpdateLoopMicroSecStats();
      const auto [reservation_controller_median, reservation_controller_min,
                  reservation_controller_max] =
          manager->GetReservationControllerUpdateLoopMicroSecStats();
      const auto reports_processed_mean = manager->GetFstReportsProcessedPerSecondMean();
      const auto system_stats_window_seconds = manager->GetSystemStatsWindowSeconds();
      const auto map_cardinality = manager->GetMapCardinalityStats();

      Json::Value entry;
      entry["id"] = "engine_meta";
      entry["type"] = "system";
      entry["estimators_loop_median_us"] = estimator_median;
      entry["estimators_loop_min_us"] = estimator_min;
      entry["estimators_loop_max_us"] = estimator_max;
      entry["fst_limits_loop_median_us"] = fst_limits_median;
      entry["fst_limits_loop_min_us"] = fst_limits_min;
      entry["fst_limits_loop_max_us"] = fst_limits_max;
      entry["reservation_controller_loop_median_us"] = reservation_controller_median;
      entry["reservation_controller_loop_min_us"] = reservation_controller_min;
      entry["reservation_controller_loop_max_us"] = reservation_controller_max;
      entry["reports_processed_per_sec_mean"] = reports_processed_mean;
      entry["system_stats_window_seconds"] =
          static_cast<Json::Value::UInt64>(system_stats_window_seconds);
      entry["detail_auto_enabled"] =
          gOFS->mTrafficShapingEngine.GetAutomaticDetailLevelEnabled();
      entry["detail_auto_low_cardinality"] = static_cast<Json::Value::UInt64>(
          gOFS->mTrafficShapingEngine.GetAutomaticDetailLevelLowCardinality());
      entry["detail_auto_high_cardinality"] = static_cast<Json::Value::UInt64>(
          gOFS->mTrafficShapingEngine.GetAutomaticDetailLevelHighCardinality());
      entry["detail_auto_indicator"] = "node_state_streams";
      entry["node_states_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.node_states);
      entry["node_state_streams_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.node_state_streams);
      entry["global_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.global_stats);
      entry["node_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.node_stats);
      entry["disk_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.disk_stats);
      entry["detailed_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.detailed_stats);
      entry["global_cumulative_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.global_cumulative_stats);
      entry["node_cumulative_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.node_cumulative_stats);
      entry["disk_cumulative_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.disk_cumulative_stats);
      entry["detailed_cumulative_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.detailed_cumulative_stats);
      entry["node_entity_stats_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.node_entity_stats);
      entry["uid_policies_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.uid_policies);
      entry["gid_policies_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.gid_policies);
      entry["app_policies_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.app_policies);
      entry["node_fst_io_delay_configs_cardinality"] =
          static_cast<Json::Value::UInt64>(map_cardinality.node_fst_io_delay_configs);
      entry["published_fst_io_delay_configs_cardinality"] =
          static_cast<Json::Value::UInt64>(
              map_cardinality.published_fst_io_delay_configs);
      json.append(entry);
    }

    oss << CompactJsonString(json);
  } else {
    std::string header_name = "ID";
    if (list_req.show_apps()) {
      header_name = "Application";
    } else if (list_req.show_users()) {
      header_name = "UID";
    } else if (list_req.show_groups()) {
      header_name = "GID";
    } else if (list_req.show_nodes()) {
      header_name = "Storage Node";
    }

    oss << "--- IO Rates (" << window_sec << "s simple moving average) ---\n";

    if (list_req.show_all()) {
      oss << std::left << std::setw(40) << "Storage Node" << std::right << std::setw(10)
          << "FSID" << std::setw(24) << "Application" << std::setw(18) << "UID"
          << std::setw(18) << "GID" << std::setw(15) << "Read Rate" << std::setw(15)
          << "Write Rate" << std::setw(12) << "Read IOPS" << std::setw(12) << "Write IOPS"
          << "\n";

      oss << std::string(164, '-') << "\n";

      for (const auto& [detailed_key, stat] : detailed_agg_stats) {
        const std::string uid_display = uid_label(detailed_key.stream.uid);
        const std::string gid_display = gid_label(detailed_key.stream.gid);
        oss << std::left << std::setw(40) << detailed_key.node_id << std::right
            << std::setw(10) << detailed_key.stream.fsid << std::setw(24)
            << detailed_key.stream.app << std::setw(18) << uid_display << std::setw(18)
            << gid_display << std::setw(15) << format_rate(stat.read_rate)
            << std::setw(15) << format_rate(stat.write_rate) << std::fixed
            << std::setprecision(2) << std::setw(12) << stat.read_iops << std::setw(12)
            << stat.write_iops << "\n";
      }

      oss << std::string(164, '-') << "\n";
      oss << std::left << std::setw(40) << "Total" << std::right << std::setw(10) << ""
          << std::setw(24) << "" << std::setw(18) << "" << std::setw(18) << ""
          << std::setw(15) << format_rate(total_sma_metrics.read_rate_bps)
          << std::setw(15) << format_rate(total_sma_metrics.write_rate_bps) << std::fixed
          << std::setprecision(2) << std::setw(12) << total_sma_metrics.read_iops
          << std::setw(12) << total_sma_metrics.write_iops << "\n";
    } else if (list_req.show_fs()) {
      oss << std::left << std::setw(40) << "Storage Node" << std::right << std::setw(10)
          << "FSID" << std::setw(15) << "Read Rate" << std::setw(15) << "Write Rate"
          << std::setw(12) << "Read IOPS" << std::setw(12) << "Write IOPS" << "\n";

      oss << std::string(104, '-') << "\n";

      for (const auto& [fs_key, stat] : fs_agg_stats) {
        oss << std::left << std::setw(40) << fs_key.node_id << std::right << std::setw(10)
            << fs_key.fsid << std::setw(15) << format_rate(stat.read_rate)
            << std::setw(15) << format_rate(stat.write_rate) << std::fixed
            << std::setprecision(2) << std::setw(12) << stat.read_iops << std::setw(12)
            << stat.write_iops << "\n";
      }

      oss << std::string(104, '-') << "\n";
      oss << std::left << std::setw(40) << "Total" << std::right << std::setw(10) << ""
          << std::setw(15) << format_rate(total_sma_metrics.read_rate_bps)
          << std::setw(15) << format_rate(total_sma_metrics.write_rate_bps) << std::fixed
          << std::setprecision(2) << std::setw(12) << total_sma_metrics.read_iops
          << std::setw(12) << total_sma_metrics.write_iops << "\n";
    } else {
      oss << std::left << std::setw(40) << header_name << std::right << std::setw(15)
          << "Read Rate" << std::setw(15) << "Write Rate" << std::setw(12) << "Read IOPS"
          << std::setw(12) << "Write IOPS";

      if (list_req.show_apps()) {
        oss << std::setw(12) << "Read Press" << std::setw(12) << "Write Press";
      }

      oss << "\n";

      oss << std::string(list_req.show_apps() ? 118 : 94, '-') << "\n";

      for (const auto& [name, stat] : agg_stats) {
        std::string display_name = name;
        if (resolve_ids && list_req.show_users()) {
          display_name = uid_label(std::stoul(name));
        } else if (resolve_ids && list_req.show_groups()) {
          display_name = gid_label(std::stoul(name));
        }

        oss << std::left << std::setw(40) << display_name << std::right << std::setw(15)
            << format_rate(stat.read_rate) << std::setw(15)
            << format_rate(stat.write_rate) << std::fixed << std::setprecision(2)
            << std::setw(12) << stat.read_iops << std::setw(12) << stat.write_iops;

        if (list_req.show_apps()) {
          const auto pressure_it = reserved_app_io_pressure.find(name);

          if (pressure_it != reserved_app_io_pressure.end()) {
            oss << std::setw(12)
                << (pressure_it->second.has_read
                        ? format_io_pressure(pressure_it->second.read)
                        : "-")
                << std::setw(12)
                << (pressure_it->second.has_write
                        ? format_io_pressure(pressure_it->second.write)
                        : "-");
          } else {
            oss << std::setw(12) << "-" << std::setw(12) << "-";
          }
        }

        oss << "\n";
      }

      oss << std::string(list_req.show_apps() ? 118 : 94, '-') << "\n";
      oss << std::left << std::setw(40) << "Total" << std::right << std::setw(15)
          << format_rate(total_sma_metrics.read_rate_bps) << std::setw(15)
          << format_rate(total_sma_metrics.write_rate_bps) << std::fixed
          << std::setprecision(2) << std::setw(12) << total_sma_metrics.read_iops
          << std::setw(12) << total_sma_metrics.write_iops;

      if (list_req.show_apps()) {
        oss << std::setw(12) << "" << std::setw(12) << "";
      }

      oss << "\n";
    }

    if (list_req.system_stats()) {
      const auto [estimator_median, estimator_min, estimator_max] =
          manager->GetEstimatorsUpdateLoopMicroSecStats();
      const auto [fst_limits_median, fst_limits_min, fst_limits_max] =
          manager->GetFstLimitsUpdateLoopMicroSecStats();
      const auto [reservation_controller_median, reservation_controller_min,
                  reservation_controller_max] =
          manager->GetReservationControllerUpdateLoopMicroSecStats();
      const auto reports_processed_mean = manager->GetFstReportsProcessedPerSecondMean();
      const auto system_stats_window_seconds = manager->GetSystemStatsWindowSeconds();
      const auto map_cardinality = manager->GetMapCardinalityStats();

      oss << "\n--- System Statistics (averaged over last " << system_stats_window_seconds
          << " seconds) ---\n";
      oss << std::left << std::setw(30) << "Estimators Update:"
          << "Median = " << format_duration_us(estimator_median) << " | "
          << "Min = " << format_duration_us(estimator_min) << " | "
          << "Max = " << format_duration_us(estimator_max) << "\n";

      oss << std::left << std::setw(30) << "FST Policy Update:"
          << "Median = " << format_duration_us(fst_limits_median) << " | "
          << "Min = " << format_duration_us(fst_limits_min) << " | "
          << "Max = " << format_duration_us(fst_limits_max) << "\n";

      oss << std::left << std::setw(30) << "Reservation Controller:"
          << "Median = " << format_duration_us(reservation_controller_median) << " | "
          << "Min = " << format_duration_us(reservation_controller_min) << " | "
          << "Max = " << format_duration_us(reservation_controller_max) << "\n";

      oss << std::left << std::setw(30) << "FST Reports Per Second:"
          << "Mean = " << std::fixed << std::setprecision(2) << reports_processed_mean
          << "\n";

      oss << std::left << std::setw(30) << "Automatic Detail:"
          << "enabled="
          << (gOFS->mTrafficShapingEngine.GetAutomaticDetailLevelEnabled() ? "true"
                                                                           : "false")
          << " indicator=node_state_streams"
          << " fs_threshold="
          << gOFS->mTrafficShapingEngine.GetAutomaticDetailLevelLowCardinality()
          << " aggregate_threshold="
          << gOFS->mTrafficShapingEngine.GetAutomaticDetailLevelHighCardinality() << "\n";

      oss << std::left << std::setw(30) << "Map Cardinality:"
          << "node_states=" << map_cardinality.node_states
          << " node_state_streams=" << map_cardinality.node_state_streams
          << " global_stats=" << map_cardinality.global_stats
          << " node_stats=" << map_cardinality.node_stats
          << " node_entity_stats=" << map_cardinality.node_entity_stats
          << " disk_stats=" << map_cardinality.disk_stats
          << " detailed_stats=" << map_cardinality.detailed_stats
          << " global_cumulative_stats=" << map_cardinality.global_cumulative_stats
          << " node_cumulative_stats=" << map_cardinality.node_cumulative_stats
          << " disk_cumulative_stats=" << map_cardinality.disk_cumulative_stats
          << " detailed_cumulative_stats=" << map_cardinality.detailed_cumulative_stats
          << " app_policies=" << map_cardinality.app_policies
          << " uid_policies=" << map_cardinality.uid_policies
          << " gid_policies=" << map_cardinality.gid_policies
          << " node_fst_io_delay_configs=" << map_cardinality.node_fst_io_delay_configs
          << " published_fst_io_delay_configs="
          << map_cardinality.published_fst_io_delay_configs << "\n";
    }
  }

  reply.set_retc(0);
  reply.set_std_out(oss.str());
}

void
ShapingPolicyList(
    const eos::console::IoProto_ShapingProto_PolicyAction_ListAction& list_req,
    eos::console::ReplyProto& reply)
{
  const auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();
  if (!manager) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  const bool show_all =
      !list_req.filter_apps() && !list_req.filter_users() && !list_req.filter_groups();
  std::ostringstream oss;

  if (list_req.json_output()) {
    Json::Value json(Json::arrayValue);

    auto print_json_entry = [&](const std::string& type_str, const std::string& id_str,
                                const auto& policy) {
      Json::Value entry;
      entry["id"] = id_str;
      entry["type"] = type_str;
      entry["is_enabled"] = policy.is_enabled;
      entry["limit_read_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(policy.limit_read_bytes_per_sec);
      entry["limit_write_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(policy.limit_write_bytes_per_sec);
      entry["reservation_read_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(policy.reservation_read_bytes_per_sec);
      entry["reservation_write_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(policy.reservation_write_bytes_per_sec);
      entry["controller_limit_read_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(policy.controller_limit_read_bytes_per_sec);
      entry["controller_limit_write_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(policy.controller_limit_write_bytes_per_sec);
      json.append(entry);
    };

    if (show_all || list_req.filter_apps()) {
      for (const auto& [app, policy] : manager->GetAppPolicies()) {
        print_json_entry("app", app, policy);
      }
    }

    if (show_all || list_req.filter_users()) {
      for (const auto& [uid, policy] : manager->GetUidPolicies()) {
        print_json_entry("uid", std::to_string(uid), policy);
      }
    }

    if (show_all || list_req.filter_groups()) {
      for (const auto& [gid, policy] : manager->GetGidPolicies()) {
        print_json_entry("gid", std::to_string(gid), policy);
      }
    }

    oss << CompactJsonString(json);

  } else {
    const bool show_ctrl = list_req.show_controller_limits();

    auto print_header = [&oss, show_ctrl](const std::string& title,
                                          const std::string& id_col) {
      oss << "--- " << title << " ---\n";
      oss << std::left << std::setw(40) << id_col << std::setw(10) << "Status"
          << std::right << std::setw(15) << "Read Limit" << std::setw(15)
          << "Write Limit";

      if (show_ctrl) {
        oss << std::setw(15) << "Ctrl Rd Lim" << std::setw(15) << "Ctrl Wr Lim";
      }

      oss << std::setw(15) << "Read Rsv." << std::setw(15) << "Write Rsv." << "\n";
      oss << std::string(show_ctrl ? 140 : 110, '-') << "\n";
    };

    auto print_row = [&oss,
                      show_ctrl](const std::string& id,
                                 const traffic_shaping::TrafficShapingPolicy& policy) {
      oss << std::left << std::setw(40) << id << std::setw(10)
          << (policy.is_enabled ? "Enabled" : "Disabled") << std::right << std::setw(15)
          << format_rate(policy.limit_read_bytes_per_sec) << std::setw(15)
          << format_rate(policy.limit_write_bytes_per_sec);

      if (show_ctrl) {
        oss << std::setw(15) << format_rate(policy.controller_limit_read_bytes_per_sec)
            << std::setw(15) << format_rate(policy.controller_limit_write_bytes_per_sec);
      }

      oss << std::setw(15) << format_rate(policy.reservation_read_bytes_per_sec)
          << std::setw(15) << format_rate(policy.reservation_write_bytes_per_sec) << "\n";
    };

    bool has_output = false;

    if (show_all || list_req.filter_apps()) {
      if (auto policies = manager->GetAppPolicies(); !policies.empty()) {
        print_header("Application Policies", "Application");
        for (const auto& [app, policy] : policies) {
          print_row(app, policy);
        }
        oss << "\n";
        has_output = true;
      }
    }

    if (show_all || list_req.filter_users()) {
      if (auto policies = manager->GetUidPolicies(); !policies.empty()) {
        print_header("User (UID) Policies", "UID");
        for (const auto& [uid, policy] : policies) {
          print_row(std::to_string(uid), policy);
        }
        oss << "\n";
        has_output = true;
      }
    }

    if (show_all || list_req.filter_groups()) {
      if (auto policies = manager->GetGidPolicies(); !policies.empty()) {
        print_header("Group (GID) Policies", "GID");
        for (const auto& [gid, policy] : policies) {
          print_row(std::to_string(gid), policy);
        }
        oss << "\n";
        has_output = true;
      }
    }

    if (!has_output) {
      oss << "No traffic shaping policies configured.\n";
    }
  }

  reply.set_retc(0);
  reply.set_std_out(oss.str());
}

void
ShapingPressureList(
    const eos::console::IoProto_ShapingProto_PressureAction_ListAction& list_req,
    eos::console::ReplyProto& reply)
{
  const auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();

  if (!manager) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  const auto snapshots = manager->GetReservedAppNodeIoPressure();
  std::ostringstream oss;

  if (list_req.json_output()) {
    Json::Value json(Json::arrayValue);

    for (const auto& snapshot : snapshots) {
      const std::string node_label = NodeLabel(LabelOrUnknown(snapshot.node_id));
      Json::Value entry;
      entry["type"] = "app_node_pressure";
      entry["app"] = snapshot.app;
      entry["node_id"] = node_label;
      entry["node_io_pressure"] = snapshot.node_io_pressure;
      entry["has_node_io_pressure"] = snapshot.has_node_io_pressure;
      entry["read_rate_bps"] = snapshot.read_rate_bps;
      entry["write_rate_bps"] = snapshot.write_rate_bps;
      entry["global_read_rate_bps"] = snapshot.global_read_rate_bps;
      entry["global_write_rate_bps"] = snapshot.global_write_rate_bps;
      entry["reservation_read_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(snapshot.reservation_read_bytes_per_sec);
      entry["reservation_write_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(snapshot.reservation_write_bytes_per_sec);
      entry["read_reservation_deficit_bps"] = snapshot.read_reservation_deficit_bps;
      entry["write_reservation_deficit_bps"] = snapshot.write_reservation_deficit_bps;
      entry["has_read_io_pressure"] = snapshot.has_read_io_pressure;
      entry["has_write_io_pressure"] = snapshot.has_write_io_pressure;
      entry["read_pressure_active"] = snapshot.read_pressure_active;
      entry["write_pressure_active"] = snapshot.write_pressure_active;
      entry["read_reservation_deficit_active"] = snapshot.read_reservation_deficit_active;
      entry["write_reservation_deficit_active"] =
          snapshot.write_reservation_deficit_active;
      entry["read_triggers_competitor_throttling"] =
          snapshot.read_triggers_competitor_throttling;
      entry["write_triggers_competitor_throttling"] =
          snapshot.write_triggers_competitor_throttling;
      entry["node_has_pressured_read_reservation"] =
          snapshot.node_has_pressured_read_reservation;
      entry["node_has_pressured_write_reservation"] =
          snapshot.node_has_pressured_write_reservation;

      if (snapshot.has_read_io_pressure) {
        entry["read_io_pressure"] = snapshot.node_io_pressure;
      }

      if (snapshot.has_write_io_pressure) {
        entry["write_io_pressure"] = snapshot.node_io_pressure;
      }

      json.append(entry);
    }

    oss << CompactJsonString(json);
  } else {
    constexpr size_t kPressureAppWidth = 40;
    constexpr size_t kPressureNodeWidth = 42;
    constexpr size_t kPressureSeparatorWidth = 176;

    oss << "--- Reserved Application IO Pressure by Node ---\n";
    oss << std::left << std::setw(kPressureAppWidth) << "Application"
        << std::setw(kPressureNodeWidth) << "Node" << std::right << std::setw(14)
        << "Read Rate" << std::setw(14) << "Write Rate" << std::setw(12) << "Node Press"
        << std::setw(12) << "Read Press" << std::setw(12) << "Write Press"
        << std::setw(10) << "Rd Trig" << std::setw(10) << "Wr Trig" << std::setw(10)
        << "Node Rd" << std::setw(10) << "Node Wr" << "\n";
    oss << std::string(kPressureSeparatorWidth, '-') << "\n";

    for (const auto& snapshot : snapshots) {
      const std::string node_label = NodeLabel(LabelOrUnknown(snapshot.node_id));
      oss << std::left << std::setw(kPressureAppWidth)
          << format_table_label(snapshot.app, kPressureAppWidth)
          << std::setw(kPressureNodeWidth)
          << format_table_label(node_label, kPressureNodeWidth) << std::right
          << std::setw(14) << format_rate(snapshot.read_rate_bps) << std::setw(14)
          << format_rate(snapshot.write_rate_bps) << std::setw(12)
          << format_optional_io_pressure(snapshot.has_node_io_pressure,
                                         snapshot.node_io_pressure)
          << std::setw(12)
          << format_optional_io_pressure(snapshot.has_read_io_pressure,
                                         snapshot.node_io_pressure)
          << std::setw(12)
          << format_optional_io_pressure(snapshot.has_write_io_pressure,
                                         snapshot.node_io_pressure)
          << std::setw(10) << format_bool(snapshot.read_triggers_competitor_throttling)
          << std::setw(10) << format_bool(snapshot.write_triggers_competitor_throttling)
          << std::setw(10) << format_bool(snapshot.node_has_pressured_read_reservation)
          << std::setw(10) << format_bool(snapshot.node_has_pressured_write_reservation)
          << "\n";
    }

    if (snapshots.empty()) {
      oss << "No application reservations configured.\n";
    }
  }

  reply.set_retc(0);
  reply.set_std_out(oss.str());
}

void
ShapingConfig(const eos::console::IoProto_ShapingProto_ConfigAction& config_req,
              eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;

  switch (config_req.subcmd_case()) {
  case eos::console::IoProto_ShapingProto_ConfigAction::kList: {
    std::ostringstream oss;

    if (config_req.list().json_output()) {
      Json::Value json;
      json["enabled"] = engine.IsEnabled();
      json["estimators_update_period_ms"] = static_cast<Json::Value::UInt64>(
          engine.GetEstimatorsUpdateThreadPeriodMilliseconds());
      json["fst_io_policy_update_period_ms"] = static_cast<Json::Value::UInt64>(
          engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds());
      json["fst_io_stats_reporting_period_ms"] = static_cast<Json::Value::UInt64>(
          engine.GetFstIoStatsReportThreadPeriodMilliseconds());
      json["detail_level"] = engine.GetDetailLevel();
      json["detail_auto_enabled"] = engine.GetAutomaticDetailLevelEnabled();
      json["detail_auto_low_cardinality"] = static_cast<Json::Value::UInt64>(
          engine.GetAutomaticDetailLevelLowCardinality());
      json["detail_auto_high_cardinality"] = static_cast<Json::Value::UInt64>(
          engine.GetAutomaticDetailLevelHighCardinality());
      json["limits_enabled"] = engine.GetLimitsEnabled();
      json["reservations_enabled"] = engine.GetReservationsEnabled();
      json["controller_min_limit_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(engine.GetControllerMinLimit());
      json["active_node_rate_threshold_bytes_per_sec"] =
          static_cast<Json::Value::UInt64>(engine.GetActiveNodeRateThreshold());
      json["io_pressure_threshold"] = engine.GetIoPressureThreshold();
      json["garbage_collection_idle_seconds"] =
          static_cast<Json::Value::UInt64>(engine.GetGarbageCollectionIdleSeconds());
      json["system_stats_time_window_seconds"] =
          static_cast<Json::Value::UInt64>(engine.GetSystemStatsWindowSeconds());
      oss << CompactJsonString(json);
    } else {
      oss << "--- Traffic Shaping Thread Configuration ---\n"
          << std::left << std::setw(45)
          << "Traffic Shaping Enabled:" << (engine.IsEnabled() ? "true" : "false") << "\n"
          << std::left << std::setw(45) << "Estimators Update Period:"
          << engine.GetEstimatorsUpdateThreadPeriodMilliseconds() << " ms\n"
          << std::setw(45) << "FST IO Policy Update Period:"
          << engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds() << " ms\n"
          << std::setw(45) << "FST IO Stats Reporting Period:"
          << engine.GetFstIoStatsReportThreadPeriodMilliseconds() << " ms\n"
          << std::setw(45) << "Stats Detail Level:" << engine.GetDetailLevel() << "\n"
          << std::setw(45) << "Automatic Detail Level:"
          << (engine.GetAutomaticDetailLevelEnabled() ? "true" : "false")
          << " (fs <= " << engine.GetAutomaticDetailLevelLowCardinality()
          << ", aggregate > " << engine.GetAutomaticDetailLevelHighCardinality()
          << " node-state streams)\n"
          << std::setw(45)
          << "Limits Enabled:" << (engine.GetLimitsEnabled() ? "true" : "false") << "\n"
          << std::setw(45) << "Reservations Enabled:"
          << (engine.GetReservationsEnabled() ? "true" : "false") << "\n"
          << std::setw(45)
          << "Controller Minimum Limit:" << format_rate(engine.GetControllerMinLimit())
          << "\n"
          << std::setw(45) << "Active Node Rate Threshold:"
          << format_rate(engine.GetActiveNodeRateThreshold()) << "\n"
          << std::setw(45) << "IO Pressure Threshold:"
          << format_io_pressure(engine.GetIoPressureThreshold()) << "\n"
          << std::setw(45)
          << "Garbage Collection Idle Time:" << engine.GetGarbageCollectionIdleSeconds()
          << " s\n"
          << std::setw(45)
          << "System Stats Time Window:" << engine.GetSystemStatsWindowSeconds()
          << " s\n";
    }

    reply.set_retc(0);
    reply.set_std_out(oss.str());
    break;
  }

  case eos::console::IoProto_ShapingProto_ConfigAction::kSet: {
    const auto& set_req = config_req.set();
    std::ostringstream oss;

    std::optional<uint64_t> requested_detail_auto_low;
    std::optional<uint64_t> requested_detail_auto_high;
    std::optional<bool> requested_detail_auto_enabled;
    const bool has_detail_auto_cardinality =
        set_req.has_detail_level() &&
        ParseAutomaticDetailCardinalityConfig(
            set_req.detail_level(), requested_detail_auto_low, requested_detail_auto_high,
            requested_detail_auto_enabled);

    if (set_req.has_detail_level() && !has_detail_auto_cardinality &&
        set_req.detail_level() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE &&
        set_req.detail_level() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM &&
        set_req.detail_level() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AUTO &&
        set_req.detail_level() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_MANUAL) {
      reply.set_retc(EINVAL);
      reply.set_std_err("error: detail level must be 'aggregate', 'fs', 'auto', "
                        "'manual', or automatic cardinality thresholds.\n");
      break;
    }

    if (has_detail_auto_cardinality) {
      const uint64_t low = requested_detail_auto_low.value_or(
          engine.GetAutomaticDetailLevelLowCardinality());
      const uint64_t high = requested_detail_auto_high.value_or(
          engine.GetAutomaticDetailLevelHighCardinality());

      if (low > high) {
        reply.set_retc(EINVAL);
        reply.set_std_err("error: automatic detail low cardinality must be <= high "
                          "cardinality.\n");
        break;
      }
    }

    if (set_req.has_io_pressure_threshold() && (set_req.io_pressure_threshold() < 0.0 ||
                                                set_req.io_pressure_threshold() > 1.0)) {
      reply.set_retc(EINVAL);
      reply.set_std_err("error: IO pressure threshold must be between 0 and 1.\n");
      break;
    }

    if (set_req.has_update_estimators_thread_period_ms()) {
      engine.SetEstimatorsUpdateThreadPeriodMilliseconds(
          set_req.update_estimators_thread_period_ms());
      oss << "success: Set estimators update period to "
          << engine.GetEstimatorsUpdateThreadPeriodMilliseconds() << " ms\n";
    }

    if (set_req.has_fst_io_policy_update_thread_period_ms()) {
      engine.SetFstIoPolicyUpdateThreadPeriodMilliseconds(
          set_req.fst_io_policy_update_thread_period_ms());
      oss << "success: Set FST IO policy update period to "
          << engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds() << " ms\n";
    }

    if (set_req.has_fst_io_stats_reporting_thread_period_ms()) {
      engine.SetFstIoStatsReportThreadPeriodMilliseconds(
          set_req.fst_io_stats_reporting_thread_period_ms());
      oss << "success: Set FST IO stats reporting period to "
          << engine.GetFstIoStatsReportThreadPeriodMilliseconds() << " ms\n";
    }

    if (set_req.has_system_stats_time_window_seconds()) {
      engine.SetSystemStatsWindowSeconds(set_req.system_stats_time_window_seconds());
      oss << "success: Set system stats time window to "
          << engine.GetSystemStatsWindowSeconds() << " s\n";
    }

    if (set_req.has_detail_level()) {
      const std::string detail_level = set_req.detail_level();

      if (has_detail_auto_cardinality) {
        const uint64_t low = requested_detail_auto_low.value_or(
            engine.GetAutomaticDetailLevelLowCardinality());
        const uint64_t high = requested_detail_auto_high.value_or(
            engine.GetAutomaticDetailLevelHighCardinality());
        if (requested_detail_auto_enabled.has_value()) {
          engine.SetAutomaticDetailLevelEnabled(*requested_detail_auto_enabled);
        }
        engine.SetAutomaticDetailLevelCardinality(low, high);
        if (requested_detail_auto_enabled.has_value()) {
          oss << "success: Set automatic stats detail level to "
              << (engine.GetAutomaticDetailLevelEnabled() ? "enabled" : "disabled")
              << "\n";
        }
        oss << "success: Set automatic stats detail thresholds to fs <= "
            << engine.GetAutomaticDetailLevelLowCardinality() << ", aggregate > "
            << engine.GetAutomaticDetailLevelHighCardinality() << " node-state streams\n";
      } else if (detail_level == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AUTO) {
        engine.SetAutomaticDetailLevelEnabled(true);
        oss << "success: Set automatic stats detail level to enabled\n";
      } else if (detail_level == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_MANUAL) {
        engine.SetAutomaticDetailLevelEnabled(false);
        oss << "success: Set automatic stats detail level to disabled\n";
      } else {
        engine.SetAutomaticDetailLevelEnabled(false);
        engine.SetDetailLevel(detail_level);
        oss << "success: Set stats detail level to " << engine.GetDetailLevel()
            << " (automatic detail disabled)\n";
      }
    }

    if (set_req.has_limits_enabled()) {
      engine.SetLimitsEnabled(set_req.limits_enabled());
      oss << "success: Set limits enabled to "
          << (engine.GetLimitsEnabled() ? "true" : "false") << "\n";
    }

    if (set_req.has_reservations_enabled()) {
      engine.SetReservationsEnabled(set_req.reservations_enabled());
      oss << "success: Set reservations enabled to "
          << (engine.GetReservationsEnabled() ? "true" : "false") << "\n";
    }

    if (set_req.has_controller_min_limit_bytes_per_sec()) {
      engine.SetControllerMinLimit(set_req.controller_min_limit_bytes_per_sec());
      oss << "success: Set controller minimum limit to "
          << format_rate(engine.GetControllerMinLimit()) << "\n";
    }

    if (set_req.has_active_node_rate_threshold_bytes_per_sec()) {
      engine.SetActiveNodeRateThreshold(
          set_req.active_node_rate_threshold_bytes_per_sec());
      oss << "success: Set active node rate threshold to "
          << format_rate(engine.GetActiveNodeRateThreshold()) << "\n";
    }

    if (set_req.has_io_pressure_threshold()) {
      engine.SetIoPressureThreshold(set_req.io_pressure_threshold());
      oss << "success: Set IO pressure threshold to "
          << format_io_pressure(engine.GetIoPressureThreshold()) << "\n";
    }

    if (set_req.has_garbage_collection_idle_seconds()) {
      engine.SetGarbageCollectionIdleSeconds(set_req.garbage_collection_idle_seconds());
      oss << "success: Set garbage collection idle time to "
          << engine.GetGarbageCollectionIdleSeconds() << " s\n";
    }

    if (oss.str().empty()) {
      reply.set_retc(EINVAL);
      reply.set_std_err("error: No configuration parameters provided to set.\n");
    } else {
      reply.set_retc(0);
      reply.set_std_out(oss.str());
    }
    break;
  }

  case eos::console::IoProto_ShapingProto_ConfigAction::SUBCMD_NOT_SET:
  default:
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Shaping config: invalid or missing subcommand (ls/set).\n");
    break;
  }
}

void
IoCmd::ShapingSubcommand(const eos::console::IoProto_ShapingProto& shaping,
                         eos::console::ReplyProto& reply)
{
  switch (shaping.subcmd_case()) {

  case eos::console::IoProto_ShapingProto::kList: {
    ShapingList(shaping.list(), reply);
    break;
  }

  case eos::console::IoProto_ShapingProto::kEnable: {
    ShapingTrafficEnable(reply);
    break;
  }

  case eos::console::IoProto_ShapingProto::kDisable: {
    ShapingTrafficDisable(reply);
    break;
  }

  case eos::console::IoProto_ShapingProto::kPolicy: {

    switch (const auto& policy = shaping.policy(); policy.subcmd_case()) {
    case eos::console::IoProto_ShapingProto_PolicyAction::kList:
      ShapingPolicyList(policy.list(), reply);
      break;

    case eos::console::IoProto_ShapingProto_PolicyAction::kSet:
      ShapingPolicySet(policy.set(), reply);
      break;

    case eos::console::IoProto_ShapingProto_PolicyAction::kRemove:
      ShapingPolicyDelete(policy.remove(), reply); // Note the trailing underscore!
      break;

    default:
      reply.set_retc(EINVAL);
      reply.set_std_err(
          "error: Shaping policy: invalid or missing subcommand (list/set/delete).\n");
      break;
    }

    break;
  }

  case eos::console::IoProto_ShapingProto::kConfig: {
    ShapingConfig(shaping.config(), reply);
    break;
  }
  case eos::console::IoProto_ShapingProto::kPressure: {
    switch (const auto& pressure = shaping.pressure(); pressure.subcmd_case()) {
    case eos::console::IoProto_ShapingProto_PressureAction::kList:
      ShapingPressureList(pressure.list(), reply);
      break;

    default:
      reply.set_retc(EINVAL);
      reply.set_std_err(
          "error: Shaping pressure: invalid or missing subcommand (list).\n");
      break;
    }

    break;
  }
  case eos::console::IoProto_ShapingProto::SUBCMD_NOT_SET:
  default:
    reply.set_retc(EINVAL);
    reply.set_std_err(
        "error: Shaping command: sub-command (traffic/policy) not specified.\n");
    break;
  }
}
} // namespace eos::mgm
