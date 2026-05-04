#include "IoCmd.hh"
#include "common/Constants.hh"
#include "common/Mapping.hh"
#include "common/shaping/IoStatsKey.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/shaping/TrafficShaping.hh"

#include "proto/ConsoleReply.pb.h"

#include <cstdint>
#include <json/json.h>
#include <map>
#include <tuple>

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

namespace {

using eos::common::traffic_shaping::kUnknownId;

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

std::string
FormatResolvedId(const uint32_t id, const std::string& name, const int errc)
{
  const std::string id_string = std::to_string(id);

  if (errc || name.empty() || name == id_string) {
    return id_string;
  }

  return id_string + "(" + name + ")";
}

std::string
UidLabel(const uint32_t uid)
{
  int errc = 0;
  const auto name = eos::common::Mapping::UidToUserName(static_cast<uid_t>(uid), errc);
  return FormatResolvedId(uid, name, errc);
}

std::string
GidLabel(const uint32_t gid)
{
  int errc = 0;
  const auto name = eos::common::Mapping::GidToGroupName(static_cast<gid_t>(gid), errc);
  return FormatResolvedId(gid, name, errc);
}

} // namespace

namespace eos::mgm {

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
        DetailedKey group_key{LabelOrUnknown(detailed_key.node_id),
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
      DiskKey group_key{LabelOrUnknown(disk_key.node_id), disk_key.fsid};
      accumulate(fs_agg_stats[group_key], snapshot);
    }
  } else if (list_req.show_nodes()) {
    for (const auto& [node_id, snapshot] : manager->GetNodeStats()) {
      accumulate(agg_stats[LabelOrUnknown(node_id)], snapshot);
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
        json.append(entry);
      }
    }

    if (list_req.system_stats()) {
      const auto [estimator_median, estimator_min, estimator_max] =
          manager->GetEstimatorsUpdateLoopMicroSecStats();
      const auto [fst_limits_median, fst_limits_min, fst_limits_max] =
          manager->GetFstLimitsUpdateLoopMicroSecStats();
      const auto reports_processed_mean = manager->GetFstReportsProcessedPerSecondMean();
      const auto system_stats_window_seconds = manager->GetSystemStatsWindowSeconds();

      Json::Value entry;
      entry["id"] = "engine_meta";
      entry["type"] = "system";
      entry["estimators_loop_median_us"] = estimator_median;
      entry["estimators_loop_min_us"] = estimator_min;
      entry["estimators_loop_max_us"] = estimator_max;
      entry["fst_limits_loop_median_us"] = fst_limits_median;
      entry["fst_limits_loop_min_us"] = fst_limits_min;
      entry["fst_limits_loop_max_us"] = fst_limits_max;
      entry["reports_processed_per_sec_mean"] = reports_processed_mean;
      entry["system_stats_window_seconds"] =
          static_cast<Json::Value::UInt64>(system_stats_window_seconds);
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
          << std::setw(12) << "Write IOPS" << "\n";

      oss << std::string(94, '-') << "\n";

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
            << std::setw(12) << stat.read_iops << std::setw(12) << stat.write_iops
            << "\n";
      }

      oss << std::string(94, '-') << "\n";
      oss << std::left << std::setw(40) << "Total" << std::right << std::setw(15)
          << format_rate(total_sma_metrics.read_rate_bps) << std::setw(15)
          << format_rate(total_sma_metrics.write_rate_bps) << std::fixed
          << std::setprecision(2) << std::setw(12) << total_sma_metrics.read_iops
          << std::setw(12) << total_sma_metrics.write_iops << "\n";
    }

    if (list_req.system_stats()) {
      const auto [estimator_median, estimator_min, estimator_max] =
          manager->GetEstimatorsUpdateLoopMicroSecStats();
      const auto [fst_limits_median, fst_limits_min, fst_limits_max] =
          manager->GetFstLimitsUpdateLoopMicroSecStats();
      const auto reports_processed_mean = manager->GetFstReportsProcessedPerSecondMean();
      const auto system_stats_window_seconds = manager->GetSystemStatsWindowSeconds();

      oss << "\n--- System Statistics (averaged over last " << system_stats_window_seconds
          << " seconds) ---\n";
      oss << std::left << std::setw(30) << "Estimators Update:"
          << "Median = " << std::fixed << std::setprecision(2) << estimator_median
          << " us | "
          << "Min = " << estimator_min << " us | "
          << "Max = " << estimator_max << " us\n";

      oss << std::left << std::setw(30) << "FST Policy Update:"
          << "Median = " << std::fixed << std::setprecision(2) << fst_limits_median
          << " us | "
          << "Min = " << fst_limits_min << " us | "
          << "Max = " << fst_limits_max << " us\n";

      oss << std::left << std::setw(30) << "FST Reports Per Second:"
          << "Mean = " << std::fixed << std::setprecision(2) << reports_processed_mean
          << "\n";
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

    if (set_req.has_detail_level() &&
        set_req.detail_level() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_AGGREGATE &&
        set_req.detail_level() != eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
      reply.set_retc(EINVAL);
      reply.set_std_err("error: detail level must be 'aggregate' or 'fs'.\n");
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
      engine.SetDetailLevel(detail_level);
      oss << "success: Set stats detail level to " << engine.GetDetailLevel() << "\n";
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
  case eos::console::IoProto_ShapingProto::SUBCMD_NOT_SET:
  default:
    reply.set_retc(EINVAL);
    reply.set_std_err(
        "error: Shaping command: sub-command (traffic/policy) not specified.\n");
    break;
  }
}
} // namespace eos::mgm
