#include "IoCmd.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"

#include <common/CLI11.hpp>

#include "proto/ConsoleReply.pb.h"

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
};

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

  if (set_req.has_limit_read_bytes_per_sec()) {
    policy.limit_read_bytes_per_sec = set_req.limit_read_bytes_per_sec();
  }

  if (set_req.has_limit_write_bytes_per_sec()) {
    policy.limit_write_bytes_per_sec = set_req.limit_write_bytes_per_sec();
  }

  if (set_req.has_reservation_read_bytes_per_sec()) {
    policy.reservation_read_bytes_per_sec = set_req.reservation_read_bytes_per_sec();
  }

  if (set_req.has_reservation_write_bytes_per_sec()) {
    policy.reservation_write_bytes_per_sec = set_req.reservation_write_bytes_per_sec();
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

  std::string status_str = policy.is_enabled ? "Enabled" : "Disabled";

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
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  auto global_stats = manager->GetGlobalStats();
  auto node_stats = manager->GetNodeStats();
  auto total_stats = manager->GetTotalStats();

  struct AggregatedStats {
    double read_rate = 0.0;
    double write_rate = 0.0;
    double read_iops = 0.0;
    double write_iops = 0.0;
  };

  std::map<std::string, AggregatedStats> agg_stats;

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

  if (list_req.show_nodes()) {
    for (const auto& [node_id, snapshot] : node_stats) {
      std::string group_key = node_id.empty() ? "<unknown>" : node_id;
      auto& entry = agg_stats[group_key];

      const auto& sma_metrics = snapshot.sma[sma_idx];
      entry.read_rate += sma_metrics.read_rate_bps;
      entry.write_rate += sma_metrics.write_rate_bps;
      entry.read_iops += sma_metrics.read_iops;
      entry.write_iops += sma_metrics.write_iops;
    }
  } else {
    for (const auto& [key, snapshot] : global_stats) {
      std::string group_key;

      if (list_req.show_apps()) {
        group_key = key.app.empty() ? "<unknown>" : key.app;
      } else if (list_req.show_users()) {
        group_key = std::to_string(key.uid);
      } else if (list_req.show_groups()) {
        group_key = std::to_string(key.gid);
      } else {
        group_key = "app:" + key.app; // Default fallback
      }

      auto& entry = agg_stats[group_key];

      const auto& sma_metrics = snapshot.sma[sma_idx];
      entry.read_rate += sma_metrics.read_rate_bps;
      entry.write_rate += sma_metrics.write_rate_bps;
      entry.read_iops += sma_metrics.read_iops;
      entry.write_iops += sma_metrics.write_iops;
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

    oss << "[\n";
    bool first = true;
    for (const auto& [name, stat] : agg_stats) {
      if (!first) {
        oss << ",\n";
      }
      first = false;
      oss << "  {\n"
          << "    \"id\": \"" << name << "\",\n"
          << "    \"type\": \"" << type_str << "\",\n"
          << "    \"window_sec\": " << window_sec << ",\n"
          << "    \"read_rate_bps\": " << std::fixed << std::setprecision(2)
          << stat.read_rate << ",\n"
          << "    \"write_rate_bps\": " << stat.write_rate << ",\n"
          << "    \"read_iops\": " << stat.read_iops << ",\n"
          << "    \"write_iops\": " << stat.write_iops << "\n"
          << "  }";
    }

    if (list_req.system_stats()) {
      const auto [estimator_mean, estimator_min, estimator_max] =
          manager->GetEstimatorsUpdateLoopMicroSecStats();
      const auto [fst_limits_mean, fst_limits_min, fst_limits_max] =
          manager->GetFstLimitsUpdateLoopMicroSecStats();
      const auto system_stats_window_seconds = manager->GetSystemStatsWindowSeconds();

      if (!first) {
        oss << ",\n";
      }
      oss << "  {\n"
          << "    \"id\": \"engine_meta\",\n"
          << "    \"type\": \"system\",\n"
          << "    \"estimators_loop_mean_us\": " << std::fixed << std::setprecision(2)
          << estimator_mean << ",\n"
          << "    \"estimators_loop_min_us\": " << estimator_min << ",\n"
          << "    \"estimators_loop_max_us\": " << estimator_max << ",\n"
          << "    \"fst_limits_loop_mean_us\": " << std::fixed << std::setprecision(2)
          << fst_limits_mean << ",\n"
          << "    \"fst_limits_loop_min_us\": " << fst_limits_min << ",\n"
          << "    \"fst_limits_loop_max_us\": " << fst_limits_max << ",\n"
          << "    \"system_stats_window_seconds\": " << system_stats_window_seconds
          << "\n"
          << "  }";
    }

    oss << "\n]\n";
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

    oss << std::left << std::setw(40) << header_name << std::right << std::setw(15)
        << "Read Rate" << std::setw(15) << "Write Rate" << std::setw(12) << "Read IOPS"
        << std::setw(12) << "Write IOPS" << "\n";

    oss << std::string(94, '-') << "\n";

    for (const auto& [name, stat] : agg_stats) {
      oss << std::left << std::setw(40) << name << std::right << std::setw(15)
          << format_rate(stat.read_rate) << std::setw(15) << format_rate(stat.write_rate)
          << std::fixed << std::setprecision(2) << std::setw(12) << stat.read_iops
          << std::setw(12) << stat.write_iops << "\n";
    }

    oss << std::string(94, '-') << "\n";
    oss << std::left << std::setw(40) << "Total" << std::right << std::setw(15)
        << format_rate(total_sma_metrics.read_rate_bps) << std::setw(15)
        << format_rate(total_sma_metrics.write_rate_bps) << std::fixed
        << std::setprecision(2) << std::setw(12) << total_sma_metrics.read_iops
        << std::setw(12) << total_sma_metrics.write_iops << "\n";

    if (list_req.system_stats()) {
      const auto [estimator_mean, estimator_min, estimator_max] =
          manager->GetEstimatorsUpdateLoopMicroSecStats();
      const auto [fst_limits_mean, fst_limits_min, fst_limits_max] =
          manager->GetFstLimitsUpdateLoopMicroSecStats();
      const auto system_stats_window_seconds = manager->GetSystemStatsWindowSeconds();

      oss << "\n--- System Statistics (averaged over last " << system_stats_window_seconds
          << " seconds) ---\n";
      oss << std::left << std::setw(25) << "Estimators Update:"
          << "Mean = " << std::fixed << std::setprecision(2) << estimator_mean << " us | "
          << "Min = " << estimator_min << " us | "
          << "Max = " << estimator_max << " us\n";

      oss << std::left << std::setw(25) << "FST Policy Update:"
          << "Mean = " << std::fixed << std::setprecision(2) << fst_limits_mean
          << " us | "
          << "Min = " << fst_limits_min << " us | "
          << "Max = " << fst_limits_max << " us\n";
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
  auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<traffic_shaping::TrafficShapingManager> manager =
      engine.GetManager();
  if (!manager) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  bool show_all =
      !list_req.filter_apps() && !list_req.filter_users() && !list_req.filter_groups();
  std::ostringstream oss;

  if (list_req.json_output()) {
    oss << "[\n";
    bool first = true;

    auto print_json_entry = [&](const std::string& type_str, const std::string& id_str,
                                const auto& policy) {
      if (!first) {
        oss << ",\n";
      }
      first = false;
      oss << "  {\n"
          << "    \"id\": \"" << id_str << "\",\n"
          << "    \"type\": \"" << type_str << "\",\n"
          << "    \"is_enabled\": " << (policy.is_enabled ? "true" : "false") << ",\n"
          << "    \"limit_read_bytes_per_sec\": " << policy.limit_read_bytes_per_sec
          << ",\n"
          << "    \"limit_write_bytes_per_sec\": " << policy.limit_write_bytes_per_sec
          << ",\n"
          << "    \"reservation_read_bytes_per_sec\": "
          << policy.reservation_read_bytes_per_sec << ",\n"
          << "    \"reservation_write_bytes_per_sec\": "
          << policy.reservation_write_bytes_per_sec << "\n"
          << "  }";
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

    if (!first) {
      oss << "\n";
    }
    oss << "]\n";

  } else {
    auto print_header = [&oss](const std::string& title, const std::string& id_col) {
      oss << "--- " << title << " ---\n";
      oss << std::left << std::setw(40) << id_col << std::setw(10) << "Status"
          << std::right << std::setw(15) << "Read Limit" << std::setw(15) << "Write Limit"
          << std::setw(15) << "Read Rsv." << std::setw(15) << "Write Rsv." << "\n";
      oss << std::string(110, '-') << "\n";
    };

    auto print_row = [&oss](const std::string& id,
                            const traffic_shaping::TrafficShapingPolicy& policy) {
      oss << std::left << std::setw(40) << id << std::setw(10)
          << (policy.is_enabled ? "Enabled" : "Disabled") << std::right << std::setw(15)
          << format_rate(policy.limit_read_bytes_per_sec) << std::setw(15)
          << format_rate(policy.limit_write_bytes_per_sec) << std::setw(15)
          << format_rate(policy.reservation_read_bytes_per_sec) << std::setw(15)
          << format_rate(policy.reservation_write_bytes_per_sec) << "\n";
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
    oss << "--- Traffic Shaping Thread Configuration ---\n"
        << std::left << std::setw(45) << "Estimators Update Period:"
        << engine.GetEstimatorsUpdateThreadPeriodMilliseconds() << " ms\n"
        << std::setw(45) << "FST IO Policy Update Period:"
        << engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds() << " ms\n"
        << std::setw(45) << "FST IO Stats Reporting Period:"
        << engine.GetFstIoStatsReportThreadPeriodMilliseconds() << " ms\n"
        << std::setw(45)
        << "System Stats Time Window:" << engine.GetSystemStatsWindowSeconds() << " s\n";

    reply.set_retc(0);
    reply.set_std_out(oss.str());
    break;
  }

  case eos::console::IoProto_ShapingProto_ConfigAction::kSet: {
    const auto& set_req = config_req.set();
    std::ostringstream oss;

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
  eos::common::RWMutexWriteLock wr_lock(FsView::gFsView.ViewMutex);

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
