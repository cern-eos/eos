#include "IoCmd.hh"
#include "fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"

#include <common/CLI11.hpp>

#include "proto/ConsoleReply.pb.h"
#include "proto/Io.pb.h"

std::string
format_rate(double bytes_per_sec)
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
MonitorPolicySet(const eos::console::IoProto_MonitorProto_PolicyAction_SetAction& set_req,
                 eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<eos::mgm::TrafficShaping> brain = engine.GetBrain();
  if (!brain) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  eos::mgm::TrafficShapingPolicy policy; // Starts completely empty
  std::string target_desc;               // For nice output logging

  // 1. READ (Fetch existing policy if it exists)
  if (set_req.has_app()) {
    target_desc = "App '" + set_req.app() + "'";
    policy =
        brain->GetAppPolicy(set_req.app()).value_or(eos::mgm::TrafficShapingPolicy{});
  } else if (set_req.has_uid()) {
    target_desc = "UID " + std::to_string(set_req.uid());
    policy =
        brain->GetUidPolicy(set_req.uid()).value_or(eos::mgm::TrafficShapingPolicy{});
  } else if (set_req.has_gid()) {
    target_desc = "GID " + std::to_string(set_req.gid());
    policy =
        brain->GetGidPolicy(set_req.gid()).value_or(eos::mgm::TrafficShapingPolicy{});
  } else {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: You must specify a target (--app, --uid, or --gid).\n");
    return;
  }

  // 2. MODIFY (Apply only the fields the user explicitly provided)
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
    brain->SetAppPolicy(set_req.app(), policy);
  } else if (set_req.has_uid()) {
    brain->SetUidPolicy(set_req.uid(), policy);
  } else if (set_req.has_gid()) {
    brain->SetGidPolicy(set_req.gid(), policy);
  }

  reply.set_retc(0);
  reply.set_std_out("success: Updated shaping policy for " + target_desc + "\n");
}

void
MonitorPolicyDelete(
    const eos::console::IoProto_MonitorProto_PolicyAction_DeleteAction& del_req,
    eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<eos::mgm::TrafficShaping> brain = engine.GetBrain();
  if (!brain) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  std::string target_desc;

  if (del_req.has_app()) {
    brain->RemoveAppPolicy(del_req.app());
    target_desc = "App '" + del_req.app() + "'";
  } else if (del_req.has_uid()) {
    brain->RemoveUidPolicy(del_req.uid());
    target_desc = "UID " + std::to_string(del_req.uid());
  } else if (del_req.has_gid()) {
    brain->RemoveGidPolicy(del_req.gid());
    target_desc = "GID " + std::to_string(del_req.gid());
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
MonitorTraffic(const eos::console::IoProto_MonitorProto_TrafficAction& traffic_req,
               eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  const std::shared_ptr<eos::mgm::TrafficShaping> brain = engine.GetBrain();
  if (!brain) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  auto global_stats = brain->GetGlobalStats();

  // 1. Aggregate the stats based on the requested grouping
  struct AggregatedStats {
    double read_rate = 0.0;
    double write_rate = 0.0;
    double read_iops = 0.0;
    double write_iops = 0.0;
  };

  // Use std::map to automatically sort the output by the grouping key
  std::map<std::string, AggregatedStats> agg_stats;

  for (const auto& [key, snapshot] : global_stats) {
    std::string group_key;

    if (traffic_req.show_apps()) {
      group_key = key.app.empty() ? "<unknown>" : key.app;
    } else if (traffic_req.show_users()) {
      group_key = std::to_string(key.uid);
    } else if (traffic_req.show_groups()) {
      group_key = std::to_string(key.gid);
    } else {
      // Fallback (though CLI11 ensures one is picked)
      group_key = "app:" + key.app;
    }

    auto& entry = agg_stats[group_key];
    entry.read_rate += snapshot.read_rate_sma_5s;
    entry.write_rate += snapshot.write_rate_sma_5s;
    entry.read_iops += snapshot.read_iops_sma_5s;
    entry.write_iops += snapshot.write_iops_sma_5s;
  }

  std::ostringstream oss;

  std::string header_name = "ID";
  if (traffic_req.show_apps()) {
    header_name = "Application";
  } else if (traffic_req.show_users()) {
    header_name = "UID";
  } else if (traffic_req.show_groups()) {
    header_name = "GID";
  }

  // Table Header
  oss << std::left << std::setw(20) << header_name << std::right << std::setw(15)
      << "Read Rate" << std::setw(15) << "Write Rate" << std::setw(12) << "Read IOPS"
      << std::setw(12) << "Write IOPS" << "\n";

  oss << std::string(74, '-') << "\n";

  // Table Body
  for (const auto& [name, stat] : agg_stats) {
    oss << std::left << std::setw(20) << name << std::right << std::setw(15)
        << format_rate(stat.read_rate) << std::setw(15) << format_rate(stat.write_rate)
        << std::fixed << std::setprecision(2) << std::setw(12) << stat.read_iops
        << std::setw(12) << stat.write_iops << "\n";
  }

  reply.set_retc(0);
  reply.set_std_out(oss.str());
}

void
MonitorPolicyList(
    const eos::console::IoProto_MonitorProto_PolicyAction_ListAction& list_req,
    eos::console::ReplyProto& reply)
{
  auto& engine = gOFS->mTrafficShapingEngine;
  // Note: Match the exact type returned by GetBrain() in your environment
  const std::shared_ptr<eos::mgm::TrafficShaping> brain = engine.GetBrain();
  if (!brain) {
    reply.set_retc(EINVAL);
    reply.set_std_err("error: Traffic shaping engine is not initialized.\n");
    return;
  }

  bool show_all =
      !list_req.filter_apps() && !list_req.filter_users() && !list_req.filter_groups();
  std::ostringstream oss;

  // Reusable table header formatter
  auto print_header = [&oss](const std::string& title, const std::string& id_col) {
    oss << "--- " << title << " ---\n";
    oss << std::left << std::setw(20) << id_col << std::setw(10) << "Status" << std::right
        << std::setw(15) << "Read Limit" << std::setw(15) << "Write Limit"
        << std::setw(15) << "Read Rsv." << std::setw(15) << "Write Rsv." << "\n";
    oss << std::string(90, '-') << "\n";
  };

  // Reusable table row formatter
  auto print_row = [&oss](const std::string& id,
                          const eos::mgm::TrafficShapingPolicy& policy) {
    oss << std::left << std::setw(20) << id << std::setw(10)
        << (policy.is_enabled ? "Enabled" : "Disabled") << std::right << std::setw(15)
        << format_rate(policy.limit_read_bytes_per_sec) << std::setw(15)
        << format_rate(policy.limit_write_bytes_per_sec) << std::setw(15)
        << format_rate(policy.reservation_read_bytes_per_sec) << std::setw(15)
        << format_rate(policy.reservation_write_bytes_per_sec) << "\n";
  };

  if (show_all || list_req.filter_apps()) {
    if (auto policies = brain->GetAppPolicies(); !policies.empty()) {
      print_header("Application Policies", "Application");
      for (const auto& [app, policy] : policies) {
        print_row(app, policy);
      }
      oss << "\n";
    }
  }

  // 2. Users (UID)
  if (show_all || list_req.filter_users()) {
    if (auto policies = brain->GetUidPolicies(); !policies.empty()) {
      print_header("User (UID) Policies", "UID");
      for (const auto& [uid, policy] : policies) {
        print_row(std::to_string(uid), policy);
      }
      oss << "\n";
    }
  }

  // 3. Groups (GID)
  if (show_all || list_req.filter_groups()) {
    if (auto policies = brain->GetGidPolicies(); !policies.empty()) {
      print_header("Group (GID) Policies", "GID");
      for (const auto& [gid, policy] : policies) {
        print_row(std::to_string(gid), policy);
      }
      oss << "\n";
    }
  }

  if (oss.str().empty()) {
    oss << "No traffic shaping policies configured.\n";
  }

  reply.set_retc(0);
  reply.set_std_out(oss.str());
}

void
IoCmd::MonitorSubcommand(const eos::console::IoProto_MonitorProto& monitor,
                         eos::console::ReplyProto& reply)
{
  eos::common::RWMutexWriteLock wr_lock(eos::mgm::FsView::gFsView.ViewMutex);

  switch (monitor.subcmd_case()) {

  case eos::console::IoProto_MonitorProto::kTraffic: {
    MonitorTraffic(monitor.traffic(), reply);
    break;
  }

  case eos::console::IoProto_MonitorProto::kPolicy: {

    switch (const auto& policy = monitor.policy(); policy.subcmd_case()) {
    case eos::console::IoProto_MonitorProto_PolicyAction::kList:
      MonitorPolicyList(policy.list(), reply);
      break;

    case eos::console::IoProto_MonitorProto_PolicyAction::kSet:
      MonitorPolicySet(policy.set(), reply);
      break;

    case eos::console::IoProto_MonitorProto_PolicyAction::kDelete:
      MonitorPolicyDelete(policy.delete_(), reply); // Note the trailing underscore!
      break;

    default:
      reply.set_retc(EINVAL);
      reply.set_std_err(
          "error: Monitor policy: invalid or missing subcommand (list/set/delete).\n");
      break;
    }
    break;
  }

  case eos::console::IoProto_MonitorProto::SUBCMD_NOT_SET:
  default:
    reply.set_retc(EINVAL);
    reply.set_std_err(
        "error: Monitor command: sub-command (traffic/policy) not specified.\n");
    break;
  }
}

} // namespace eos::mgm
