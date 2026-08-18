#include "mgm/monitoring/TrafficShapingCollector.hh"

#include "common/Constants.hh"
#include "common/StringUtils.hh"
#include "common/shaping/Identity.hh"
#include "common/shaping/IoStatsKey.hh"
#include "mgm/monitoring/PrometheusFormatter.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace eos::mgm::monitoring {
namespace {

using eos::common::traffic_shaping::GidLabel;
using eos::common::traffic_shaping::NodeLabel;
using eos::common::traffic_shaping::UidLabel;
using eos::mgm::traffic_shaping::AppIoPressureSnapshot;
using eos::mgm::traffic_shaping::DurationHistogramSnapshot;
using eos::mgm::traffic_shaping::LoopTimingSnapshot;
using eos::mgm::traffic_shaping::RateSnapshot;
using eos::mgm::traffic_shaping::TrafficShapingEngine;
using eos::mgm::traffic_shaping::TrafficShapingManager;
using eos::mgm::traffic_shaping::TrafficShapingPolicy;

constexpr std::size_t kMaxAllTagsMetricEntries = 50000;

struct EntityTotals {
  uint64_t read_bytes = 0;
  uint64_t write_bytes = 0;
  uint64_t read_ops = 0;
  uint64_t write_ops = 0;

  void
  Add(const RateSnapshot& snapshot)
  {
    read_bytes += snapshot.bytes_read_total;
    write_bytes += snapshot.bytes_written_total;
    read_ops += snapshot.read_ops_total;
    write_ops += snapshot.write_ops_total;
  }

  void
  Add(const EntityTotals& totals)
  {
    read_bytes += totals.read_bytes;
    write_bytes += totals.write_bytes;
    read_ops += totals.read_ops;
    write_ops += totals.write_ops;
  }
};

struct StandardKey {
  std::string type;
  std::string id;

  bool
  operator<(const StandardKey& other) const
  {
    return std::tie(type, id) < std::tie(other.type, other.id);
  }
};

struct AllKey {
  std::string node_id;
  uint64_t fsid = 0;
  std::string app;
  uint32_t uid = 0;
  uint32_t gid = 0;

  bool
  operator<(const AllKey& other) const
  {
    return std::tie(node_id, fsid, app, uid, gid) <
           std::tie(other.node_id, other.fsid, other.app, other.uid, other.gid);
  }
};

const std::string&
LabelOrUnknown(const std::string& value)
{
  static const std::string unknown(eos::common::traffic_shaping::kUnknownId);
  return value.empty() ? unknown : value;
}

void
AddReadWriteCounters(std::string& out, std::string_view bytes_name,
                     std::string_view ops_name, std::map<std::string, std::string> labels,
                     const EntityTotals& totals)
{
  labels["operation"] = "read";
  FormatCounterMetric(out, bytes_name, labels, totals.read_bytes);
  FormatCounterMetric(out, ops_name, labels, totals.read_ops);

  labels["operation"] = "write";
  FormatCounterMetric(out, bytes_name, labels, totals.write_bytes);
  FormatCounterMetric(out, ops_name, labels, totals.write_ops);
}

void
AddPolicyMetrics(std::string& out, const std::string& cluster, const std::string& type,
                 const std::string& id, const TrafficShapingPolicy& policy)
{
  auto labels = [&cluster, &type, &id](const std::string& rule,
                                       const std::string& operation) {
    return std::map<std::string, std::string>{{"cluster", cluster},
                                              {"type", type},
                                              {"id", id},
                                              {"rule", rule},
                                              {"operation", operation}};
  };

  const auto user_policy_value = [&policy](const uint64_t value) {
    return policy.is_enabled ? static_cast<double>(value) : 0.0;
  };

  FormatGaugeMetric(out, "eos_io_shaping_policy_bytes", labels("limit", "read"),
                    user_policy_value(policy.limit_read_bytes_per_sec));
  FormatGaugeMetric(out, "eos_io_shaping_policy_bytes", labels("limit", "write"),
                    user_policy_value(policy.limit_write_bytes_per_sec));
  FormatGaugeMetric(out, "eos_io_shaping_policy_bytes", labels("reservation", "read"),
                    user_policy_value(policy.reservation_read_bytes_per_sec));
  FormatGaugeMetric(out, "eos_io_shaping_policy_bytes", labels("reservation", "write"),
                    user_policy_value(policy.reservation_write_bytes_per_sec));
  FormatGaugeMetric(out, "eos_io_shaping_policy_bytes",
                    labels("controller_limit", "read"),
                    static_cast<double>(policy.controller_limit_read_bytes_per_sec));
  FormatGaugeMetric(out, "eos_io_shaping_policy_bytes",
                    labels("controller_limit", "write"),
                    static_cast<double>(policy.controller_limit_write_bytes_per_sec));
}

std::size_t
GetAllTagsEntryCount(TrafficShapingEngine& engine, TrafficShapingManager& manager)
{
  const auto cardinality = manager.GetMapCardinalityStats();

  if (engine.GetDetailLevel() == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
    return static_cast<std::size_t>(cardinality.detailed_cumulative_stats);
  }

  return static_cast<std::size_t>(cardinality.global_cumulative_stats);
}

std::map<AllKey, EntityTotals>
CollectAllTotals(TrafficShapingEngine& engine, TrafficShapingManager& manager)
{
  std::map<AllKey, EntityTotals> all_totals;

  if (engine.GetDetailLevel() == eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
    for (const auto& [detailed_key, snapshot] : manager.GetDetailedCumulativeStats()) {
      AllKey key{NodeLabel(LabelOrUnknown(detailed_key.node_id)),
                 detailed_key.stream.fsid, LabelOrUnknown(detailed_key.stream.app),
                 detailed_key.stream.uid, detailed_key.stream.gid};
      all_totals[key].Add(snapshot);
    }
  } else {
    for (const auto& [key, snapshot] : manager.GetGlobalCumulativeStats()) {
      AllKey all_key{eos::common::traffic_shaping::kUnknownId, 0, LabelOrUnknown(key.app),
                     key.uid, key.gid};
      all_totals[all_key].Add(snapshot);
    }
  }

  return all_totals;
}

void
AddCounterFamilies(std::string& out, TrafficShapingEngine& engine,
                   TrafficShapingManager& manager, const std::string& cluster)
{
  std::map<StandardKey, EntityTotals> standard_totals;
  const auto all_entries_available = GetAllTagsEntryCount(engine, manager);
  const bool export_all_tags = all_entries_available <= kMaxAllTagsMetricEntries;

  FormatHeader(out, "eos_io_shaping_all_entries", "gauge",
               "Number of all-tags IO shaping entries available for export.");
  FormatGaugeMetric(out, "eos_io_shaping_all_entries", {{"cluster", cluster}},
                    static_cast<double>(all_entries_available));

  FormatHeader(out, "eos_io_shaping_all_entries_limited", "gauge",
               "All-tags IO shaping export limit status (1 if all-tags metrics were "
               "suppressed, 0 otherwise).");
  FormatGaugeMetric(out, "eos_io_shaping_all_entries_limited", {{"cluster", cluster}},
                    export_all_tags ? 0.0 : 1.0);

  FormatHeader(out, "eos_io_shaping_all_entries_exported", "gauge",
               "Number of all-tags IO shaping entries exported in this scrape.");

  const auto projection_totals = manager.GetProjectionCumulativeStats();

  for (const auto& [app, snapshot] : projection_totals.app) {
    standard_totals[{"app", LabelOrUnknown(app)}].Add(snapshot);
  }

  for (const auto& [uid, snapshot] : projection_totals.uid) {
    standard_totals[{"uid", UidLabel(uid)}].Add(snapshot);
  }

  for (const auto& [gid, snapshot] : projection_totals.gid) {
    standard_totals[{"gid", GidLabel(gid)}].Add(snapshot);
  }

  FormatHeader(out, "eos_io_shaping_all_bytes_total", "counter",
               "Total IO shaping all-tags bytes observed");
  FormatHeader(out, "eos_io_shaping_all_operations_total", "counter",
               "Total IO shaping all-tags operations observed");

  if (export_all_tags) {
    const auto all_totals = CollectAllTotals(engine, manager);
    FormatGaugeMetric(out, "eos_io_shaping_all_entries_exported", {{"cluster", cluster}},
                      static_cast<double>(all_totals.size()));

    for (const auto& [key, totals] : all_totals) {
      const std::string uid_label = UidLabel(key.uid);
      const std::string gid_label = GidLabel(key.gid);
      AddReadWriteCounters(out, "eos_io_shaping_all_bytes_total",
                           "eos_io_shaping_all_operations_total",
                           {{"cluster", cluster},
                            {"node_id", key.node_id},
                            {"fsid", std::to_string(key.fsid)},
                            {"app", key.app},
                            {"uid", uid_label},
                            {"uid_id", std::to_string(key.uid)},
                            {"uid_name", uid_label},
                            {"gid", gid_label},
                            {"gid_id", std::to_string(key.gid)},
                            {"gid_name", gid_label},
                            {"groups", gid_label}},
                           totals);
    }
  } else {
    FormatGaugeMetric(out, "eos_io_shaping_all_entries_exported", {{"cluster", cluster}},
                      0.0);
  }

  for (const auto& [node_id, snapshot] : projection_totals.node) {
    standard_totals[{"node", NodeLabel(LabelOrUnknown(node_id))}].Add(snapshot);
  }

  FormatHeader(out, "eos_io_shaping_bytes_total", "counter",
               "Total IO shaping bytes observed");
  FormatHeader(out, "eos_io_shaping_operations_total", "counter",
               "Total IO shaping operations observed");

  for (const auto& [key, totals] : standard_totals) {
    AddReadWriteCounters(
        out, "eos_io_shaping_bytes_total", "eos_io_shaping_operations_total",
        {{"cluster", cluster}, {"type", key.type}, {"id", key.id}}, totals);
  }

  FormatHeader(out, "eos_io_shaping_fs_bytes_total", "counter",
               "Total IO shaping filesystem bytes observed");
  FormatHeader(out, "eos_io_shaping_fs_operations_total", "counter",
               "Total IO shaping filesystem operations observed");

  for (const auto& [key, snapshot] : manager.GetDiskCumulativeStats()) {
    EntityTotals totals;
    totals.Add(snapshot);
    AddReadWriteCounters(out, "eos_io_shaping_fs_bytes_total",
                         "eos_io_shaping_fs_operations_total",
                         {{"cluster", cluster},
                          {"node_id", NodeLabel(LabelOrUnknown(key.node_id))},
                          {"fsid", std::to_string(key.fsid)}},
                         totals);
  }
}

void
AddSystemFamilies(std::string& out, TrafficShapingManager& manager,
                  const std::string& cluster)
{
  const auto timing = manager.GetSystemTimingSnapshot();

  FormatHeader(out, "eos_io_shaping_loop_duration_seconds", "histogram",
               "Cumulative duration distribution of traffic shaping loops in seconds.");
  FormatHeader(out, "eos_io_shaping_loop_iterations_total", "counter",
               "Total iterations executed by traffic shaping loops.");
  FormatHeader(
      out, "eos_io_shaping_loop_last_completed_timestamp_seconds", "gauge",
      "Unix timestamp in seconds when each traffic shaping loop last completed.");
  FormatHeader(out, "eos_io_shaping_slow_iterations_total", "counter",
               "Traffic shaping loop iterations exceeding their warning threshold.");

  auto add_loop_timing = [&out, &cluster](const std::string& loop_name,
                                          const LoopTimingSnapshot& loop) {
    const std::map<std::string, std::string> labels{{"cluster", cluster},
                                                    {"loop_name", loop_name}};
    FormatHistogramMetric(out, "eos_io_shaping_loop_duration_seconds", labels,
                          loop.duration);
    FormatCounterMetric(out, "eos_io_shaping_loop_iterations_total", labels,
                        loop.iterations_total);
    FormatGaugeMetric(out, "eos_io_shaping_loop_last_completed_timestamp_seconds", labels,
                      static_cast<double>(loop.last_completed_timestamp_seconds));
  };

  add_loop_timing("estimators", timing.estimators);
  add_loop_timing("fst_policy", timing.fst_policy);
  add_loop_timing("io_pressure", timing.io_pressure);
  add_loop_timing("reservation_controller", timing.reservation_controller);
  add_loop_timing("fst_limits", timing.fst_limits);
  add_loop_timing("garbage_collection", timing.garbage_collection);

  FormatCounterMetric(out, "eos_io_shaping_slow_iterations_total",
                      {{"cluster", cluster}, {"loop_name", "fst_policy"}},
                      timing.fst_policy_slow_iterations_total);

  FormatHeader(out, "eos_io_shaping_fsview_lock_duration_seconds", "histogram",
               "Cumulative traffic shaping FsView lock wait and hold duration "
               "distribution in seconds.");
  auto add_lock_timing = [&out, &cluster](const std::string& lock_name,
                                          const DurationHistogramSnapshot& duration) {
    const std::map<std::string, std::string> labels{{"cluster", cluster},
                                                    {"lock_name", lock_name}};
    FormatHistogramMetric(out, "eos_io_shaping_fsview_lock_duration_seconds", labels,
                          duration);
  };

  add_lock_timing("wait", timing.fsview_lock_wait);
  add_lock_timing("hold", timing.fsview_lock_hold);

  const std::map<std::string, std::string> cluster_label{{"cluster", cluster}};

  FormatHeader(out, "eos_io_shaping_reports_processed_per_sec", "gauge",
               "FST IO reports processed per second");
  FormatGaugeMetric(out, "eos_io_shaping_reports_processed_per_sec", cluster_label,
                    manager.GetFstReportsProcessedPerSecondMean());

  FormatHeader(
      out, "eos_io_shaping_report_queue_depth", "gauge",
      "Current number of FST IO reports waiting for traffic shaping processing.");
  FormatGaugeMetric(out, "eos_io_shaping_report_queue_depth", cluster_label,
                    static_cast<double>(manager.GetFstReportQueueDepth()));

  FormatHeader(
      out, "eos_io_shaping_report_queue_oldest_age_seconds", "gauge",
      "Age in seconds of the oldest FST IO report awaiting or undergoing processing.");
  FormatGaugeMetric(out, "eos_io_shaping_report_queue_oldest_age_seconds", cluster_label,
                    manager.GetFstReportQueueOldestAgeSeconds());

  FormatHeader(out, "eos_io_shaping_report_queue_estimated_bytes", "gauge",
               "Estimated memory footprint of queued FST IO reports.");
  FormatGaugeMetric(out, "eos_io_shaping_report_queue_estimated_bytes", cluster_label,
                    static_cast<double>(manager.GetFstReportQueueEstimatedBytes()));

  FormatHeader(
      out, "eos_io_shaping_reports_dropped_total", "counter",
      "FST IO reports rejected or evicted by traffic shaping queue safety bounds.");
  FormatCounterMetric(out, "eos_io_shaping_reports_dropped_total", cluster_label,
                      manager.GetFstReportsDroppedTotal());

  const auto memory = manager.GetMemoryStats();

  FormatHeader(out, "eos_io_shaping_stream_state_estimated_bytes", "gauge",
               "Conservative estimated memory charged to admitted FST stream state.");
  FormatGaugeMetric(out, "eos_io_shaping_stream_state_estimated_bytes", cluster_label,
                    static_cast<double>(memory.stream_state_estimated_bytes));

  FormatHeader(out, "eos_io_shaping_estimated_memory_bytes", "gauge",
               "Conservative estimated memory attributable to bounded traffic shaping "
               "stream state and queued reports; this is not allocator RSS.");
  FormatGaugeMetric(out, "eos_io_shaping_estimated_memory_bytes", cluster_label,
                    static_cast<double>(memory.estimated_bytes));

  FormatHeader(out, "eos_io_shaping_memory_limit_bytes", "gauge",
               "Traffic shaping memory admission safety bound by component.");
  FormatGaugeMetric(out, "eos_io_shaping_memory_limit_bytes",
                    {{"cluster", cluster}, {"component", "queue"}},
                    static_cast<double>(memory.report_queue_limit_bytes));
  FormatGaugeMetric(out, "eos_io_shaping_memory_limit_bytes",
                    {{"cluster", cluster}, {"component", "stream_state"}},
                    static_cast<double>(memory.stream_state_limit_bytes));
  FormatGaugeMetric(out, "eos_io_shaping_memory_limit_bytes",
                    {{"cluster", cluster}, {"component", "total"}},
                    static_cast<double>(memory.limit_bytes));

  FormatHeader(out, "eos_io_shaping_stream_state_limit_entries", "gauge",
               "Maximum admitted FST stream states across all nodes.");
  FormatGaugeMetric(out, "eos_io_shaping_stream_state_limit_entries", cluster_label,
                    static_cast<double>(memory.stream_state_limit_entries));

  const auto cardinality = manager.GetMapCardinalityStats();
  FormatHeader(out, "eos_io_shaping_map_cardinality", "gauge",
               "Traffic shaping internal map cardinality by map name.");
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "node_states"}},
                    static_cast<double>(cardinality.node_states));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "node_state_streams"}},
                    static_cast<double>(cardinality.node_state_streams));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "global_stats"}},
                    static_cast<double>(cardinality.global_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "node_stats"}},
                    static_cast<double>(cardinality.node_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "disk_stats"}},
                    static_cast<double>(cardinality.disk_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "detailed_stats"}},
                    static_cast<double>(cardinality.detailed_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "global_cumulative_stats"}},
                    static_cast<double>(cardinality.global_cumulative_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "node_cumulative_stats"}},
                    static_cast<double>(cardinality.node_cumulative_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "detailed_cumulative_stats"}},
                    static_cast<double>(cardinality.detailed_cumulative_stats));
  FormatGaugeMetric(out, "eos_io_shaping_map_cardinality",
                    {{"cluster", cluster}, {"map_name", "disk_cumulative_stats"}},
                    static_cast<double>(cardinality.disk_cumulative_stats));
  FormatGaugeMetric(
      out, "eos_io_shaping_map_cardinality",
      {{"cluster", cluster}, {"map_name", "projection_app_cumulative_stats"}},
      static_cast<double>(cardinality.projection_app_cumulative_stats));
  FormatGaugeMetric(
      out, "eos_io_shaping_map_cardinality",
      {{"cluster", cluster}, {"map_name", "projection_uid_cumulative_stats"}},
      static_cast<double>(cardinality.projection_uid_cumulative_stats));
  FormatGaugeMetric(
      out, "eos_io_shaping_map_cardinality",
      {{"cluster", cluster}, {"map_name", "projection_gid_cumulative_stats"}},
      static_cast<double>(cardinality.projection_gid_cumulative_stats));
  FormatGaugeMetric(
      out, "eos_io_shaping_map_cardinality",
      {{"cluster", cluster}, {"map_name", "projection_node_cumulative_stats"}},
      static_cast<double>(cardinality.projection_node_cumulative_stats));
}

void
AddPolicyFamilies(std::string& out, TrafficShapingManager& manager,
                  const std::string& cluster)
{
  FormatHeader(out, "eos_io_shaping_policy_bytes", "gauge",
               "Configured limits and reservations in bytes per second (0 if user policy "
               "is disabled, but controller limits bypass this)");

  for (const auto& [app, policy] : manager.GetAppPolicies()) {
    AddPolicyMetrics(out, cluster, "app", LabelOrUnknown(app), policy);
  }

  for (const auto& [uid, policy] : manager.GetUidPolicies()) {
    AddPolicyMetrics(out, cluster, "uid", UidLabel(uid), policy);
  }

  for (const auto& [gid, policy] : manager.GetGidPolicies()) {
    AddPolicyMetrics(out, cluster, "gid", GidLabel(gid), policy);
  }
}

void
AddPressureFamilies(std::string& out, TrafficShapingManager& manager,
                    const std::string& cluster)
{
  const auto pressure_snapshots = manager.GetReservedAppIoPressure();

  FormatHeader(
      out, "eos_io_shaping_app_io_pressure", "gauge",
      "Maximum node IO pressure observed by reserved application and operation.");
  FormatHeader(out, "eos_io_shaping_app_io_pressure_sample", "gauge",
               "Reserved application IO pressure sample availability (1 if present, 0 if "
               "absent).");

  for (const auto& [app_name, snapshot] : pressure_snapshots) {
    const auto app = LabelOrUnknown(app_name);
    FormatGaugeMetric(out, "eos_io_shaping_app_io_pressure",
                      std::map<std::string, std::string>{
                          {"app", app}, {"cluster", cluster}, {"operation", "read"}},
                      snapshot.read);
    FormatGaugeMetric(out, "eos_io_shaping_app_io_pressure",
                      std::map<std::string, std::string>{
                          {"app", app}, {"cluster", cluster}, {"operation", "write"}},
                      snapshot.write);
    FormatGaugeMetric(out, "eos_io_shaping_app_io_pressure_sample",
                      std::map<std::string, std::string>{
                          {"app", app}, {"cluster", cluster}, {"operation", "read"}},
                      snapshot.has_read ? 1.0 : 0.0);
    FormatGaugeMetric(out, "eos_io_shaping_app_io_pressure_sample",
                      std::map<std::string, std::string>{
                          {"app", app}, {"cluster", cluster}, {"operation", "write"}},
                      snapshot.has_write ? 1.0 : 0.0);
  }

  FormatHeader(out, "eos_io_shaping_app_node_io_pressure", "gauge",
               "Reservation IO pressure by recently observed application, node and "
               "pressure scope.");
  FormatHeader(out, "eos_io_shaping_app_node_reservation_deficit_bytes", "gauge",
               "Reservation deficit by recently observed application, node and operation "
               "in bytes per second.");
  FormatHeader(out, "eos_io_shaping_app_node_reservation_deficit_active", "gauge",
               "Reservation deficit activity by recently observed application, node and "
               "operation.");
  FormatHeader(out, "eos_io_shaping_app_node_pressure_active", "gauge",
               "Reservation pressure activity by recently observed application, node and "
               "operation.");
  FormatHeader(out, "eos_io_shaping_app_node_reservation_trigger_active", "gauge",
               "Reservation pressure that triggers competitor throttling by recently "
               "observed application, node and operation.");
  FormatHeader(out, "eos_io_shaping_node_pressured_reservation_active", "gauge",
               "Node has at least one pressured reservation by operation.");

  std::map<std::pair<std::string, std::string>, bool> node_pressured_reservations;

  for (const auto& snapshot : manager.GetReservedAppNodeIoPressure()) {
    const auto app = LabelOrUnknown(snapshot.app);
    const auto node_id = NodeLabel(LabelOrUnknown(snapshot.node_id));

    const std::map<std::string, std::string> node_read_labels = {
        {"app", app}, {"cluster", cluster}, {"node_id", node_id}, {"scope", "node_read"}};
    const std::map<std::string, std::string> node_write_labels = {
        {"app", app},
        {"cluster", cluster},
        {"node_id", node_id},
        {"scope", "node_write"}};
    const std::map<std::string, std::string> res_read_labels = {
        {"app", app},
        {"cluster", cluster},
        {"node_id", node_id},
        {"scope", "reservation_read"}};
    const std::map<std::string, std::string> res_write_labels = {
        {"app", app},
        {"cluster", cluster},
        {"node_id", node_id},
        {"scope", "reservation_write"}};

    FormatGaugeMetric(out, "eos_io_shaping_app_node_io_pressure", node_read_labels,
                      snapshot.has_read_io_pressure ? snapshot.node_io_pressure : 0.0);
    FormatGaugeMetric(out, "eos_io_shaping_app_node_io_pressure", node_write_labels,
                      snapshot.has_write_io_pressure ? snapshot.node_io_pressure : 0.0);
    FormatGaugeMetric(out, "eos_io_shaping_app_node_io_pressure", res_read_labels,
                      snapshot.read_pressure_active ? snapshot.node_io_pressure : 0.0);
    FormatGaugeMetric(out, "eos_io_shaping_app_node_io_pressure", res_write_labels,
                      snapshot.write_pressure_active ? snapshot.node_io_pressure : 0.0);

    auto add_read_write = [&out, &cluster, &app, &node_id](std::string_view name,
                                                           const double read_value,
                                                           const double write_value) {
      const std::map<std::string, std::string> read_labels = {{"app", app},
                                                              {"cluster", cluster},
                                                              {"node_id", node_id},
                                                              {"operation", "read"}};
      const std::map<std::string, std::string> write_labels = {{"app", app},
                                                               {"cluster", cluster},
                                                               {"node_id", node_id},
                                                               {"operation", "write"}};
      FormatGaugeMetric(out, name, read_labels, read_value);
      FormatGaugeMetric(out, name, write_labels, write_value);
    };

    add_read_write("eos_io_shaping_app_node_reservation_deficit_bytes",
                   snapshot.read_reservation_deficit_bps,
                   snapshot.write_reservation_deficit_bps);
    add_read_write("eos_io_shaping_app_node_reservation_deficit_active",
                   snapshot.read_reservation_deficit_active ? 1.0 : 0.0,
                   snapshot.write_reservation_deficit_active ? 1.0 : 0.0);
    add_read_write("eos_io_shaping_app_node_pressure_active",
                   snapshot.read_pressure_active ? 1.0 : 0.0,
                   snapshot.write_pressure_active ? 1.0 : 0.0);
    add_read_write("eos_io_shaping_app_node_reservation_trigger_active",
                   snapshot.read_triggers_competitor_throttling ? 1.0 : 0.0,
                   snapshot.write_triggers_competitor_throttling ? 1.0 : 0.0);

    node_pressured_reservations[std::make_pair(node_id, std::string("read"))] |=
        snapshot.node_has_pressured_read_reservation;
    node_pressured_reservations[std::make_pair(node_id, std::string("write"))] |=
        snapshot.node_has_pressured_write_reservation;
  }

  for (const auto& [key, active] : node_pressured_reservations) {
    const auto& [node_id, operation] = key;
    const std::map<std::string, std::string> labels = {
        {"cluster", cluster}, {"node_id", node_id}, {"operation", operation}};
    FormatGaugeMetric(out, "eos_io_shaping_node_pressured_reservation_active", labels,
                      active ? 1.0 : 0.0);
  }
}

void
AddControllerFamilies(std::string& out, TrafficShapingManager& manager,
                      const std::string& cluster)
{
  FormatHeader(out, "eos_io_shaping_node_controller_limit_bytes_per_second", "gauge",
               "Active node-local controller limit by application and operation.");
  FormatHeader(out, "eos_io_shaping_node_controller_applied_reduction_bytes_per_second",
               "gauge", "Rate removed by the latest node-local controller action.");
  FormatHeader(out, "eos_io_shaping_node_controller_protected_gain_bytes_per_second",
               "gauge",
               "Protected rate gained in the latest evaluated controller response.");
  FormatHeader(out, "eos_io_shaping_node_controller_response_ratio", "gauge",
               "Conservative protected gain divided by assigned competitor reduction.");
  FormatHeader(
      out, "eos_io_shaping_node_controller_cohort_apps", "gauge",
      "Number of protected applications in the active or failed response cohort.");
  FormatHeader(out, "eos_io_shaping_node_controller_ineffective_probes", "gauge",
               "Consecutive ineffective controller probes by node and operation.");
  FormatHeader(out, "eos_io_shaping_node_controller_suppression_seconds", "gauge",
               "Seconds remaining before an ineffective controller probe may retry.");
  FormatHeader(out, "eos_io_shaping_node_controller_deficit_samples", "gauge",
               "Consecutive qualifying reservation-deficit samples.");
  FormatHeader(out, "eos_io_shaping_node_controller_state", "gauge",
               "Current controller phase by node and operation (value is always 1).");

  const auto snapshot = manager.GetNodeReservationControllerSnapshot();

  for (const auto& limit : snapshot.limits) {
    const auto node_id = NodeLabel(LabelOrUnknown(limit.node_id));
    if (limit.read_bytes_per_sec > 0) {
      const auto labels =
          std::map<std::string, std::string>{{"cluster", cluster},
                                             {"node_id", node_id},
                                             {"operation", "read"},
                                             {"app", LabelOrUnknown(limit.app)}};
      FormatGaugeMetric(out, "eos_io_shaping_node_controller_limit_bytes_per_second",
                        labels, static_cast<double>(limit.read_bytes_per_sec));
    }
    if (limit.write_bytes_per_sec > 0) {
      const auto labels =
          std::map<std::string, std::string>{{"cluster", cluster},
                                             {"node_id", node_id},
                                             {"operation", "write"},
                                             {"app", LabelOrUnknown(limit.app)}};
      FormatGaugeMetric(out, "eos_io_shaping_node_controller_limit_bytes_per_second",
                        labels, static_cast<double>(limit.write_bytes_per_sec));
    }
  }

  for (const auto& fb : snapshot.feedback) {
    const auto node_id = NodeLabel(LabelOrUnknown(fb.node_id));
    const std::string op = fb.is_write ? "write" : "read";
    const auto node_operation_labels = std::map<std::string, std::string>{
        {"cluster", cluster}, {"node_id", node_id}, {"operation", op}};

    FormatGaugeMetric(out,
                      "eos_io_shaping_node_controller_applied_reduction_bytes_per_second",
                      node_operation_labels, fb.applied_reduction_bps);
    FormatGaugeMetric(out,
                      "eos_io_shaping_node_controller_protected_gain_bytes_per_second",
                      node_operation_labels, fb.observed_protected_gain_bps);
    FormatGaugeMetric(out, "eos_io_shaping_node_controller_response_ratio",
                      node_operation_labels, fb.response_ratio);

    auto cohort_labels = node_operation_labels;
    cohort_labels["status"] = "active";
    FormatGaugeMetric(out, "eos_io_shaping_node_controller_cohort_apps", cohort_labels,
                      static_cast<double>(fb.protected_app_count));
    cohort_labels["status"] = "failed";
    FormatGaugeMetric(out, "eos_io_shaping_node_controller_cohort_apps", cohort_labels,
                      static_cast<double>(fb.failed_protected_app_count));

    FormatGaugeMetric(out, "eos_io_shaping_node_controller_ineffective_probes",
                      node_operation_labels,
                      static_cast<double>(fb.ineffective_probe_count));
    FormatGaugeMetric(out, "eos_io_shaping_node_controller_suppression_seconds",
                      node_operation_labels, fb.suppression_remaining_seconds);
    FormatGaugeMetric(out, "eos_io_shaping_node_controller_deficit_samples",
                      node_operation_labels,
                      static_cast<double>(fb.consecutive_deficit_samples));

    if (!fb.phase.empty()) {
      auto state_labels = node_operation_labels;
      state_labels["phase"] = fb.phase;
      FormatGaugeMetric(out, "eos_io_shaping_node_controller_state", state_labels, 1.0);
    }
  }
}

void
AddBroadcastActuatorFamilies(std::string& out, TrafficShapingManager& manager,
                             const std::string& cluster)
{
  const auto snapshot = manager.GetBroadcastActuatorSnapshot();

  FormatHeader(out, "eos_io_shaping_broadcast_pending_nodes", "gauge",
               "FST nodes whose latest actuator configuration has not been published.");
  FormatGaugeMetric(out, "eos_io_shaping_broadcast_pending_nodes", {{"cluster", cluster}},
                    static_cast<double>(snapshot.pending_nodes));

  FormatHeader(out, "eos_io_shaping_broadcast_delay_seconds", "gauge",
               "Successfully published legacy FST delay aggregated across target nodes.");
  FormatHeader(
      out, "eos_io_shaping_broadcast_delay_nodes", "gauge",
      "Nodes receiving a nonzero actuator command, split by active/capped state.");
  FormatHeader(out, "eos_io_shaping_broadcast_assigned_rate_bytes_per_second", "gauge",
               "Sum of successfully published node-local shared rate assignments.");

  for (const auto& entry : snapshot.entries) {
    auto labels =
        std::map<std::string, std::string>{{"cluster", cluster},
                                           {"identity_type", entry.identity_type},
                                           {"identity", LabelOrUnknown(entry.identity)},
                                           {"operation", entry.operation}};

    if (entry.delay_max_seconds > 0.0) {
      labels["quantile"] = "0.50";
      FormatGaugeMetric(out, "eos_io_shaping_broadcast_delay_seconds", labels,
                        entry.delay_p50_seconds);
      labels["quantile"] = "0.95";
      FormatGaugeMetric(out, "eos_io_shaping_broadcast_delay_seconds", labels,
                        entry.delay_p95_seconds);
      labels["quantile"] = "1.00";
      FormatGaugeMetric(out, "eos_io_shaping_broadcast_delay_seconds", labels,
                        entry.delay_max_seconds);
    }

    labels.erase("quantile");
    labels["state"] = "active";
    FormatGaugeMetric(out, "eos_io_shaping_broadcast_delay_nodes", labels,
                      static_cast<double>(entry.active_nodes));
    labels["state"] = "capped";
    FormatGaugeMetric(out, "eos_io_shaping_broadcast_delay_nodes", labels,
                      static_cast<double>(entry.capped_nodes));

    labels.erase("state");
    if (entry.assigned_rate_bytes_per_second > 0) {
      FormatGaugeMetric(out, "eos_io_shaping_broadcast_assigned_rate_bytes_per_second",
                        labels,
                        static_cast<double>(entry.assigned_rate_bytes_per_second));
    }
  }
}

void
AddFstActuatorFamilies(std::string& out, TrafficShapingManager& manager,
                       const std::string& cluster)
{
  const auto snapshots = manager.GetFstActuatorSnapshots();

  FormatHeader(out, "eos_io_shaping_fst_actuator_active_waiters", "gauge",
               "Current FST IO operations waiting for a shaping permit.");
  FormatHeader(out, "eos_io_shaping_fst_actuator_wait_events_total", "counter",
               "FST IO operations that waited for a shaping permit.");
  FormatHeader(out, "eos_io_shaping_fst_actuator_wait_seconds_total", "counter",
               "Cumulative time FST IO operations spent waiting for shaping permits.");

  for (const auto& snapshot : snapshots) {
    const std::string node_id = NodeLabel(LabelOrUnknown(snapshot.node_id));
    const auto read_labels = std::map<std::string, std::string>{
        {"cluster", cluster}, {"node_id", node_id}, {"operation", "read"}};
    const auto write_labels = std::map<std::string, std::string>{
        {"cluster", cluster}, {"node_id", node_id}, {"operation", "write"}};

    FormatGaugeMetric(out, "eos_io_shaping_fst_actuator_active_waiters", read_labels,
                      static_cast<double>(snapshot.read_active_waiters));
    FormatCounterMetric(out, "eos_io_shaping_fst_actuator_wait_events_total", read_labels,
                        snapshot.read_wait_events);
    FormatCounterMetric(out, "eos_io_shaping_fst_actuator_wait_seconds_total",
                        read_labels,
                        static_cast<double>(snapshot.read_wait_microseconds) / 1e6);

    FormatGaugeMetric(out, "eos_io_shaping_fst_actuator_active_waiters", write_labels,
                      static_cast<double>(snapshot.write_active_waiters));
    FormatCounterMetric(out, "eos_io_shaping_fst_actuator_wait_events_total",
                        write_labels, snapshot.write_wait_events);
    FormatCounterMetric(out, "eos_io_shaping_fst_actuator_wait_seconds_total",
                        write_labels,
                        static_cast<double>(snapshot.write_wait_microseconds) / 1e6);
  }
}

void
AddConfigFamilies(std::string& out, TrafficShapingEngine& engine,
                  const std::string& cluster)
{
  const std::map<std::string, std::string> labels{{"cluster", cluster}};
  const double enabled = engine.IsEnabled() ? 1.0 : 0.0;

  FormatHeader(out, "eos_io_shaping_config_enabled", "gauge",
               "Traffic shaping configuration status (1 if enabled, 0 if disabled).");
  FormatGaugeMetric(out, "eos_io_shaping_config_enabled", labels, enabled);

  FormatHeader(
      out, "eos_ns_traffic_shaping_enabled", "gauge",
      "Traffic shaping status reported by eos ns stat (1 if enabled, 0 if disabled).");
  FormatGaugeMetric(out, "eos_ns_traffic_shaping_enabled", labels, enabled);

  FormatHeader(out, "eos_io_shaping_config_limits_enabled", "gauge",
               "Traffic shaping limit enforcement status (1 if enabled, 0 if disabled).");
  FormatGaugeMetric(out, "eos_io_shaping_config_limits_enabled", labels,
                    engine.GetLimitsEnabled() ? 1.0 : 0.0);

  FormatHeader(
      out, "eos_io_shaping_config_reservations_enabled", "gauge",
      "Traffic shaping reservation enforcement status (1 if enabled, 0 if disabled).");
  FormatGaugeMetric(out, "eos_io_shaping_config_reservations_enabled", labels,
                    engine.GetReservationsEnabled() ? 1.0 : 0.0);

  FormatHeader(out, "eos_io_shaping_config_controller_min_limit_bytes", "gauge",
               "Configured minimum controller-generated limit in bytes per second.");
  FormatGaugeMetric(out, "eos_io_shaping_config_controller_min_limit_bytes", labels,
                    static_cast<double>(engine.GetControllerMinLimit()));

  FormatHeader(out, "eos_io_shaping_config_active_node_rate_threshold_bytes", "gauge",
               "Configured active node rate threshold in bytes per second.");
  FormatGaugeMetric(out, "eos_io_shaping_config_active_node_rate_threshold_bytes", labels,
                    static_cast<double>(engine.GetActiveNodeRateThreshold()));

  FormatHeader(out, "eos_io_shaping_config_io_pressure_threshold", "gauge",
               "Configured IO pressure threshold for reservation pressure.");
  FormatGaugeMetric(out, "eos_io_shaping_config_io_pressure_threshold", labels,
                    engine.GetIoPressureThreshold());

  FormatHeader(out, "eos_io_shaping_config_max_delay_seconds", "gauge",
               "Configured maximum legacy per-MiB FST delay in seconds.");
  FormatGaugeMetric(out, "eos_io_shaping_config_max_delay_seconds", labels,
                    static_cast<double>(engine.GetMaxDelayMilliseconds()) / 1000.0);

  FormatHeader(
      out, "eos_io_shaping_config_garbage_collection_idle_seconds", "gauge",
      "Configured idle time before traffic shaping runtime stats garbage collection.");
  FormatGaugeMetric(out, "eos_io_shaping_config_garbage_collection_idle_seconds", labels,
                    static_cast<double>(engine.GetGarbageCollectionIdleSeconds()));

  FormatHeader(out, "eos_io_shaping_config_estimators_update_period_milliseconds",
               "gauge",
               "Configured IO shaping estimators update period in milliseconds.");
  FormatGaugeMetric(
      out, "eos_io_shaping_config_estimators_update_period_milliseconds", labels,
      static_cast<double>(engine.GetEstimatorsUpdateThreadPeriodMilliseconds()));

  FormatHeader(out, "eos_io_shaping_config_fst_io_policy_update_period_milliseconds",
               "gauge", "Configured FST IO policy update period in milliseconds.");
  FormatGaugeMetric(
      out, "eos_io_shaping_config_fst_io_policy_update_period_milliseconds", labels,
      static_cast<double>(engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds()));

  FormatHeader(out, "eos_io_shaping_config_fst_io_stats_reporting_period_milliseconds",
               "gauge", "Configured FST IO stats reporting period in milliseconds.");
  FormatGaugeMetric(
      out, "eos_io_shaping_config_fst_io_stats_reporting_period_milliseconds", labels,
      static_cast<double>(engine.GetFstIoStatsReportThreadPeriodMilliseconds()));

  FormatHeader(out, "eos_io_shaping_config_detail_filesystem", "gauge",
               "Traffic shaping stats detail level (1 if filesystem, 0 otherwise).");
  FormatGaugeMetric(out, "eos_io_shaping_config_detail_filesystem", labels,
                    engine.GetDetailLevel() ==
                            eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM
                        ? 1.0
                        : 0.0);

  FormatHeader(out, "eos_io_shaping_config_detail_auto_enabled", "gauge",
               "Traffic shaping automatic stats detail level status (1 if enabled, 0 if "
               "disabled).");
  FormatGaugeMetric(out, "eos_io_shaping_config_detail_auto_enabled", labels,
                    engine.GetAutomaticDetailLevelEnabled() ? 1.0 : 0.0);

  FormatHeader(out, "eos_io_shaping_config_detail_auto_low_cardinality", "gauge",
               "Node stream cardinality at or below which automatic detail level selects "
               "filesystem detail.");
  FormatGaugeMetric(out, "eos_io_shaping_config_detail_auto_low_cardinality", labels,
                    static_cast<double>(engine.GetAutomaticDetailLevelLowCardinality()));

  FormatHeader(out, "eos_io_shaping_config_detail_auto_high_cardinality", "gauge",
               "Node stream cardinality above which automatic detail level selects "
               "aggregate detail.");
  FormatGaugeMetric(out, "eos_io_shaping_config_detail_auto_high_cardinality", labels,
                    static_cast<double>(engine.GetAutomaticDetailLevelHighCardinality()));

  FormatHeader(out, "eos_io_shaping_config_system_stats_time_window_seconds", "gauge",
               "Configured IO shaping system stats time window in seconds.");
  FormatGaugeMetric(out, "eos_io_shaping_config_system_stats_time_window_seconds", labels,
                    static_cast<double>(engine.GetSystemStatsWindowSeconds()));
}

} // namespace

TrafficShapingCollector::TrafficShapingCollector(TrafficShapingEngine& engine,
                                                 std::string cluster)
    : mEngine(engine)
    , mCluster(std::move(cluster))
{
}

void
TrafficShapingCollector::Collect(std::string& out) const
{
  auto manager = mEngine.GetManager();

  if (!manager) {
    return;
  }

  AddCounterFamilies(out, mEngine, *manager, mCluster);
  AddSystemFamilies(out, *manager, mCluster);
  AddPolicyFamilies(out, *manager, mCluster);
  AddPressureFamilies(out, *manager, mCluster);
  AddControllerFamilies(out, *manager, mCluster);
  AddBroadcastActuatorFamilies(out, *manager, mCluster);
  AddFstActuatorFamilies(out, *manager, mCluster);
  AddConfigFamilies(out, mEngine, mCluster);
}

} // namespace eos::mgm::monitoring
