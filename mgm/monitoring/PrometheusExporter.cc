#include "mgm/monitoring/PrometheusExporter.hh"

#include "common/Constants.hh"
#include "common/StringUtils.hh"
#include "common/shaping/Identity.hh"
#include "common/shaping/IoStatsKey.hh"
#include "mgm/monitoring/CachedCollectable.hh"
#include "prometheus/client_metric.h"
#include "prometheus/collectable.h"
#include "prometheus/exposer.h"
#include "prometheus/metric_family.h"
#include "prometheus/metric_type.h"

#include <cstddef>
#include <cstdint>
#include <functional>
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

constexpr std::size_t kPrometheusExporterThreads = 8;
constexpr std::size_t kMaxAllTagsMetricEntries = 50000;

class MasterOnlyCollectable : public prometheus::Collectable {
public:
  MasterOnlyCollectable(std::shared_ptr<prometheus::Collectable> collectable,
                        std::function<bool()> should_collect)
      : mCollectable(std::move(collectable))
      , mShouldCollect(std::move(should_collect))
  {
  }

  std::vector<prometheus::MetricFamily>
  Collect() const override
  {
    if (!mShouldCollect || !mShouldCollect()) {
      return {};
    }

    return mCollectable->Collect();
  }

private:
  std::shared_ptr<prometheus::Collectable> mCollectable;
  std::function<bool()> mShouldCollect;
};

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

prometheus::ClientMetric
MakeMetric(const std::map<std::string, std::string>& labels, const double value)
{
  prometheus::ClientMetric metric;
  metric.gauge.value = value;

  for (const auto& [name, label_value] : labels) {
    metric.label.push_back({name, label_value});
  }

  return metric;
}

void
AddGauge(prometheus::MetricFamily& family,
         const std::map<std::string, std::string>& labels, const double value)
{
  family.metric.push_back(MakeMetric(labels, value));
}

prometheus::MetricFamily
MakeGaugeFamily(const std::string& name, const std::string& help)
{
  prometheus::MetricFamily family;
  family.name = name;
  family.help = help;
  family.type = prometheus::MetricType::Gauge;
  return family;
}

prometheus::ClientMetric
MakeCounterMetric(const std::map<std::string, std::string>& labels, const double value)
{
  prometheus::ClientMetric metric;
  metric.counter.value = value;

  for (const auto& [name, label_value] : labels) {
    metric.label.push_back({name, label_value});
  }

  return metric;
}

void
AddCounter(prometheus::MetricFamily& family,
           const std::map<std::string, std::string>& labels, const uint64_t value)
{
  family.metric.push_back(MakeCounterMetric(labels, static_cast<double>(value)));
}

prometheus::MetricFamily
MakeCounterFamily(const std::string& name, const std::string& help)
{
  prometheus::MetricFamily family;
  family.name = name;
  family.help = help;
  family.type = prometheus::MetricType::Counter;
  return family;
}

prometheus::ClientMetric
MakeHistogramMetric(const std::map<std::string, std::string>& labels,
                    const DurationHistogramSnapshot& snapshot)
{
  prometheus::ClientMetric metric;
  metric.histogram.sample_count = snapshot.sample_count;
  metric.histogram.sample_sum = snapshot.sample_sum_seconds;

  const size_t bucket_count = std::min(snapshot.upper_bounds_seconds.size(),
                                       snapshot.cumulative_bucket_counts.size());
  metric.histogram.bucket.reserve(bucket_count + 1);
  for (size_t i = 0; i < bucket_count; ++i) {
    metric.histogram.bucket.push_back(
        {snapshot.cumulative_bucket_counts[i], snapshot.upper_bounds_seconds[i]});
  }
  metric.histogram.bucket.push_back(
      {snapshot.sample_count, std::numeric_limits<double>::infinity()});

  for (const auto& [name, label_value] : labels) {
    metric.label.push_back({name, label_value});
  }

  return metric;
}

void
AddHistogram(prometheus::MetricFamily& family,
             const std::map<std::string, std::string>& labels,
             const DurationHistogramSnapshot& snapshot)
{
  family.metric.push_back(MakeHistogramMetric(labels, snapshot));
}

prometheus::MetricFamily
MakeHistogramFamily(const std::string& name, const std::string& help)
{
  prometheus::MetricFamily family;
  family.name = name;
  family.help = help;
  family.type = prometheus::MetricType::Histogram;
  return family;
}

void
AddReadWriteCounters(prometheus::MetricFamily& bytes_family,
                     prometheus::MetricFamily& ops_family,
                     std::map<std::string, std::string> labels,
                     const EntityTotals& totals)
{
  labels["operation"] = "read";
  AddCounter(bytes_family, labels, totals.read_bytes);
  AddCounter(ops_family, labels, totals.read_ops);

  labels["operation"] = "write";
  AddCounter(bytes_family, labels, totals.write_bytes);
  AddCounter(ops_family, labels, totals.write_ops);
}

void
AddPolicyMetrics(prometheus::MetricFamily& family, const std::string& cluster,
                 const std::string& type, const std::string& id,
                 const TrafficShapingPolicy& policy)
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

  AddGauge(family, labels("limit", "read"),
           user_policy_value(policy.limit_read_bytes_per_sec));
  AddGauge(family, labels("limit", "write"),
           user_policy_value(policy.limit_write_bytes_per_sec));
  AddGauge(family, labels("reservation", "read"),
           user_policy_value(policy.reservation_read_bytes_per_sec));
  AddGauge(family, labels("reservation", "write"),
           user_policy_value(policy.reservation_write_bytes_per_sec));
  AddGauge(family, labels("controller_limit", "read"),
           static_cast<double>(policy.controller_limit_read_bytes_per_sec));
  AddGauge(family, labels("controller_limit", "write"),
           static_cast<double>(policy.controller_limit_write_bytes_per_sec));
}

class MonitoringCollector : public prometheus::Collectable {
public:
  MonitoringCollector(std::string cluster, const std::chrono::milliseconds cache_ttl)
      : mCluster(std::move(cluster))
      , mCacheTtlSeconds(
            std::chrono::duration_cast<std::chrono::duration<double>>(cache_ttl).count())
  {
  }

  std::vector<prometheus::MetricFamily>
  Collect() const override
  {
    auto cache_ttl =
        MakeGaugeFamily("eos_monit_cache_ttl_seconds",
                        "Monitoring Prometheus endpoint scrape cache TTL in seconds.");

    const std::map<std::string, std::string> labels{{"cluster", mCluster}};
    AddGauge(cache_ttl, labels, mCacheTtlSeconds);

    return {std::move(cache_ttl)};
  }

private:
  std::string mCluster;
  double mCacheTtlSeconds = 0.0;
};

class TrafficShapingCollector : public prometheus::Collectable {
public:
  TrafficShapingCollector(TrafficShapingEngine& engine, std::string cluster)
      : mEngine(engine)
      , mCluster(std::move(cluster))
  {
  }

  std::vector<prometheus::MetricFamily>
  Collect() const override
  {
    auto manager = mEngine.GetManager();

    if (!manager) {
      return {};
    }

    auto all_entries = MakeGaugeFamily("eos_io_shaping_all_entries",
                                       "Number of all-tags IO shaping entries available "
                                       "for export.");
    auto all_entries_exported =
        MakeGaugeFamily("eos_io_shaping_all_entries_exported",
                        "Number of all-tags IO shaping entries exported in this scrape.");
    auto all_entries_limited =
        MakeGaugeFamily("eos_io_shaping_all_entries_limited",
                        "All-tags IO shaping export limit status (1 if all-tags "
                        "metrics were suppressed, 0 otherwise).");
    auto bytes_total = MakeCounterFamily("eos_io_shaping_bytes_total",
                                         "Total IO shaping bytes observed");
    auto operations_total = MakeCounterFamily("eos_io_shaping_operations_total",
                                              "Total IO shaping operations observed");
    auto fs_bytes_total = MakeCounterFamily("eos_io_shaping_fs_bytes_total",
                                            "Total IO shaping filesystem bytes observed");
    auto fs_operations_total =
        MakeCounterFamily("eos_io_shaping_fs_operations_total",
                          "Total IO shaping filesystem operations observed");
    auto all_bytes_total = MakeCounterFamily("eos_io_shaping_all_bytes_total",
                                             "Total IO shaping all-tags bytes observed");
    auto all_operations_total =
        MakeCounterFamily("eos_io_shaping_all_operations_total",
                          "Total IO shaping all-tags operations observed");
    auto system_loop_duration = MakeHistogramFamily(
        "eos_io_shaping_loop_duration_seconds",
        "Cumulative traffic shaping loop duration distribution in seconds.");
    auto fsview_lock_duration = MakeHistogramFamily(
        "eos_io_shaping_fsview_lock_duration_seconds",
        "Cumulative traffic shaping FsView lock wait and hold duration distribution "
        "in seconds.");
    auto loop_iterations = MakeCounterFamily(
        "eos_io_shaping_loop_iterations_total",
        "Completed traffic shaping loop iterations since process start.");
    auto loop_last_completed = MakeGaugeFamily(
        "eos_io_shaping_loop_last_completed_timestamp_seconds",
        "Unix timestamp of the most recently completed traffic shaping loop "
        "iteration.");
    auto reports_processed = MakeGaugeFamily("eos_io_shaping_reports_processed_per_sec",
                                             "FST IO reports processed per second");
    auto report_queue_depth = MakeGaugeFamily(
        "eos_io_shaping_report_queue_depth",
        "Current number of FST IO reports waiting for traffic shaping processing.");
    auto report_queue_estimated_bytes =
        MakeGaugeFamily("eos_io_shaping_report_queue_estimated_bytes",
                        "Estimated memory footprint of queued FST IO reports.");
    auto reports_dropped = MakeCounterFamily(
        "eos_io_shaping_reports_dropped_total",
        "FST IO reports rejected or evicted by traffic shaping queue safety bounds.");
    auto stream_state_estimated_bytes = MakeGaugeFamily(
        "eos_io_shaping_stream_state_estimated_bytes",
        "Conservative estimated memory charged to admitted FST stream state.");
    auto estimated_memory_bytes = MakeGaugeFamily(
        "eos_io_shaping_estimated_memory_bytes",
        "Conservative estimated memory attributable to bounded traffic shaping "
        "stream state and queued reports; this is not allocator RSS.");
    auto memory_limit_bytes =
        MakeGaugeFamily("eos_io_shaping_memory_limit_bytes",
                        "Traffic shaping memory admission safety bound by component.");
    auto stream_state_limit_entries =
        MakeGaugeFamily("eos_io_shaping_stream_state_limit_entries",
                        "Maximum admitted FST stream states across all nodes.");
    auto stream_states_rejected = MakeCounterFamily(
        "eos_io_shaping_stream_states_rejected_total",
        "New FST streams rejected by traffic shaping state safety bounds.");
    auto garbage_collection_removed_entries = MakeCounterFamily(
        "eos_io_shaping_garbage_collection_removed_entries_total",
        "Traffic shaping runtime entries removed by garbage collection.");
    auto map_cardinality =
        MakeGaugeFamily("eos_io_shaping_map_cardinality",
                        "Traffic shaping internal map cardinality by map name.");
    auto policy_bytes = MakeGaugeFamily(
        "eos_io_shaping_policy_bytes",
        "Configured limits and reservations in bytes per second (0 if user policy is "
        "disabled, but controller limits bypass this)");
    auto app_io_pressure = MakeGaugeFamily(
        "eos_io_shaping_app_io_pressure",
        "Maximum node IO pressure observed by reserved application and operation.");
    auto app_io_pressure_sample = MakeGaugeFamily(
        "eos_io_shaping_app_io_pressure_sample",
        "Reserved application IO pressure sample availability (1 if present, 0 if "
        "absent).");
    auto app_node_io_pressure = MakeGaugeFamily(
        "eos_io_shaping_app_node_io_pressure",
        "Reservation IO pressure by recently observed application, node and pressure "
        "scope.");
    auto app_node_reservation_deficit_bytes = MakeGaugeFamily(
        "eos_io_shaping_app_node_reservation_deficit_bytes",
        "Reservation deficit by recently observed application, node and operation in "
        "bytes per second.");
    auto app_node_reservation_deficit_active = MakeGaugeFamily(
        "eos_io_shaping_app_node_reservation_deficit_active",
        "Reservation deficit activity by recently observed application, node and "
        "operation.");
    auto app_node_pressure_active = MakeGaugeFamily(
        "eos_io_shaping_app_node_pressure_active",
        "Reservation pressure activity by recently observed application, node and "
        "operation.");
    auto app_node_reservation_trigger_active = MakeGaugeFamily(
        "eos_io_shaping_app_node_reservation_trigger_active",
        "Reservation pressure that triggers competitor throttling by recently observed "
        "application, node and operation.");
    auto node_pressured_reservation_active =
        MakeGaugeFamily("eos_io_shaping_node_pressured_reservation_active",
                        "Node has at least one pressured reservation by operation.");
    auto node_controller_limit_bytes_per_second = MakeGaugeFamily(
        "eos_io_shaping_node_controller_limit_bytes_per_second",
        "Active node-local controller limit by application and operation.");
    auto node_controller_applied_reduction_bytes_per_second = MakeGaugeFamily(
        "eos_io_shaping_node_controller_applied_reduction_bytes_per_second",
        "Rate removed by the latest node-local controller action.");
    auto node_controller_protected_gain_bytes_per_second = MakeGaugeFamily(
        "eos_io_shaping_node_controller_protected_gain_bytes_per_second",
        "Protected rate gained in the latest evaluated controller response.");
    auto node_controller_response_ratio = MakeGaugeFamily(
        "eos_io_shaping_node_controller_response_ratio",
        "Conservative protected gain divided by assigned competitor reduction.");
    auto node_controller_cohort_apps = MakeGaugeFamily(
        "eos_io_shaping_node_controller_cohort_apps",
        "Number of protected applications in the active or failed response cohort.");
    auto node_controller_ineffective_probes = MakeGaugeFamily(
        "eos_io_shaping_node_controller_ineffective_probes",
        "Consecutive ineffective controller probes by node and operation.");
    auto node_controller_suppression_seconds = MakeGaugeFamily(
        "eos_io_shaping_node_controller_suppression_seconds",
        "Seconds remaining before an ineffective controller probe may retry.");
    auto node_controller_deficit_samples =
        MakeGaugeFamily("eos_io_shaping_node_controller_deficit_samples",
                        "Consecutive qualifying reservation-deficit samples.");
    auto node_controller_state = MakeGaugeFamily(
        "eos_io_shaping_node_controller_state",
        "Current controller phase by node and operation (value is always 1).");
    auto config_enabled = MakeGaugeFamily(
        "eos_io_shaping_config_enabled",
        "Traffic shaping configuration status (1 if enabled, 0 if disabled).");
    auto config_limits_enabled = MakeGaugeFamily(
        "eos_io_shaping_config_limits_enabled",
        "Traffic shaping limit enforcement status (1 if enabled, 0 if disabled).");
    auto config_reservations_enabled = MakeGaugeFamily(
        "eos_io_shaping_config_reservations_enabled",
        "Traffic shaping reservation enforcement status (1 if enabled, 0 if disabled).");
    auto config_controller_min_limit_bytes = MakeGaugeFamily(
        "eos_io_shaping_config_controller_min_limit_bytes",
        "Configured minimum controller-generated limit in bytes per second.");
    auto config_active_node_rate_threshold_bytes =
        MakeGaugeFamily("eos_io_shaping_config_active_node_rate_threshold_bytes",
                        "Configured active node rate threshold in bytes per second.");
    auto config_io_pressure_threshold =
        MakeGaugeFamily("eos_io_shaping_config_io_pressure_threshold",
                        "Configured IO pressure threshold for reservation pressure.");
    auto config_garbage_collection_idle_seconds = MakeGaugeFamily(
        "eos_io_shaping_config_garbage_collection_idle_seconds",
        "Configured idle time before traffic shaping runtime stats garbage collection.");
    auto config_estimators_update_period_ms = MakeGaugeFamily(
        "eos_io_shaping_config_estimators_update_period_milliseconds",
        "Configured IO shaping estimators update period in milliseconds.");
    auto config_fst_io_policy_update_period_ms =
        MakeGaugeFamily("eos_io_shaping_config_fst_io_policy_update_period_milliseconds",
                        "Configured FST IO policy update period in milliseconds.");
    auto config_fst_io_stats_reporting_period_ms = MakeGaugeFamily(
        "eos_io_shaping_config_fst_io_stats_reporting_period_milliseconds",
        "Configured FST IO stats reporting period in milliseconds.");
    auto config_detail_filesystem = MakeGaugeFamily(
        "eos_io_shaping_config_detail_filesystem",
        "Traffic shaping stats detail level (1 if filesystem, 0 otherwise).");
    auto config_detail_auto_enabled =
        MakeGaugeFamily("eos_io_shaping_config_detail_auto_enabled",
                        "Traffic shaping automatic stats detail level status "
                        "(1 if enabled, 0 if disabled).");
    auto config_detail_auto_low_cardinality = MakeGaugeFamily(
        "eos_io_shaping_config_detail_auto_low_cardinality",
        "Node stream cardinality at or below which automatic detail level selects "
        "filesystem detail.");
    auto config_detail_auto_high_cardinality = MakeGaugeFamily(
        "eos_io_shaping_config_detail_auto_high_cardinality",
        "Node stream cardinality above which automatic detail level selects aggregate "
        "detail.");
    auto config_system_stats_time_window_sec =
        MakeGaugeFamily("eos_io_shaping_config_system_stats_time_window_seconds",
                        "Configured IO shaping system stats time window in seconds.");
    auto ns_traffic_shaping_enabled = MakeGaugeFamily(
        "eos_ns_traffic_shaping_enabled",
        "Traffic shaping status reported by eos ns stat (1 if enabled, 0 if disabled).");

    AddCounterFamilies(*manager, bytes_total, operations_total, fs_bytes_total,
                       fs_operations_total, all_bytes_total, all_operations_total,
                       all_entries, all_entries_exported, all_entries_limited);
    AddSystemFamilies(
        *manager, system_loop_duration, fsview_lock_duration, loop_iterations,
        loop_last_completed, reports_processed, report_queue_depth,
        report_queue_estimated_bytes, reports_dropped, stream_state_estimated_bytes,
        estimated_memory_bytes, memory_limit_bytes, stream_state_limit_entries,
        stream_states_rejected, garbage_collection_removed_entries, map_cardinality);
    AddPolicyFamilies(*manager, policy_bytes);
    AddPressureFamilies(*manager, app_io_pressure, app_io_pressure_sample,
                        app_node_io_pressure, app_node_reservation_deficit_bytes,
                        app_node_reservation_deficit_active, app_node_pressure_active,
                        app_node_reservation_trigger_active,
                        node_pressured_reservation_active);
    AddControllerFamilies(*manager, node_controller_limit_bytes_per_second,
                          node_controller_applied_reduction_bytes_per_second,
                          node_controller_protected_gain_bytes_per_second,
                          node_controller_response_ratio, node_controller_cohort_apps,
                          node_controller_ineffective_probes,
                          node_controller_suppression_seconds,
                          node_controller_deficit_samples, node_controller_state);
    AddConfigFamilies(
        config_enabled, config_estimators_update_period_ms,
        config_fst_io_policy_update_period_ms, config_fst_io_stats_reporting_period_ms,
        config_detail_filesystem, config_detail_auto_enabled,
        config_detail_auto_low_cardinality, config_detail_auto_high_cardinality,
        config_system_stats_time_window_sec, config_limits_enabled,
        config_reservations_enabled, config_controller_min_limit_bytes,
        config_active_node_rate_threshold_bytes, config_io_pressure_threshold,
        config_garbage_collection_idle_seconds, ns_traffic_shaping_enabled);

    std::vector<prometheus::MetricFamily> metrics = {
        std::move(bytes_total),
        std::move(operations_total),
        std::move(fs_bytes_total),
        std::move(fs_operations_total),
        std::move(all_bytes_total),
        std::move(all_operations_total),
        std::move(all_entries),
        std::move(all_entries_exported),
        std::move(all_entries_limited),
        std::move(system_loop_duration),
        std::move(fsview_lock_duration),
        std::move(loop_iterations),
        std::move(loop_last_completed),
        std::move(reports_processed),
        std::move(report_queue_depth),
        std::move(report_queue_estimated_bytes),
        std::move(reports_dropped),
        std::move(stream_state_estimated_bytes),
        std::move(estimated_memory_bytes),
        std::move(memory_limit_bytes),
        std::move(stream_state_limit_entries),
        std::move(stream_states_rejected),
        std::move(garbage_collection_removed_entries),
        std::move(map_cardinality),
        std::move(policy_bytes),
        std::move(app_io_pressure),
        std::move(app_io_pressure_sample),
        std::move(app_node_io_pressure),
        std::move(app_node_reservation_deficit_bytes),
        std::move(app_node_reservation_deficit_active),
        std::move(app_node_pressure_active),
        std::move(app_node_reservation_trigger_active),
        std::move(node_pressured_reservation_active),
        std::move(node_controller_limit_bytes_per_second),
        std::move(node_controller_applied_reduction_bytes_per_second),
        std::move(node_controller_protected_gain_bytes_per_second),
        std::move(node_controller_response_ratio),
        std::move(node_controller_cohort_apps),
        std::move(node_controller_ineffective_probes),
        std::move(node_controller_suppression_seconds),
        std::move(node_controller_deficit_samples),
        std::move(node_controller_state),
        std::move(config_enabled),
        std::move(config_limits_enabled),
        std::move(config_reservations_enabled),
        std::move(config_controller_min_limit_bytes),
        std::move(config_active_node_rate_threshold_bytes),
        std::move(config_io_pressure_threshold),
        std::move(config_garbage_collection_idle_seconds),
        std::move(config_estimators_update_period_ms),
        std::move(config_fst_io_policy_update_period_ms),
        std::move(config_fst_io_stats_reporting_period_ms),
        std::move(config_detail_filesystem),
        std::move(config_detail_auto_enabled),
        std::move(config_detail_auto_low_cardinality),
        std::move(config_detail_auto_high_cardinality),
        std::move(config_system_stats_time_window_sec),
        std::move(ns_traffic_shaping_enabled)};

    return metrics;
  }

private:
  void
  AddCounterFamilies(TrafficShapingManager& manager,
                     prometheus::MetricFamily& bytes_total,
                     prometheus::MetricFamily& operations_total,
                     prometheus::MetricFamily& fs_bytes_total,
                     prometheus::MetricFamily& fs_operations_total,
                     prometheus::MetricFamily& all_bytes_total,
                     prometheus::MetricFamily& all_operations_total,
                     prometheus::MetricFamily& all_entries,
                     prometheus::MetricFamily& all_entries_exported,
                     prometheus::MetricFamily& all_entries_limited) const
  {
    std::map<StandardKey, EntityTotals> standard_totals;
    const auto all_entries_available = GetAllTagsEntryCount(manager);
    const bool export_all_tags = all_entries_available <= kMaxAllTagsMetricEntries;

    AddGauge(all_entries, {{"cluster", mCluster}},
             static_cast<double>(all_entries_available));
    AddGauge(all_entries_limited, {{"cluster", mCluster}}, export_all_tags ? 0.0 : 1.0);

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

    if (export_all_tags) {
      const auto all_totals = CollectAllTotals(manager);
      AddGauge(all_entries_exported, {{"cluster", mCluster}},
               static_cast<double>(all_totals.size()));

      for (const auto& [key, totals] : all_totals) {
        const std::string uid_label = UidLabel(key.uid);
        const std::string gid_label = GidLabel(key.gid);
        AddReadWriteCounters(all_bytes_total, all_operations_total,
                             {{"cluster", mCluster},
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
      AddGauge(all_entries_exported, {{"cluster", mCluster}}, 0.0);
    }

    for (const auto& [node_id, snapshot] : projection_totals.node) {
      standard_totals[{"node", NodeLabel(LabelOrUnknown(node_id))}].Add(snapshot);
    }

    for (const auto& [key, totals] : standard_totals) {
      AddReadWriteCounters(bytes_total, operations_total,
                           {{"cluster", mCluster}, {"type", key.type}, {"id", key.id}},
                           totals);
    }

    for (const auto& [key, snapshot] : manager.GetDiskCumulativeStats()) {
      EntityTotals totals;
      totals.Add(snapshot);
      AddReadWriteCounters(fs_bytes_total, fs_operations_total,
                           {{"cluster", mCluster},
                            {"node_id", NodeLabel(LabelOrUnknown(key.node_id))},
                            {"fsid", std::to_string(key.fsid)}},
                           totals);
    }
  }

  std::size_t
  GetAllTagsEntryCount(TrafficShapingManager& manager) const
  {
    const auto cardinality = manager.GetMapCardinalityStats();

    if (mEngine.GetDetailLevel() ==
        eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
      return static_cast<std::size_t>(cardinality.detailed_cumulative_stats);
    }

    return static_cast<std::size_t>(cardinality.global_cumulative_stats);
  }

  std::map<AllKey, EntityTotals>
  CollectAllTotals(TrafficShapingManager& manager) const
  {
    std::map<AllKey, EntityTotals> all_totals;

    if (mEngine.GetDetailLevel() ==
        eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM) {
      for (const auto& [detailed_key, snapshot] : manager.GetDetailedCumulativeStats()) {
        AllKey key{NodeLabel(LabelOrUnknown(detailed_key.node_id)),
                   detailed_key.stream.fsid, LabelOrUnknown(detailed_key.stream.app),
                   detailed_key.stream.uid, detailed_key.stream.gid};
        all_totals[key].Add(snapshot);
      }
    } else {
      for (const auto& [key, snapshot] : manager.GetGlobalCumulativeStats()) {
        AllKey all_key{eos::common::traffic_shaping::kUnknownId, 0,
                       LabelOrUnknown(key.app), key.uid, key.gid};
        all_totals[all_key].Add(snapshot);
      }
    }

    return all_totals;
  }

  void
  AddSystemFamilies(TrafficShapingManager& manager,
                    prometheus::MetricFamily& system_loop_duration,
                    prometheus::MetricFamily& fsview_lock_duration,
                    prometheus::MetricFamily& loop_iterations,
                    prometheus::MetricFamily& loop_last_completed,
                    prometheus::MetricFamily& reports_processed,
                    prometheus::MetricFamily& report_queue_depth,
                    prometheus::MetricFamily& report_queue_estimated_bytes,
                    prometheus::MetricFamily& reports_dropped,
                    prometheus::MetricFamily& stream_state_estimated_bytes,
                    prometheus::MetricFamily& estimated_memory_bytes,
                    prometheus::MetricFamily& memory_limit_bytes,
                    prometheus::MetricFamily& stream_state_limit_entries,
                    prometheus::MetricFamily& stream_states_rejected,
                    prometheus::MetricFamily& garbage_collection_removed_entries,
                    prometheus::MetricFamily& map_cardinality) const
  {
    const auto timing = manager.GetSystemTimingSnapshot();
    auto add_loop_timing = [this, &system_loop_duration, &loop_iterations,
                            &loop_last_completed](const std::string& loop_name,
                                                  const LoopTimingSnapshot& loop) {
      const std::map<std::string, std::string> labels{{"cluster", mCluster},
                                                      {"loop_name", loop_name}};
      AddHistogram(system_loop_duration, labels, loop.duration);
      AddCounter(loop_iterations, labels, loop.iterations_total);
      AddGauge(loop_last_completed, labels,
               static_cast<double>(loop.last_completed_timestamp_seconds));
    };
    add_loop_timing("estimators", timing.estimators);
    add_loop_timing("reservation_controller", timing.reservation_controller);
    add_loop_timing("fst_limits", timing.fst_limits);
    add_loop_timing("garbage_collection", timing.garbage_collection);
    AddHistogram(fsview_lock_duration, {{"cluster", mCluster}, {"phase", "wait"}},
                 timing.fsview_lock_wait);
    AddHistogram(fsview_lock_duration, {{"cluster", mCluster}, {"phase", "hold"}},
                 timing.fsview_lock_hold);
    AddGauge(reports_processed, {{"cluster", mCluster}, {"stat", "mean"}},
             manager.GetFstReportsProcessedPerSecondMean());
    AddGauge(report_queue_depth, {{"cluster", mCluster}},
             static_cast<double>(manager.GetFstReportQueueDepth()));
    const auto memory = manager.GetMemoryStats();
    AddGauge(report_queue_estimated_bytes, {{"cluster", mCluster}},
             static_cast<double>(memory.report_queue_estimated_bytes));
    AddCounter(reports_dropped, {{"cluster", mCluster}},
               manager.GetFstReportsDroppedTotal());

    AddGauge(stream_state_estimated_bytes, {{"cluster", mCluster}},
             static_cast<double>(memory.stream_state_estimated_bytes));
    AddGauge(estimated_memory_bytes, {{"cluster", mCluster}},
             static_cast<double>(memory.estimated_bytes));
    AddGauge(memory_limit_bytes, {{"cluster", mCluster}, {"component", "total"}},
             static_cast<double>(memory.limit_bytes));
    AddGauge(memory_limit_bytes, {{"cluster", mCluster}, {"component", "stream_state"}},
             static_cast<double>(memory.stream_state_limit_bytes));
    AddGauge(memory_limit_bytes, {{"cluster", mCluster}, {"component", "report_queue"}},
             static_cast<double>(memory.report_queue_limit_bytes));
    AddGauge(stream_state_limit_entries, {{"cluster", mCluster}},
             static_cast<double>(memory.stream_state_limit_entries));

    const auto cardinality = manager.GetMapCardinalityStats();
    AddCounter(stream_states_rejected, {{"cluster", mCluster}},
               cardinality.node_state_rejections_total);
    AddCounter(garbage_collection_removed_entries,
               {{"cluster", mCluster}, {"map", "nodes"}},
               cardinality.garbage_collection_removed_nodes_total);
    AddCounter(garbage_collection_removed_entries,
               {{"cluster", mCluster}, {"map", "node_streams"}},
               cardinality.garbage_collection_removed_node_streams_total);
    AddCounter(garbage_collection_removed_entries,
               {{"cluster", mCluster}, {"map", "global_streams"}},
               cardinality.garbage_collection_removed_global_streams_total);
    AddCounter(garbage_collection_removed_entries,
               {{"cluster", mCluster}, {"map", "disk_stats"}},
               cardinality.garbage_collection_removed_disk_stats_total);
    AddCounter(garbage_collection_removed_entries,
               {{"cluster", mCluster}, {"map", "detailed_stats"}},
               cardinality.garbage_collection_removed_detailed_stats_total);
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "node_states"}},
             static_cast<double>(cardinality.node_states));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "node_state_streams"}},
             static_cast<double>(cardinality.node_state_streams));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "global_stats"}},
             static_cast<double>(cardinality.global_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "node_stats"}},
             static_cast<double>(cardinality.node_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "disk_stats"}},
             static_cast<double>(cardinality.disk_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "detailed_stats"}},
             static_cast<double>(cardinality.detailed_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "global_cumulative_stats"}},
             static_cast<double>(cardinality.global_cumulative_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "node_cumulative_stats"}},
             static_cast<double>(cardinality.node_cumulative_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "disk_cumulative_stats"}},
             static_cast<double>(cardinality.disk_cumulative_stats));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "detailed_cumulative_stats"}},
             static_cast<double>(cardinality.detailed_cumulative_stats));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "projection_app_cumulative_stats"}},
             static_cast<double>(cardinality.projection_app_cumulative_stats));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "projection_uid_cumulative_stats"}},
             static_cast<double>(cardinality.projection_uid_cumulative_stats));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "projection_gid_cumulative_stats"}},
             static_cast<double>(cardinality.projection_gid_cumulative_stats));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "projection_node_cumulative_stats"}},
             static_cast<double>(cardinality.projection_node_cumulative_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "node_entity_stats"}},
             static_cast<double>(cardinality.node_entity_stats));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "uid_policies"}},
             static_cast<double>(cardinality.uid_policies));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "gid_policies"}},
             static_cast<double>(cardinality.gid_policies));
    AddGauge(map_cardinality, {{"cluster", mCluster}, {"map", "app_policies"}},
             static_cast<double>(cardinality.app_policies));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "node_fst_io_delay_configs"}},
             static_cast<double>(cardinality.node_fst_io_delay_configs));
    AddGauge(map_cardinality,
             {{"cluster", mCluster}, {"map", "published_fst_io_delay_configs"}},
             static_cast<double>(cardinality.published_fst_io_delay_configs));
  }

  void
  AddPolicyFamilies(TrafficShapingManager& manager,
                    prometheus::MetricFamily& policy_bytes) const
  {
    for (const auto& [app, policy] : manager.GetAppPolicies()) {
      AddPolicyMetrics(policy_bytes, mCluster, "app", LabelOrUnknown(app), policy);
    }

    for (const auto& [uid, policy] : manager.GetUidPolicies()) {
      AddPolicyMetrics(policy_bytes, mCluster, "uid", UidLabel(uid), policy);
    }

    for (const auto& [gid, policy] : manager.GetGidPolicies()) {
      AddPolicyMetrics(policy_bytes, mCluster, "gid", GidLabel(gid), policy);
    }
  }

  void
  AddPressureFamilies(TrafficShapingManager& manager,
                      prometheus::MetricFamily& app_io_pressure,
                      prometheus::MetricFamily& app_io_pressure_sample,
                      prometheus::MetricFamily& app_node_io_pressure,
                      prometheus::MetricFamily& app_node_reservation_deficit_bytes,
                      prometheus::MetricFamily& app_node_reservation_deficit_active,
                      prometheus::MetricFamily& app_node_pressure_active,
                      prometheus::MetricFamily& app_node_reservation_trigger_active,
                      prometheus::MetricFamily& node_pressured_reservation_active) const
  {
    std::map<std::string, AppIoPressureSnapshot> app_pressure_by_app;

    for (const auto& [app, policy] : manager.GetAppPolicies()) {
      if (policy.is_enabled && (policy.reservation_read_bytes_per_sec > 0 ||
                                policy.reservation_write_bytes_per_sec > 0)) {
        app_pressure_by_app[LabelOrUnknown(app)] = {};
      }
    }

    std::map<std::pair<std::string, std::string>, bool> node_pressure_flags;
    std::vector<std::string> online_nodes;
    const auto node_pressure_snapshots =
        manager.GetReservedAppNodeIoPressure(&online_nodes);
    for (const auto& node : online_nodes) {
      const std::string node_label = NodeLabel(LabelOrUnknown(node));
      node_pressure_flags[{node_label, "read"}] = false;
      node_pressure_flags[{node_label, "write"}] = false;
    }

    for (const auto& snapshot : node_pressure_snapshots) {
      const std::string node_label = NodeLabel(LabelOrUnknown(snapshot.node_id));
      const std::map<std::string, std::string> base_labels{
          {"cluster", mCluster},
          {"app", LabelOrUnknown(snapshot.app)},
          {"node_id", node_label}};
      auto& app_pressure = app_pressure_by_app[LabelOrUnknown(snapshot.app)];

      if (snapshot.has_node_io_pressure) {
        auto labels = base_labels;
        labels["scope"] = "node";
        AddGauge(app_node_io_pressure, labels, snapshot.node_io_pressure);
      }

      auto add_operation_metrics = [&](const std::string& operation,
                                       const bool has_io_pressure,
                                       const bool pressure_active,
                                       const bool deficit_active,
                                       const bool triggers_competitor_throttling,
                                       const double deficit_bps) {
        auto labels = base_labels;
        labels["operation"] = operation;
        AddGauge(app_node_reservation_deficit_bytes, labels, deficit_bps);
        AddGauge(app_node_reservation_deficit_active, labels, deficit_active ? 1.0 : 0.0);
        AddGauge(app_node_pressure_active, labels, pressure_active ? 1.0 : 0.0);
        AddGauge(app_node_reservation_trigger_active, labels,
                 triggers_competitor_throttling ? 1.0 : 0.0);

        if (has_io_pressure) {
          auto pressure_labels = base_labels;
          pressure_labels["scope"] = operation;
          AddGauge(app_node_io_pressure, pressure_labels, snapshot.node_io_pressure);

          if (operation == "read") {
            app_pressure.read = std::max(app_pressure.read, snapshot.node_io_pressure);
            app_pressure.has_read = true;
          } else {
            app_pressure.write = std::max(app_pressure.write, snapshot.node_io_pressure);
            app_pressure.has_write = true;
          }
        }
      };

      add_operation_metrics("read", snapshot.has_read_io_pressure,
                            snapshot.read_pressure_active,
                            snapshot.read_reservation_deficit_active,
                            snapshot.read_triggers_competitor_throttling,
                            snapshot.read_reservation_deficit_bps);
      add_operation_metrics("write", snapshot.has_write_io_pressure,
                            snapshot.write_pressure_active,
                            snapshot.write_reservation_deficit_active,
                            snapshot.write_triggers_competitor_throttling,
                            snapshot.write_reservation_deficit_bps);

      node_pressure_flags[{node_label, "read"}] =
          node_pressure_flags[{node_label, "read"}] ||
          snapshot.node_has_pressured_read_reservation;
      node_pressure_flags[{node_label, "write"}] =
          node_pressure_flags[{node_label, "write"}] ||
          snapshot.node_has_pressured_write_reservation;
    }

    for (const auto& [key, active] : node_pressure_flags) {
      AddGauge(node_pressured_reservation_active,
               {{"cluster", mCluster}, {"node_id", key.first}, {"operation", key.second}},
               active ? 1.0 : 0.0);
    }

    for (const auto& [app, pressure] : app_pressure_by_app) {
      auto labels =
          std::map<std::string, std::string>{{"cluster", mCluster}, {"app", app}};

      labels["operation"] = "read";
      AddGauge(app_io_pressure_sample, labels, pressure.has_read ? 1.0 : 0.0);
      if (pressure.has_read) {
        AddGauge(app_io_pressure, labels, pressure.read);
      }

      labels["operation"] = "write";
      AddGauge(app_io_pressure_sample, labels, pressure.has_write ? 1.0 : 0.0);
      if (pressure.has_write) {
        AddGauge(app_io_pressure, labels, pressure.write);
      }
    }
  }

  void
  AddControllerFamilies(
      TrafficShapingManager& manager,
      prometheus::MetricFamily& node_controller_limit_bytes_per_second,
      prometheus::MetricFamily& node_controller_applied_reduction_bytes_per_second,
      prometheus::MetricFamily& node_controller_protected_gain_bytes_per_second,
      prometheus::MetricFamily& node_controller_response_ratio,
      prometheus::MetricFamily& node_controller_cohort_apps,
      prometheus::MetricFamily& node_controller_ineffective_probes,
      prometheus::MetricFamily& node_controller_suppression_seconds,
      prometheus::MetricFamily& node_controller_deficit_samples,
      prometheus::MetricFamily& node_controller_state) const
  {
    const auto snapshot = manager.GetNodeReservationControllerSnapshot();

    for (const auto& limit : snapshot.limits) {
      auto labels = std::map<std::string, std::string>{
          {"cluster", mCluster},
          {"node_id", NodeLabel(LabelOrUnknown(limit.node_id))},
          {"app", LabelOrUnknown(limit.app)}};
      if (limit.read_bytes_per_sec > 0) {
        labels["operation"] = "read";
        AddGauge(node_controller_limit_bytes_per_second, labels,
                 static_cast<double>(limit.read_bytes_per_sec));
      }
      if (limit.write_bytes_per_sec > 0) {
        labels["operation"] = "write";
        AddGauge(node_controller_limit_bytes_per_second, labels,
                 static_cast<double>(limit.write_bytes_per_sec));
      }
    }

    for (const auto& feedback : snapshot.feedback) {
      auto labels = std::map<std::string, std::string>{
          {"cluster", mCluster},
          {"node_id", NodeLabel(LabelOrUnknown(feedback.node_id))},
          {"operation", feedback.is_write ? "write" : "read"}};
      AddGauge(node_controller_applied_reduction_bytes_per_second, labels,
               feedback.applied_reduction_bps);
      AddGauge(node_controller_protected_gain_bytes_per_second, labels,
               feedback.observed_protected_gain_bps);
      AddGauge(node_controller_response_ratio, labels, feedback.response_ratio);
      AddGauge(node_controller_ineffective_probes, labels,
               static_cast<double>(feedback.ineffective_probe_count));
      AddGauge(node_controller_suppression_seconds, labels,
               feedback.suppression_remaining_seconds);
      AddGauge(node_controller_deficit_samples, labels,
               static_cast<double>(feedback.consecutive_deficit_samples));

      auto cohort_labels = labels;
      cohort_labels["cohort"] = "active";
      AddGauge(node_controller_cohort_apps, cohort_labels,
               static_cast<double>(feedback.protected_app_count));
      cohort_labels["cohort"] = "failed";
      AddGauge(node_controller_cohort_apps, cohort_labels,
               static_cast<double>(feedback.failed_protected_app_count));

      std::string phase = feedback.phase;
      if (phase.empty()) {
        phase = "idle";
        if (feedback.suppressed) {
          phase = "suppressed";
        } else if (feedback.awaiting_response) {
          phase = "awaiting_response";
        } else if (feedback.consecutive_deficit_samples > 0) {
          phase = "qualifying";
        } else if (feedback.applied_reduction_bps > 0.0) {
          phase = "holding";
        }
      }
      labels["phase"] = phase;
      AddGauge(node_controller_state, labels, 1.0);
    }
  }

  void
  AddConfigFamilies(prometheus::MetricFamily& config_enabled,
                    prometheus::MetricFamily& config_estimators_update_period_ms,
                    prometheus::MetricFamily& config_fst_io_policy_update_period_ms,
                    prometheus::MetricFamily& config_fst_io_stats_reporting_period_ms,
                    prometheus::MetricFamily& config_detail_filesystem,
                    prometheus::MetricFamily& config_detail_auto_enabled,
                    prometheus::MetricFamily& config_detail_auto_low_cardinality,
                    prometheus::MetricFamily& config_detail_auto_high_cardinality,
                    prometheus::MetricFamily& config_system_stats_time_window_sec,
                    prometheus::MetricFamily& config_limits_enabled,
                    prometheus::MetricFamily& config_reservations_enabled,
                    prometheus::MetricFamily& config_controller_min_limit_bytes,
                    prometheus::MetricFamily& config_active_node_rate_threshold_bytes,
                    prometheus::MetricFamily& config_io_pressure_threshold,
                    prometheus::MetricFamily& config_garbage_collection_idle_seconds,
                    prometheus::MetricFamily& ns_traffic_shaping_enabled) const
  {
    const std::map<std::string, std::string> labels{{"cluster", mCluster}};
    const double enabled = mEngine.IsEnabled() ? 1.0 : 0.0;

    AddGauge(config_enabled, labels, enabled);
    AddGauge(ns_traffic_shaping_enabled, labels, enabled);
    AddGauge(config_limits_enabled, labels, mEngine.GetLimitsEnabled() ? 1.0 : 0.0);
    AddGauge(config_reservations_enabled, labels,
             mEngine.GetReservationsEnabled() ? 1.0 : 0.0);
    AddGauge(config_controller_min_limit_bytes, labels,
             static_cast<double>(mEngine.GetControllerMinLimit()));
    AddGauge(config_active_node_rate_threshold_bytes, labels,
             static_cast<double>(mEngine.GetActiveNodeRateThreshold()));
    AddGauge(config_io_pressure_threshold, labels, mEngine.GetIoPressureThreshold());
    AddGauge(config_garbage_collection_idle_seconds, labels,
             static_cast<double>(mEngine.GetGarbageCollectionIdleSeconds()));
    AddGauge(config_estimators_update_period_ms, labels,
             static_cast<double>(mEngine.GetEstimatorsUpdateThreadPeriodMilliseconds()));
    AddGauge(config_fst_io_policy_update_period_ms, labels,
             static_cast<double>(mEngine.GetFstIoPolicyUpdateThreadPeriodMilliseconds()));
    AddGauge(config_fst_io_stats_reporting_period_ms, labels,
             static_cast<double>(mEngine.GetFstIoStatsReportThreadPeriodMilliseconds()));
    AddGauge(config_detail_filesystem, labels,
             mEngine.GetDetailLevel() ==
                     eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM
                 ? 1.0
                 : 0.0);
    AddGauge(config_detail_auto_enabled, labels,
             mEngine.GetAutomaticDetailLevelEnabled() ? 1.0 : 0.0);
    AddGauge(config_detail_auto_low_cardinality, labels,
             static_cast<double>(mEngine.GetAutomaticDetailLevelLowCardinality()));
    AddGauge(config_detail_auto_high_cardinality, labels,
             static_cast<double>(mEngine.GetAutomaticDetailLevelHighCardinality()));
    AddGauge(config_system_stats_time_window_sec, labels,
             static_cast<double>(mEngine.GetSystemStatsWindowSeconds()));
  }

  TrafficShapingEngine& mEngine;
  std::string mCluster;
};

} // namespace

PrometheusExporter::PrometheusExporter(std::string bind_address,
                                       TrafficShapingEngine& engine, std::string cluster,
                                       const std::chrono::milliseconds cache_ttl,
                                       std::function<bool()> should_collect)
    : mExposer(std::make_unique<prometheus::Exposer>(std::move(bind_address),
                                                     kPrometheusExporterThreads))
{
  auto monitoring_collector = std::make_shared<MasterOnlyCollectable>(
      std::make_shared<MonitoringCollector>(cluster, cache_ttl), should_collect);
  auto traffic_shaping_collector = std::make_shared<MasterOnlyCollectable>(
      std::make_shared<CachedCollectable>(
          std::make_shared<TrafficShapingCollector>(engine, std::move(cluster)),
          cache_ttl),
      std::move(should_collect));

  mCollectors.emplace_back(std::move(monitoring_collector));
  mCollectors.emplace_back(std::move(traffic_shaping_collector));

  for (const auto& collector : mCollectors) {
    mExposer->RegisterCollectable(collector);
  }
}

PrometheusExporter::~PrometheusExporter() = default;

} // namespace eos::mgm::monitoring
