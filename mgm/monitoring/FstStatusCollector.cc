#include "mgm/monitoring/FstStatusCollector.hh"

#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "common/mq/SharedHashWrapper.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "prometheus/client_metric.h"
#include "prometheus/metric_type.h"

#include <charconv>
#include <chrono>
#include <map>
#include <utility>

namespace eos::mgm::monitoring {
namespace {

constexpr const char* kUnknownStatus = "unknown";

struct NodeStatusSource {
  std::string node_id;
  std::string active_status;
  common::SharedHashLocator hash_locator;
  std::size_t filesystem_count = 0;
};

struct FilesystemStatusSource {
  std::string node_id;
  std::uint64_t fsid = 0;
  common::SharedHashLocator hash_locator;
  common::ActiveStatus active_status = common::ActiveStatus::kUndefined;
};

prometheus::MetricFamily
MakeGaugeFamily(const std::string& name, const std::string& help)
{
  prometheus::MetricFamily family;
  family.name = name;
  family.help = help;
  family.type = prometheus::MetricType::Gauge;
  return family;
}

void
AddGauge(prometheus::MetricFamily& family,
         const std::map<std::string, std::string>& labels, const double value)
{
  prometheus::ClientMetric metric;
  metric.gauge.value = value;

  for (const auto& [name, label_value] : labels) {
    metric.label.push_back({name, label_value});
  }

  family.metric.push_back(std::move(metric));
}

std::string
ValueOrUnknown(const std::map<std::string, std::string>& values, const std::string& key)
{
  const auto it = values.find(key);
  return it == values.end() || it->second.empty() ? kUnknownStatus : it->second;
}

std::string
CanonicalConfigStatus(const std::map<std::string, std::string>& values)
{
  const auto raw = ValueOrUnknown(values, "configstatus");
  return common::FileSystem::GetConfigStatusAsString(
      common::FileSystem::GetConfigStatusFromString(raw.c_str()));
}

std::string
CanonicalDrainStatus(const std::map<std::string, std::string>& values)
{
  const auto raw = ValueOrUnknown(values, "local.drain");

  if (raw == "wait" || raw == "waiting") {
    return "waiting";
  }

  if (raw == "nodrain" || raw == "prepare" || raw == "draining" || raw == "drained" ||
      raw == "stalling" || raw == "expired" || raw == "failed") {
    return raw;
  }

  return kUnknownStatus;
}

std::string
CanonicalBootStatus(const std::map<std::string, std::string>& values)
{
  const auto raw = ValueOrUnknown(values, "stat.boot");

  if (raw == "down" || raw == "opserror" || raw == "bootfailure" || raw == "bootsent" ||
      raw == "booting" || raw == "booted") {
    return raw;
  }

  return kUnknownStatus;
}

std::optional<std::uint64_t>
NonNegativeInteger(const std::map<std::string, std::string>& values,
                   const std::string& key)
{
  const auto it = values.find(key);

  if (it == values.end() || it->second.empty()) {
    return std::nullopt;
  }

  std::uint64_t value = 0;
  const auto* begin = it->second.data();
  const auto* end = begin + it->second.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, value);
  return error == std::errc{} && parsed_end == end ? std::optional<std::uint64_t>{value}
                                                   : std::nullopt;
}

FstStatusSnapshot
CollectFstStatusSnapshot()
{
  const auto collection_started = std::chrono::steady_clock::now();
  std::vector<NodeStatusSource> node_sources;
  std::vector<FilesystemStatusSource> filesystem_sources;

  {
    common::RWMutexReadLock view_lock(FsView::gFsView.ViewMutex);
    node_sources.reserve(FsView::gFsView.mNodeView.size());
    filesystem_sources.reserve(FsView::gFsView.mIdView.size());

    // Copy only stable identity and atomic state while FsView protects object
    // lifetimes. Shared-hash reads happen below, after releasing ViewMutex.
    for (const auto& [node_id, node] : FsView::gFsView.mNodeView) {
      if (!node) {
        continue;
      }

      node_sources.push_back({node_id, node->GetStatus(),
                              common::SharedHashLocator::makeForNode(node_id),
                              node->size()});
    }

    // Walk the registry independently so a temporarily inconsistent node view
    // cannot hide a registered filesystem from operational monitoring.
    for (const auto& [fsid, fs] : FsView::gFsView.mIdView) {
      if (!fs) {
        continue;
      }

      filesystem_sources.push_back({fs->GetQueue(), static_cast<std::uint64_t>(fsid),
                                    fs->getHashLocator(), fs->GetActiveStatus()});
    }
  }

  FstStatusSnapshot snapshot;
  snapshot.nodes.reserve(node_sources.size());
  snapshot.filesystems.reserve(filesystem_sources.size());

  static const std::vector<std::string> node_keys{"status", "stat.geotag"};
  for (const auto& source : node_sources) {
    std::map<std::string, std::string> values;
    mq::SharedHashWrapper hash(gOFS->mMessagingRealm.get(), source.hash_locator, true,
                               false);
    hash.get(node_keys, values);
    snapshot.nodes.push_back(
        {source.node_id,
         source.active_status.empty() ? kUnknownStatus : source.active_status,
         ValueOrUnknown(values, "status"),
         values.count("stat.geotag") ? values["stat.geotag"] : std::string{},
         source.filesystem_count});
  }

  static const std::vector<std::string> filesystem_keys{
      "configstatus", "local.drain", "stat.boot", "stat.statfs.capacity",
      "stat.statfs.usedbytes"};
  for (const auto& source : filesystem_sources) {
    std::map<std::string, std::string> values;
    mq::SharedHashWrapper hash(gOFS->mMessagingRealm.get(), source.hash_locator, true,
                               false);
    hash.get(filesystem_keys, values);
    snapshot.filesystems.push_back(
        {source.node_id, source.fsid,
         common::FileSystem::GetActiveStatusAsString(source.active_status),
         CanonicalConfigStatus(values), CanonicalDrainStatus(values),
         CanonicalBootStatus(values), NonNegativeInteger(values, "stat.statfs.capacity"),
         NonNegativeInteger(values, "stat.statfs.usedbytes")});
  }

  snapshot.collected_timestamp_seconds =
      static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
  snapshot.collection_duration_seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - collection_started)
          .count();
  return snapshot;
}

} // namespace

std::vector<prometheus::MetricFamily>
BuildFstStatusMetricFamilies(const FstStatusSnapshot& snapshot,
                             const std::string& cluster)
{
  auto node_status =
      MakeGaugeFamily("eos_fst_node_status_info",
                      "Current FST node active and configured status as labels.");
  auto node_filesystems = MakeGaugeFamily(
      "eos_fst_node_filesystems", "Number of filesystems registered to an FST node.");
  auto filesystem_status = MakeGaugeFamily(
      "eos_fst_filesystem_status_info",
      "Current filesystem active, configured, drain, and boot status as labels.");
  auto filesystem_capacity = MakeGaugeFamily("eos_fst_filesystem_capacity_bytes",
                                             "Current filesystem capacity in bytes.");
  auto filesystem_used = MakeGaugeFamily("eos_fst_filesystem_used_bytes",
                                         "Current filesystem used space in bytes.");
  auto snapshot_timestamp = MakeGaugeFamily(
      "eos_fst_status_snapshot_timestamp_seconds",
      "Unix timestamp of the FST status snapshot exposed by this scrape.");
  auto collection_duration = MakeGaugeFamily(
      "eos_fst_status_collection_duration_seconds",
      "Time spent collecting the current FST status snapshot in seconds.");

  for (const auto& node : snapshot.nodes) {
    const std::map<std::string, std::string> labels{{"active_status", node.active_status},
                                                    {"cluster", cluster},
                                                    {"config_status", node.config_status},
                                                    {"geotag", node.geotag},
                                                    {"node_id", node.node_id}};
    AddGauge(node_status, labels, 1.0);
    AddGauge(node_filesystems, {{"cluster", cluster}, {"node_id", node.node_id}},
             static_cast<double>(node.filesystem_count));
  }

  for (const auto& filesystem : snapshot.filesystems) {
    const std::map<std::string, std::string> labels{
        {"cluster", cluster},
        {"fsid", std::to_string(filesystem.fsid)},
        {"node_id", filesystem.node_id}};
    auto status_labels = labels;
    status_labels.insert({{"active_status", filesystem.active_status},
                          {"boot_status", filesystem.boot_status},
                          {"config_status", filesystem.config_status},
                          {"drain_status", filesystem.drain_status}});
    AddGauge(filesystem_status, status_labels, 1.0);

    if (filesystem.capacity_bytes) {
      AddGauge(filesystem_capacity, labels,
               static_cast<double>(*filesystem.capacity_bytes));
    }

    if (filesystem.used_bytes) {
      AddGauge(filesystem_used, labels, static_cast<double>(*filesystem.used_bytes));
    }
  }

  const std::map<std::string, std::string> cluster_label{{"cluster", cluster}};
  AddGauge(snapshot_timestamp, cluster_label,
           static_cast<double>(snapshot.collected_timestamp_seconds));
  AddGauge(collection_duration, cluster_label, snapshot.collection_duration_seconds);

  return {std::move(node_status),        std::move(node_filesystems),
          std::move(filesystem_status),  std::move(filesystem_capacity),
          std::move(filesystem_used),    std::move(snapshot_timestamp),
          std::move(collection_duration)};
}

FstStatusCollector::FstStatusCollector(std::string cluster)
    : mCluster(std::move(cluster))
{
}

std::vector<prometheus::MetricFamily>
FstStatusCollector::Collect() const
{
  if (!gOFS || !gOFS->mMessagingRealm) {
    return {};
  }

  return BuildFstStatusMetricFamilies(CollectFstStatusSnapshot(), mCluster);
}

} // namespace eos::mgm::monitoring
