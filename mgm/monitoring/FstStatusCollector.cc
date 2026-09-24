#include "mgm/monitoring/FstStatusCollector.hh"

#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "common/mq/SharedHashWrapper.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/monitoring/PrometheusFormatter.hh"
#include "mgm/ofs/XrdMgmOfs.hh"

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
  const auto status = common::FileSystem::GetConfigStatusFromString(raw.c_str());
  // A value the current model has no name for - a leftover "draindead", say -
  // is reported as unknown rather than folded into one of the six
  return status.has_value() ? common::FileSystem::GetConfigStatusAsString(*status)
                            : kUnknownStatus;
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

void
EmitFstStatusMetrics(const FstStatusSnapshot& snapshot, const std::string& cluster,
                     std::string& out)
{
  FormatHeader(out, "eos_fst_node_status_info", "gauge",
               "Current FST node active and configured status as labels.");
  for (const auto& node : snapshot.nodes) {
    const std::map<std::string, std::string> labels{{"active_status", node.active_status},
                                                    {"cluster", cluster},
                                                    {"config_status", node.config_status},
                                                    {"geotag", node.geotag},
                                                    {"node_id", node.node_id}};
    FormatGaugeMetric(out, "eos_fst_node_status_info", labels, 1.0);
  }

  FormatHeader(out, "eos_fst_node_filesystems", "gauge",
               "Number of filesystems registered to an FST node.");
  for (const auto& node : snapshot.nodes) {
    FormatGaugeMetric(out, "eos_fst_node_filesystems",
                      {{"cluster", cluster}, {"node_id", node.node_id}},
                      static_cast<double>(node.filesystem_count));
  }

  FormatHeader(
      out, "eos_fst_filesystem_status_info", "gauge",
      "Current filesystem active, configured, drain, and boot status as labels.");
  for (const auto& filesystem : snapshot.filesystems) {
    const std::map<std::string, std::string> status_labels{
        {"active_status", filesystem.active_status},
        {"boot_status", filesystem.boot_status},
        {"cluster", cluster},
        {"config_status", filesystem.config_status},
        {"drain_status", filesystem.drain_status},
        {"fsid", std::to_string(filesystem.fsid)},
        {"node_id", filesystem.node_id}};
    FormatGaugeMetric(out, "eos_fst_filesystem_status_info", status_labels, 1.0);
  }

  FormatHeader(out, "eos_fst_filesystem_capacity_bytes", "gauge",
               "Current filesystem capacity in bytes.");
  for (const auto& filesystem : snapshot.filesystems) {
    if (filesystem.capacity_bytes) {
      FormatGaugeMetric(out, "eos_fst_filesystem_capacity_bytes",
                        {{"cluster", cluster},
                         {"fsid", std::to_string(filesystem.fsid)},
                         {"node_id", filesystem.node_id}},
                        static_cast<double>(*filesystem.capacity_bytes));
    }
  }

  FormatHeader(out, "eos_fst_filesystem_used_bytes", "gauge",
               "Current filesystem used space in bytes.");
  for (const auto& filesystem : snapshot.filesystems) {
    if (filesystem.used_bytes) {
      FormatGaugeMetric(out, "eos_fst_filesystem_used_bytes",
                        {{"cluster", cluster},
                         {"fsid", std::to_string(filesystem.fsid)},
                         {"node_id", filesystem.node_id}},
                        static_cast<double>(*filesystem.used_bytes));
    }
  }

  FormatHeader(out, "eos_fst_status_snapshot_timestamp_seconds", "gauge",
               "Unix timestamp of the FST status snapshot exposed by this scrape.");
  FormatGaugeMetric(out, "eos_fst_status_snapshot_timestamp_seconds",
                    {{"cluster", cluster}},
                    static_cast<double>(snapshot.collected_timestamp_seconds));

  FormatHeader(out, "eos_fst_status_collection_duration_seconds", "gauge",
               "Time spent collecting the current FST status snapshot in seconds.");
  FormatGaugeMetric(out, "eos_fst_status_collection_duration_seconds",
                    {{"cluster", cluster}}, snapshot.collection_duration_seconds);
}

FstStatusCollector::FstStatusCollector(std::string cluster)
    : mCluster(std::move(cluster))
{
}

void
FstStatusCollector::Collect(std::string& out) const
{
  if (!gOFS || !gOFS->mMessagingRealm) {
    return;
  }

  EmitFstStatusMetrics(CollectFstStatusSnapshot(), mCluster, out);
}

} // namespace eos::mgm::monitoring
