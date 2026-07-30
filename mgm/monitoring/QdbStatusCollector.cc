#include "mgm/monitoring/QdbStatusCollector.hh"

#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "prometheus/client_metric.h"
#include "prometheus/metric_type.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <map>
#include <qclient/QClient.hh>
#include <sstream>
#include <utility>

namespace eos::mgm::monitoring {
namespace {

std::string
AfterPrefix(const std::string& value, const std::string& prefix)
{
  return value.compare(0, prefix.size(), prefix) == 0 ? value.substr(prefix.size())
                                                      : std::string{};
}

void
AddNode(std::map<std::string, QdbNodeStatusSnapshot>& nodes, const std::string& endpoint)
{
  if (!endpoint.empty()) {
    nodes.try_emplace(endpoint, QdbNodeStatusSnapshot{endpoint});
  }
}

std::vector<std::string>
Split(const std::string& value, const char delimiter)
{
  std::vector<std::string> parts;
  std::istringstream stream(value);
  std::string part;
  while (std::getline(stream, part, delimiter)) {
    const auto first = part.find_first_not_of(' ');
    const auto last = part.find_last_not_of(' ');
    if (first != std::string::npos) {
      parts.emplace_back(part.substr(first, last - first + 1));
    }
  }
  return parts;
}

void
AddGauge(prometheus::MetricFamily& family,
         const std::map<std::string, std::string>& labels)
{
  prometheus::ClientMetric metric;
  metric.gauge.value = 1.0;
  for (const auto& [name, value] : labels) {
    metric.label.push_back({name, value});
  }
  family.metric.push_back(std::move(metric));
}

} // namespace

std::vector<std::string>
CollectQdbRaftInfo(const QdbContactDetails& contact_details)
{
  std::vector<std::string> entries;
  if (contact_details.empty()) {
    return entries;
  }

  try {
    auto options = contact_details.constructOptions();
    options.retryStrategy = qclient::RetryStrategy::NoRetries();
    options.tcpTimeout = std::chrono::seconds(2);
    options.responseTimeout = std::chrono::seconds(2);
    qclient::QClient client(contact_details.members, std::move(options));
    const auto reply = client.exec("raft-info").get();
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
      return entries;
    }

    entries.reserve(reply->elements);
    for (std::size_t i = 0; i < reply->elements; ++i) {
      const auto* element = reply->element[i];
      if (!element ||
          (element->type != REDIS_REPLY_STRING && element->type != REDIS_REPLY_STATUS)) {
        continue;
      }
      entries.emplace_back(element->str ? element->str : "", element->len);
    }
  } catch (const std::exception&) {
    // A temporary QuarkDB monitoring failure removes only this optional
    // family; it must never make the Prometheus scrape fail.
  }
  return entries;
}

std::vector<QdbNodeStatusSnapshot>
ParseQdbRaftInfo(const std::vector<std::string>& entries)
{
  std::map<std::string, QdbNodeStatusSnapshot> nodes;
  std::string leader;
  std::string local;
  std::string local_version;
  std::string local_status = "unknown";

  for (const auto& entry : entries) {
    if (const auto value = AfterPrefix(entry, "LEADER "); !value.empty()) {
      leader = value;
      AddNode(nodes, value);
    } else if (const auto value = AfterPrefix(entry, "MYSELF "); !value.empty()) {
      local = value;
      AddNode(nodes, value);
    } else if (const auto value = AfterPrefix(entry, "VERSION "); !value.empty()) {
      local_version = value;
    } else if (const auto value = AfterPrefix(entry, "STATUS "); !value.empty()) {
      local_status = value == "LEADER" || value == "FOLLOWER" || value == "leader" ||
                             value == "follower"
                         ? "online"
                         : "unknown";
    } else if (const auto value = AfterPrefix(entry, "NODES "); !value.empty()) {
      for (const auto& endpoint : Split(value, ',')) {
        AddNode(nodes, endpoint);
      }
    } else if (const auto value = AfterPrefix(entry, "REPLICA "); !value.empty()) {
      const auto parts = Split(value, '|');
      if (parts.empty()) {
        continue;
      }
      AddNode(nodes, parts[0]);
      auto& node = nodes.at(parts[0]);
      for (const auto& part : parts) {
        if (part == "ONLINE") {
          node.status = "online";
        } else if (part == "OFFLINE") {
          node.status = "offline";
        } else if (const auto version = AfterPrefix(part, "VERSION "); !version.empty()) {
          node.version = version;
        }
      }
    }
  }

  if (!local.empty()) {
    auto& node = nodes.at(local);
    node.version = local_version;
    node.status = local_status;
  }
  if (!leader.empty()) {
    nodes.at(leader).is_leader = true;
  }

  std::vector<QdbNodeStatusSnapshot> snapshots;
  snapshots.reserve(nodes.size());
  for (auto& [id, node] : nodes) {
    snapshots.emplace_back(std::move(node));
  }
  std::stable_sort(
      snapshots.begin(), snapshots.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.is_leader && !rhs.is_leader; });
  return snapshots;
}

std::vector<prometheus::MetricFamily>
BuildQdbStatusMetricFamilies(const std::vector<QdbNodeStatusSnapshot>& snapshots,
                             const std::string& cluster)
{
  prometheus::MetricFamily info;
  info.name = "eos_qdb_info";
  info.help = "QuarkDB Raft membership, build version, availability, and leadership.";
  info.type = prometheus::MetricType::Gauge;

  for (const auto& snapshot : snapshots) {
    AddGauge(info, {{"cluster", cluster},
                    {"qdb_id", snapshot.qdb_id},
                    {"qdb_version", snapshot.version},
                    {"role", snapshot.is_leader ? "leader" : "follower"},
                    {"status", snapshot.status}});
  }
  return {std::move(info)};
}

QdbStatusCollector::QdbStatusCollector(
    std::string cluster, std::function<std::vector<std::string>()> raft_info_snapshot)
    : mCluster(std::move(cluster))
    , mRaftInfoSnapshot(std::move(raft_info_snapshot))
{
}

std::vector<prometheus::MetricFamily>
QdbStatusCollector::Collect() const
{
  return BuildQdbStatusMetricFamilies(ParseQdbRaftInfo(mRaftInfoSnapshot
                                                           ? mRaftInfoSnapshot()
                                                           : std::vector<std::string>{}),
                                      mCluster);
}

} // namespace eos::mgm::monitoring
