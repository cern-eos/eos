#include "mgm/monitoring/MgmStatusCollector.hh"

#include "prometheus/client_metric.h"
#include "prometheus/metric_type.h"

#include <algorithm>
#include <map>
#include <sstream>
#include <utility>

namespace eos::mgm::monitoring {
namespace {

constexpr const char* kUnknownMgm = "unknown";

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

} // namespace

std::vector<MgmStatusSnapshot>
BuildMgmStatusSnapshots(const std::string& local_id, const bool local_is_master,
                        const std::string& master_id, const int mgm_port,
                        const std::vector<std::string>& candidate_hosts)
{
  std::vector<MgmStatusSnapshot> snapshots;
  snapshots.reserve(candidate_hosts.size() + 1);

  for (const auto& host : candidate_hosts) {
    if (host.empty() || (mgm_port <= 0)) {
      continue;
    }

    std::ostringstream endpoint;
    endpoint << host << ":" << mgm_port;
    const std::string mgm_id = endpoint.str();
    if (std::any_of(snapshots.begin(), snapshots.end(),
                    [&](const auto& snapshot) { return snapshot.mgm_id == mgm_id; })) {
      continue;
    }
    const bool is_local = mgm_id == local_id;
    snapshots.push_back(
        {mgm_id, master_id, is_local ? local_is_master : mgm_id == master_id});
  }

  const auto local = std::find_if(snapshots.begin(), snapshots.end(),
                                  [&](const auto& s) { return s.mgm_id == local_id; });
  if (!local_id.empty() && (local == snapshots.end())) {
    snapshots.push_back({local_id, master_id, local_is_master});
  }

  return snapshots;
}

std::vector<prometheus::MetricFamily>
BuildMgmStatusMetricFamilies(const std::vector<MgmStatusSnapshot>& snapshots,
                             const std::string& cluster)
{
  prometheus::MetricFamily role;
  role.name = "eos_mgm_master";
  role.help = "Configured MGM candidates and role (1 master, 0 follower); master_id is "
              "the lease holder observed by the exporting process.";
  role.type = prometheus::MetricType::Gauge;

  for (const auto& snapshot : snapshots) {
    AddGauge(
        role,
        {{"cluster", cluster},
         {"master_id", snapshot.master_id.empty() ? kUnknownMgm : snapshot.master_id},
         {"mgm_id", snapshot.mgm_id.empty() ? kUnknownMgm : snapshot.mgm_id}},
        snapshot.is_master ? 1.0 : 0.0);
  }

  return {std::move(role)};
}

MgmStatusCollector::MgmStatusCollector(
    std::string cluster, std::function<std::vector<MgmStatusSnapshot>()> status_snapshot)
    : mCluster(std::move(cluster))
    , mStatusSnapshot(std::move(status_snapshot))
{
}

std::vector<prometheus::MetricFamily>
MgmStatusCollector::Collect() const
{
  return BuildMgmStatusMetricFamilies(
      mStatusSnapshot ? mStatusSnapshot() : std::vector<MgmStatusSnapshot>{}, mCluster);
}

} // namespace eos::mgm::monitoring
