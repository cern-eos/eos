#include "mgm/monitoring/MgmStatusCollector.hh"

#include "mgm/monitoring/PrometheusFormatter.hh"

#include <algorithm>
#include <map>
#include <sstream>
#include <utility>

namespace eos::mgm::monitoring {
namespace {

constexpr const char* kUnknownMgm = "unknown";

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

void
EmitMgmStatusMetrics(const std::vector<MgmStatusSnapshot>& snapshots,
                     const std::string& cluster, std::string& out)
{
  if (snapshots.empty()) {
    return;
  }

  FormatHeader(out, "eos_mgm_master", "gauge",
               "Configured MGM candidates and role (1 master, 0 follower); master_id is "
               "the lease holder observed by the exporting process.");

  for (const auto& snapshot : snapshots) {
    FormatGaugeMetric(
        out, "eos_mgm_master",
        {{"cluster", cluster},
         {"master_id", snapshot.master_id.empty() ? kUnknownMgm : snapshot.master_id},
         {"mgm_id", snapshot.mgm_id.empty() ? kUnknownMgm : snapshot.mgm_id}},
        snapshot.is_master ? 1.0 : 0.0);
  }
}

MgmStatusCollector::MgmStatusCollector(
    std::string cluster, std::function<std::vector<MgmStatusSnapshot>()> status_snapshot)
    : mCluster(std::move(cluster))
    , mStatusSnapshot(std::move(status_snapshot))
{
}

void
MgmStatusCollector::Collect(std::string& out) const
{
  EmitMgmStatusMetrics(mStatusSnapshot ? mStatusSnapshot()
                                       : std::vector<MgmStatusSnapshot>{},
                       mCluster, out);
}

} // namespace eos::mgm::monitoring
