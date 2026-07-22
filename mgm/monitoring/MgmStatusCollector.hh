#pragma once

#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"

#include <functional>
#include <string>
#include <vector>

namespace eos::mgm::monitoring {

struct MgmStatusSnapshot {
  std::string mgm_id;
  std::string master_id;
  bool is_master = false;
};

std::vector<MgmStatusSnapshot>
BuildMgmStatusSnapshots(const std::string& local_id, bool local_is_master,
                        const std::string& master_id, int mgm_port,
                        const std::vector<std::string>& candidate_hosts);

std::vector<prometheus::MetricFamily>
BuildMgmStatusMetricFamilies(const std::vector<MgmStatusSnapshot>& snapshots,
                             const std::string& cluster);

class MgmStatusCollector : public prometheus::Collectable {
public:
  MgmStatusCollector(std::string cluster,
                     std::function<std::vector<MgmStatusSnapshot>()> status_snapshot);

  std::vector<prometheus::MetricFamily> Collect() const override;

private:
  std::string mCluster;
  std::function<std::vector<MgmStatusSnapshot>()> mStatusSnapshot;
};

} // namespace eos::mgm::monitoring
