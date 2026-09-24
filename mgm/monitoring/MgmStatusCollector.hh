#pragma once

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

void EmitMgmStatusMetrics(const std::vector<MgmStatusSnapshot>& snapshots,
                          const std::string& cluster, std::string& out);

class MgmStatusCollector {
public:
  MgmStatusCollector(std::string cluster,
                     std::function<std::vector<MgmStatusSnapshot>()> status_snapshot);

  void Collect(std::string& out) const;

private:
  std::string mCluster;
  std::function<std::vector<MgmStatusSnapshot>()> mStatusSnapshot;
};

} // namespace eos::mgm::monitoring
