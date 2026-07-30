#pragma once

#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"

#include <functional>
#include <string>
#include <vector>

namespace eos {
class QdbContactDetails;
}

namespace eos::mgm::monitoring {

struct QdbNodeStatusSnapshot {
  std::string qdb_id;
  std::string version;
  std::string status = "unknown";
  bool is_leader = false;
};

std::vector<QdbNodeStatusSnapshot>
ParseQdbRaftInfo(const std::vector<std::string>& entries);

std::vector<std::string> CollectQdbRaftInfo(const QdbContactDetails& contact_details);

std::vector<prometheus::MetricFamily>
BuildQdbStatusMetricFamilies(const std::vector<QdbNodeStatusSnapshot>& snapshots,
                             const std::string& cluster);

class QdbStatusCollector : public prometheus::Collectable {
public:
  QdbStatusCollector(std::string cluster,
                     std::function<std::vector<std::string>()> raft_info_snapshot);

  std::vector<prometheus::MetricFamily> Collect() const override;

private:
  std::string mCluster;
  std::function<std::vector<std::string>()> mRaftInfoSnapshot;
};

} // namespace eos::mgm::monitoring
