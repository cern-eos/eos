#pragma once

#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace eos::mgm::monitoring {

// Temporary eos-orbit bridge until equivalent XRootD status monitoring is
// available. The collector and its metric-building types intentionally live in
// this isolated unit so the compatibility surface can be removed atomically.
struct FstNodeStatusSnapshot {
  std::string node_id;
  std::string active_status;
  std::string config_status;
  std::string geotag;
  std::size_t filesystem_count = 0;
};

struct FstFilesystemStatusSnapshot {
  std::string node_id;
  std::uint64_t fsid = 0;
  std::string active_status;
  std::string config_status;
  std::string drain_status;
  std::string boot_status;
  std::optional<std::uint64_t> capacity_bytes;
  std::optional<std::uint64_t> used_bytes;
};

struct FstStatusSnapshot {
  std::vector<FstNodeStatusSnapshot> nodes;
  std::vector<FstFilesystemStatusSnapshot> filesystems;
  std::uint64_t collected_timestamp_seconds = 0;
  double collection_duration_seconds = 0.0;
};

std::vector<prometheus::MetricFamily>
BuildFstStatusMetricFamilies(const FstStatusSnapshot& snapshot,
                             const std::string& cluster);

class FstStatusCollector : public prometheus::Collectable {
public:
  explicit FstStatusCollector(std::string cluster);

  std::vector<prometheus::MetricFamily> Collect() const override;

private:
  std::string mCluster;
};

} // namespace eos::mgm::monitoring
