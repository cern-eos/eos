#pragma once

#include <chrono>
#include <mutex>
#include <string>

namespace eos::mgm::monitoring {

struct EosExporterViewSnapshot {
  std::string filesystems;
  std::string nodes;
  std::string groups;
  std::string spaces;
  std::string namespace_stats;
  std::string io_stats;
  std::string io_app_stats;
  std::string quotas;
  std::string recycle;
  std::string fsck;
  std::string fusex;
  std::string inspector;
};

//! Emit the legacy eos_exporter filesystem-view metric contract from monitoring
//! command output. Kept public so the parser and contract can be unit tested
//! without a live MGM filesystem view.
void EmitEosExporterViewMetrics(const EosExporterViewSnapshot& snapshot,
                                const std::string& cluster, std::string& out);

class EosExporterCollector {
public:
  explicit EosExporterCollector(std::string cluster,
                                std::chrono::seconds cache_ttl = std::chrono::seconds{5});

  void Collect(std::string& out) const;

private:
  std::string mCluster;
  std::chrono::seconds mCacheTtl;
  mutable std::mutex mCacheMutex;
  mutable std::chrono::steady_clock::time_point mCacheUpdated;
  mutable std::string mCache;
};

} // namespace eos::mgm::monitoring
