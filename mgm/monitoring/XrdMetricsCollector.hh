#pragma once

#include "mgm/monitoring/EosExporterCollector.hh"
#include "mgm/monitoring/FstStatusCollector.hh"
#include "mgm/monitoring/MgmStatusCollector.hh"
#include "mgm/monitoring/TrafficShapingCollector.hh"
#include "mgm/shaping/TrafficShaping.hh"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace XrdMetrics {
class Collector;
} // namespace XrdMetrics

namespace eos::mgm::monitoring {

class XrdMetricsCollector {
public:
  XrdMetricsCollector(
      traffic_shaping::TrafficShapingEngine& engine, std::string cluster,
      std::function<bool()> should_collect,
      std::function<std::vector<MgmStatusSnapshot>()> mgm_status_snapshot);
  ~XrdMetricsCollector();

  XrdMetricsCollector(const XrdMetricsCollector&) = delete;
  XrdMetricsCollector& operator=(const XrdMetricsCollector&) = delete;

  void Collect(std::string& out) const;

private:
  std::string mCluster;
  std::function<bool()> mShouldCollect;
  MgmStatusCollector mMgmStatusCollector;
  FstStatusCollector mFstStatusCollector;
  EosExporterCollector mEosExporterCollector;
  TrafficShapingCollector mTrafficShapingCollector;
  std::unique_ptr<XrdMetrics::Collector> mCollector;
};

} // namespace eos::mgm::monitoring
