#pragma once

#include "mgm/shaping/TrafficShaping.hh"

#include <string>

namespace eos::mgm::monitoring {

class TrafficShapingCollector {
public:
  TrafficShapingCollector(traffic_shaping::TrafficShapingEngine& engine,
                          std::string cluster);

  void Collect(std::string& out) const;

private:
  traffic_shaping::TrafficShapingEngine& mEngine;
  std::string mCluster;
};

} // namespace eos::mgm::monitoring
