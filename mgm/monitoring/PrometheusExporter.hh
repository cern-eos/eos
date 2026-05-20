#pragma once

#include "mgm/shaping/TrafficShaping.hh"

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace prometheus {
class Collectable;
class Exposer;
class Registry;
} // namespace prometheus

namespace eos::mgm::monitoring {

class PrometheusExporter {
public:
  PrometheusExporter(std::string bind_address,
                     traffic_shaping::TrafficShapingEngine& engine, std::string cluster,
                     std::chrono::milliseconds cache_ttl,
                     std::function<bool()> should_collect);
  ~PrometheusExporter();

  PrometheusExporter(const PrometheusExporter&) = delete;
  PrometheusExporter& operator=(const PrometheusExporter&) = delete;

private:
  std::vector<std::shared_ptr<prometheus::Collectable>> mCollectors;
  std::unique_ptr<prometheus::Exposer> mExposer;
};

} // namespace eos::mgm::monitoring
