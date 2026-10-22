#pragma once

#include "mgm/monitoring/MgmStatusCollector.hh"

#include "mgm/shaping/TrafficShaping.hh"

#include <chrono>
#include <cstdint>
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

struct TrafficShapingLabelResolvers {
  std::function<std::string(uint32_t)> uid;
  std::function<std::string(uint32_t)> gid;
  std::function<std::string(const std::string&)> node;
};

std::shared_ptr<prometheus::Collectable>
CreateTrafficShapingCollector(traffic_shaping::TrafficShapingEngine& engine,
                              std::string cluster,
                              TrafficShapingLabelResolvers label_resolvers = {});

class PrometheusExporter {
public:
  PrometheusExporter(std::string bind_address,
                     traffic_shaping::TrafficShapingEngine& engine, std::string cluster,
                     std::chrono::milliseconds cache_ttl,
                     std::function<bool()> should_collect,
                     std::function<std::vector<MgmStatusSnapshot>()> mgm_status_snapshot);
  ~PrometheusExporter();

  PrometheusExporter(const PrometheusExporter&) = delete;
  PrometheusExporter& operator=(const PrometheusExporter&) = delete;

private:
  std::vector<std::shared_ptr<prometheus::Collectable>> mCollectors;
  std::unique_ptr<prometheus::Exposer> mExposer;
};

} // namespace eos::mgm::monitoring
