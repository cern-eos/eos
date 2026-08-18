#include "mgm/monitoring/XrdMetricsCollector.hh"

#include "XrdMetrics/XrdMetricsRegistry.hh"
#include "mgm/shaping/TrafficShaping.hh"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace eos::mgm::monitoring {
namespace {

TEST(XrdMetricsCollector, RegistersAndExposesMetricsViaRegistry)
{
  traffic_shaping::TrafficShapingEngine engine;
  engine.Start();

  bool is_master = true;
  auto should_collect = [&is_master]() -> bool { return is_master; };
  auto mgm_snapshot = []() -> std::vector<MgmStatusSnapshot> {
    return std::vector<MgmStatusSnapshot>{
        {"mgm1.cern.ch:1094", "mgm1.cern.ch:1094", true}};
  };

  {
    XrdMetricsCollector collector(engine, "test-cluster", should_collect, mgm_snapshot);

    std::string out;
    for (auto* c : XrdMetrics::CollectorRegistry::instance().collectors()) {
      c->runTextCollectors(out);
    }

    // Verify MGM master metric is present
    EXPECT_NE(out.find("eos_mgm_master"), std::string::npos);
    EXPECT_NE(out.find("test-cluster"), std::string::npos);

    // Verify traffic shaping metrics are present when is_master is true
    EXPECT_NE(out.find("eos_io_shaping_config_enabled"), std::string::npos);
    EXPECT_NE(out.find("eos_io_shaping_all_entries"), std::string::npos);

    // When follower (is_master is false)
    is_master = false;
    out.clear();
    for (auto* c : XrdMetrics::CollectorRegistry::instance().collectors()) {
      c->runTextCollectors(out);
    }
    EXPECT_NE(out.find("eos_mgm_master"), std::string::npos);
    EXPECT_EQ(out.find("eos_io_shaping_config_enabled"), std::string::npos);
  }

  // After destruction, collector should be unregistered
  std::string out_after;
  for (auto* c : XrdMetrics::CollectorRegistry::instance().collectors()) {
    c->runTextCollectors(out_after);
  }
  EXPECT_EQ(out_after.find("test-cluster"), std::string::npos);

  engine.Stop();
}

} // namespace
} // namespace eos::mgm::monitoring
