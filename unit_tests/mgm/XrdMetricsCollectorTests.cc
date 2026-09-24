#include "mgm/monitoring/XrdMetricsCollector.hh"

#include "XrdMetrics/XrdMetricsRegistry.hh"
#include "mgm/shaping/TrafficShaping.hh"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace eos::mgm::monitoring {
namespace {

void
ExpectCounter(const std::string& text, const std::string& name,
              const std::map<std::string, std::string>& labels, const uint64_t value)
{
  std::istringstream input(text);
  std::string line;
  std::size_t matches = 0;
  while (std::getline(input, line)) {
    if (line.compare(0, name.size() + 1, name + "{") != 0) {
      continue;
    }
    bool matches_labels = true;
    for (const auto& [key, val] : labels) {
      const auto label = key + "=\"" + val + "\"";
      if (line.find("{" + label) == std::string::npos &&
          line.find("," + label) == std::string::npos) {
        matches_labels = false;
      }
    }
    if (matches_labels) {
      ++matches;
      EXPECT_EQ(std::to_string(value), line.substr(line.rfind(' ') + 1));
    }
  }
  EXPECT_EQ(1u, matches) << name;
}

TEST(XrdMetricsCollector, RegistersAndExposesMetricsViaRegistry)
{
  traffic_shaping::TrafficShapingEngine engine;
  engine.Start();
  auto manager = engine.GetManager();
  for (const auto& node :
       {"/eos/fst-a.example:1095/fst", "/eos/fst-b.example:1095/fst"}) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(1000);
    auto* entry = report.add_entries();
    entry->set_app_name("registry-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(3);
    entry->set_generation_id(1);
    manager->ProcessReport(report);
    report.set_timestamp_ms(2000);
    const bool first_node = std::string(node).find("fst-a") != std::string::npos;
    entry->set_total_bytes_written(first_node ? 8192 : 4096);
    entry->set_total_bytes_read(first_node ? 4096 : 2048);
    entry->set_total_write_ops(first_node ? 2 : 1);
    entry->set_total_read_ops(first_node ? 4 : 2);
    manager->ProcessReport(report);
  }

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

    for (const auto& [node, written] : std::map<std::string, uint64_t>{
             {"fst-a.example:1095", 8192}, {"fst-b.example:1095", 4096}}) {
      std::map<std::string, std::string> labels{{"cluster", "test-cluster"},
                                                {"app", "registry-app"},
                                                {"uid_id", "1"},
                                                {"gid_id", "2"},
                                                {"fsid", "0"},
                                                {"node_id", node},
                                                {"operation", "write"}};
      ExpectCounter(out, "eos_io_shaping_all_bytes_total", labels, written);
      ExpectCounter(out, "eos_io_shaping_all_operations_total", labels, written / 4096);
      labels["operation"] = "read";
      ExpectCounter(out, "eos_io_shaping_all_bytes_total", labels, written / 2);
      ExpectCounter(out, "eos_io_shaping_all_operations_total", labels, written / 2048);
    }

    // When follower (is_master is false)
    is_master = false;
    out.clear();
    for (auto* c : XrdMetrics::CollectorRegistry::instance().collectors()) {
      c->runTextCollectors(out);
    }
    EXPECT_NE(out.find("eos_mgm_master"), std::string::npos);
    EXPECT_EQ(out.find("eos_io_shaping_config_enabled"), std::string::npos);
    EXPECT_EQ(out.find("eos_io_shaping_all_bytes_total"), std::string::npos);
    EXPECT_EQ(out.find("eos_io_shaping_all_operations_total"), std::string::npos);
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
