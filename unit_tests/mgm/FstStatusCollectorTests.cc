#include "mgm/monitoring/FstStatusCollector.hh"

#include <gtest/gtest.h>

#include <string>

namespace eos::mgm::monitoring {
namespace {

TEST(FstStatusCollector, PreservesIndependentStatusDimensions)
{
  FstStatusSnapshot snapshot;
  snapshot.nodes = {{"/eos/fst-01.example:1095/fst", "online", "on", "site::rack", 2},
                    {"/eos/fst-02.example:1095/fst", "offline", "off", "", 1}};
  snapshot.filesystems = {
      {"/eos/fst-01.example:1095/fst", 1, "online", "rw", "nodrain", "booted", 1000, 250},
      {"/eos/fst-01.example:1095/fst", 2, "online", "drain", "failed", "bootfailure",
       std::nullopt, std::nullopt},
      {"/eos/fst-02.example:1095/fst", 3, "offline", "wo", "waiting", "down", 2000,
       1500}};
  snapshot.collected_timestamp_seconds = 1234;
  snapshot.collection_duration_seconds = 0.012;

  std::string out;
  EmitFstStatusMetrics(snapshot, "test-cluster", out);

  EXPECT_NE(out.find("eos_fst_node_status_info{active_status=\"online\",cluster=\"test-"
                     "cluster\",config_status=\"on\",geotag=\"site::rack\",node_id=\"/"
                     "eos/fst-01.example:1095/fst\"} 1"),
            std::string::npos);
  EXPECT_NE(out.find("eos_fst_node_status_info{active_status=\"offline\",cluster=\"test-"
                     "cluster\",config_status=\"off\",geotag=\"\",node_id=\"/eos/"
                     "fst-02.example:1095/fst\"} 1"),
            std::string::npos);
  EXPECT_NE(out.find("eos_fst_node_filesystems{cluster=\"test-cluster\",node_id=\"/eos/"
                     "fst-01.example:1095/fst\"} 2"),
            std::string::npos);
  EXPECT_NE(
      out.find(
          "eos_fst_filesystem_status_info{active_status=\"online\",boot_status="
          "\"bootfailure\",cluster=\"test-cluster\",config_status=\"drain\",drain_"
          "status=\"failed\",fsid=\"2\",node_id=\"/eos/fst-01.example:1095/fst\"} 1"),
      std::string::npos);
  EXPECT_NE(out.find("eos_fst_filesystem_capacity_bytes{cluster=\"test-cluster\",fsid="
                     "\"1\",node_id=\"/eos/fst-01.example:1095/fst\"} 1000"),
            std::string::npos);
  EXPECT_NE(out.find("eos_fst_filesystem_used_bytes{cluster=\"test-cluster\",fsid=\"1\","
                     "node_id=\"/eos/fst-01.example:1095/fst\"} 250"),
            std::string::npos);
}

TEST(FstStatusCollector, ExposesSnapshotFreshnessAndCollectionCost)
{
  FstStatusSnapshot snapshot;
  snapshot.collected_timestamp_seconds = 9876;
  snapshot.collection_duration_seconds = 0.25;

  std::string out;
  EmitFstStatusMetrics(snapshot, "test-cluster", out);

  EXPECT_NE(
      out.find(
          "eos_fst_status_snapshot_timestamp_seconds{cluster=\"test-cluster\"} 9876"),
      std::string::npos);
  EXPECT_NE(
      out.find(
          "eos_fst_status_collection_duration_seconds{cluster=\"test-cluster\"} 0.25"),
      std::string::npos);
}

} // namespace
} // namespace eos::mgm::monitoring
