#include "mgm/monitoring/EosExporterCollector.hh"
#include "mgm/monitoring/PrometheusFormatter.hh"

#include <gtest/gtest.h>

#include <string>

namespace eos::mgm::monitoring {
namespace {

std::size_t
CountOccurrences(const std::string& text, const std::string& needle)
{
  std::size_t count = 0;
  std::size_t position = 0;
  while ((position = text.find(needle, position)) != std::string::npos) {
    ++count;
    position += needle.size();
  }
  return count;
}

TEST(EosExporterCollector, EmitsFilesystemViewCompatibilityContract)
{
  EosExporterViewSnapshot snapshot;
  snapshot.filesystems =
      "host=fst.example port=1095 id=42 stat.boot=booting configstatus=drain "
      "stat.disk.load=1.5 stat.statfs.usedbytes=1024 stat.statfs.freebytes=2048 "
      "stat.statfs.capacity=3072 stat.statfs.fused=2 stat.statfs.ffree=3 "
      "stat.statfs.files=5 stat.active=offline stat.disk.iops=12 "
      "stat.disk.bw=34.5 stat.geotag=dc::rack stat.health=OK\n";
  snapshot.nodes = "hostport=fst.example:1095 status=online cfg.status=on nofs=1 "
                   "heartbeatdelta=2 sum.stat.statfs.freebytes=2048 "
                   "sum.stat.statfs.usedbytes=1024 sum.stat.statfs.capacity=3072 "
                   "cfg.stat.sys.eos.version=5.3 cfg.stat.sys.xrootd.version=6.2 "
                   "cfg.stat.sys.kernel=linux cfg.stat.geotag=dc::rack\n";
  snapshot.groups = "name=default.0 cfg.status=on nofs=1 avg.stat.disk.load=1.5 "
                    "sum.stat.statfs.capacity=3072\n";
  snapshot.spaces = "name=default cfg.groupsize=8 cfg.groupmod=24 nofs=1 "
                    "sum.stat.statfs.usedbytes=1024 sum.stat.statfs.freebytes=2048 "
                    "sum.stat.statfs.capacity=3072 sum.<n>?configstatus@rw=1 "
                    "cfg.quota=on cfg.nominalsize=??? cfg.balancer=off\n";

  std::string out;
  EmitEosExporterViewMetrics(snapshot, "test-cluster", out);

  EXPECT_NE(out.find("eos_fs_boot_status{cluster=\"test-cluster\",fs=\"42\","
                     "geotag=\"dc::rack\",node=\"fst.example\"} 1"),
            std::string::npos);
  EXPECT_NE(out.find("eos_fs_config_status{cluster=\"test-cluster\",fs=\"42\","
                     "geotag=\"dc::rack\",node=\"fst.example\"} 2"),
            std::string::npos);
  EXPECT_NE(out.find("eos_fs_status{cluster=\"test-cluster\",fs=\"42\","
                     "geotag=\"dc::rack\",node=\"fst.example\"} 0"),
            std::string::npos);
  EXPECT_NE(out.find("eos_node_status{cluster=\"test-cluster\",node=\"fst.example\","
                     "port=\"1095\"} 1"),
            std::string::npos);
  EXPECT_NE(out.find("eos_node_info{cluster=\"test-cluster\",eos_version=\"5.3\","),
            std::string::npos);
  EXPECT_NE(out.find("eos_group_cfg_status{cluster=\"test-cluster\","
                     "group=\"default.0\"} 1"),
            std::string::npos);
  EXPECT_NE(out.find("eos_space_cfg_nominalsize{cluster=\"test-cluster\","
                     "space=\"default\"} 0"),
            std::string::npos);
}

TEST(EosExporterCollector, PreservesQuotedMonitoringValues)
{
  EosExporterViewSnapshot snapshot;
  snapshot.nodes =
      "hostport=fst.example:1095 status=online cfg.status=on "
      "cfg.stat.sys.kernel=\"Linux kernel with spaces\" cfg.stat.geotag=local\n";

  std::string out;
  EmitEosExporterViewMetrics(snapshot, "quoted", out);

  EXPECT_NE(out.find("kernel=\"Linux kernel with spaces\""), std::string::npos);
}

TEST(EosExporterCollector, EmitsInspectorTagsAndFullPrecisionValues)
{
  EosExporterViewSnapshot snapshot;
  snapshot.filesystems =
      "host=fst.example id=42 stat.disk.load=1.23456789012345 stat.geotag=local\n";
  snapshot.inspector =
      "key=last tag=layout layout=00100012 type=replica nominal_stripes=2 "
      "blocksize=4k volume=4096\n"
      "key=last tag=accesstime::volume bin=86400 value=8192\n";

  std::string out;
  EmitEosExporterViewMetrics(snapshot, "precision", out);

  EXPECT_NE(out.find("eos_fs_disk_load{cluster=\"precision\",fs=\"42\","
                     "geotag=\"local\",node=\"fst.example\"} 1.23456789012345"),
            std::string::npos);
  EXPECT_NE(out.find("eos_inspector_layout_volume_bytes{blocksize=\"4k\","
                     "cluster=\"precision\",layout=\"00100012\",nominal_stripes=\"2\","
                     "type=\"replica\"} 4096"),
            std::string::npos);
  EXPECT_NE(out.find("eos_inspector_accesstime_volume_bytes{bin=\"1D\","
                     "cluster=\"precision\"} 8192"),
            std::string::npos);
}

TEST(EosExporterCollector, OmitsLegacyGaugeVectorsThatTheGoExporterNeverSet)
{
  EosExporterViewSnapshot snapshot;
  snapshot.filesystems = "host=fst.example id=42 stat.drainprogress=50 stat.drainfiles=3 "
                         "stat.drainbytesleft=1024 stat.geotag=local\n";
  snapshot.namespace_stats =
      "uid=all gid=all ns.hanging.since=12\n"
      "uid=all gid=all cmd=idle total=100 5s=0.00 60s=0.00 300s=0.00 "
      "3600s=0.00\n";

  std::string out;
  EmitEosExporterViewMetrics(snapshot, "omissions", out);

  EXPECT_EQ(out.find("eos_fs_drain_progress"), std::string::npos);
  EXPECT_EQ(out.find("eos_fs_drain_filesleft"), std::string::npos);
  EXPECT_EQ(out.find("eos_fs_drain_bytesleft"), std::string::npos);
  EXPECT_EQ(out.find("eos_ns_hanging_since_seconds"), std::string::npos);
  EXPECT_EQ(out.find("operation=\"idle\""), std::string::npos);
}

TEST(EosExporterCollector, DeduplicatesFusexSamplesByLegacyLabelSet)
{
  EosExporterViewSnapshot snapshot;
  snapshot.fusex = "client=eosxd host=fuse.example version=5.2.22 mount=/eos/first\n"
                   "client=eosxd host=fuse.example version=5.2.22 mount=/eos/second\n"
                   "client=eosxd host=other.example version=5.4.0 mount=/eos/third\n";

  std::string out;
  EmitEosExporterViewMetrics(snapshot, "test", out);

  EXPECT_EQ(CountOccurrences(out, "eos_fusex_info{"), 2U);
  EXPECT_EQ(CountOccurrences(out, "eos_fusex_info{cluster=\"test\",host=\"fuse.example\","
                                  "version=\"5.2.22\"} 1\n"),
            1U);
}

TEST(EosExporterCollector, DeduplicatesInspectorSamplesWithLastValueWinning)
{
  EosExporterViewSnapshot snapshot;
  snapshot.inspector = "key=last tag=accesstime::files bin=invalid-first value=1\n"
                       "key=last tag=accesstime::files bin=invalid-second value=2\n"
                       "key=last tag=birthtime::volume bin=86400 value=10\n"
                       "key=last tag=birthtime::volume bin=86400 value=20\n";

  std::string out;
  EmitEosExporterViewMetrics(snapshot, "test", out);

  EXPECT_EQ(CountOccurrences(out, "eos_inspector_accesstime_files{"), 1U);
  EXPECT_NE(out.find("eos_inspector_accesstime_files{bin=\"Invalid input\","
                     "cluster=\"test\"} 2\n"),
            std::string::npos);
  EXPECT_EQ(out.find("eos_inspector_accesstime_files{bin=\"Invalid input\","
                     "cluster=\"test\"} 1\n"),
            std::string::npos);

  EXPECT_EQ(CountOccurrences(out, "eos_inspector_birthtime_volume_bytes{"), 1U);
  EXPECT_NE(out.find("eos_inspector_birthtime_volume_bytes{bin=\"1D\","
                     "cluster=\"test\"} 20\n"),
            std::string::npos);
  EXPECT_EQ(out.find("eos_inspector_birthtime_volume_bytes{bin=\"1D\","
                     "cluster=\"test\"} 10\n"),
            std::string::npos);
}

} // namespace
} // namespace eos::mgm::monitoring
