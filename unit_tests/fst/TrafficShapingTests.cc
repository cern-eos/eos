#include "common/VirtualIdentity.hh"
#include "fst/storage/TrafficShaping.hh"

#include "gtest/gtest.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

namespace {

eos::common::VirtualIdentity
MakeVid(const std::string& app, const uint32_t uid = 1000, const uint32_t gid = 1000)
{
  eos::common::VirtualIdentity vid;
  vid.app = app;
  vid.uid = uid;
  vid.gid = gid;
  return vid;
}

} // namespace

TEST(IoStatsCollectorTest, AggregatesFilesystemKeyByDefault)
{
  eos::fst::traffic_shaping::IoStatsCollector collector;
  collector.SetEnabled(true);

  collector.RecordRead("aggregate-app", 1000, 1001, 10, 4096);
  collector.RecordRead("aggregate-app", 1000, 1001, 20, 8192);

  uint32_t fsid = 999;
  uint64_t bytes_read = 0;
  uint64_t read_iops = 0;
  size_t entries = 0;
  collector.VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey& key,
                             const eos::fst::traffic_shaping::IoStatsEntry& entry) {
    ++entries;
    fsid = key.fsid;
    bytes_read = entry.bytes_read.load(std::memory_order_relaxed);
    read_iops = entry.read_iops.load(std::memory_order_relaxed);
  });

  EXPECT_EQ(1u, entries);
  EXPECT_EQ(0u, fsid);
  EXPECT_EQ(12288u, bytes_read);
  EXPECT_EQ(2u, read_iops);
}

TEST(IoStatsCollectorTest, DetailModeChangesClearAndSwitchFilesystemKeying)
{
  eos::fst::traffic_shaping::IoStatsCollector collector;
  collector.SetEnabled(true);

  collector.RecordWrite("mode-app", 1000, 1001, 10, 4096);
  ASSERT_TRUE(collector.SetFilesystemDetailEnabled(true));

  size_t entries = 0;
  collector.VisitEntries(
      [&](const eos::fst::traffic_shaping::IoStatsKey&,
          const eos::fst::traffic_shaping::IoStatsEntry&) { ++entries; });
  ASSERT_EQ(0u, entries);

  collector.RecordWrite("mode-app", 1000, 1001, 10, 4096);
  collector.RecordWrite("mode-app", 1000, 1001, 20, 8192);

  std::unordered_map<uint32_t, uint64_t> bytes_by_fsid;
  collector.VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey& key,
                             const eos::fst::traffic_shaping::IoStatsEntry& entry) {
    bytes_by_fsid[key.fsid] = entry.bytes_written.load(std::memory_order_relaxed);
  });

  ASSERT_EQ(2u, bytes_by_fsid.size());
  EXPECT_EQ(4096u, bytes_by_fsid[10]);
  EXPECT_EQ(8192u, bytes_by_fsid[20]);

  ASSERT_TRUE(collector.SetFilesystemDetailEnabled(false));
  collector.RecordWrite("mode-app", 1000, 1001, 10, 4096);
  collector.RecordWrite("mode-app", 1000, 1001, 20, 8192);

  bytes_by_fsid.clear();
  collector.VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey& key,
                             const eos::fst::traffic_shaping::IoStatsEntry& entry) {
    bytes_by_fsid[key.fsid] = entry.bytes_written.load(std::memory_order_relaxed);
  });

  ASSERT_EQ(1u, bytes_by_fsid.size());
  EXPECT_EQ(12288u, bytes_by_fsid[0]);
}

TEST(IoDelayConfigTest, ScalesWriteDelayWithBufferSize)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_write_delay())["buffer-app"] = 100000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("buffer-app");
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            100000);
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes / 2),
            50000);
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes * 2),
            200000);
}

TEST(IoDelayConfigTest, UsesLargestMatchingPolicyDelay)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_read_delay())["mixed-policy-app"] = 90000;
  (*config.mutable_uid_read_delay())[4242] = 120000;
  (*config.mutable_gid_read_delay())[5555] = 60000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("mixed-policy-app", 4242, 5555);
  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            120000);
}

TEST(IoDelayConfigTest, ClearsDelayWhenDisabled)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_uid_write_delay())[4242] = 100000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("disabled-app", 4242, 1000);
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            100000);

  delay_config.SetEnabled(false);
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            0);
}

TEST(IoDelayConfigTest, CapsScaledDelay)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_write_delay())["large-buffer-app"] = 1000000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("large-buffer-app");
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(vid, UINT64_MAX),
            eos::fst::traffic_shaping::kMaxScaledIoDelayUs);
}
