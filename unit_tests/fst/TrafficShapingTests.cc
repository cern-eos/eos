#include "common/VirtualIdentity.hh"
#include "fst/storage/TrafficShaping.hh"

#include "gtest/gtest.h"

#include <cstdint>
#include <new>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

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

TEST(IoStatsCollectorTest, ValidatesReportingPeriodWithoutNarrowingOrZero)
{
  using eos::fst::traffic_shaping::ParseFstIoStatsReportingPeriodMilliseconds;

  uint32_t period_ms = 777;
  EXPECT_TRUE(ParseFstIoStatsReportingPeriodMilliseconds("50", period_ms));
  EXPECT_EQ(50u, period_ms);
  EXPECT_TRUE(ParseFstIoStatsReportingPeriodMilliseconds("200", period_ms));
  EXPECT_EQ(200u, period_ms);
  EXPECT_TRUE(ParseFstIoStatsReportingPeriodMilliseconds("3000", period_ms));
  EXPECT_EQ(3000u, period_ms);

  for (const std::string value :
       {"", "0", "49", "3001", "4294967296", "18446744073709551616", "-1", "+200", " 200",
        "200ms"}) {
    period_ms = 777;
    EXPECT_FALSE(ParseFstIoStatsReportingPeriodMilliseconds(value, period_ms)) << value;
    EXPECT_EQ(777u, period_ms) << value;
  }
}

TEST(IoStatsCollectorTest, SanitizesUnsafeReportingPeriodToDefault)
{
  using eos::fst::traffic_shaping::SanitizeFstIoStatsReportingPeriodMilliseconds;

  EXPECT_EQ(200u, SanitizeFstIoStatsReportingPeriodMilliseconds(0));
  EXPECT_EQ(200u, SanitizeFstIoStatsReportingPeriodMilliseconds(49));
  EXPECT_EQ(50u, SanitizeFstIoStatsReportingPeriodMilliseconds(50));
  EXPECT_EQ(3000u, SanitizeFstIoStatsReportingPeriodMilliseconds(3000));
  EXPECT_EQ(200u, SanitizeFstIoStatsReportingPeriodMilliseconds(3001));
  EXPECT_EQ(200u, SanitizeFstIoStatsReportingPeriodMilliseconds(UINT32_MAX));
}

TEST(IoDelayConfigTest, RejectsUnsafeConfigBounds)
{
  using eos::fst::traffic_shaping::ValidateFstIoDelayConfig;

  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_uid_read_delay())[1000] = 1000;
  (*config.mutable_app_write_delay())["bounded-app"] = 2000;
  size_t entries = 0;
  EXPECT_TRUE(ValidateFstIoDelayConfig(config, entries));
  EXPECT_EQ(2u, entries);

  (*config.mutable_uid_read_delay())[1000] =
      eos::fst::traffic_shaping::kMaxScaledIoDelayUs + 1;
  EXPECT_FALSE(ValidateFstIoDelayConfig(config, entries));

  config.mutable_uid_read_delay()->clear();
  config.mutable_app_write_delay()->clear();
  (*config.mutable_app_write_delay())[std::string(
      eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES + 1, 'x')] = 1;
  EXPECT_FALSE(ValidateFstIoDelayConfig(config, entries));
}

TEST(IoStatsCollectorTest, BoundsUntrustedApplicationIdentity)
{
  eos::fst::traffic_shaping::IoStatsCollector collector;
  collector.SetEnabled(true);

  const std::string oversized_app(eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES + 1,
                                  'x');
  collector.RecordRead(oversized_app, 1000, 1001, 10, 4096);

  size_t entries = 0;
  collector.VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey& key,
                             const eos::fst::traffic_shaping::IoStatsEntry&) {
    ++entries;
    EXPECT_EQ(eos::common::traffic_shaping::kUnknownId, key.app);
  });
  EXPECT_EQ(1u, entries);
}

TEST(IoStatsCollectorTest, BoundsConcurrentUntrustedStreamCardinality)
{
  using eos::fst::traffic_shaping::kMaxIoStatsEntries;

  eos::fst::traffic_shaping::IoStatsCollector collector;
  collector.SetEnabled(true);

  constexpr size_t thread_count = 8;
  constexpr size_t entries_per_thread = 1100;
  std::vector<std::thread> workers;
  workers.reserve(thread_count);
  for (size_t thread = 0; thread < thread_count; ++thread) {
    workers.emplace_back([&, thread] {
      for (size_t entry = 0; entry < entries_per_thread; ++entry) {
        collector.RecordWrite("flood-" + std::to_string(thread) + "-" +
                                  std::to_string(entry),
                              1000, 1001, 10, 1);
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }

  size_t admitted = 0;
  collector.VisitEntries(
      [&](const eos::fst::traffic_shaping::IoStatsKey&,
          const eos::fst::traffic_shaping::IoStatsEntry&) { ++admitted; });
  EXPECT_EQ(kMaxIoStatsEntries, admitted);
  EXPECT_EQ(thread_count * entries_per_thread - kMaxIoStatsEntries,
            collector.GetRejectedEntryCount());
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

TEST(IoStatsCollectorTest, ClearInvalidatesCachedEntry)
{
  eos::fst::traffic_shaping::IoStatsCollector collector;
  collector.SetEnabled(true);

  collector.RecordRead("cached-app", 1000, 1001, 10, 4096);
  collector.Clear();
  collector.RecordRead("cached-app", 1000, 1001, 10, 8192);

  size_t entries = 0;
  uint64_t bytes_read = 0;
  uint64_t read_iops = 0;
  collector.VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey&,
                             const eos::fst::traffic_shaping::IoStatsEntry& entry) {
    ++entries;
    bytes_read = entry.bytes_read.load(std::memory_order_relaxed);
    read_iops = entry.read_iops.load(std::memory_order_relaxed);
  });

  EXPECT_EQ(1u, entries);
  EXPECT_EQ(8192u, bytes_read);
  EXPECT_EQ(1u, read_iops);
}

TEST(IoStatsCollectorTest, PruningInvalidatesCachedEntry)
{
  eos::fst::traffic_shaping::IoStatsCollector collector;
  collector.SetEnabled(true);

  collector.RecordWrite("cached-app", 1000, 1001, 10, 4096);
  ASSERT_EQ(1u, collector.PruneStaleEntries(-1));
  collector.RecordWrite("cached-app", 1000, 1001, 10, 8192);

  size_t entries = 0;
  uint64_t bytes_written = 0;
  uint64_t write_iops = 0;
  collector.VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey&,
                             const eos::fst::traffic_shaping::IoStatsEntry& entry) {
    ++entries;
    bytes_written = entry.bytes_written.load(std::memory_order_relaxed);
    write_iops = entry.write_iops.load(std::memory_order_relaxed);
  });

  EXPECT_EQ(1u, entries);
  EXPECT_EQ(8192u, bytes_written);
  EXPECT_EQ(1u, write_iops);
}

TEST(IoStatsCollectorTest, DoesNotReuseCacheAcrossCollectorLifetimes)
{
  using Collector = eos::fst::traffic_shaping::IoStatsCollector;
  alignas(Collector) unsigned char storage[sizeof(Collector)];

  auto* collector = new (storage) Collector();
  collector->SetEnabled(true);
  collector->RecordRead("cached-app", 1000, 1001, 10, 4096);
  collector->~Collector();

  collector = new (storage) Collector();
  collector->SetEnabled(true);
  collector->RecordRead("cached-app", 1000, 1001, 10, 8192);

  size_t entries = 0;
  uint64_t bytes_read = 0;
  collector->VisitEntries([&](const eos::fst::traffic_shaping::IoStatsKey&,
                              const eos::fst::traffic_shaping::IoStatsEntry& entry) {
    ++entries;
    bytes_read = entry.bytes_read.load(std::memory_order_relaxed);
  });

  EXPECT_EQ(1u, entries);
  EXPECT_EQ(8192u, bytes_read);
  collector->~Collector();
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

TEST(IoDelayConfigTest, OversizedAppStillAppliesUidAndGidPolicies)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  const std::string oversized_app(eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES + 1,
                                  'x');
  (*config.mutable_app_read_delay())[oversized_app] = 900000;
  (*config.mutable_uid_read_delay())[4242] = 120000;
  (*config.mutable_gid_read_delay())[5555] = 60000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid(oversized_app, 4242, 5555);
  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            120000);
}

TEST(IoDelayConfigTest, ConfigUpdateInvalidatesCachedDelay)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig first_config;
  (*first_config.mutable_app_read_delay())["cached-app"] = 90000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(first_config));

  const auto vid = MakeVid("cached-app");
  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            90000);

  eos::traffic_shaping::TrafficShapingFstIoDelayConfig second_config;
  (*second_config.mutable_app_read_delay())["cached-app"] = 140000;
  delay_config.UpdateConfig(std::move(second_config));

  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            140000);
}

TEST(IoDelayConfigTest, CachesReadAndWriteDelaysIndependently)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_read_delay())["duplex-app"] = 60000;
  (*config.mutable_app_write_delay())["duplex-app"] = 120000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("duplex-app");
  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            60000);
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            120000);
  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes / 2),
            30000);
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

  delay_config.SetEnabled(true);
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
