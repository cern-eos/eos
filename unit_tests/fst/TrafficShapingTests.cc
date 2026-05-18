#include "common/VirtualIdentity.hh"
#include "fst/storage/TrafficShaping.hh"

#include "gtest/gtest.h"

#include <algorithm>
#include <cstdint>
#include <string>
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

void
ExpectNearDelay(const uint64_t actual, const uint64_t expected)
{
  constexpr uint64_t tolerance_us = 5000;
  EXPECT_GE(actual, expected > tolerance_us ? expected - tolerance_us : 0);
  EXPECT_LE(actual, expected + tolerance_us);
}

} // namespace

TEST(IoDelayConfigTest, ReservesPipelinedWriteDelayForSameApp)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_write_delay())["pipeline-app"] = 100000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("pipeline-app");
  EXPECT_EQ(delay_config.GetWriteDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            100000);

  ExpectNearDelay(delay_config.ReserveWriteDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  400000);
  ExpectNearDelay(delay_config.ReserveWriteDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  800000);
  ExpectNearDelay(delay_config.ReserveWriteDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  1200000);
  ExpectNearDelay(delay_config.ReserveWriteDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  1600000);
}

TEST(IoDelayConfigTest, ReservesPipelinedReadDelayForSameApp)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_read_delay())["pipeline-app"] = 90000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("pipeline-app");
  EXPECT_EQ(delay_config.GetReadDelayForAppUidGid(
                vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
            120000);

  ExpectNearDelay(delay_config.ReserveReadDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  480000);
  ExpectNearDelay(delay_config.ReserveReadDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  960000);
  ExpectNearDelay(delay_config.ReserveReadDelayForAppUidGid(
                      vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  1440000);
}

TEST(IoDelayConfigTest, ReservesUidPolicyAcrossApps)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_uid_write_delay())[4242] = 100000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto first_vid = MakeVid("first-app", 4242, 1000);
  const auto second_vid = MakeVid("second-app", 4242, 1000);

  ExpectNearDelay(delay_config.ReserveWriteDelayForAppUidGid(
                      first_vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  400000);
  ExpectNearDelay(delay_config.ReserveWriteDelayForAppUidGid(
                      second_vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes),
                  800000);
}

TEST(IoDelayConfigTest, ParallelReservationFactorCompensatesIndependentLanes)
{
  constexpr int kLaneCount = 4;
  constexpr int kChunkCount = 10;

  std::vector<eos::fst::traffic_shaping::IoDelayConfig> lane_configs(kLaneCount);
  for (auto& delay_config : lane_configs) {
    eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
    (*config.mutable_app_write_delay())["pipeline-app"] = 1000000;
    delay_config.SetEnabled(true);
    delay_config.UpdateConfig(std::move(config));
  }

  const auto vid = MakeVid("pipeline-app");
  std::vector<uint64_t> lane_totals(kLaneCount, 0);

  for (int chunk = 0; chunk < kChunkCount; ++chunk) {
    const int lane = chunk % kLaneCount;
    lane_totals[lane] = lane_configs[lane].ReserveWriteDelayForAppUidGid(
        vid, eos::fst::traffic_shaping::kIoDelayReferenceBytes);
  }

  ExpectNearDelay(*std::max_element(lane_totals.begin(), lane_totals.end()), 12000000);
}

TEST(IoDelayConfigTest, SmallBufferReservationsRemainProportional)
{
  eos::fst::traffic_shaping::IoDelayConfig delay_config;
  eos::traffic_shaping::TrafficShapingFstIoDelayConfig config;
  (*config.mutable_app_write_delay())["small-buffer-app"] = 10000;

  delay_config.SetEnabled(true);
  delay_config.UpdateConfig(std::move(config));

  const auto vid = MakeVid("small-buffer-app");
  constexpr uint64_t chunk_size = 512;
  constexpr uint64_t expected_delay_us = 40000;
  uint64_t total_delay_us = 0;

  for (uint64_t bytes = 0; bytes < eos::fst::traffic_shaping::kIoDelayReferenceBytes;
       bytes += chunk_size) {
    total_delay_us = delay_config.ReserveWriteDelayForAppUidGid(vid, chunk_size);
  }

  EXPECT_NEAR(static_cast<double>(total_delay_us), static_cast<double>(expected_delay_us),
              2000.0);
}
