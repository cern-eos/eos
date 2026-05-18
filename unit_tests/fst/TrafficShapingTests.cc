#include "common/VirtualIdentity.hh"
#include "fst/storage/TrafficShaping.hh"

#include "gtest/gtest.h"

#include <cstdint>
#include <string>
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
