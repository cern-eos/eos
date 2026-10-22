#include "mgm/shaping/IoPressure.hh"
#include "gtest/gtest.h"

#define IN_TEST_HARNESS
#include "mgm/shaping/TrafficShaping.hh"
#undef IN_TEST_HARNESS

#include <optional>
#include <string>
#include <utility>
#include <vector>

using eos::mgm::traffic_shaping::CollectMaximumIoPressure;
using eos::mgm::traffic_shaping::ParseFreshFilesystemIoPressureForTest;

TEST(IoPressure, UnknownSamplesDoNotBecomeValidZero)
{
  const std::vector<std::optional<double>> unknown{std::nullopt, std::nullopt};
  EXPECT_FALSE(
      CollectMaximumIoPressure(unknown, [](const auto& value) { return value; }, true));
  const std::vector<std::optional<double>> zero{std::nullopt, 0.0, std::nullopt};
  const auto result =
      CollectMaximumIoPressure(zero, [](const auto& value) { return value; }, true);
  ASSERT_TRUE(result);
  EXPECT_DOUBLE_EQ(0.0, *result);
}

TEST(IoPressure, OnlyFreshUsableSaturationCanSkipRemainingSources)
{
  using Sample = std::pair<std::string, std::string>;
  constexpr int64_t now_ms = 1'000'000;
  const std::vector<Sample> sources{
      {"1", "954999"},     {"1", "1005001"},   {"nan", "1000000"}, {"bad", "1000000"},
      {"0.25", "1000000"}, {"1.5", "1000000"}, {"0.75", "1000000"}};
  size_t visited = 0;
  const auto result = CollectMaximumIoPressure(
      sources,
      [&](const auto& sample) {
        ++visited;
        return ParseFreshFilesystemIoPressureForTest(sample.first, sample.second, now_ms);
      },
      true);
  ASSERT_TRUE(result);
  EXPECT_DOUBLE_EQ(1.0, *result);
  EXPECT_EQ(6u, visited);
}

TEST(IoPressure, FilesystemSnapshotsVisitEverySourceAfterSaturation)
{
  const std::vector<std::optional<double>> sources{0.3, 1.0, std::nullopt, 0.7};
  size_t visited = 0;
  const auto result = CollectMaximumIoPressure(
      sources,
      [&](const auto& value) {
        ++visited;
        return value;
      },
      false);
  ASSERT_TRUE(result);
  EXPECT_DOUBLE_EQ(1.0, *result);
  EXPECT_EQ(sources.size(), visited);
}

TEST(IoPressure, NodeMaximumMatchesExhaustiveReduction)
{
  for (size_t saturated = 0; saturated < 20; ++saturated) {
    std::vector<std::optional<double>> sources;
    for (size_t i = 0; i < 20; ++i) {
      sources.emplace_back(i % 4 == 0 ? std::nullopt
                                      : std::optional<double>{(i % 10) / 10.0});
    }
    sources[saturated] = 1.0;
    const auto read = [](const auto& value) { return value; };
    EXPECT_EQ(CollectMaximumIoPressure(sources, read, false),
              CollectMaximumIoPressure(sources, read, true));
  }
}
