//------------------------------------------------------------------------------
//! @file RecycleCliTest.cc
//! @brief Verify existing recycle argument handling and serialized requests.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class RecycleCliTest : public ConsoleTestFixture {};

struct InvalidRecycleArguments {
  const char* name;
  std::vector<std::string> arguments;
};

class InvalidRecycleCliTest
    : public ConsoleTestFixture,
      public ::testing::WithParamInterface<InvalidRecycleArguments> {};
} // namespace

TEST_F(RecycleCliTest, AcceptsCalendarDatesAndListingShard)
{
  for (const auto& date : {"2024/02/29", "2000/02/29", "2026/10/15/0", "2026/12/31/42"}) {
    SCOPED_TRACE(date);
    run("recycle", {"ls", date});
    EXPECT_EQ(protobuf(false).recycle().ls().date(), date);
  }
  run("recycle", {"purge", "--uid", "2024/02/29"});
  EXPECT_EQ(protobuf(false).recycle().purge().date(), "2024/02/29");
}

TEST_F(RecycleCliTest, NumericValuesRemainDecimalWithLeadingZeroes)
{
  run("recycle", {"ls", "2026", "010"});
  EXPECT_EQ(protobuf(false).recycle().ls().maxentries(), 10);
  run("recycle", {"config", "--collect-interval", "010"});
  EXPECT_EQ(protobuf(false).recycle().config().size(), 10u);
  run("recycle", {"config", "--lifetime", "0100"});
  EXPECT_EQ(protobuf(false).recycle().config().lifetimesec(), 100);
  run("recycle", {"config", "--size", "010K"});
  EXPECT_EQ(protobuf(false).recycle().config().size(), 10000u);
}

TEST_P(InvalidRecycleCliTest, RejectsBeforeSendingRequest)
{
  expectRejected("recycle", GetParam().arguments);
}

INSTANTIATE_TEST_SUITE_P(
    StrictArguments, InvalidRecycleCliTest,
    ::testing::Values(
        InvalidRecycleArguments{"PurgeTrailing", {"purge", "2026", "extra"}},
        InvalidRecycleArguments{"RestoreEmptyKey", {"restore", ""}},
        InvalidRecycleArguments{"ProjectEmptyPath", {"project", "--path", ""}},
        InvalidRecycleArguments{"ProjectTrailing",
                                {"project", "--path", "/eos/a", "ignored"}},
        InvalidRecycleArguments{"ConfigNegatedDump", {"config", "--dump=false"}},
        InvalidRecycleArguments{"ConfigSizeOverflow",
                                {"config", "--size", "18446744073709551616"}},
        InvalidRecycleArguments{"ConfigUnitOverflow",
                                {"config", "--size", "18446744073709551615K"}}),
    [](const ::testing::TestParamInfo<InvalidRecycleArguments>& info) {
      return info.param.name;
    });
