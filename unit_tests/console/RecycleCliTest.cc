//------------------------------------------------------------------------------
//! @file RecycleCliTest.cc
//! @brief Validate recycle CLI11 argument handling and serialized requests.
//------------------------------------------------------------------------------

#include "console/commands/helpers/RecycleHelper.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include <limits>

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

TEST_F(RecycleCliTest, ListFlagsAfterDateRemainFlags)
{
  run("recycle", {"ls", "2026/10/15", "-m", "-n"});
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.recycle().ls().date(), "2026/10/15");
  EXPECT_TRUE(proto.recycle().ls().monitorfmt());
  EXPECT_TRUE(proto.recycle().ls().numericids());
  EXPECT_EQ(proto.recycle().ls().maxentries(), 0);
  run("recycle", {"ls", "2026", "-m", "100", "--rid", "42"});
  const auto limited = protobuf(false);
  EXPECT_EQ(limited.recycle().ls().maxentries(), 100);
  EXPECT_TRUE(limited.recycle().ls().monitorfmt());
  EXPECT_EQ(limited.recycle().ls().recycleid(), "42");
}

TEST_F(RecycleCliTest, PreservesPathsKeysAndAclsFromArgv)
{
  run("recycle", {"ls", "--project", "/eos/project with spaces&x=y"});
  EXPECT_EQ(protobuf(false).recycle().ls().path(), "/eos/project with spaces&x=y");
  run("recycle", {"project", "--acl", "u:123=rx, g:456=rx", "--path", "/eos/a b&c=d"});
  const auto project = protobuf(false).recycle().project();
  EXPECT_EQ(project.path(), "/eos/a b&c=d");
  EXPECT_EQ(project.acl(), "u:123=rx, g:456=rx");
  run("recycle", {"restore", "key with spaces&x=y"});
  EXPECT_EQ(protobuf(false).recycle().restore().key(), "key with spaces&x=y");
  run("recycle", {"config", "--add-bin", "/eos/bin with spaces&x=y"});
  EXPECT_EQ(protobuf(false).recycle().config().subtree(), "/eos/bin with spaces&x=y");
}

TEST_F(RecycleCliTest, BinConfigurationPreservesLiteralPathsAndIdentifiers)
{
  using Config = eos::console::RecycleProto::ConfigProto;
  for (const auto* operand : {"/eos/bin with spaces",
                              "/eos/bin&x=y",
                              "/eos/bin&mgm.fixture=value",
                              "/eos/bin&eos.fixture=value",
                              "/eos/bin&xrd.fixture=value",
                              "/eos/bin&callback=value",
                              "/eos/bin&authz=value",
                              "/eos/bin%26mgm.fixture=value",
                              "/eos/bin&x=y&mgm.fixture=value",
                              "/eos/bin#AND#literal",
                              "cid:42",
                              "cxid:2a",
                              "pid:42",
                              "pxid:2a",
                              "fid:42",
                              "fxid:2a",
                              "cid:00042",
                              "cxid:0002A",
                              "pid:00042",
                              "pxid:0002A",
                              "fid:00042",
                              "fxid:0002A",
                              "cid:18446744073709551615",
                              "cxid:FFFFFFFFFFFFFFFF",
                              "pid:18446744073709551615",
                              "pxid:FFFFFFFFFFFFFFFF",
                              "fid:18446744073709551615",
                              "fxid:FFFFFFFFFFFFFFFF"}) {
    for (const auto* option : {"--add-bin", "--remove-bin"}) {
      SCOPED_TRACE(option);
      SCOPED_TRACE(operand);
      run("recycle", {"config", option, operand});
      const auto config = protobuf(false).recycle().config();
      EXPECT_EQ(config.op(),
                std::string(option) == "--add-bin" ? Config::ADD_BIN : Config::RM_BIN);
      EXPECT_EQ(config.subtree(), operand);
    }
  }
}

TEST_F(RecycleCliTest, BinConfigurationRejectsMalformedIdentifiers)
{
  for (const auto* option : {"--add-bin", "--remove-bin"}) {
    SCOPED_TRACE(option);
    for (const auto* prefix : {"cid:", "cxid:", "pid:", "pxid:", "fid:", "fxid:"}) {
      SCOPED_TRACE(prefix);
      for (const auto* number : {"", "0", "000", "-1", "+1", " 1", "1 ", "42junk", "0x2a",
                                 "2g", "42&mgm.attr.key=user.fixture"}) {
        SCOPED_TRACE(number);
        expectRejected("recycle", {"config", option, std::string(prefix) + number});
      }
      const bool hexadecimal = std::string(prefix).find('x') != std::string::npos;
      expectRejected("recycle",
                     {"config", option,
                      std::string(prefix) +
                          (hexadecimal ? "10000000000000000" : "18446744073709551616")});
      if (!hexadecimal) {
        expectRejected("recycle", {"config", option, std::string(prefix) + "2a"});
      }
    }
    for (const auto* operand : {"", "relative", "inode:42", "gid:42", "CID:42"}) {
      SCOPED_TRACE(operand);
      expectRejected("recycle", {"config", option, operand});
    }
  }
}

TEST_F(RecycleCliTest, ProjectPathsStillRequireAbsolutePaths)
{
  for (const auto* operand :
       {"cid:42", "cxid:2a", "pid:42", "pxid:2a", "fid:42", "fxid:2a"}) {
    SCOPED_TRACE(operand);
    expectRejected("recycle", {"ls", "--project", operand});
    expectRejected("recycle", {"project", "--path", operand});
  }
}

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

TEST_F(RecycleCliTest, AcceptsNumericBoundariesWithoutTruncation)
{
  run("recycle", {"ls", "--rid", "18446744073709551615", "2026", "2147483647"});
  const auto listing = protobuf(false).recycle().ls();
  EXPECT_EQ(listing.recycleid(), "18446744073709551615");
  EXPECT_EQ(listing.maxentries(), std::numeric_limits<int32_t>::max());
  run("recycle", {"config", "--lifetime", "2147483647"});
  EXPECT_EQ(protobuf(false).recycle().config().lifetimesec(),
            std::numeric_limits<int32_t>::max());
  run("recycle", {"config", "--collect-interval", "9223372036854775807"});
  EXPECT_EQ(protobuf(false).recycle().config().size(),
            std::numeric_limits<int64_t>::max());
  run("recycle", {"config", "--size", "18446744073709551615"});
  EXPECT_EQ(protobuf(false).recycle().config().size(),
            std::numeric_limits<uint64_t>::max());
  run("recycle", {"config", "--remove-interval", "10"});
  EXPECT_EQ(protobuf(false).recycle().config().size(), 10u);
  run("recycle", {"config", "--ratio", "0.99"});
  EXPECT_FLOAT_EQ(protobuf(false).recycle().config().ratio(), 0.99f);
}

TEST_F(RecycleCliTest, SizesUseExplicitDecimalAndBinaryUnits)
{
  for (const auto& input :
       {std::make_pair("5M", 5000000ull), std::make_pair("2KiB", 2048ull),
        std::make_pair("1.5G", 1500000000ull), std::make_pair("1.5KiB", 1536ull),
        std::make_pair("0.001K", 1ull), std::make_pair("0", 0ull)}) {
    SCOPED_TRACE(input.first);
    run("recycle", {"config", "--size", input.first});
    EXPECT_EQ(protobuf(false).recycle().config().size(), input.second);
    run("recycle", {"config", "--inodes", input.first});
    EXPECT_EQ(protobuf(false).recycle().config().size(), input.second);
  }
}

TEST(RecycleHelperCli, LegacyStringEntryPointUsesSameParser)
{
  RecycleHelper helper(GlobalOptions{});
  ASSERT_TRUE(helper.ParseCommand(
      "project --path \"/eos/a b&c=d\" --acl \"u:123=rx, g:456=rx\""));
  EXPECT_EQ(helper.GetRequest().recycle().project().path(), "/eos/a b&c=d");
  EXPECT_EQ(helper.GetRequest().recycle().project().acl(), "u:123=rx, g:456=rx");
  EXPECT_FALSE(helper.ParseCommand("ls 2026junk"));
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

TEST_F(RecycleCliTest, LifetimeRetainsLegacyDurationUnits)
{
  for (const auto& input : {std::make_pair("2d", 172800), std::make_pair("1.5h", 5400),
                            std::make_pair("2min", 120), std::make_pair("60s", 60),
                            std::make_pair("1w", 604800), std::make_pair("1mo", 2678400),
                            std::make_pair("1y", 31536000)}) {
    SCOPED_TRACE(input.first);
    run("recycle", {"config", "--lifetime", input.first});
    EXPECT_EQ(protobuf(false).recycle().config().lifetimesec(), input.second);
  }
  expectRejected("recycle", {"config", "--lifetime", "30000d"});
}

TEST_F(RecycleCliTest, FractionalQuotaValuesMustConvertExactlyWithoutOverflow)
{
  run("recycle", {"config", "--size", "18446744073709551615.0B"});
  EXPECT_EQ(protobuf(false).recycle().config().size(),
            std::numeric_limits<uint64_t>::max());
  run("recycle", {"config", "--size", "0.000000000931322574615478515625GiB"});
  EXPECT_EQ(protobuf(false).recycle().config().size(), 1u);
  for (const auto* invalid : {"0.1B", "0.0001K", "1.1KiB", "18446744073709551615.1B",
                              "18446744073709551616.0B", "1.", ".", "1e3G"}) {
    SCOPED_TRACE(invalid);
    expectRejected("recycle", {"config", "--size", invalid});
  }
}

TEST_F(RecycleCliTest, HelpTextInsidePathAndAclRemainsLiteral)
{
  run("recycle", {"ls", "--project", "/eos/a --help b"});
  EXPECT_EQ(protobuf(false).recycle().ls().path(), "/eos/a --help b");
  run("recycle",
      {"project", "--path", "/eos/a --help b", "--acl", "u:123=rx --help g:456=rx"});
  const auto result = protobuf(false).recycle().project();
  EXPECT_EQ(result.path(), "/eos/a --help b");
  EXPECT_EQ(result.acl(), "u:123=rx --help g:456=rx");
}

TEST_P(InvalidRecycleCliTest, RejectsBeforeSendingRequest)
{
  expectRejected("recycle", GetParam().arguments);
}

INSTANTIATE_TEST_SUITE_P(
    StrictArguments, InvalidRecycleCliTest,
    ::testing::Values(
        InvalidRecycleArguments{"StatusTrailing", {"-m", "ignored"}},
        InvalidRecycleArguments{"ListMissingId", {"ls", "--rid"}},
        InvalidRecycleArguments{"ListIdNegative", {"ls", "--rid", "-1"}},
        InvalidRecycleArguments{"ListIdSuffix", {"ls", "--rid", "12junk"}},
        InvalidRecycleArguments{"ListIdOverflow",
                                {"ls", "--rid", "18446744073709551616"}},
        InvalidRecycleArguments{"ConflictingSelectors", {"ls", "--all", "--uid"}},
        InvalidRecycleArguments{"ConflictingIdSelector",
                                {"purge", "--all", "--rid", "1", "2026"}},
        InvalidRecycleArguments{"MissingPurgeEntry", {"purge", "--all"}},
        InvalidRecycleArguments{"EmptyPurgeKey", {"purge", "-k", ""}},
        InvalidRecycleArguments{"PurgeTrailing", {"purge", "2026", "extra"}},
        InvalidRecycleArguments{"RestoreTrailing", {"restore", "key", "ignored"}},
        InvalidRecycleArguments{"RestoreUnknownFlag", {"restore", "--unknown", "key"}},
        InvalidRecycleArguments{"RestoreEmptyKey", {"restore", ""}},
        InvalidRecycleArguments{"ProjectEmptyPath", {"project", "--path", ""}},
        InvalidRecycleArguments{"ProjectTrailing",
                                {"project", "--path", "/eos/a", "ignored"}},
        InvalidRecycleArguments{"ProjectEmptyAcl",
                                {"project", "--path", "/eos/a", "--acl", ""}},
        InvalidRecycleArguments{"DateSuffix", {"ls", "2026junk"}},
        InvalidRecycleArguments{"NegativeDate", {"ls", "-2026"}},
        InvalidRecycleArguments{"DateEmptyComponent", {"ls", "2026//01"}},
        InvalidRecycleArguments{"DateTrailingSlash", {"ls", "2026/"}},
        InvalidRecycleArguments{"DateMonthZero", {"ls", "2026/00"}},
        InvalidRecycleArguments{"DateMonthTooLarge", {"ls", "2026/13"}},
        InvalidRecycleArguments{"DateDayZero", {"ls", "2026/01/00"}},
        InvalidRecycleArguments{"DateWrongDay", {"ls", "2026/04/31"}},
        InvalidRecycleArguments{"DateNotLeapYear", {"ls", "2026/02/29"}},
        InvalidRecycleArguments{"DateCenturyNotLeapYear", {"ls", "1900/02/29"}},
        InvalidRecycleArguments{"DateShardSuffix", {"ls", "2026/01/01/1x"}},
        InvalidRecycleArguments{"PurgeShard", {"purge", "2026/01/01/0"}},
        InvalidRecycleArguments{"LimitNegative", {"ls", "2026", "-1"}},
        InvalidRecycleArguments{"LimitZero", {"ls", "2026", "0"}},
        InvalidRecycleArguments{"LimitSuffix", {"ls", "2026", "100junk"}},
        InvalidRecycleArguments{"LimitOverflow", {"ls", "2026", "2147483648"}},
        InvalidRecycleArguments{"ConfigMultipleActions",
                                {"config", "--enable", "off", "--dump"}},
        InvalidRecycleArguments{"ConfigTrailing", {"config", "--dump", "ignored"}},
        InvalidRecycleArguments{"ConfigNegatedDump", {"config", "--dump=false"}},
        InvalidRecycleArguments{"ConfigRelativeSubtree",
                                {"config", "--add-bin", "relative"}},
        InvalidRecycleArguments{"ConfigLifetimeTooShort", {"config", "--lifetime", "59"}},
        InvalidRecycleArguments{"ConfigLifetimeOverflow",
                                {"config", "--lifetime", "2147483648"}},
        InvalidRecycleArguments{"ConfigLifetimeSuffix",
                                {"config", "--lifetime", "60junk"}},
        InvalidRecycleArguments{"ConfigNegativeSize", {"config", "--size", "-1"}},
        InvalidRecycleArguments{"ConfigMalformedSize", {"config", "--size", "pickles"}},
        InvalidRecycleArguments{"ConfigSizeOverflow",
                                {"config", "--size", "18446744073709551616"}},
        InvalidRecycleArguments{"ConfigUnitOverflow",
                                {"config", "--size", "18446744073709551615K"}},
        InvalidRecycleArguments{"ConfigNanRatio", {"config", "--ratio", "nan"}},
        InvalidRecycleArguments{"ConfigInfiniteRatio", {"config", "--ratio", "inf"}},
        InvalidRecycleArguments{"ConfigZeroRatio", {"config", "--ratio", "0"}},
        InvalidRecycleArguments{"ConfigTooLargeRatio", {"config", "--ratio", "1"}},
        InvalidRecycleArguments{"ConfigRatioSuffix", {"config", "--ratio", "0.5junk"}},
        InvalidRecycleArguments{"ConfigCollectTooShort",
                                {"config", "--collect-interval", "9"}},
        InvalidRecycleArguments{"ConfigRemoveNegative",
                                {"config", "--remove-interval", "-1"}},
        InvalidRecycleArguments{"ConfigIntervalSuffix",
                                {"config", "--collect-interval", "10junk"}},
        InvalidRecycleArguments{"ConfigIntervalOverflow",
                                {"config", "--collect-interval", "9223372036854775808"}},
        InvalidRecycleArguments{"ConfigInvalidEnable", {"config", "--enable", "yes"}},
        InvalidRecycleArguments{"ConfigInvalidEnforce", {"config", "--enforce", "true"}},
        InvalidRecycleArguments{"ConfigInvalidDryRun", {"config", "--dry-run", "on"}}),
    [](const ::testing::TestParamInfo<InvalidRecycleArguments>& info) {
      return info.param.name;
    });
