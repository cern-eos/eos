//------------------------------------------------------------------------------
//! @file ConsoleCliValidationTest.cc
//! @brief Verify existing command rejection and route request contracts.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class ConsoleCliValidationTest : public ConsoleTestFixture {};

struct InvalidCliCase {
  const char* id;
  const char* command;
  std::vector<std::string> args;
};

class InvalidConsoleCliTest : public ConsoleCliValidationTest,
                              public ::testing::WithParamInterface<InvalidCliCase> {};

TEST_P(InvalidConsoleCliTest, RejectsBeforeSending)
{
  expectRejected(GetParam().command, GetParam().args);
}

INSTANTIATE_TEST_SUITE_P(
    StrictGrammar, InvalidConsoleCliTest,
    ::testing::ValuesIn(std::vector<InvalidCliCase>{
        {"ConfigListMultipleSubcommands", "config", {"ls", "reset"}},
        {"ConfigSaveExtra", "config", {"save", "name", "unused"}},
        {"ConfigChangelogNegative", "config", {"changelog", "-1"}},
        {"ConfigChangelogOverflow", "config", {"changelog", "18446744073709551616"}},
        {"ConfigChangelogServerOverflow", "config", {"changelog", "4294967296"}},
        {"ConfigChangelogEmpty", "config", {"changelog", ""}},
        {"GroupFractionalDepth", "group", {"ls", "-g", "1.5"}},
        {"GroupHexDepth", "group", {"ls", "-g", "0x10"}},
        {"GroupServerDepthOverflow", "group", {"ls", "-g", "4294967296"}},
        {"GroupEmptyDepth", "group", {"ls", "-g", ""}},
        {"GroupSetUppercaseState", "group", {"set", "default.0", "ON"}},
        {"DfMonitorHumanConflict", "df", {"-m", "-H"}},
        {"DfMonitorBytesConflict", "df", {"-m", "-b"}},
        {"DfHumanBytesConflict", "df", {"-H", "-b"}},
        {"DfExtraAfterPath", "df", {"/eos/test", "unused"}},
        {"RouteEmptyListPath", "route", {"ls", ""}},
        {"RouteEmptyUnlinkPath", "route", {"unlink", ""}},
        {"RouteWhitespacePath", "route", {"link", "/eos/a b", "mgm.example.org"}},
        {"RouteTrailingParent", "route", {"link", "/eos/..", "mgm.example.org"}},
        {"RouteTrailingDot", "route", {"link", "/eos/.", "mgm.example.org"}},
        {"RouteMultipleSubcommands", "route", {"ls", "unlink", "/eos/test"}},
        {"RouteDestinationWhitespace",
         "route",
         {"link", "/eos/test", "mgm.example.org, other.example.org"}},
    }),
    [](const ::testing::TestParamInfo<InvalidCliCase>& info) { return info.param.id; });
} // namespace

TEST_F(ConsoleCliValidationTest, RoutePreservesValidHostnamesAndAddresses)
{
  for (const auto& hostname : {std::string("localhost"), std::string("mgm-1.Example.org"),
                               std::string("mgm.example.org."), std::string("127.0.0.1"),
                               std::string(63, 'a') + ".example.org"}) {
    SCOPED_TRACE(hostname);
    run("route", {"link", "/eos/route", hostname + ":1095:8001"});
    const auto proto = protobuf(false);
    ASSERT_EQ(proto.route().link().endpoints_size(), 1);
    EXPECT_EQ(proto.route().link().endpoints(0).fqdn(), hostname);
    EXPECT_EQ(proto.route().link().endpoints(0).xrd_port(), 1095u);
    EXPECT_EQ(proto.route().link().endpoints(0).http_port(), 8001u);
  }
}
