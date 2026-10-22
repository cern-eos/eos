//------------------------------------------------------------------------------
//! @file ConsoleCliValidationTest.cc
//! @brief Validate CLI11 command grammars and the protobufs sent to MGM.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"
#include <limits>

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
        {"ConfigListExtra", "config", {"ls", "--backup", "unused"}},
        {"ConfigListMultipleSubcommands", "config", {"ls", "reset"}},
        {"ConfigDumpMultipleNames", "config", {"dump", "one", "two"}},
        {"ConfigDumpEmptyName", "config", {"dump", ""}},
        {"ConfigLoadExtra", "config", {"load", "name", "unused"}},
        {"ConfigLoadEmptyName", "config", {"load", ""}},
        {"ConfigExportExtra", "config", {"export", "name", "-f", "unused"}},
        {"ConfigExportEmptyName", "config", {"export", ""}},
        {"ConfigSaveEmptyName", "config", {"save", ""}},
        {"ConfigSaveMissingComment", "config", {"save", "name", "-c"}},
        {"ConfigSaveDuplicateComment",
         "config",
         {"save", "name", "-c", "one", "-c", "two"}},
        {"ConfigSaveExtra", "config", {"save", "name", "unused"}},
        {"ConfigChangelogSuffix", "config", {"changelog", "12junk"}},
        {"ConfigChangelogHex", "config", {"changelog", "0x10"}},
        {"ConfigChangelogFraction", "config", {"changelog", "1.5"}},
        {"ConfigChangelogNegative", "config", {"changelog", "-1"}},
        {"ConfigChangelogOverflow", "config", {"changelog", "18446744073709551616"}},
        {"ConfigChangelogServerOverflow", "config", {"changelog", "4294967296"}},
        {"ConfigChangelogExtra", "config", {"changelog", "1", "unused"}},
        {"ConfigChangelogEmpty", "config", {"changelog", ""}},
        {"GroupMultipleSelections", "group", {"ls", "default", "another"}},
        {"GroupEmptySelection", "group", {"ls", ""}},
        {"GroupConflictingMonitorListing", "group", {"ls", "-m", "-l"}},
        {"GroupConflictingMonitorIo", "group", {"ls", "-m", "--io"}},
        {"GroupConflictingMonitorIoFs", "group", {"ls", "-m", "--IO"}},
        {"GroupConflictingListingIo", "group", {"ls", "-l", "--io"}},
        {"GroupConflictingListingIoFs", "group", {"ls", "-l", "--IO"}},
        {"GroupConflictingIoModes", "group", {"ls", "--io", "--IO"}},
        {"GroupDuplicateDepth", "group", {"ls", "-g", "1", "-g", "2"}},
        {"GroupFractionalDepth", "group", {"ls", "-g", "1.5"}},
        {"GroupHexDepth", "group", {"ls", "-g", "0x10"}},
        {"GroupServerDepthOverflow", "group", {"ls", "-g", "4294967296"}},
        {"GroupEmptyDepth", "group", {"ls", "-g", ""}},
        {"GroupRemoveExtra", "group", {"rm", "default.0", "unused"}},
        {"GroupRemoveEmpty", "group", {"rm", ""}},
        {"GroupSetExtra", "group", {"set", "default.0", "on", "unused"}},
        {"GroupSetEmpty", "group", {"set", "", "on"}},
        {"GroupSetUppercaseState", "group", {"set", "default.0", "ON"}},
        {"DfEmptyPath", "df", {""}},
        {"DfMonitorHumanConflict", "df", {"-m", "-H"}},
        {"DfMonitorBytesConflict", "df", {"-m", "-b"}},
        {"DfHumanBytesConflict", "df", {"-H", "-b"}},
        {"DfExtraAfterPath", "df", {"/eos/test", "unused"}},
        {"RouteEmptyListPath", "route", {"ls", ""}},
        {"RouteEmptyUnlinkPath", "route", {"unlink", ""}},
        {"RouteWhitespacePath", "route", {"link", "/eos/a b", "mgm.example.org"}},
        {"RouteTabPath", "route", {"link", "/eos/a\tb", "mgm.example.org"}},
        {"RouteNewlinePath", "route", {"link", "/eos/a\nb", "mgm.example.org"}},
        {"RouteTrailingParent", "route", {"link", "/eos/..", "mgm.example.org"}},
        {"RouteTrailingDot", "route", {"link", "/eos/.", "mgm.example.org"}},
        {"RouteMultipleSubcommands", "route", {"ls", "unlink", "/eos/test"}},
        {"RouteDestinationWhitespace",
         "route",
         {"link", "/eos/test", "mgm.example.org, other.example.org"}},
        {"RouteEmbeddedPortSpace",
         "route",
         {"link", "/eos/test", "mgm.example.org: 1094"}},
        {"RouteDestinationUnknownOption", "route", {"link", "/eos/test", "--unknown"}},
    }),
    [](const ::testing::TestParamInfo<InvalidCliCase>& info) { return info.param.id; });
} // namespace

TEST_F(ConsoleCliValidationTest, ConfigPreservesNamesAndCommentBoundaries)
{
  const std::string name = "configuration 'a' \"b\" & c=d";
  const std::string comment = "literal --help -f text, spaces & key=value \"quotes\"";
  run("config", {"save", "-f", name, "--comment", comment});
  EXPECT_EQ(global_retc, ENOENT);
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.config().save().file(), name);
  EXPECT_TRUE(proto.config().save().force());
  EXPECT_EQ(proto.comment(), comment);

  for (const auto* command : {"dump", "export", "load"}) {
    SCOPED_TRACE(command);
    run("config", {command, name});
    EXPECT_EQ(global_retc, ENOENT);
    const auto request = protobuf(false);
    if (std::string(command) == "dump") {
      EXPECT_EQ(request.config().dump().file(), name);
    } else if (std::string(command) == "export") {
      EXPECT_EQ(request.config().exp().file(), name);
    } else {
      EXPECT_EQ(request.config().load().file(), name);
    }
  }
}

TEST_F(ConsoleCliValidationTest, ConfigUsesContextGlobalOptions)
{
  GlobalOptions options = gGlobalOpts;
  options.mComment = "context comment";
  options.mJsonFormat = true;
  context.globalOpts = &options;
  run("config", {"save", "test"});
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.comment(), "context comment");
  EXPECT_EQ(proto.format(), eos::console::RequestProto::JSON);

  run("config", {"save", "test", "-c", ""});
  EXPECT_TRUE(protobuf(false).comment().empty());
}

TEST_F(ConsoleCliValidationTest, UnsignedCountsPreserveServerRange)
{
  for (const auto* value : {"0", "08", "010", "2147483648", "4294967295"}) {
    SCOPED_TRACE(value);
    const uint64_t expected = std::stoull(value);
    run("config", {"changelog", value});
    EXPECT_EQ(global_retc, ENOENT);
    EXPECT_EQ(protobuf(false).config().changelog().lines(), expected);
    run("group", {"ls", "-g", value});
    EXPECT_EQ(global_retc, ENOENT);
    EXPECT_EQ(protobuf(false).group().ls().outdepth(), expected);
  }
}

TEST_F(ConsoleCliValidationTest, GroupPreservesQuotedNamesAndSelection)
{
  const std::string name = "group with 'single' and \"double\" quotes & x=y";
  run("group", {"ls", name, "--brief", "-g", "3", "--io"});
  const auto selected = protobuf(false);
  EXPECT_EQ(selected.group().ls().selection(), name);
  EXPECT_EQ(selected.group().ls().outdepth(), 3u);
  EXPECT_TRUE(selected.group().ls().outhost());
  EXPECT_EQ(selected.group().ls().outformat(), eos::console::GroupProto_LsProto::IOGROUP);
  run("group", {"rm", name});
  EXPECT_EQ(protobuf(false).group().rm().group(), name);
  run("group", {"set", name, "drain"});
  EXPECT_EQ(protobuf(false).group().set().group(), name);
}

TEST_F(ConsoleCliValidationTest, DfPreservesAbsolutePathAndAcceptsTrailingOptions)
{
  const std::string path = "/eos/a b'c\"d&x=y";
  run("df", {path, "-H"});
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.df().path(), path);
  EXPECT_FALSE(proto.df().si());
  EXPECT_TRUE(proto.df().readable());
}

TEST_F(ConsoleCliValidationTest, RoutePreservesValidPathBytesAndNormalizesSlash)
{
  const std::string path = "/eos/a'b\"c&x=y";
  run("route", {"link", path, "mgm.example.org:1:65535,127.0.0.1"});
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.route().link().path(), path + '/');
  ASSERT_EQ(proto.route().link().endpoints_size(), 2);
  EXPECT_EQ(proto.route().link().endpoints(0).fqdn(), "mgm.example.org");
  EXPECT_EQ(proto.route().link().endpoints(0).xrd_port(), 1u);
  EXPECT_EQ(proto.route().link().endpoints(0).http_port(), 65535u);
  EXPECT_EQ(proto.route().link().endpoints(1).fqdn(), "127.0.0.1");
  EXPECT_EQ(proto.route().link().endpoints(1).xrd_port(), 1094u);
  EXPECT_EQ(proto.route().link().endpoints(1).http_port(), 8000u);
}

TEST_F(ConsoleCliValidationTest, GroupCompletionUsesValidatedCommandGrammar)
{
  auto* group = CommandRegistry::instance().find("group");
  ASSERT_NE(group, nullptr);
  EXPECT_TRUE(group->complete({"ls", "one", "two"}).empty());
  EXPECT_TRUE(group->complete({"ls", "-g"}).empty());
  EXPECT_TRUE(group->complete({"ls", "--"}).empty());
  EXPECT_TRUE(group->complete({"set", ""}).empty());
  EXPECT_TRUE(group->complete({"set", "default.0", "off"}).empty());
  const auto flags = group->complete({"ls"});
  EXPECT_NE(std::find(flags.begin(), flags.end(), "--brief"), flags.end());
  EXPECT_NE(std::find(flags.begin(), flags.end(), "-g"), flags.end());
}

TEST_F(ConsoleCliValidationTest, ConfigCompletionUsesRealSubcommandsAndOptions)
{
  const auto* config = CommandRegistry::instance().find("config");
  ASSERT_NE(config, nullptr);
  const auto commands = config->complete({});
  EXPECT_NE(std::find(commands.begin(), commands.end(), "save"), commands.end());
  EXPECT_NE(std::find(commands.begin(), commands.end(), "changelog"), commands.end());
  const auto flags = config->complete({"save"});
  EXPECT_NE(std::find(flags.begin(), flags.end(), "--comment"), flags.end());
  EXPECT_NE(std::find(flags.begin(), flags.end(), "-c"), flags.end());
  EXPECT_NE(std::find(flags.begin(), flags.end(), "-f"), flags.end());
  EXPECT_TRUE(config->complete({"save", "name", "-c"}).empty());
  EXPECT_TRUE(config->complete({"save", "name", "--"}).empty());
  EXPECT_TRUE(config->complete({"save", "name", "extra"}).empty());
  EXPECT_NE(config->helpText().find("--comment"), std::string::npos);
}

TEST_F(ConsoleCliValidationTest, RouteRejectsMalformedHostnamesBeforeSending)
{
  for (const auto& hostname :
       {std::string("."), std::string("-"), std::string("a..example.org"),
        std::string("-mgm.example.org"), std::string("mgm-.example.org"),
        std::string(".mgm.example.org"), std::string("mgm.example.org.."),
        std::string("256.1.2.3"), std::string("127.1"), std::string("127.0.0.01"),
        std::string(64, 'a') + ".example.org",
        std::string(63, 'a') + "." + std::string(63, 'b') + "." + std::string(63, 'c') +
            "." + std::string(62, 'd')}) {
    SCOPED_TRACE(hostname);
    expectRejected("route", {"link", "/eos/route", hostname});
  }
}

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
