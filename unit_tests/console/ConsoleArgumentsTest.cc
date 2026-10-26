//------------------------------------------------------------------------------
//! @file ConsoleArgumentsTest.cc
//! @brief Cover console prefix options, strict REPL lexing and actual dispatch.
//------------------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/ConsoleMetadata.hh"
#include "console/ConsoleOptions.hh"
#include "mgm/proc/ProcCommand.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include <algorithm>
#include <optional>

namespace {
class LegacyCommentProcCommand : public eos::mgm::ProcCommand {
public:
  std::string
  comment() const
  {
    return mComment.c_str();
  }
};

struct LexicalCase {
  const char* name;
  const char* line;
  std::vector<std::string> expected;
};
class ConsoleLexerTest : public ::testing::TestWithParam<LexicalCase> {};
class ConsoleArgumentsTest : public ConsoleTestFixture {
protected:
  void
  execute(std::string line)
  {
    {
      std::lock_guard<std::mutex> lock(mutex);
      requests.clear();
    }
    execute_line(line.data());
  }
};
} // namespace

TEST_P(ConsoleLexerTest, PreservesArgumentBoundariesWithoutShellExpansion)
{
  std::vector<std::string> arguments;
  std::string error;
  ASSERT_TRUE(eos::console::TokenizeArguments(GetParam().line, arguments, &error))
      << error;
  EXPECT_EQ(arguments, GetParam().expected);
}

INSTANTIATE_TEST_SUITE_P(
    ShellWords, ConsoleLexerTest,
    ::testing::Values(
        LexicalCase{"Whitespace", "  evict \t /eos/a  \r\n", {"evict", "/eos/a"}},
        LexicalCase{"DoubleQuoted", R"(evict "/eos/a b&c=d")", {"evict", "/eos/a b&c=d"}},
        LexicalCase{"SingleQuoted", R"(evict '/eos/a b&c=d')", {"evict", "/eos/a b&c=d"}},
        LexicalCase{"QuotedEmpty", R"(evict "" '')", {"evict", "", ""}},
        LexicalCase{"EscapedSpaces", R"(evict /eos/a\ b)", {"evict", "/eos/a b"}},
        LexicalCase{"EscapedQuotes", R"(evict /eos/a\"b\'c)", {"evict", "/eos/a\"b'c"}},
        LexicalCase{"DoubleEscapedQuote", R"(evict "/eos/a\"b")", {"evict", "/eos/a\"b"}},
        LexicalCase{"DoubleBackslash", R"(evict "/eos/a\\b")", {"evict", "/eos/a\\b"}},
        LexicalCase{
            "PreservedBackslash", R"(evict "/eos/a\qb")", {"evict", "/eos/a\\qb"}},
        LexicalCase{
            "SingleQuoteLiteral", R"(evict '/eos/a\\b"c')", {"evict", "/eos/a\\\\b\"c"}},
        LexicalCase{
            "ConcatenatedQuotes", R"(evict /eos/'a b'" c")", {"evict", "/eos/a b c"}},
        LexicalCase{"LiteralExpansion",
                    R"(evict '$HOME/$(touch x)`command`*')",
                    {"evict", "$HOME/$(touch x)`command`*"}},
        LexicalCase{"OnlyWhitespace", " \t \n", {}},
        LexicalCase{"Ampersand",
                    "register /eos/a&b attr=user.key=a=b&c",
                    {"register", "/eos/a&b", "attr=user.key=a=b&c"}}),
    [](const ::testing::TestParamInfo<LexicalCase>& info) { return info.param.name; });

TEST(ConsoleLexer, RejectsUnterminatedQuotesAndEscapes)
{
  for (const auto* line :
       {"evict '/eos/a", "evict \"/eos/a", "evict /eos/a\\", "evict \"/eos/a\\"}) {
    std::vector<std::string> arguments{"stale"};
    std::string error;
    EXPECT_FALSE(eos::console::TokenizeArguments(line, arguments, &error));
    EXPECT_TRUE(arguments.empty());
    EXPECT_FALSE(error.empty());
  }
}

TEST(ConsoleLexer, HelpDetectionHonorsTokenBoundariesAndDelimiter)
{
  EXPECT_TRUE(wants_help("\"--help\""));
  EXPECT_TRUE(wants_help("-h"));
  EXPECT_FALSE(wants_help("-h", true));
  EXPECT_FALSE(wants_help("-- --help"));
  EXPECT_FALSE(wants_help("-- -h"));
  EXPECT_FALSE(wants_help("\"/eos/name --help suffix\""));
  EXPECT_FALSE(wants_help("\"/eos/name -h suffix\""));
  RegisterNativeConsoleCommands();
  for (const char* name : {"touch", "mv", "file"}) {
    auto* command = CommandRegistry::instance().find(name);
    ASSERT_NE(command, nullptr);
    EXPECT_TRUE(command->requiresMgmArguments({"--", "--help"})) << name;
    EXPECT_TRUE(command->requiresMgmArguments({"--", "-h"})) << name;
    EXPECT_FALSE(command->requiresMgmArguments({"--help"})) << name;
  }
}

TEST(ConsoleLexer, ExpandedHelpIsLocalForCommandsAndHonorsDelimiter)
{
  EXPECT_FALSE(wants_help("--help-all"));
  EnsureNativeCommandRegistryInitialized();
  const auto* ns = CommandRegistry::instance().find("ns");
  ASSERT_NE(ns, nullptr);
  EXPECT_FALSE(ns->requiresMgmArguments({"--help-all"}));
  EXPECT_FALSE(ns->requiresMgmArguments({"cache", "set", "--help-all"}));
  EXPECT_TRUE(ns->requiresMgmArguments({"cache", "--", "--help-all"}));
  EXPECT_TRUE(ns->requiresMgmArguments({"cache", "name --help-all suffix"}));
  const auto* file = CommandRegistry::instance().find("file");
  ASSERT_NE(file, nullptr);
  EXPECT_FALSE(file->requiresMgmArguments({"--help-all"}));
}

TEST(ConsoleOptions, PrefixOptionsAcceptEveryOrder)
{
  std::vector<unsigned> order{0, 1, 2, 3};
  const std::vector<std::vector<std::string>> groups{
      {"-j"}, {"--batch"}, {"--app", "my app"}, {"--role", "123", "456"}};
  do {
    std::vector<std::string> arguments;
    for (auto i : order) {
      arguments.insert(arguments.end(), groups[i].begin(), groups[i].end());
    }
    arguments.insert(arguments.end(), {"root://example", "evict", "--help"});
    eos::console::ConsoleOptions options;
    std::string error;
    ASSERT_TRUE(eos::console::ParseConsoleOptions(arguments, options, error)) << error;
    EXPECT_TRUE(options.json);
    EXPECT_TRUE(options.batch);
    EXPECT_EQ(options.application, "my app");
    EXPECT_EQ(options.role, (std::vector<std::string>{"123", "456"}));
    EXPECT_EQ(options.endpoint, "root://example");
    EXPECT_EQ(options.command, (std::vector<std::string>{"evict", "--help"}));
  } while (std::next_permutation(order.begin(), order.end()));
}

TEST(ConsoleOptions, LeavesAllCommandArgumentsUntouched)
{
  const std::vector<std::string> command{"register", "/eos/O'Brien \\\"quoted\\\"",
                                         "",         "attr=user.key=a=b&c",
                                         "--role",   "literal"};
  eos::console::ConsoleOptions options;
  std::string error;
  ASSERT_TRUE(eos::console::ParseConsoleOptions(command, options, error));
  EXPECT_EQ(options.command, command);
  EXPECT_TRUE(options.role.empty());
  ASSERT_TRUE(
      eos::console::ParseConsoleOptions({"--", "evict", "--", "--help"}, options, error));
  EXPECT_EQ(options.command, (std::vector<std::string>{"evict", "--", "--help"}));
}

TEST(ConsoleOptions, RejectsMissingMalformedAndUnknownPrefixOptions)
{
  for (const auto& arguments :
       std::vector<std::vector<std::string>>{{"--role"},
                                             {"-r", "123"},
                                             {"-r", "", "456"},
                                             {"-r", "-1", "456"},
                                             {"--app"},
                                             {"-a", ""},
                                             {"--unknown"},
                                             {"--unknown", "-j", "evict"},
                                             {"-s", "evict"},
                                             {"--app", "a", "--app", "b"}}) {
    SCOPED_TRACE(::testing::PrintToString(arguments));
    eos::console::ConsoleOptions options;
    std::string error;
    EXPECT_FALSE(eos::console::ParseConsoleOptions(arguments, options, error));
    EXPECT_FALSE(error.empty());
  }
}

TEST(ConsoleOptions, AcceptsGlobalActionsAndEndpointOnly)
{
  eos::console::ConsoleOptions options;
  std::string error;
  ASSERT_TRUE(eos::console::ParseConsoleOptions({"-j", "-s", "ipc://"}, options, error));
  EXPECT_TRUE(options.status);
  EXPECT_TRUE(options.json);
  EXPECT_EQ(options.endpoint, "ipc://");
  EXPECT_TRUE(options.command.empty());
  ASSERT_TRUE(eos::console::ParseConsoleOptions({"--version"}, options, error));
  EXPECT_TRUE(options.version);
  ASSERT_TRUE(eos::console::ParseConsoleOptions({"-h"}, options, error));
  EXPECT_TRUE(options.help);
  ASSERT_TRUE(eos::console::ParseConsoleOptions({"root://example"}, options, error));
  EXPECT_EQ(options.endpoint, "root://example");
  EXPECT_TRUE(options.command.empty());
}

TEST_F(ConsoleArgumentsTest, ExecuteLinePreservesEscapedQuotesAndPaths)
{
  execute(R"(register "/eos/O'Brien \"quoted\" \\backslash" 'attr=user.key=a=b&c')");
  const auto result = protobuf(true).record();
  EXPECT_EQ(result.path(), "/eos/O'Brien \"quoted\" \\backslash");
  EXPECT_EQ(result.attr().at("user.key"), "a=b&c");
}

TEST_F(ConsoleArgumentsTest, ExecuteLineExtractsCommentAsTokenMetadata)
{
  execute(R"(evict "/eos/a --comment b" --comment "audit \"quote\" & x=y")");
  const auto result = protobuf(false);
  EXPECT_EQ(result.evict().file(0).path(), "/eos/a --comment b");
  EXPECT_EQ(result.comment(), "audit \"quote\" & x=y");
  execute(R"(evict -- --comment)");
  const auto literal = protobuf(false);
  EXPECT_EQ(literal.evict().file(0).path(), "/eos/dispatch-test/--comment");
  EXPECT_TRUE(literal.comment().empty());
}

TEST_F(ConsoleArgumentsTest, AttrOriginalArgvPreservesQuotesAndBackslashes)
{
  for (const std::string& value : {"value\"quote", "\"leading", "\"", "\"quoted\"",
                                   "back\\slash", "tail\\", "two words", ""}) {
    SCOPED_TRACE(value);
    run("attr", {"set", "user.attr.json4=" + value, "/eos/attribute"});
    EXPECT_EQ(global_retc, ENOENT);
    const auto wire = request();
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    ASSERT_NE(env.Get("mgm.attr.value"), nullptr);
    std::string decoded;
    EXPECT_TRUE(eos::common::SymKey::DeBase64(env.Get("mgm.attr.value"), decoded));
    EXPECT_EQ(decoded, value);
  }
}

TEST_F(ConsoleArgumentsTest, AttrExecuteLineDecodesOnlyCommandLineEscapes)
{
  const std::vector<std::pair<std::string, std::string>> cases{
      {R"(attr set 'user.attr.json4=value"quote' /eos/attribute)", "value\"quote"},
      {R"(attr set user.attr.json4=value\"quote /eos/attribute)", "value\"quote"},
      {R"(attr set 'user.attr.json4=value\"quote' /eos/attribute)", "value\\\"quote"},
      {R"(attr set 'user.attr.json4="leading' /eos/attribute)", "\"leading"}};
  for (const auto& [line, expected] : cases) {
    SCOPED_TRACE(line);
    execute(line);
    EXPECT_EQ(global_retc, ENOENT);
    const auto wire = request();
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    ASSERT_NE(env.Get("mgm.attr.value"), nullptr);
    std::string decoded;
    EXPECT_TRUE(eos::common::SymKey::DeBase64(env.Get("mgm.attr.value"), decoded));
    EXPECT_EQ(decoded, expected);
  }
}

TEST_F(ConsoleArgumentsTest, ExecuteLineRejectsMalformedBeforeTransport)
{
  for (const auto* line : {"evict '/eos/a", "evict \"/eos/a", "evict /eos/a\\",
                           "evict /eos/a --comment", "evict /eos/a --comment \"\"",
                           "evict /eos/a --comment one --comment two", "evict \"\""}) {
    SCOPED_TRACE(line);
    execute(line);
    EXPECT_NE(global_retc, 0);
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_TRUE(requests.empty());
  }
}

namespace {
class ConsoleMetadataTest : public ConsoleTestFixture {
protected:
  void
  SetUp() override
  {
    if (const char* previous = getenv("EOSAPP")) {
      savedApplication = previous;
    }
    ConsoleTestFixture::SetUp();
    unsetenv("EOSAPP");
  }
  void
  TearDown() override
  {
    if (savedApplication) {
      setenv("EOSAPP", savedApplication->c_str(), 1);
    } else {
      unsetenv("EOSAPP");
    }
    ConsoleTestFixture::TearDown();
  }
  std::optional<std::string> savedApplication;
};
} // namespace

TEST(ConsoleMetadata, RejectsCgiSeparatorsAndEveryControlByte)
{
  EXPECT_TRUE(eos::console::IsSafeCgiMetadata("name /?x=1%20 + :._-"));
  EXPECT_FALSE(eos::console::IsSafeCgiMetadata("app&mgm.option=p"));
  for (unsigned character = 0; character <= 0x7f; ++character) {
    if (character < 0x20 || character == 0x7f) {
      SCOPED_TRACE(character);
      EXPECT_FALSE(eos::console::IsSafeCgiMetadata(
          "app" + std::string(1, static_cast<char>(character)) + "name"));
    }
  }
}

TEST(ConsoleMetadata, MarkedCommentCodecKeepsOldUnmarkedText)
{
  for (const auto& comment : {std::string("audit&mgm.option=p&eos.ruid=0"),
                              std::string("quotes \" and % and /#curl# literal"),
                              std::string("line one\nline two")}) {
    const auto encoded = eos::common::StringConversion::curl_escaped(comment);
    EXPECT_EQ(encoded.find('&'), std::string::npos);
    EXPECT_EQ(eos::common::StringConversion::curl_unescaped(encoded), comment);
  }
  EXPECT_EQ(eos::common::StringConversion::curl_unescaped("old audit 100% %2F text"),
            "old audit 100% %2F text");
}

TEST(ConsoleMetadata, LegacyMgmDecodesMarkedCommentsAndPreservesUnmarkedText)
{
  const std::string comment = "audit \"quote\"\\line\n100%&mgm.option=p&eos.ruid=0";
  const std::vector<std::pair<std::string, std::string>> cases{
      {eos::common::StringConversion::curl_escaped(comment), comment},
      {"old audit 100% %2F text", "old audit 100% %2F text"},
      {"", ""}};
  for (const auto& [encoded, expected] : cases) {
    SCOPED_TRACE(encoded);
    LegacyCommentProcCommand command;
    auto identity = eos::common::VirtualIdentity::Root();
    const std::string query = "mgm.cmd=__comment_codec_test__&mgm.comment=" + encoded;
    ASSERT_EQ(command.open("/proc/user/", query.c_str(), identity, nullptr), 0);
    EXPECT_EQ(command.comment(), expected);
  }
}

TEST_F(ConsoleMetadataTest, LegacyCommentCannotInjectCommandOptions)
{
  for (const std::string& comment :
       {std::string("audit&mgm.option=p&eos.ruid=0"),
        std::string("quotes \" and \\ and %2F and /#curl# literal\nsecond line")}) {
    SCOPED_TRACE(comment);
    global_comment = comment.c_str();
    run("mkdir", {"/eos/test/a/b"});
    const auto wire = request();
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    ASSERT_NE(env.Get("mgm.comment"), nullptr);
    EXPECT_EQ(eos::common::StringConversion::curl_unescaped(env.Get("mgm.comment")),
              comment);
    EXPECT_EQ(env.Get("mgm.option"), nullptr);
    EXPECT_EQ(env.Get("eos.ruid"), nullptr);
    EXPECT_EQ(global_comment.length(), 0);
  }
}

TEST_F(ConsoleMetadataTest, ProtobufCommentRemainsBinaryRequestMetadata)
{
  const std::string comment = "audit&mgm.option=p&eos.ruid=0\nsecond line";
  gGlobalOpts.mComment = comment;
  run("evict", {"fid:1"});
  EXPECT_EQ(protobuf(false).comment(), comment);
  const auto wire = request();
  XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
  EXPECT_EQ(env.Get("mgm.comment"), nullptr);
  EXPECT_EQ(env.Get("mgm.option"), nullptr);
  EXPECT_EQ(env.Get("eos.ruid"), nullptr);
}

TEST_F(ConsoleMetadataTest, BothTransportsRejectUnsafeAppAndRoles)
{
  for (const auto& invalid : {std::string("name&mgm.option=p"),
                              std::string("name\nvalue"), std::string("name\rvalue"),
                              std::string("name\tvalue"), std::string("name\x7fvalue")}) {
    SCOPED_TRACE(invalid);
    app = invalid.c_str();
    setenv("EOSAPP", invalid.c_str(), 1);
    expectRejected("mkdir", {"/eos/test"});
    expectRejected("evict", {"fid:1"});
    app = "";
    unsetenv("EOSAPP");
    user_role = invalid.c_str();
    gGlobalOpts.mUserRole = invalid;
    expectRejected("mkdir", {"/eos/test"});
    expectRejected("evict", {"fid:1"});
    user_role = "";
    gGlobalOpts.mUserRole.clear();
    group_role = invalid.c_str();
    gGlobalOpts.mGroupRole = invalid;
    expectRejected("mkdir", {"/eos/test"});
    expectRejected("evict", {"fid:1"});
    group_role = "";
    gGlobalOpts.mGroupRole.clear();
  }
  gGlobalOpts.mUserRole = std::string("a\0b", 3);
  expectRejected("evict", {"fid:1"});
  gGlobalOpts.mUserRole.clear();
  gGlobalOpts.mGroupRole = std::string("a\0b", 3);
  expectRejected("evict", {"fid:1"});
}

TEST_F(ConsoleMetadataTest, SafeAppAndRolesPreserveSpacesAndPunctuation)
{
  const std::string value = "audit /?x=1%20 + :._-";
  app = value.c_str();
  user_role = value.c_str();
  group_role = value.c_str();
  setenv("EOSAPP", value.c_str(), 1);
  gGlobalOpts.mUserRole = value;
  gGlobalOpts.mGroupRole = value;
  for (const auto* command : {"mkdir", "evict"}) {
    run(command, {"/eos/test"});
    const auto wire = request();
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    EXPECT_STREQ(env.Get("eos.app"), value.c_str());
    EXPECT_STREQ(env.Get("eos.ruid"), value.c_str());
    EXPECT_STREQ(env.Get("eos.rgid"), value.c_str());
  }
}

TEST_F(ConsoleMetadataTest, InvalidRoleCommandDoesNotChangeSelectedRole)
{
  run("role", {"123", "456"});
  for (const auto& arguments :
       std::vector<std::vector<std::string>>{{},
                                             {""},
                                             {"-1", "456"},
                                             {"123", "-1"},
                                             {"bad&mgm.option=p", "456"},
                                             {"123", "bad\nrole"},
                                             {std::string("a\0b", 3), "456"},
                                             {"123", "456", "ignored"}}) {
    SCOPED_TRACE(::testing::PrintToString(arguments));
    expectRejected("role", arguments);
    EXPECT_STREQ(user_role.c_str(), "123");
    EXPECT_STREQ(group_role.c_str(), "456");
    EXPECT_EQ(gGlobalOpts.mUserRole, "123");
    EXPECT_EQ(gGlobalOpts.mGroupRole, "456");
  }
}

TEST(ConsoleMetadata, PrefixRejectsUnsafeMetadataBeforeSideEffects)
{
  for (const auto& invalid :
       {std::string("name&mgm.option=p"), std::string("name\nvalue"),
        std::string("name\0value", 10)}) {
    for (const auto& arguments : std::vector<std::vector<std::string>>{
             {"--app", invalid, "evict", "fid:1"},
             {"--role", invalid, "456", "evict", "fid:1"},
             {"--role", "123", invalid, "evict", "fid:1"}}) {
      eos::console::ConsoleOptions options;
      std::string error;
      EXPECT_FALSE(eos::console::ParseConsoleOptions(arguments, options, error));
    }
  }
}
