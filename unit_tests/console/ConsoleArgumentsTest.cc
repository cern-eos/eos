//------------------------------------------------------------------------------
//! @file ConsoleArgumentsTest.cc
//! @brief Cover metadata encoding and request values against existing behavior.
//------------------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/ConsoleMetadata.hh"
#include "mgm/proc/ProcCommand.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
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
