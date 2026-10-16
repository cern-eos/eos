//------------------------------------------------------------------------------
//! @file ConsoleArgumentsTest.cc
//! @brief Cover metadata encoding and request values against existing behavior.
//------------------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include <optional>

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
