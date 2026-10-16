//------------------------------------------------------------------------------
//! @file LegacyCliValidationTest.cc
//! @brief Verify CLI validation and escaping at legacy CGI request boundaries.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class LegacyCliValidationTest : public ConsoleTestFixture {
protected:
  std::string
  field(const char* name)
  {
    const auto wire = request();
    EXPECT_EQ(wire.substr(0, wire.find('?')), "/proc/user/");
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    const char* value = env.Get(name);
    EXPECT_NE(value, nullptr) << name;
    return value ? value : "";
  }
};
} // namespace

TEST_F(LegacyCliValidationTest, ChownNoDereferenceIsNotHelp)
{
  auto* command = CommandRegistry::instance().find("chown");
  ASSERT_NE(command, nullptr);
  EXPECT_TRUE(command->requiresMgm("-h user /eos/data"));
  EXPECT_FALSE(command->requiresMgm("--help"));
  run("chown", {"--nodereference", "user", "/eos/data"});
  EXPECT_EQ(field("mgm.chown.option"), "h");
}
