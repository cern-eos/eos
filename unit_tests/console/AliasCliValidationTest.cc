//------------------------------------------------------------------------------
//! @file AliasCliValidationTest.cc
//! @brief Keep help detection separate from literal path and option values.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class AliasCliValidationTest : public ConsoleTestFixture {};
} // namespace

TEST_F(AliasCliValidationTest, ActualHelpFlagsDoNotSendRequests)
{
  for (const auto* name : {"touch", "mv", "cp"}) {
    SCOPED_TRACE(name);
    expectRejected(name, {"--help"});
    expectRejected(name, {"-h"});
  }
}
