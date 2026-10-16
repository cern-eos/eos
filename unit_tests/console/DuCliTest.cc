//------------------------------------------------------------------------------
//! @file DuCliTest.cc
//! @brief Verify du CLI parsing and the serialized find request.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class DuCliTest : public ConsoleTestFixture {};
} // namespace

TEST_F(DuCliTest, EveryFlagCombinationPreservesFindSemantics)
{
  for (unsigned mask = 0; mask < 16; ++mask) {
    SCOPED_TRACE(mask);
    std::vector<std::string> args;
    if (mask & 1) {
      args.emplace_back("-a");
    }
    if (mask & 2) {
      args.emplace_back("-h");
    }
    if (mask & 4) {
      args.emplace_back("-s");
    }
    if (mask & 8) {
      args.emplace_back("--si");
    }
    args.emplace_back("/eos/data");
    run("du", args);
    const auto request = protobuf(false);
    const auto& find = request.find();
    EXPECT_TRUE(find.du());
    EXPECT_EQ(find.directories(), !(mask & 1));
    EXPECT_EQ(find.dureadable(), bool(mask & 2));
    EXPECT_EQ(find.has_maxdepth(), bool(mask & 4));
    EXPECT_EQ(find.maxdepth(), 0u);
    EXPECT_EQ(find.dusi(), bool(mask & 8));
  }
}
