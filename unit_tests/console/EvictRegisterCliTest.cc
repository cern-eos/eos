//------------------------------------------------------------------------------
//! @file EvictRegisterCliTest.cc
//! @brief Validate native protobuf command arguments through the IPC transport.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
struct InvalidArguments {
  const char* name;
  const char* command;
  std::vector<std::string> arguments;
};
class InvalidEvictRegisterCliTest
    : public ConsoleTestFixture,
      public ::testing::WithParamInterface<InvalidArguments> {};
} // namespace

TEST_P(InvalidEvictRegisterCliTest, RejectsBeforeSendingRequest)
{
  expectRejected(GetParam().command, GetParam().arguments);
}

INSTANTIATE_TEST_SUITE_P(StrictInputs, InvalidEvictRegisterCliTest,
                         ::testing::Values(InvalidArguments{"EvictRepeatedFilesystem",
                                                            "evict",
                                                            {"--ignore-evict-counter",
                                                             "--fsid", "1", "--fsid", "2",
                                                             "fid:1"}}),
                         [](const ::testing::TestParamInfo<InvalidArguments>& info) {
                           return info.param.name;
                         });
