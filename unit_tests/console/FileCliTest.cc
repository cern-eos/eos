//------------------------------------------------------------------------------
//! @file FileCliTest.cc
//! @brief Verify native file argument forwarding and validation before IPC.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class FileCliTest : public ConsoleTestFixture {};
} // namespace

TEST_F(FileCliTest, PreservesContainerRenameAndImmediateLockExpiry)
{
  run("file", {"rename", "cxid:2a", "/eos/renamed-directory"});
  const auto rename = protobuf(false);
  EXPECT_EQ(rename.file().md().path(), "cxid:2a");
  EXPECT_EQ(rename.file().md().id(), 0u);
  run("file", {"touch", "-l", "/eos/lock", "0"});
  const auto touch = protobuf(false);
  EXPECT_EQ(touch.file().touch().lockop(), "lock");
  EXPECT_EQ(touch.file().touch().lockop_lifetime(), "0");
}

TEST_F(FileCliTest, PreservesValidContainerDestinations)
{
  for (const auto* destination : {"cid:42", "cxid:2a"}) {
    SCOPED_TRACE(destination);
    run("file", {"rename", "/eos/source", destination});
    EXPECT_EQ(protobuf(false).file().rename().new_path(), destination);
    run("file", {"rename_with_symlink", "fid:42", destination});
    EXPECT_EQ(protobuf(false).file().rename_with_symlink().destination_dir(),
              destination);
    run("file", {"symlink", "/eos/link", destination});
    EXPECT_EQ(protobuf(false).file().symlink().target_path(), destination);
    run("file", {"copy", "/eos/source", destination});
    EXPECT_EQ(protobuf(false).file().copy().dst(), destination);
  }
}
