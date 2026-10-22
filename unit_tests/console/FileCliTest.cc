//------------------------------------------------------------------------------
//! @file FileCliTest.cc
//! @brief Verify native file argument forwarding and validation before IPC.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"
#include <ctime>

namespace {
class FileCliTest : public ConsoleTestFixture {};
} // namespace

TEST_F(FileCliTest, PreservesPathAndDestinationBytes)
{
  const std::string source = "/eos/a b'c\"d\\e&x=y";
  const std::string destination = "/eos/f g'hi\"j\\k&x=z";
  run("file", {"rename", source, destination});
  EXPECT_EQ(global_retc, ENOENT);
  const auto renamed = protobuf(false);
  EXPECT_EQ(renamed.file().md().path(), source);
  EXPECT_EQ(renamed.file().rename().new_path(), destination);
  run("file", {"copy", "-fsc", source, destination});
  const auto copied = protobuf(false);
  EXPECT_EQ(copied.file().md().path(), source);
  EXPECT_EQ(copied.file().copy().dst(), destination);
  EXPECT_TRUE(copied.file().copy().force());
  EXPECT_TRUE(copied.file().copy().silent());
  EXPECT_TRUE(copied.file().copy().clone());
}

TEST_F(FileCliTest, IdentifierTextInsidePathDoesNotTargetAnotherFile)
{
  run("file", {"drop", "/eos/filename-fid:123", "1"});
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.file().md().path(), "/eos/filename-fid:123");
  EXPECT_EQ(proto.file().md().id(), 0u);
}

TEST_F(FileCliTest, RejectsInvalidExplicitIdentifiersBeforeSending)
{
  for (const auto* identifier :
       {"fid:", "fid:-1", "fid:1junk", "fxid:1g", "fid:18446744073709551616", "pid:1junk",
        "pxid:-1", "inode:1junk"}) {
    SCOPED_TRACE(identifier);
    expectRejected("file", {"info", identifier});
  }
  expectRejected("file", {"check", "fid:1junk"});
}

TEST_F(FileCliTest, RejectsExtraOperandsBeforeSending)
{
  for (const auto& args :
       std::vector<std::vector<std::string>>{{"rename", "/eos/a", "/eos/b", "extra"},
                                             {"move", "/eos/a", "1", "2", "extra"},
                                             {"replicate", "/eos/a", "1", "2", "extra"},
                                             {"copy", "/eos/a", "/eos/b", "extra"},
                                             {"check", "/eos/a", "%size", "extra"},
                                             {"touch", "-u", "/eos/a", "ignored"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("file", args);
  }
}

TEST_F(FileCliTest, PreservesInfoIdentifierTypesAndGlobalOptions)
{
  gGlobalOpts.mComment = "metadata request comment";
  gGlobalOpts.mJsonFormat = true;
  run("file", {"info", "cid:42", "--path", "--env", "-m"});
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.file().fileinfo().md().type(), eos::console::CONTAINER);
  EXPECT_EQ(proto.file().fileinfo().md().id(), 42u);
  EXPECT_TRUE(proto.file().fileinfo().path());
  EXPECT_TRUE(proto.file().fileinfo().env());
  EXPECT_TRUE(proto.file().fileinfo().monitoring());
  EXPECT_EQ(proto.comment(), "metadata request comment");
  EXPECT_EQ(proto.format(), eos::console::RequestProto::JSON);
}

TEST_F(FileCliTest, ShareExpirationIsAbsolute)
{
  const auto before = std::time(nullptr);
  run("file", {"share", "/eos/a", "2h"});
  const auto after = std::time(nullptr);
  const auto proto = protobuf(false);
  EXPECT_GE(proto.file().share().expires(), before + 7200);
  EXPECT_LE(proto.file().share().expires(), after + 7200);
}

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

TEST_F(FileCliTest, RejectsMalformedDestinationIdentifiersBeforeSending)
{
  for (const auto* command : {"rename", "rename_with_symlink", "symlink", "copy"}) {
    for (const auto* destination : {"fid:", "fid:+1", "fid: 1", "fid:-1", "fxid:1g",
                                    "cid:1junk", "cxid:10000000000000000"}) {
      SCOPED_TRACE(command);
      SCOPED_TRACE(destination);
      expectRejected("file", {command, "/eos/source", destination});
    }
  }
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

TEST_F(FileCliTest, VersionRejectsZeroWhilePurgePreservesIt)
{
  expectRejected("file", {"version", "/eos/source", "0"});
  expectRejected("file", {"version", "/eos/source", "-0"});
  run("file", {"purge", "/eos/source", "0"});
  EXPECT_EQ(protobuf(false).file().purge().purge_version(), 0);
}
