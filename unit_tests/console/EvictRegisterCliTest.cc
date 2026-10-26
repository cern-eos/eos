//------------------------------------------------------------------------------
//! @file EvictRegisterCliTest.cc
//! @brief Validate native protobuf command arguments through the IPC transport.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"
#include <limits>

namespace {
class EvictRegisterCliTest : public ConsoleTestFixture {};
struct InvalidArguments {
  const char* name;
  const char* command;
  std::vector<std::string> arguments;
};
class InvalidEvictRegisterCliTest
    : public ConsoleTestFixture,
      public ::testing::WithParamInterface<InvalidArguments> {};
} // namespace

TEST_F(EvictRegisterCliTest, EvictPreservesLiteralIdentifierSuffixesAndFileOrder)
{
  run("evict",
      {"/eos/a/fid:123", "relative/fxid:ab", "/eos/a /eos/b", "fid:42", "fxid:ff"});
  const auto result = protobuf(false).evict();
  ASSERT_EQ(result.file_size(), 5);
  EXPECT_EQ(result.file(0).path(), "/eos/a/fid:123");
  EXPECT_EQ(result.file(1).path(), "/eos/dispatch-test/relative/fxid:ab");
  EXPECT_EQ(result.file(2).path(), "/eos/a /eos/b");
  EXPECT_EQ(result.file(3).fid(), 42u);
  EXPECT_EQ(result.file(4).fid(), 255u);
}

TEST_F(EvictRegisterCliTest, EvictHelpTextInsidePathRemainsLiteral)
{
  run("evict", {"/eos/a --help b"});
  EXPECT_EQ(protobuf(false).evict().file(0).path(), "/eos/a --help b");
  run("evict", {"--", "--help"});
  EXPECT_EQ(protobuf(false).evict().file(0).path(), "/eos/dispatch-test/--help");
}

TEST_F(EvictRegisterCliTest, EvictTypedOptionsAcceptMaximumAndReorderedFlags)
{
  run("evict", {"fid:1", "--ignore-removal-on-fst", "--fsid", "18446744073709551615",
                "--ignore-evict-counter"});
  const auto result = protobuf(false).evict();
  EXPECT_EQ(result.evictsinglereplica().fsid(), std::numeric_limits<uint64_t>::max());
  EXPECT_TRUE(result.ignoreevictcounter());
  EXPECT_TRUE(result.ignoreremovalonfst());
}

TEST_F(EvictRegisterCliTest, RegisterOptionsAndLegacyTagsProduceSameRequest)
{
  run("register", {"-u", "/eos/a b&c=d", "uid=123", "gid=456", "mode=0640", "size=42",
                   "layoutid=123", "location=1", "location=2", "attr=user.key=a=b&c",
                   "atimeifnewer=123.456", "checksum=abcdef01"});
  const auto legacy = protobuf(true).record().SerializeAsString();
  run("register",
      {"-u",         "--path",     "/eos/a b&c=d", "--uid",          "123",
       "--gid",      "456",        "--mode",       "0640",           "--size",
       "42",         "--layoutid", "123",          "--location",     "1",
       "--location", "2",          "--attr",       "user.key=a=b&c", "--atimeifnewer",
       "123.456",    "--checksum", "abcdef01"});
  EXPECT_EQ(protobuf(true).record().SerializeAsString(), legacy);
}

TEST_F(EvictRegisterCliTest, RegisterPathAndMetadataContainingHelpRemainLiteral)
{
  run("register", {"path=/eos/a --help b", "attr=user.help= --help ", "username=alice"});
  const auto result = protobuf(true).record();
  EXPECT_EQ(result.path(), "/eos/a --help b");
  EXPECT_EQ(result.attr().at("user.help"), " --help ");
  EXPECT_EQ(result.owner().username(), "alice");
}

TEST_F(EvictRegisterCliTest, RegisterKeepsRepeatedAttributeAndLocationOrdering)
{
  run("register", {"/eos/test", "location=2", "location=1", "location=2",
                   "attr=user.key=old", "xattr=user.key=new", "mode=7777"});
  const auto result = protobuf(true).record();
  ASSERT_EQ(result.locations_size(), 3);
  EXPECT_EQ(result.locations(0), 2u);
  EXPECT_EQ(result.locations(1), 1u);
  EXPECT_EQ(result.locations(2), 2u);
  EXPECT_EQ(result.attr().at("user.key"), "new");
  EXPECT_EQ(result.mode(), 07777);
}

TEST_F(EvictRegisterCliTest, RegisterPreservesTimestampAndChecksumBoundaries)
{
  const std::string checksum(64, 'F');
  run("register", {"/eos/test", "btime=9223372036854775807.999999999", "ctime=0.0",
                   "checksum=" + checksum});
  const auto result = protobuf(true).record();
  EXPECT_EQ(result.btime().sec(), std::numeric_limits<int64_t>::max());
  EXPECT_EQ(result.btime().nsec(), 999999999);
  EXPECT_TRUE(result.has_ctime());
  EXPECT_EQ(result.ctime().sec(), 0);
  EXPECT_EQ(result.ctime().nsec(), 0);
  EXPECT_EQ(result.checksum(), checksum);
}

TEST_F(EvictRegisterCliTest, RegisterNamedOwnersAndNumericZeroAreExplicit)
{
  run("register", {"/eos/test", "uid=alice", "gid=users"});
  const auto named = protobuf(true).record();
  EXPECT_EQ(named.owner().username(), "alice");
  EXPECT_EQ(named.owner().groupname(), "users");
  run("register", {"/eos/test", "uid=0", "gid=0"});
  const auto numeric = protobuf(true).record();
  EXPECT_TRUE(numeric.has_owner());
  EXPECT_EQ(numeric.owner().uid(), 0u);
  EXPECT_EQ(numeric.owner().gid(), 0u);
  EXPECT_TRUE(numeric.owner().username().empty());
  EXPECT_TRUE(numeric.owner().groupname().empty());
}

TEST_F(EvictRegisterCliTest, NumericFieldsUseDecimalEvenWithLeadingZeroes)
{
  run("evict", {"--ignore-evict-counter", "--fsid", "010", "fid:1"});
  EXPECT_EQ(protobuf(false).evict().evictsinglereplica().fsid(), 10u);
  run("register",
      {"/eos/test", "uid=010", "gid=010", "size=010", "layoutid=010", "location=010"});
  const auto result = protobuf(true).record();
  EXPECT_EQ(result.owner().uid(), 10u);
  EXPECT_EQ(result.owner().gid(), 10u);
  EXPECT_EQ(result.size(), 10u);
  EXPECT_EQ(result.layoutid(), 10);
  ASSERT_EQ(result.locations_size(), 1);
  EXPECT_EQ(result.locations(0), 10u);
}

TEST_P(InvalidEvictRegisterCliTest, RejectsBeforeSendingRequest)
{
  expectRejected(GetParam().command, GetParam().arguments);
}

INSTANTIATE_TEST_SUITE_P(
    StrictInputs, InvalidEvictRegisterCliTest,
    ::testing::Values(
        InvalidArguments{"EvictEmptyDecimalId", "evict", {"fid:"}},
        InvalidArguments{"EvictEmptyHexId", "evict", {"fxid:"}},
        InvalidArguments{"EvictNegativeId", "evict", {"fid:-1"}},
        InvalidArguments{"EvictPositiveSignId", "evict", {"fid:+1"}},
        InvalidArguments{"EvictDecimalSuffix", "evict", {"fid:12junk"}},
        InvalidArguments{"EvictDecimalOverflow", "evict", {"fid:18446744073709551616"}},
        InvalidArguments{"EvictHexOverflow", "evict", {"fxid:10000000000000000"}},
        InvalidArguments{"EvictHexPrefix", "evict", {"fxid:0xab"}},
        InvalidArguments{"EvictContainerId", "evict", {"cid:1"}},
        InvalidArguments{"EvictInode", "evict", {"inode:1"}},
        InvalidArguments{"EvictInvalidAfterValid", "evict", {"/eos/a", "fid:bad"}},
        InvalidArguments{"EvictHexFilesystem",
                         "evict",
                         {"--ignore-evict-counter", "--fsid", "0x1", "fid:1"}},
        InvalidArguments{
            "EvictRepeatedFilesystem",
            "evict",
            {"--ignore-evict-counter", "--fsid", "1", "--fsid", "2", "fid:1"}},
        InvalidArguments{"EvictFalseIgnoreCounter",
                         "evict",
                         {"--ignore-evict-counter=false", "--fsid", "1", "fid:1"}},
        InvalidArguments{"RegisterNegativeUid", "register", {"/eos/a", "uid=-1"}},
        InvalidArguments{"RegisterNegativeGid", "register", {"/eos/a", "gid=-1"}},
        InvalidArguments{"RegisterPositiveSignUid", "register", {"/eos/a", "uid=+1"}},
        InvalidArguments{
            "RegisterNumericOwnerSuffix", "register", {"/eos/a", "uid=123junk"}},
        InvalidArguments{
            "RegisterDuplicatePositionalPath", "register", {"/eos/a", "/eos/b"}},
        InvalidArguments{
            "RegisterDuplicatePathTag", "register", {"/eos/a", "path=/eos/b"}},
        InvalidArguments{
            "RegisterRepeatedPathTag", "register", {"path=/eos/a", "path=/eos/b"}},
        InvalidArguments{
            "RegisterMixedUser", "register", {"/eos/a", "uid=123", "username=alice"}},
        InvalidArguments{"RegisterMixedUserReverse",
                         "register",
                         {"/eos/a", "username=alice", "uid=123"}},
        InvalidArguments{
            "RegisterMixedGroup", "register", {"/eos/a", "gid=123", "groupname=users"}},
        InvalidArguments{"RegisterMixedAccessTime",
                         "register",
                         {"/eos/a", "atime=1", "atimeifnewer=2"}},
        InvalidArguments{
            "RegisterRepeatedSize", "register", {"/eos/a", "size=1", "size=2"}},
        InvalidArguments{"RegisterModeTooLarge", "register", {"/eos/a", "mode=10000"}},
        InvalidArguments{"RegisterModeNonOctal", "register", {"/eos/a", "mode=888"}},
        InvalidArguments{"RegisterModeNegative", "register", {"/eos/a", "mode=-1"}},
        InvalidArguments{"RegisterOddChecksum", "register", {"/eos/a", "checksum=abc"}},
        InvalidArguments{
            "RegisterInvalidChecksum", "register", {"/eos/a", "checksum=abxz"}},
        InvalidArguments{"RegisterLongChecksum",
                         "register",
                         {"/eos/a", "checksum=" + std::string(66, 'a')}},
        InvalidArguments{
            "RegisterTimestampSuffix", "register", {"/eos/a", "mtime=1junk"}},
        InvalidArguments{"RegisterNegativeTimestamp", "register", {"/eos/a", "mtime=-1"}},
        InvalidArguments{"RegisterEmptyNanoseconds", "register", {"/eos/a", "mtime=1."}},
        InvalidArguments{
            "RegisterNanosecondsTooLarge", "register", {"/eos/a", "mtime=1.1000000000"}},
        InvalidArguments{"RegisterUnknownOption", "register", {"/eos/a", "--unknown"}},
        InvalidArguments{"RegisterUnknownTag", "register", {"/eos/a", "unknown=value"}}),
    [](const ::testing::TestParamInfo<InvalidArguments>& info) {
      return info.param.name;
    });
