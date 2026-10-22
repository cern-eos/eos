#include "ConsoleTestFixture.hh"
#include "console/commands/helpers/PathIdentifier.hh"
#include <limits>

using ConsoleIdentifierTest = ConsoleTestFixture;

TEST(ConsoleIdentifier, ExactPrefixesAndBounds)
{
  using eos::console::IdentifierType;
  using eos::console::ParsePathIdentifier;
  for (const auto* path :
       {"/eos/fid:123", "relative/fxid:ab", "/eos/cid:42", "/eos/pxid:ff", "plain", ""}) {
    SCOPED_TRACE(path);
    EXPECT_EQ(ParsePathIdentifier(path).type, IdentifierType::Path);
  }
  for (const auto* invalid :
       {"fid:", "fid:-1", "fid:+1", "fid:1junk", "fid:1\n", "fid: 1",
        "fid:18446744073709551616", "fxid:10000000000000000", "fxid:0x1", "cid:1.5",
        "inode:-1", "pid:1a", "pxid:gg"}) {
    SCOPED_TRACE(invalid);
    EXPECT_EQ(ParsePathIdentifier(invalid).type, IdentifierType::Invalid);
  }
  EXPECT_EQ(ParsePathIdentifier("fid:0").type, IdentifierType::File);
  EXPECT_EQ(ParsePathIdentifier("fid:0").value, 0u);
  EXPECT_EQ(ParsePathIdentifier("fid:18446744073709551615").value,
            std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(ParsePathIdentifier("fxid:FFFFFFFFFFFFFFFF").value,
            std::numeric_limits<uint64_t>::max());
  EXPECT_EQ(ParsePathIdentifier("cid:42").type, IdentifierType::Container);
  EXPECT_EQ(ParsePathIdentifier("pxid:ab").value, 171u);
}

TEST(ConsoleIdentifier, LegacyAdaptersDoNotMutatePathsOrOutputOnFailure)
{
  for (const auto* raw :
       {"/eos/fid:123", "fid:-1", "fid:18446744073709551616", "fxid:xyz"}) {
    XrdOucString path = raw;
    unsigned long long id = 999;
    EXPECT_FALSE(Path2FileDenominator(path, id));
    EXPECT_STREQ(path.c_str(), raw);
    EXPECT_EQ(id, 999u);
  }
  XrdOucString path = "fxid:ab";
  unsigned long long id = 0;
  EXPECT_TRUE(Path2FileDenominator(path, id));
  EXPECT_EQ(id, 171u);
  EXPECT_STREQ(path.c_str(), "fxid:ab");
}

TEST_F(ConsoleIdentifierTest, LiteralPathsNeverBecomeIdentifiers)
{
  for (const auto* path :
       {"/eos/fid:123", "/eos/fxid:ab", "/eos/cid:42", "/eos/cxid:ff"}) {
    run("rm", {path});
    const auto proto = protobuf(false);
    EXPECT_EQ(proto.rm().path(), path);
    EXPECT_EQ(proto.rm().fileid(), 0u);
    EXPECT_EQ(proto.rm().containerid(), 0u);
  }
}

TEST_F(ConsoleIdentifierTest, RemovePreservesAllArgumentBoundaries)
{
  const std::vector<std::string> paths = {"first",         "second",
                                          "/eos/a /eos/b", "/eos/a fid:123",
                                          "/eos/a&b=c",    "/eos/quote\"and\\slash"};
  run("rm", paths);
  std::vector<std::string> captured;
  {
    std::lock_guard<std::mutex> lock(mutex);
    captured = requests;
  }
  ASSERT_EQ(captured.size(), paths.size());
  for (size_t i = 0; i < paths.size(); ++i) {
    const auto& wire = captured[i];
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    ASSERT_NE(env.Get("mgm.cmd.proto"), nullptr);
    std::string bytes;
    ASSERT_TRUE(eos::common::SymKey::Base64Decode(env.Get("mgm.cmd.proto"), bytes));
    eos::console::RequestProto proto;
    ASSERT_TRUE(proto.ParseFromString(bytes));
    EXPECT_EQ(proto.rm().path(), i < 2 ? "/eos/dispatch-test/" + paths[i] : paths[i]);
  }
}

TEST_F(ConsoleIdentifierTest, RemoveValidatesWholeBatchBeforeSending)
{
  for (const auto* operand : {"fid:18446744073709551616", "fid:-1", "fxid:xyz",
                              "cid:", "cxid:10000000000000000", "--unknown", ""}) {
    SCOPED_TRACE(operand);
    expectRejected("rm", {"/eos/valid", operand});
  }
}

TEST_F(ConsoleIdentifierTest, RemoveHonorsDelimiterAndGroupedFlags)
{
  run("rm", {"--", "--help"});
  EXPECT_EQ(protobuf(false).rm().path(), "/eos/dispatch-test/--help");
  for (const auto* flags : {"-rf", "-fr", "-rF", "-Fr"}) {
    run("rm", {flags, "--no-confirmation", "/eos/test/tree"});
    const auto proto = protobuf(false);
    EXPECT_TRUE(proto.rm().recursive());
    EXPECT_EQ(proto.rm().bypassrecycle(),
              std::string(flags).find('F') != std::string::npos);
  }
  expectRejected("rm", {"-f", "/eos/test"});
}
