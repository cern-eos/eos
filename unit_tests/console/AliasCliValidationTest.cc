//------------------------------------------------------------------------------
//! @file AliasCliValidationTest.cc
//! @brief Keep help detection separate from literal path and option values.
//------------------------------------------------------------------------------

#include "console/commands/native/CpOptions.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"

namespace {
class AliasCliValidationTest : public ConsoleTestFixture {};
} // namespace

TEST_F(AliasCliValidationTest, TouchPreservesHelpTextWithinOnePath)
{
  const std::string path = "/eos/one --help literal -h path";
  run("touch", {path});
  EXPECT_EQ(global_retc, ENOENT);
  EXPECT_EQ(protobuf(false).file().md().path(), path);
}

TEST_F(AliasCliValidationTest, MovePreservesHelpTextWithinOnePath)
{
  const std::string source = "/eos/source --help literal -h path";
  const std::string destination = "/eos/destination --help literal -h path";
  run("mv", {source, destination});
  EXPECT_EQ(global_retc, ENOENT);
  const auto proto = protobuf(false);
  EXPECT_EQ(proto.file().md().path(), source);
  EXPECT_EQ(proto.file().rename().new_path(), destination);
}

TEST_F(AliasCliValidationTest, AliasDelimiterPreservesLiteralHelpNames)
{
  for (const auto* name : {"--help", "-h"}) {
    SCOPED_TRACE(name);
    run("touch", {"--", name});
    EXPECT_EQ(protobuf(false).file().md().path(),
              std::string("/eos/dispatch-test/") + name);
    run("mv", {"--", name, "target"});
    const auto proto = protobuf(false);
    EXPECT_EQ(proto.file().md().path(), std::string("/eos/dispatch-test/") + name);
    EXPECT_EQ(proto.file().rename().new_path(), "/eos/dispatch-test/target");
  }
}

TEST_F(AliasCliValidationTest, ActualHelpFlagsDoNotSendRequests)
{
  for (const auto* name : {"touch", "mv", "cp"}) {
    SCOPED_TRACE(name);
    expectHelp(name, {"--help"});
    expectHelp(name, {"-h"});
  }
}

TEST(CpCliValidation, PreservesHelpTextWithinSingleOperands)
{
  const std::vector<std::string> sources{"/eos/one --help literal -h path",
                                         "root://example.org/path with --help text"};
  const std::string destination = "/tmp/destination --help text";
  CpOptions options;
  ASSERT_TRUE(ParseCpArgs({sources[0], sources[1], destination}, options));
  EXPECT_EQ(options.sources, sources);
  EXPECT_EQ(options.target, destination);
}

TEST(CpCliValidation, DelimiterPreservesLiteralHelpNames)
{
  CpOptions options;
  ASSERT_TRUE(ParseCpArgs({"--", "--help", "-h"}, options));
  ASSERT_EQ(options.sources.size(), 1u);
  EXPECT_EQ(options.sources.front(), "--help");
  EXPECT_EQ(options.target, "-h");
  EXPECT_FALSE(options.recursive);
  EXPECT_FALSE(options.silent);
}

TEST(CpCliValidation, OptionValuesAreConsumedByTheirSchema)
{
  for (const auto& arguments : std::vector<std::vector<std::string>>{
           {"--rate", "--help", "--streams", "-h", "source", "target"},
           {"--rate=--help", "--streams=-h", "source", "target"}}) {
    SCOPED_TRACE(::testing::PrintToString(arguments));
    CpOptions options;
    ASSERT_TRUE(ParseCpArgs(arguments, options));
    EXPECT_EQ(options.rate, "--help");
    EXPECT_EQ(options.streams, "-h");
    EXPECT_EQ(options.sources, std::vector<std::string>{"source"});
    EXPECT_EQ(options.target, "target");
  }
}

TEST(CpCliValidation, RetainsExistingCopyOptionsAndSourceOrdering)
{
  CpOptions options;
  ASSERT_TRUE(ParseCpArgs({"--atomic", "--rate", "10M",        "--streams", "4",
                           "--depth",  "3",      "--checksum", "-a",        "-p",
                           "-n",       "-S",     "-d",         "-s",        "-k",
                           "-P",       "-r",     "source-a",   "source-b",  "target"},
                          options));
  EXPECT_EQ(options.atomic, "&eos.atomic=1");
  EXPECT_EQ(options.rate, "10M");
  EXPECT_EQ(options.streams, "4");
  EXPECT_EQ(options.depth, 3u);
  EXPECT_TRUE(options.checksums);
  EXPECT_TRUE(options.append);
  EXPECT_TRUE(options.makeparent);
  EXPECT_TRUE(options.noprogress);
  EXPECT_TRUE(options.summary);
  EXPECT_TRUE(options.debug);
  EXPECT_TRUE(options.silent);
  EXPECT_TRUE(options.nooverwrite);
  EXPECT_TRUE(options.preserve);
  EXPECT_TRUE(options.recursive);
  EXPECT_EQ(options.sources, (std::vector<std::string>{"source-a", "source-b"}));
  EXPECT_EQ(options.target, "target");
}

TEST(CpCliValidation, RejectsHelpMissingOperandsAndUnknownOptions)
{
  for (const auto& arguments :
       std::vector<std::vector<std::string>>{{"--help"},
                                             {"-h"},
                                             {},
                                             {"source"},
                                             {"--rate"},
                                             {"--unknown", "source", "target"}}) {
    SCOPED_TRACE(::testing::PrintToString(arguments));
    CpOptions options;
    EXPECT_FALSE(ParseCpArgs(arguments, options));
  }
}
