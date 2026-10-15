//------------------------------------------------------------------------------
//! @file DuCliTest.cc
//! @brief Verify du CLI parsing and the serialized find request.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"
#include <fstream>
#include <map>
#include <optional>
#include <sys/stat.h>

namespace {
class DuEnvironmentGuard {
public:
  DuEnvironmentGuard(std::initializer_list<const char*> names)
  {
    for (const char* name : names) {
      const char* value = getenv(name);
      saved.emplace(name, value ? std::optional<std::string>(value) : std::nullopt);
    }
  }
  ~DuEnvironmentGuard()
  {
    for (const auto& [name, value] : saved) {
      if (value) {
        setenv(name.c_str(), value->c_str(), 1);
      } else {
        unsetenv(name.c_str());
      }
    }
  }

private:
  std::map<std::string, std::optional<std::string>> saved;
};

class DuCliTest : public ConsoleTestFixture {
protected:
  void
  TearDown() override
  {
    for (const auto& path : createdFiles) {
      unlink(path.c_str());
    }
    for (auto it = createdDirectories.rbegin(); it != createdDirectories.rend(); ++it) {
      rmdir(it->c_str());
    }
    ConsoleTestFixture::TearDown();
  }
  std::string
  directory(const std::string& name)
  {
    const auto path = tempDir + "/" + name;
    EXPECT_EQ(mkdir(path.c_str(), 0700), 0);
    createdDirectories.push_back(path);
    return path;
  }
  void
  file(const std::string& path, const std::string& contents = "test")
  {
    std::ofstream stream(path);
    stream << contents;
    EXPECT_TRUE(stream.good());
    createdFiles.push_back(path);
  }
  bool
  noRequest()
  {
    std::lock_guard<std::mutex> lock(mutex);
    return requests.empty();
  }

private:
  std::vector<std::string> createdFiles, createdDirectories;
};
} // namespace

TEST_F(DuCliTest, DefaultRequestTargetsDirectories)
{
  run("du", {"/eos/data"});
  EXPECT_EQ(global_retc, ENOENT);
  const auto request = protobuf(false);
  ASSERT_TRUE(request.has_find());
  const auto& find = request.find();
  EXPECT_EQ(find.path(), "/eos/data/");
  EXPECT_TRUE(find.du());
  EXPECT_TRUE(find.directories());
  EXPECT_FALSE(find.files());
  EXPECT_FALSE(find.dusi());
  EXPECT_FALSE(find.dureadable());
  EXPECT_FALSE(find.has_maxdepth());
}

TEST_F(DuCliTest, PreservesEveryArgumentBoundary)
{
  for (const std::string& path :
       {std::string("/eos/a b&c=d"), std::string("/eos/a\"b'c"),
        std::string("/eos/with\twhitespace"), std::string("/eos/échantillon")}) {
    SCOPED_TRACE(path);
    run("du", {path});
    EXPECT_EQ(protobuf(false).find().path(), path + "/");
  }
}

TEST_F(DuCliTest, ResolvesRelativePathsAndPreservesRoot)
{
  run("du", {"relative path"});
  EXPECT_EQ(protobuf(false).find().path(), "/eos/dispatch-test/relative path/");
  run("du", {"/"});
  EXPECT_EQ(protobuf(false).find().path(), "/");
  run("du", {"/eos/data/"});
  EXPECT_EQ(protobuf(false).find().path(), "/eos/data/");
}

TEST_F(DuCliTest, ExplicitUriPreflightPreservesArgumentBoundaries)
{
  auto* command = CommandRegistry::instance().find("du");
  ASSERT_NE(command, nullptr);
  for (const std::string& path :
       {"file:/tmp/my files", "file:", "root://test.invalid//my files",
        "as3:bucket/my files"}) {
    SCOPED_TRACE(path);
    EXPECT_FALSE(command->requiresMgmArguments({path}));
    EXPECT_FALSE(command->requiresMgmArguments({"-ahs", "--si", "--", path}));
  }
  EXPECT_TRUE(command->requiresMgmArguments({"/eos/file:/tmp/my files"}));
  EXPECT_TRUE(command->requiresMgmArguments({"--", "--help"}));
  EXPECT_FALSE(command->requiresMgmArguments({"--unknown", "/eos/data"}));
}

TEST_F(DuCliTest, ExplicitLocalUriUsesRealFindTraversalWithoutMgm)
{
  const auto path = directory("local space \"quote\" \\slash");
  const auto child = directory("local space \"quote\" \\slash/child directory");
  file(path + "/visible file");
  file(child + "/nested file");
  for (const auto& args : std::vector<std::vector<std::string>>{
           {"file:" + path}, {"-ahs", "--si", "file:" + path}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    ::testing::internal::CaptureStdout();
    run("du", args);
    const auto output = ::testing::internal::GetCapturedStdout();
    EXPECT_EQ(global_retc, 0);
    EXPECT_NE(output.find(path + "/visible file\n"), std::string::npos);
    EXPECT_NE(output.find(child + "/nested file\n"), std::string::npos);
    EXPECT_TRUE(noRequest());
  }
}

TEST_F(DuCliTest, ExplicitS3UriUsesRealFindTraversalWithoutMgm)
{
  DuEnvironmentGuard environment{"PATH",          "S3_ACCESS_KEY_ID",
                                 "S3_HOSTNAME",   "S3_SECRET_ACCESS_KEY",
                                 "S3_ACCESS_KEY", "S3_ACESSS_ID"};
  const auto bin = directory("bin");
  const auto executable = bin + "/s3";
  file(executable,
       "#!/bin/sh\n"
       "[ \"$#\" = 2 ] && [ \"$1\" = list ] && [ \"$2\" = bucket ] || exit 73\n"
       "printf '%s\\n' 'tree/matching-file 1' 'other-file 1'\n");
  ASSERT_EQ(chmod(executable.c_str(), 0700), 0);
  setenv("PATH", (bin + ":/usr/bin:/bin").c_str(), 1);
  setenv("S3_ACCESS_KEY_ID", "test-id", 1);
  setenv("S3_SECRET_ACCESS_KEY", "test-secret", 1);
  setenv("S3_HOSTNAME", "test.invalid", 1);
  unsetenv("S3_ACCESS_KEY");
  unsetenv("S3_ACESSS_ID");
  ::testing::internal::CaptureStdout();
  run("du", {"-hs", "as3:bucket/tree/*"});
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(output, "as3:bucket/tree/matching-file\n");
  EXPECT_TRUE(noRequest());
}

TEST_F(DuCliTest, HumanReadableFlagIsNotHelp)
{
  auto* command = CommandRegistry::instance().find("du");
  ASSERT_NE(command, nullptr);
  EXPECT_TRUE(command->requiresMgm("-h /eos/data"));
  EXPECT_FALSE(command->requiresMgm("--help"));
  run("du", {"-h", "/eos/data"});
  EXPECT_EQ(global_retc, ENOENT);
  const auto request = protobuf(false);
  EXPECT_TRUE(request.find().dureadable());
  EXPECT_FALSE(request.find().dusi());
}

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

TEST_F(DuCliTest, AcceptsCombinedAndTrailingFlags)
{
  for (const auto& args : std::vector<std::vector<std::string>>{
           {"-ahs", "--si", "/eos/data"}, {"/eos/data", "--si", "-s", "-h", "-a"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    run("du", args);
    const auto request = protobuf(false);
    EXPECT_FALSE(request.find().directories());
    EXPECT_TRUE(request.find().dureadable());
    EXPECT_TRUE(request.find().dusi());
    EXPECT_TRUE(request.find().has_maxdepth());
  }
}

TEST_F(DuCliTest, DelimiterAllowsDashPrefixedPaths)
{
  auto* command = CommandRegistry::instance().find("du");
  ASSERT_NE(command, nullptr);
  for (const std::string& path : {std::string("-dataset"), std::string("--help")}) {
    SCOPED_TRACE(path);
    EXPECT_TRUE(command->requiresMgm("-- " + path));
    run("du", {"--", path});
    EXPECT_EQ(protobuf(false).find().path(), "/eos/dispatch-test/" + path + "/");
  }
}

TEST_F(DuCliTest, RejectsMissingEmptyUnknownAndExtraArgumentsBeforeSending)
{
  for (const auto& args :
       std::vector<std::vector<std::string>>{{},
                                             {""},
                                             {"-h"},
                                             {"-s"},
                                             {"--si"},
                                             {"--unknown", "/eos/data"},
                                             {"-z", "/eos/data"},
                                             {"--si=invalid", "/eos/data"},
                                             {"/eos/data", "/eos/other"},
                                             {"/eos/data", "ignored"},
                                             {"/eos/data", "--unknown"},
                                             {"--", ""},
                                             {"--", "first", "second"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("du", args);
  }
}

TEST_F(DuCliTest, HelpDoesNotSendRequest)
{
  expectRejected("du", {"--help"});
  auto* command = CommandRegistry::instance().find("du");
  ASSERT_NE(command, nullptr);
  const auto help = command->helpText();
  EXPECT_NE(help.find("--help"), std::string::npos);
  EXPECT_NE(help.find("-h"), std::string::npos);
  EXPECT_NE(help.find("--si"), std::string::npos);
  EXPECT_NE(help.find("path"), std::string::npos);
}

TEST_F(DuCliTest, PreservesServerReturnCodes)
{
  for (int status : {0, EACCES, ENOENT, EBUSY}) {
    SCOPED_TRACE(status);
    responseRetc = status;
    run("du", {"/eos/data"});
    EXPECT_EQ(global_retc, status);
    EXPECT_TRUE(protobuf(false).has_find());
  }
}
