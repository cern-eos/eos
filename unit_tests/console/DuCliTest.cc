//------------------------------------------------------------------------------
//! @file DuCliTest.cc
//! @brief Verify du CLI parsing and the serialized find request.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"
#include <fstream>
#include <map>
#include <optional>
#include <signal.h>
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

class DuS3CliTest : public DuCliTest {
protected:
  void
  SetUp() override
  {
    DuCliTest::SetUp();
    if (HasFatalFailure()) {
      return;
    }
    bin = directory("bin");
    capture = tempDir + "/s3-arguments";
    file(capture, "");
    setenv("PATH", bin.c_str(), 1);
    setenv("S3_ACCESS_KEY_ID", "test-id", 1);
    setenv("S3_SECRET_ACCESS_KEY", "test-secret", 1);
    setenv("S3_HOSTNAME", "test.invalid", 1);
    unsetenv("S3_ACCESS_KEY");
    unsetenv("S3_ACESSS_ID");
    setenv("EOS_FIND_TEST_CAPTURE", capture.c_str(), 1);
  }
  void
  stub(const std::string& body)
  {
    const auto executable = bin + "/s3";
    file(executable,
         "#!/bin/sh\n"
         "printf '%s\\n' \"$#\" \"$1\" \"$2\" \"$S3_ACCESS_KEY_ID\" "
         "\"$S3_SECRET_ACCESS_KEY\" \"$S3_HOSTNAME\" > \"$EOS_FIND_TEST_CAPTURE\"\n" +
             body);
    ASSERT_EQ(chmod(executable.c_str(), 0700), 0);
  }
  std::vector<std::string>
  invocation()
  {
    std::ifstream stream(capture);
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(stream, line)) {
      lines.push_back(line);
    }
    return lines;
  }

private:
  DuEnvironmentGuard environment{
      "PATH",          "S3_ACCESS_KEY_ID", "S3_HOSTNAME",          "S3_SECRET_ACCESS_KEY",
      "S3_ACCESS_KEY", "S3_ACESSS_ID",     "EOS_FIND_TEST_CAPTURE"};
  std::string bin, capture;
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
  for (const std::string path :
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

TEST_F(DuS3CliTest, ExplicitS3UriUsesRealFindTraversalWithoutMgm)
{
  stub("printf '%s\\n' 'tree/matching-file 1' 'other-file 1'\n");
  ::testing::internal::CaptureStdout();
  run("du", {"-hs", "as3:bucket/tree/*"});
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(output, "as3:bucket/tree/matching-file\n");
  EXPECT_EQ(invocation(), (std::vector<std::string>{"2", "list", "bucket", "test-id",
                                                    "test-secret", "test.invalid"}));
  EXPECT_TRUE(noRequest());
}

TEST_F(DuS3CliTest, S3ArgumentsAndCredentialsAreLiteralWithoutShell)
{
  const std::string bucket = "bucket with 'quotes' \"double\" &;$(unexecuted)";
  const std::string id = "id with 'quotes' \"double\" &;$(unexecuted)";
  const std::string key = "key with 'quotes' \"double\" &;$(unexecuted)";
  const std::string host = "host with 'quotes' \"double\" &;$(unexecuted)";
  setenv("S3_ACCESS_KEY_ID", id.c_str(), 1);
  setenv("S3_SECRET_ACCESS_KEY", key.c_str(), 1);
  setenv("S3_HOSTNAME", host.c_str(), 1);
  stub("printf '%s\\n' 'matching-file 1'\n");
  ::testing::internal::CaptureStdout();
  run("du", {"as3:" + bucket + "/matching"});
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(output, "as3:" + bucket + "/matching-file\n");
  EXPECT_EQ(invocation(), (std::vector<std::string>{"2", "list", bucket, id, key, host}));
  EXPECT_TRUE(noRequest());
}

TEST_F(DuS3CliTest, S3QueryCredentialsAndUriHostPreserveOverridePrecedence)
{
  stub("printf '%s\\n' 'matching-file 1'\n");
  const std::string uri = "as3://uri.invalid/bucket/matching?s3.id=query id 'q';"
                          "&s3.key=query key \"q\";";
  ::testing::internal::CaptureStdout();
  run("du", {uri});
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(output, "as3:bucket/matching-file\n");
  EXPECT_EQ(invocation(),
            (std::vector<std::string>{"2", "list", "bucket", "query id 'q';",
                                      "query key \"q\";", "uri.invalid"}));
  setenv("S3_ACESSS_ID", "ROOT id &;\"'", 1);
  setenv("S3_ACCESS_KEY", "ROOT key &;\"'", 1);
  ::testing::internal::CaptureStdout();
  run("du", {uri});
  ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(invocation(),
            (std::vector<std::string>{"2", "list", "bucket", "ROOT id &;\"'",
                                      "ROOT key &;\"'", "uri.invalid"}));
  EXPECT_TRUE(noRequest());
}

TEST_F(DuS3CliTest, S3FiltersPreserveEgrepAndHeaderSemantics)
{
  stub("printf '%s\\n' 'Bucket metadata' '----------' 'Key size' 'tree/file.ext 1' "
       "'file.ext 2' 'other.txt 3' 'prefix 4'\n"
       "printf '%s' 'last-key 5'\n");
  for (const auto& [filter, expected] : std::vector<std::pair<std::string, std::string>>{
           {"tree/*", "as3:bucket/tree/file.ext\n"},
           {"*.txt", "as3:bucket/other.txt\n"},
           {"(tree/|)file[.]ext", "as3:bucket/tree/file.ext\nas3:bucket/file.ext\n"},
           {"not-found", ""},
           {"", "as3:bucket/tree/file.ext\nas3:bucket/file.ext\nas3:bucket/other.txt\n"
                "as3:bucket/prefix\nas3:bucket/last-key\n"}}) {
    SCOPED_TRACE(filter);
    ::testing::internal::CaptureStdout();
    run("du", {"as3:bucket/" + filter});
    const auto output = ::testing::internal::GetCapturedStdout();
    EXPECT_EQ(global_retc, 0);
    EXPECT_EQ(output, expected);
    EXPECT_TRUE(noRequest());
  }
}

TEST_F(DuS3CliTest, MalformedS3UrisAndFiltersAreRejectedBeforeSpawning)
{
  stub("exit 73\n");
  for (const std::string uri : {"as3:", "as3://host-only", "as3:/missing-bucket",
                                "as3:*/path", "as3:bucket/[invalid"}) {
    SCOPED_TRACE(uri);
    expectRejected("du", {uri});
    EXPECT_TRUE(invocation().empty());
  }
}

TEST_F(DuS3CliTest, MissingS3ExecutableReturnsErrorWithoutExiting)
{
  ::testing::internal::CaptureStdout();
  ::testing::internal::CaptureStderr();
  run("du", {"as3:bucket/"});
  const auto error = ::testing::internal::GetCapturedStderr();
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, ENOENT);
  EXPECT_TRUE(output.empty());
  EXPECT_NE(error.find("cannot execute s3"), std::string::npos);
  EXPECT_EQ(error.find("test-secret"), std::string::npos);
  EXPECT_TRUE(noRequest());
}

TEST_F(DuS3CliTest, S3FailuresReturnDecodedChildStatusWithoutCredentials)
{
  stub("exit 23\n");
  ::testing::internal::CaptureStderr();
  run("du", {"as3:bucket/?s3.key=private-query-key"});
  const auto error = ::testing::internal::GetCapturedStderr();
  EXPECT_EQ(global_retc, 23);
  EXPECT_NE(error.find("status 23"), std::string::npos);
  EXPECT_EQ(error.find("private-query-key"), std::string::npos);
  EXPECT_EQ(error.find("test-id"), std::string::npos);
  EXPECT_TRUE(noRequest());
}

TEST_F(DuS3CliTest, S3SignalTerminationReturnsError)
{
  stub("kill -TERM $$\n");
  run("du", {"as3:bucket/"});
  EXPECT_EQ(global_retc, 128 + SIGTERM);
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
  expectHelp("du", {"--help"});
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
