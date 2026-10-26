//------------------------------------------------------------------------------
//! @file LegacyCliValidationTest.cc
//! @brief Verify CLI validation and escaping at legacy CGI request boundaries.
//------------------------------------------------------------------------------

#include "console/ConsoleCli.hh"
#include "console/ConsoleCompletion.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include <fstream>
#include <map>
#include <optional>
#include <readline/readline.h>
#include <sys/stat.h>

namespace {
class EnvironmentGuard {
public:
  EnvironmentGuard(std::initializer_list<const char*> names)
  {
    for (const char* name : names) {
      const char* value = getenv(name);
      saved.emplace(name, value ? std::optional<std::string>(value) : std::nullopt);
    }
  }
  ~EnvironmentGuard()
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

class LegacyCliValidationTest : public ConsoleTestFixture {
protected:
  void
  SetUp() override
  {
    savedPwdFile = pwdfile;
    savedStdout = rstdout;
    savedSilent = silent;
    savedCompletionType = rl_completion_type;
    ConsoleTestFixture::SetUp();
    if (HasFatalFailure()) {
      return;
    }
    pwdfile = "";
  }
  void
  TearDown() override
  {
    for (const auto& path : createdFiles) {
      unlink(path.c_str());
    }
    for (auto it = createdDirectories.rbegin(); it != createdDirectories.rend(); ++it) {
      rmdir(it->c_str());
    }
    pwdfile = savedPwdFile;
    rstdout = savedStdout;
    silent = savedSilent;
    rl_completion_type = savedCompletionType;
    ConsoleTestFixture::TearDown();
  }
  std::string
  field(const char* name)
  {
    const auto wire = request();
    EXPECT_EQ(wire.substr(0, wire.find('?')), "/proc/user/");
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    const char* value = env.Get(name);
    EXPECT_NE(value, nullptr) << name;
    return value ? value : "";
  }
  void
  expectPath(const std::string& path)
  {
    EXPECT_EQ(field("eos.encodepath"), "1");
    const auto encoded = field("mgm.path");
    EXPECT_EQ(eos::common::StringConversion::curl_unescaped(encoded), path);
    EXPECT_EQ(encoded, eos::common::StringConversion::curl_escaped(path));
  }
  std::vector<std::string>
  captured()
  {
    std::lock_guard<std::mutex> lock(mutex);
    return requests;
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

private:
  XrdOucString savedPwdFile, savedStdout;
  bool savedSilent = false;
  int savedCompletionType = 0;
  std::vector<std::string> createdFiles, createdDirectories;
};
} // namespace

TEST_F(LegacyCliValidationTest, ChmodPreservesOctalDigitsAndEscapesPath)
{
  for (const std::string& mode :
       {"0", "7", "644", "755", "1777", "2777", "4755", "7777"}) {
    SCOPED_TRACE(mode);
    run("chmod", {"-r", mode, "/eos/a b&mgm.cmd=other%20#x"});
    EXPECT_EQ(global_retc, ENOENT);
    EXPECT_EQ(field("mgm.cmd"), "chmod");
    EXPECT_EQ(field("mgm.chmod.mode"), mode);
    EXPECT_EQ(field("mgm.option"), "r");
    expectPath("/eos/a b&mgm.cmd=other%20#x");
  }
  run("chmod", {"0000755", "relative"});
  EXPECT_EQ(field("mgm.chmod.mode"), "755");
  expectPath("/eos/dispatch-test/relative");
}

TEST_F(LegacyCliValidationTest, ChmodRejectsInvalidModesAndArgumentsBeforeSending)
{
  for (const char* mode : {"", "888", "0788", "755suffix", "-1", "+755", "1.5", "0x1ff",
                           "10000", "777777777777777777777777"}) {
    SCOPED_TRACE(mode);
    expectRejected("chmod", {"--", mode, "/eos/data"});
  }
  for (const auto& args :
       std::vector<std::vector<std::string>>{{},
                                             {"755"},
                                             {"755", ""},
                                             {"755", "/eos/a", "/eos/b"},
                                             {"--unknown", "755", "/eos/data"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("chmod", args);
  }
}

TEST_F(LegacyCliValidationTest, ChownPreservesRemoteNamesAndEscapesOnlyPath)
{
  for (const std::string& owner :
       {"0", "123", ":456", "123:456", "remote.user@domain", "remote-user:remote_group",
        "computer$", "domain\\user"}) {
    SCOPED_TRACE(owner);
    run("chown", {"-r", "-h", owner, "/eos/a b&mgm.chown.owner=0%20"});
    EXPECT_EQ(global_retc, ENOENT);
    EXPECT_EQ(field("mgm.chown.owner"), owner);
    EXPECT_EQ(field("mgm.chown.option"), "rh");
    expectPath("/eos/a b&mgm.chown.owner=0%20");
  }
}

TEST_F(LegacyCliValidationTest, ChownAcceptsUint32BoundariesAndCanonicalizesLeadingZeros)
{
  for (const auto& [input, expected] : std::vector<std::pair<std::string, std::string>>{
           {"000", "0"},
           {"00123:00456", "123:456"},
           {":000", ":0"},
           {"4294967295:4294967295", "4294967295:4294967295"}}) {
    SCOPED_TRACE(input);
    run("chown", {input, "/eos/data"});
    EXPECT_EQ(field("mgm.chown.owner"), expected);
  }
}

TEST_F(LegacyCliValidationTest, ChownRejectsInvalidAndUnsafeOwnersBeforeSending)
{
  for (const std::string& owner :
       {"", ":", "user:", "user:group:other", "::group", "a&mgm.chown.owner=0", "a=b",
        "a?b", "a#b", "a%b", "a b", "a\tb", "a\nb", "a\rb", "-1", "+1", "4294967296",
        ":4294967296", "user:-1"}) {
    SCOPED_TRACE(owner);
    expectRejected("chown", {"--", owner, "/eos/data"});
  }
  for (const auto& args :
       std::vector<std::vector<std::string>>{{},
                                             {"user"},
                                             {"user", ""},
                                             {"user", "/a", "/b"},
                                             {"--unknown", "user", "/eos/data"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("chown", args);
  }
}

TEST_F(LegacyCliValidationTest, ChownNoDereferenceIsNotHelp)
{
  auto* command = CommandRegistry::instance().find("chown");
  ASSERT_NE(command, nullptr);
  EXPECT_TRUE(command->requiresMgm("-h user /eos/data"));
  EXPECT_FALSE(command->requiresMgm("--help"));
  run("chown", {"--nodereference", "user", "/eos/data"});
  EXPECT_EQ(field("mgm.chown.option"), "h");
}

TEST_F(LegacyCliValidationTest, CdEscapesPathAndRestoresDirectoryOnServerFailure)
{
  run("cd", {"/eos/a b&mgm.cmd=other%20"});
  EXPECT_EQ(global_retc, ENOENT);
  expectPath("/eos/a b&mgm.cmd=other%20/");
  EXPECT_STREQ(gPwd.c_str(), "/eos/dispatch-test/");
  responseRetc = 0;
  run("cd", {"/eos/q/\""});
  expectPath("/eos/q/\"/");
  EXPECT_STREQ(gPwd.c_str(), "/eos/q/\"/");
}

TEST_F(LegacyCliValidationTest, CdPreviousDirectorySurvivesFailedNavigation)
{
  responseRetc = 0;
  run("cd", {"/eos/first"});
  run("cd", {"/eos/second"});
  responseRetc = EACCES;
  run("cd", {"/eos/forbidden"});
  EXPECT_STREQ(gPwd.c_str(), "/eos/second/");
  responseRetc = 0;
  run("cd", {"-"});
  expectPath("/eos/first/");
  EXPECT_STREQ(gPwd.c_str(), "/eos/first/");
}

TEST_F(LegacyCliValidationTest, CdHomeUsesEnvironmentAndRejectsInvalidInput)
{
  EnvironmentGuard environment{"EOS_HOME"};
  setenv("EOS_HOME", "/eos/home with spaces&x", 1);
  for (const auto& args : std::vector<std::vector<std::string>>{{}, {"~"}}) {
    run("cd", args);
    expectPath("/eos/home with spaces&x/");
  }
  for (const auto& args :
       std::vector<std::vector<std::string>>{{""}, {"/eos/a", "/eos/b"}, {"--unknown"}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("cd", args);
  }
}

TEST_F(LegacyCliValidationTest, LsPreservesSpecialCharactersWithinOnePath)
{
  run("ls", {"-alh", "/eos/a b&c=d\\ literal%20#"});
  expectPath("/eos/a b&c=d\\ literal%20#");
  EXPECT_EQ(field("mgm.option"), "-l-a-h");
  EXPECT_EQ(global_retc, ENOENT);
}

TEST_F(LegacyCliValidationTest, LsListsEachOperandInOrderAndPreservesFirstError)
{
  run("ls", {"/eos/first path", "relative", "/eos/third"});
  EXPECT_EQ(global_retc, ENOENT);
  const auto wires = captured();
  ASSERT_EQ(wires.size(), 3u);
  const std::vector<std::string> paths{"/eos/first path", "/eos/dispatch-test/relative",
                                       "/eos/third"};
  for (size_t i = 0; i < wires.size(); ++i) {
    XrdOucEnv env(wires[i].substr(wires[i].find('?') + 1).c_str());
    ASSERT_NE(env.Get("mgm.path"), nullptr);
    EXPECT_EQ(eos::common::StringConversion::curl_unescaped(env.Get("mgm.path")),
              paths[i]);
  }
}

TEST_F(LegacyCliValidationTest, LsDefaultPathChecksumAndCompatibilityFlags)
{
  run("ls", {});
  expectPath("/eos/dispatch-test/");
  run("ls", {"-crtyindFsN", "/eos/data"});
  EXPECT_EQ(field("mgm.option"), "-l-y-i-c-d-n-F-s-N");
}

TEST_F(LegacyCliValidationTest, LsHelpAndDelimiterAgreeWithPreflight)
{
  auto* command = CommandRegistry::instance().find("ls");
  ASSERT_NE(command, nullptr);
  EXPECT_FALSE(command->requiresMgm("-h"));
  EXPECT_FALSE(command->requiresMgm("--help"));
  EXPECT_TRUE(command->requiresMgm("-h /eos/data"));
  EXPECT_TRUE(command->requiresMgm("-- --help"));
  expectHelp("ls", {"-h"});
  expectHelp("ls", {"--help"});
  run("ls", {"--", "--help"});
  expectPath("/eos/dispatch-test/--help");
}

TEST_F(LegacyCliValidationTest, LsRejectsUnknownOptionsAndEmptyOperandsBeforeSending)
{
  for (const auto& args :
       std::vector<std::vector<std::string>>{{"--unknown", "/eos/data"},
                                             {"-z", "/eos/data"},
                                             {""},
                                             {"/eos/data", "--unknown"},
                                             {"/eos/data", ""}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    expectRejected("ls", args);
  }
}

TEST_F(LegacyCliValidationTest, LocalRootAndS3ModesDoNotRequireMgm)
{
  auto* command = CommandRegistry::instance().find("ls");
  ASSERT_NE(command, nullptr);
  EXPECT_FALSE(command->requiresMgm("file:/tmp"));
  EXPECT_FALSE(command->requiresMgm("root://localhost//data"));
  EXPECT_FALSE(command->requiresMgm("as3:bucket"));
  EXPECT_TRUE(command->requiresMgm("file:/tmp /eos/data"));
  expectRejected("ls", {"root:malformed"});
}

TEST_F(LegacyCliValidationTest, LocalListingPreflightKeepsOriginalOperands)
{
  auto* command = CommandRegistry::instance().find("ls");
  ASSERT_NE(command, nullptr);
  for (const std::string& path : {"file:/tmp/my files", "file:/tmp/a'b\"c\\d",
                                  "as3:bucket/a b", "root://localhost//a b"}) {
    SCOPED_TRACE(path);
    EXPECT_FALSE(command->requiresMgmArguments({path}));
    EXPECT_FALSE(command->requiresMgmArguments({"-l", "--", path}));
    EXPECT_TRUE(command->requiresMgmArguments({path, "/eos/data"}));
  }
}

TEST_F(LegacyCliValidationTest, ActualRunnerUsesVectorPreflight)
{
  struct ScopedRegistry {
    CommandRegistry saved;
    ScopedRegistry() { std::swap(saved, CommandRegistry::instance()); }
    ~ScopedRegistry() { std::swap(saved, CommandRegistry::instance()); }
  } isolatedRegistry;
  class ProbeCommand : public IConsoleCommand {
  public:
    const char*
    name() const override
    {
      return "test-vector-preflight";
    }
    const char*
    description() const override
    {
      return "Test preflight dispatch";
    }
    bool
    requiresMgm(const std::string&) const override
    {
      ADD_FAILURE() << "The runner must use the vector preflight interface";
      return false;
    }
    bool
    requiresMgmArguments(const std::vector<std::string>& args) const override
    {
      preflight = args;
      return false;
    }
    int
    run(const std::vector<std::string>& args, CommandContext&) override
    {
      executed = args;
      return 0;
    }
    void
    printHelp() const override
    {
    }
    mutable std::vector<std::string> preflight;
    std::vector<std::string> executed;
  };
  auto command = std::make_unique<ProbeCommand>();
  auto* probe = command.get();
  CommandRegistry::instance().reg(std::move(command));
  std::string line =
      R"(test-vector-preflight 'file:/tmp/my files' "quote\"and\\slash" "")";
  execute_line(line.data());
  const std::vector<std::string> expected{"file:/tmp/my files", "quote\"and\\slash", ""};
  EXPECT_EQ(probe->preflight, expected);
  EXPECT_EQ(probe->executed, expected);
  EXPECT_TRUE(captured().empty());
}

TEST_F(LegacyCliValidationTest, LegacyPreflightAdapterPreservesArgumentBoundaries)
{
  class ProbeCommand : public IConsoleCommand {
  public:
    const char*
    name() const override
    {
      return "test-legacy-preflight";
    }
    const char*
    description() const override
    {
      return "Test string preflight adapter";
    }
    bool
    requiresMgm(const std::string& line) const override
    {
      EXPECT_TRUE(eos::console::TokenizeArguments(line.c_str(), parsed));
      return true;
    }
    int
    run(const std::vector<std::string>&, CommandContext&) override
    {
      return 0;
    }
    void
    printHelp() const override
    {
    }
    mutable std::vector<std::string> parsed;
  } command;
  const std::vector<std::string> arguments{"path with spaces", "a\"b'c\\d", "", "--"};
  EXPECT_TRUE(command.requiresMgmArguments(arguments));
  EXPECT_EQ(command.parsed, arguments);
}

TEST_F(LegacyCliValidationTest, LegacyHelpPreflightAcceptsQuotedSerialization)
{
  for (const char* name : {"cp", "stat"}) {
    SCOPED_TRACE(name);
    auto* command = CommandRegistry::instance().find(name);
    ASSERT_NE(command, nullptr);
    EXPECT_FALSE(command->requiresMgmArguments({"--help"}));
    EXPECT_FALSE(command->requiresMgmArguments({"-h"}));
    EXPECT_TRUE(command->requiresMgmArguments({"/eos/ordinary path"}));
    EXPECT_TRUE(command->requiresMgmArguments({"/eos/a --help suffix"}));
    EXPECT_TRUE(command->requiresMgmArguments({"/eos/a -h suffix"}));
  }
}

TEST_F(LegacyCliValidationTest, LocalListingHandlesMissingSlashAndReportsOpendirErrors)
{
  const auto path = directory("local space");
  file(path + "/visible file");
  ::testing::internal::CaptureStdout();
  run("ls", {"file:" + path});
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_NE(output.find("visible file"), std::string::npos);
  EXPECT_TRUE(captured().empty());
  run("ls", {"file:" + path + "/missing"});
  EXPECT_EQ(global_retc, ENOENT);
  EXPECT_TRUE(captured().empty());
}

TEST_F(LegacyCliValidationTest, LsContinuesMixedOperandsWithoutLosingEarlierErrors)
{
  const auto path = directory("local");
  responseRetc = 0;
  run("ls", {"file:" + path + "/missing", "/eos/data"});
  EXPECT_EQ(global_retc, ENOENT);
  expectPath("/eos/data");
  run("ls", {"/eos/data", "file:" + path});
  EXPECT_EQ(global_retc, 0);
  expectPath("/eos/data");
}

TEST_F(LegacyCliValidationTest, S3ListingPreservesArgumentsWithoutInvokingShell)
{
  EnvironmentGuard environment{"PATH",          "S3_ACCESS_KEY_ID",
                               "S3_HOSTNAME",   "S3_SECRET_ACCESS_KEY",
                               "S3_ACCESS_KEY", "S3_ACESSS_ID"};
  const auto bin = directory("bin");
  const auto executable = bin + "/s3";
  file(executable, "#!/bin/sh\nprintf '%s\\n' \"$#\" \"$1\" \"$2\"\n");
  ASSERT_EQ(chmod(executable.c_str(), 0700), 0);
  setenv("PATH", bin.c_str(), 1);
  setenv("S3_ACCESS_KEY_ID", "test-id", 1);
  setenv("S3_SECRET_ACCESS_KEY", "test-secret", 1);
  setenv("S3_HOSTNAME", "test.invalid", 1);
  unsetenv("S3_ACCESS_KEY");
  unsetenv("S3_ACESSS_ID");
  const std::string path = "bucket/a b;$(unexecuted)&x=y";
  ::testing::internal::CaptureStdout();
  run("ls", {"as3:" + path});
  const auto output = ::testing::internal::GetCapturedStdout();
  EXPECT_EQ(global_retc, 0);
  EXPECT_EQ(output, "2\nlist\n" + path + "\n");
  EXPECT_TRUE(captured().empty());
}

TEST_F(LegacyCliValidationTest, CompletionPreservesPathBytesAndExitStatus)
{
  responseRetc = 0;
  global_retc = EBUSY;
  silent = false;
  rl_completion_type = 9;
  char* result = eos_entry_generator("/eos/a b\\c\"d&x/partial", 0);
  free(result);
  expectPath("/eos/a b\\c\"d&x/");
  EXPECT_EQ(field("mgm.option"), "-F");
  EXPECT_EQ(global_retc, EBUSY);
  EXPECT_FALSE(silent);
}

TEST_F(LegacyCliValidationTest, CompletionRejectsOverlongPathsWithoutFixedBuffer)
{
  rstdout = "stale-entry\n";
  rl_completion_type = 9;
  global_retc = EBUSY;
  const std::string path = "/eos/" + std::string(FILENAME_MAX + 32, 'a') + "/partial";
  char* result = eos_entry_generator(path.c_str(), 0);
  EXPECT_EQ(result, nullptr);
  free(result);
  EXPECT_TRUE(captured().empty());
  EXPECT_EQ(global_retc, EBUSY);
}
