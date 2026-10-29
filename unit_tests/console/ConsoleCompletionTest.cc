//------------------------------------------------------------------------------
//! @file ConsoleCompletionTest.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/CommandCompletion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCompletion.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include "gtest/gtest.h"
#include <CLI/CLI.hpp>

#include <algorithm>
#include <cstdlib>
#include <map>
#include <readline/readline.h>
#include <set>

namespace {
class ScopedCompletionPwd {
public:
  ScopedCompletionPwd()
      : saved(gPwd)
  {
    gPwd = "/eos/project/";
  }
  ~ScopedCompletionPwd() { gPwd = saved; }

private:
  XrdOucString saved;
};

class ScopedReadlineState {
public:
  ScopedReadlineState()
      : line(rl_line_buffer)
      , attempted(rl_attempted_completion_over)
      , append(rl_completion_append_character)
      , filename(rl_filename_completion_desired)
  {
  }
  ~ScopedReadlineState()
  {
    rl_line_buffer = line;
    rl_attempted_completion_over = attempted;
    rl_completion_append_character = append;
    rl_filename_completion_desired = filename;
  }

private:
  char* line;
  int attempted, append, filename;
};

bool
contains(const std::vector<std::string>& values, const std::string& needle)
{
  return std::find(values.begin(), values.end(), needle) != values.end();
}

CommandCompletionResult
complete(const std::string& name, const std::vector<std::string>& args = {})
{
  EnsureNativeCommandRegistryInitialized();
  auto* command = CommandRegistry::instance().find(name);
  EXPECT_NE(command, nullptr) << name;
  return command ? command->completion(args) : CommandCompletionResult{};
}

void
expectCandidates(const std::string& name, const std::vector<std::string>& args,
                 const std::vector<std::string>& expected)
{
  const auto result = complete(name, args);
  for (const auto& candidate : expected) {
    EXPECT_TRUE(contains(result.candidates, candidate))
        << name << " " << ::testing::PrintToString(args) << ": " << candidate
        << " missing from " << ::testing::PrintToString(result.candidates);
  }
}
} // namespace

TEST(ConsoleCompletion, TokenizePrefixHandlesQuotedArguments)
{
  const auto tokens = eos_completion_tokenize_prefix("file info \"/eos/project alpha\" ");

  ASSERT_EQ(tokens.size(), 3u);
  EXPECT_EQ(tokens[0], "file");
  EXPECT_EQ(tokens[1], "info");
  EXPECT_EQ(tokens[2], "/eos/project alpha");
}

TEST(ConsoleCompletion, TokenizePrefixHandlesBackslashEscapedArguments)
{
  const auto tokens = eos_completion_tokenize_prefix("file info /eos/project\\ alpha");

  ASSERT_EQ(tokens.size(), 3u);
  EXPECT_EQ(tokens[0], "file");
  EXPECT_EQ(tokens[1], "info");
  EXPECT_EQ(tokens[2], "/eos/project alpha");
}

TEST(ConsoleCompletion, NativeCommandRegistryInitializationIsIdempotent)
{
  EnsureNativeCommandRegistryInitialized();
  const auto before = CommandRegistry::instance().all().size();

  RegisterNativeConsoleCommands();
  EnsureNativeCommandRegistryInitialized();

  EXPECT_EQ(CommandRegistry::instance().all().size(), before);
}

TEST(ConsoleCompletion, GroupCommandCompletesDrainState)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("group");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"set", "default.0"});

  EXPECT_TRUE(contains(suggestions, "on"));
  EXPECT_TRUE(contains(suggestions, "drain"));
  EXPECT_TRUE(contains(suggestions, "off"));
}

TEST(ConsoleCompletion, GroupCommandHelpDescribesValidatedStates)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("group");

  ASSERT_NE(cmd, nullptr);
  const auto help = cmd->helpText();
  EXPECT_NE(help.find("on"), std::string::npos);
  EXPECT_NE(help.find("drain"), std::string::npos);
  EXPECT_NE(help.find("off"), std::string::npos);
  EXPECT_NE(help.find("--brief"), std::string::npos);
}

TEST(ConsoleCompletion, SpaceCommandCompletesGroupDrainerSubcommands)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("space");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"groupdrainer"});

  EXPECT_TRUE(contains(suggestions, "status"));
  EXPECT_TRUE(contains(suggestions, "reset"));
  EXPECT_FALSE(contains(suggestions, "--detail"));
  EXPECT_FALSE(contains(suggestions, "--failed"));
}

TEST(ConsoleCompletion, FileCommandCompletesSubcommands)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("file");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({});

  EXPECT_TRUE(contains(suggestions, "info"));
  EXPECT_TRUE(contains(suggestions, "touch"));
  EXPECT_TRUE(contains(suggestions, "verify"));
}

TEST(ConsoleCompletion, FileCommandCompletesLeafOptionsFromHelp)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("file");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"verify", "/eos/dev/file"});

  EXPECT_TRUE(contains(suggestions, "-checksum"));
  EXPECT_TRUE(contains(suggestions, "-commitchecksum"));
  EXPECT_TRUE(contains(suggestions, "-commitsize"));
  EXPECT_TRUE(contains(suggestions, "-commitfmd"));
  EXPECT_TRUE(contains(suggestions, "-rate"));
  EXPECT_TRUE(contains(suggestions, "-resync"));
}

TEST(ConsoleCompletion, FsConfigCompletesKeysFromHelp)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("fs");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"config", "123"});

  EXPECT_TRUE(contains(suggestions, "configstatus=off"));
  EXPECT_TRUE(contains(suggestions, "configstatus=rw"));
  EXPECT_TRUE(contains(suggestions, "headroom="));
}

TEST(ConsoleCompletion, IoShapingPrefersSubcommandsOverLeafOptions)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("io");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"shaping"});

  EXPECT_TRUE(contains(suggestions, "ls"));
  EXPECT_TRUE(contains(suggestions, "enable"));
  EXPECT_TRUE(contains(suggestions, "disable"));
  EXPECT_TRUE(contains(suggestions, "policy"));
  EXPECT_TRUE(contains(suggestions, "config"));
  EXPECT_FALSE(contains(suggestions, "set"));
  EXPECT_FALSE(contains(suggestions, "--apps"));
  EXPECT_FALSE(contains(suggestions, "--window"));
}

TEST(ConsoleCompletion, IoShapingConfigPrefersSubcommandsOverLeafOptions)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("io");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"shaping", "config"});

  EXPECT_TRUE(contains(suggestions, "ls"));
  EXPECT_TRUE(contains(suggestions, "set"));
  EXPECT_FALSE(contains(suggestions, "--estimators-period"));
}

TEST(ConsoleCompletion, IoShapingConfigSetCompletesOptionsFromHelp)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("io");

  ASSERT_NE(cmd, nullptr);
  const auto suggestions = cmd->complete({"shaping", "config", "set"});

  EXPECT_TRUE(contains(suggestions, "--estimators-period"));
  EXPECT_TRUE(contains(suggestions, "--policy-period"));
  EXPECT_TRUE(contains(suggestions, "--report-period"));
  EXPECT_TRUE(contains(suggestions, "--system-window"));
}

TEST(ConsoleCompletion, ShellCompletionTopLevelCommands)
{
  const auto suggestions = eos_shell_completion_candidates({}, "gr");

  EXPECT_TRUE(contains(suggestions, "group"));
}

TEST(ConsoleCompletion, ShellCompletionSubcommands)
{
  const auto suggestions = eos_shell_completion_candidates({"group"}, "s");

  EXPECT_TRUE(contains(suggestions, "set"));
}

TEST(ConsoleCompletion, ShellCompletionGlobalFlags)
{
  const auto suggestions = eos_shell_completion_candidates({}, "--j");

  EXPECT_TRUE(contains(suggestions, "--json"));
}

TEST(ConsoleCompletion, ShellCompletionColorPolicyValues)
{
  EXPECT_EQ(eos_shell_completion_candidates({"--color"}, ""),
            (std::vector<std::string>{"always", "auto", "never"}));
  EXPECT_EQ(eos_shell_completion_candidates({"--color"}, "a"),
            (std::vector<std::string>{"always", "auto"}));
  EXPECT_EQ(
      eos_shell_completion_candidates({}, "--color="),
      (std::vector<std::string>{"--color=always", "--color=auto", "--color=never"}));
  EXPECT_EQ(eos_shell_completion_candidates({}, "--color=n"),
            (std::vector<std::string>{"--color=never"}));
}

TEST(ConsoleCompletion, GlobalValueArityPreventsColorOptionMisinterpretation)
{
  for (const auto& preceding : std::vector<std::vector<std::string>>{
           {"--app"}, {"-a"}, {"--role"}, {"-r", "123"}, {"--role", "--color"}}) {
    SCOPED_TRACE(::testing::PrintToString(preceding));
    EXPECT_TRUE(eos_shell_completion_candidates(preceding, "a").empty());
    EXPECT_TRUE(eos_shell_completion_candidates(preceding, "--color=a").empty());
  }
  EXPECT_EQ(eos_shell_completion_candidates({"--app", "--color"}, "ns"),
            (std::vector<std::string>{"ns"}));
  EXPECT_EQ(eos_shell_completion_candidates({"--role", "--color", "always"}, "ns"),
            (std::vector<std::string>{"ns"}));
}

TEST(ConsoleCompletion, ShellCompletionIoShapingSubcommands)
{
  const auto suggestions = eos_shell_completion_candidates({"io"}, "sh");

  EXPECT_TRUE(contains(suggestions, "shaping"));
}

TEST(ConsoleCompletion, ShellCompletionIoShapingPolicyActions)
{
  const auto suggestions = eos_shell_completion_candidates({"io", "shaping"}, "po");

  EXPECT_TRUE(contains(suggestions, "policy"));
}

TEST(ConsoleCompletion, PathCompletionModeUsesRootedPathContexts)
{
  EXPECT_EQ(eos_shell_path_completion_mode("ls", {}, ""),
            EosShellPathCompletionMode::Any);
  EXPECT_EQ(eos_shell_path_completion_mode("ls", {}, "/eos/user"),
            EosShellPathCompletionMode::Any);
  EXPECT_EQ(eos_shell_path_completion_mode("stat", {}, "/eos/user/file"),
            EosShellPathCompletionMode::Any);
  EXPECT_EQ(eos_shell_path_completion_mode("cd", {}, ""),
            EosShellPathCompletionMode::Directories);
  EXPECT_EQ(eos_shell_path_completion_mode("group", {}, ""),
            EosShellPathCompletionMode::None);
  EXPECT_EQ(eos_shell_path_completion_mode("ls", {}, "user/al"),
            EosShellPathCompletionMode::Any);
  EXPECT_EQ(eos_shell_path_completion_mode("rm", {}, "./user/al"),
            EosShellPathCompletionMode::Any);
  EXPECT_EQ(eos_shell_path_completion_mode("chmod", {"755"}, "../user/al"),
            EosShellPathCompletionMode::Any);
  EXPECT_EQ(eos_shell_path_completion_mode("cp", {}, "./local"),
            EosShellPathCompletionMode::None);
}

TEST(ConsoleCompletion, ResolveEmptyPathUsesConsoleWorkingDirectory)
{
  const ScopedCompletionPwd workingDirectory;
  std::string lookupDir;
  std::string displayPrefix;
  std::string basename;

  eos_shell_resolve_rooted_path_input("", lookupDir, displayPrefix, basename);

  EXPECT_EQ(lookupDir, "/eos/project/");
  EXPECT_EQ(displayPrefix, "");
  EXPECT_EQ(basename, "");
}

TEST(ConsoleCompletion, ResolveRelativePathUsesConsoleWorkingDirectory)
{
  const ScopedCompletionPwd workingDirectory;
  std::string lookupDir, displayPrefix, basename;
  eos_shell_resolve_rooted_path_input("child/al", lookupDir, displayPrefix, basename);
  EXPECT_EQ(lookupDir, "/eos/project/child/");
  EXPECT_EQ(displayPrefix, "child/");
  EXPECT_EQ(basename, "al");
}

TEST(ConsoleCompletion, ResolveRootedPathInputPreservesTypedPrefix)
{
  std::string lookupDir;
  std::string displayPrefix;
  std::string basename;

  eos_shell_resolve_rooted_path_input("/eos/user/al", lookupDir, displayPrefix, basename);

  EXPECT_EQ(lookupDir, "/eos/user/");
  EXPECT_EQ(displayPrefix, "/eos/user/");
  EXPECT_EQ(basename, "al");
}

TEST(ConsoleCompletion, ResolveRootedPathInputCompletesEosPrefix)
{
  std::string lookupDir;
  std::string displayPrefix;
  std::string basename;

  eos_shell_resolve_rooted_path_input("/eos", lookupDir, displayPrefix, basename);

  EXPECT_EQ(lookupDir, "/");
  EXPECT_EQ(displayPrefix, "/");
  EXPECT_EQ(basename, "eos");
}

TEST(ConsoleCompletion, EveryRegisteredCommandHasDeliberateCompletionCoverage)
{
  // A new registry entry must choose a grammar and an observable completion
  // contract here; inherited empty completion must not silently pass this test.
  const std::map<std::string, std::string> commands{{"?", "ns"},
                                                    {"access", "ban"},
                                                    {"accounting", "report"},
                                                    {"acl", "--list"},
                                                    {"archive", "create"},
                                                    {"attr", "ls"},
                                                    {"backup", "--ctime"},
                                                    {"cat", "--help"},
                                                    {"cd", "--help"},
                                                    {"chmod", "-r"},
                                                    {"chown", "-r"},
                                                    {"config", "save"},
                                                    {"convert", "config"},
                                                    {"cp", "--help"},
                                                    {"daemon", "config"},
                                                    {"debug", "get"},
                                                    {"devices", "ls"},
                                                    {"df", "-H"},
                                                    {"du", "--si"},
                                                    {"evict", "--fsid"},
                                                    {"file", "info"},
                                                    {"fileinfo", "--help"},
                                                    {"find", "--name"},
                                                    {"fs", "config"},
                                                    {"fsck", "stat"},
                                                    {"fuse", "mount"},
                                                    {"fusex", "ls"},
                                                    {"geosched", "show"},
                                                    {"group", "set"},
                                                    {"health", "all"},
                                                    {"help", "ns"},
                                                    {"info", "--help"},
                                                    {"inspector", "--current"},
                                                    {"io", "shaping"},
                                                    {"ln", "-f"},
                                                    {"ls", "--no-globbing"},
                                                    {"map", "ls"},
                                                    {"member", "--update"},
                                                    {"mkdir", "-p"},
                                                    {"monit", "config"},
                                                    {"mv", "--help"},
                                                    {"newfind", "--name"},
                                                    {"node", "ls"},
                                                    {"ns", "stat"},
                                                    {"oldfind", "-name"},
                                                    {"quota", "ls"},
                                                    {"rclone", "copy"},
                                                    {"reconnect", "gsi"},
                                                    {"recycle", "ls"},
                                                    {"register", "-u"},
                                                    {"report", "--read"},
                                                    {"rm", "--no-workflow"},
                                                    {"rmdir", "--help"},
                                                    {"role", "--help"},
                                                    {"route", "link"},
                                                    {"rtlog", "."},
                                                    {"sched", "configure"},
                                                    {"scitoken", "create"},
                                                    {"space", "ls"},
                                                    {"squash", "new"},
                                                    {"stat", "-f"},
                                                    {"test", "mkdir"},
                                                    {"token", "--path"},
                                                    {"touch", "-a"},
                                                    {"tui", "--help"},
                                                    {"version", "-m"},
                                                    {"vid", "ls"},
                                                    {"who", "-a"}};
  const std::set<std::string> noArguments{
      ".q",   "clear",  "exit",   "json",   "license", "motd",  "pwd",
      "quit", "silent", "status", "timing", "tracker", "whoami"};

  EnsureNativeCommandRegistryInitialized();
  std::set<std::string> actual;
  for (const auto* command : CommandRegistry::instance().all()) {
    const std::string name = command->name();
    SCOPED_TRACE(name);
    EXPECT_TRUE(actual.insert(name).second);
    EXPECT_TRUE(HasCommandCompletionSchema(name));
    const auto expected = commands.find(name);
    if (expected != commands.end()) {
      expectCandidates(name, {}, {expected->second});
    } else {
      ASSERT_EQ(noArguments.count(name), 1u)
          << "Register an explicit completion expectation for " << name;
      const auto result = command->completion({});
      EXPECT_EQ(result.path, CompletionPath::None);
      for (const auto& candidate : result.candidates) {
        EXPECT_TRUE(candidate == "-h" || candidate == "--help" ||
                    candidate == "--help-all")
            << candidate;
      }
    }
  }
  std::set<std::string> expected = noArguments;
  for (const auto& [name, candidate] : commands) {
    EXPECT_EQ(noArguments.count(name), 0u);
    expected.insert(name);
  }
  EXPECT_EQ(actual, expected);
  EXPECT_FALSE(HasCommandCompletionSchema("not-an-eos-command"));
}

TEST(ConsoleCompletion, NamespaceSubcommandsAndNestedValues)
{
  expectCandidates("ns", {},
                   {"stat", "mutex", "compact", "master", "recompute_tree_size",
                    "recompute_quotanode", "update_quotanode", "cache", "drain",
                    "reserve-ids", "benchmark", "tracker", "behaviour"});
  expectCandidates("ns", {"stat"}, {"-a", "-x", "-m", "-n", "--reset"});
  expectCandidates("ns", {"cache"},
                   {"set", "drop", "drop-single-file", "drop-single-container"});
  expectCandidates("ns", {"cache", "set"}, {"-d", "-f"});
  expectCandidates("ns", {"cache", "drop"}, {"-d", "-f"});
  expectCandidates("ns", {"behaviour"}, {"list", "set", "get", "clear"});
  expectCandidates("ns", {"behaviour", "set"},
                   {"rain_min_fsid_entry", "disable_access_log"});
  expectCandidates("ns", {"behaviour", "set", "rain_min_fsid_entry"}, {"on", "off"});
  expectCandidates("ns", {"behaviour", "clear"}, {"all"});
  expectCandidates("ns", {"tracker"}, {"list", "clear"});
  expectCandidates("ns", {"tracker", "list"}, {"--name"});
  expectCandidates("ns", {"tracker", "list", "--name"},
                   {"drain", "balance", "fsck", "convert", "all"});
  const auto pending = complete("ns", {"mutex", "--setblockedtime"});
  EXPECT_TRUE(pending.candidates.empty());
  EXPECT_EQ(pending.path, CompletionPath::None);
  expectCandidates("ns", {"mutex", "--setblockedtime", "100"},
                   {"--toggleorder", "--toggledeadlock", "--smplrate10"});
}

TEST(ConsoleCompletion, MonitoringSubcommandsDoNotLeakNestedOptions)
{
  expectCandidates("monit", {}, {"enable", "disable", "config"});
  expectCandidates("monit", {"config"}, {"ls", "set"});
  const auto root = complete("monit");
  EXPECT_FALSE(contains(root.candidates, "--port"));
  EXPECT_FALSE(contains(root.candidates, "set"));
  expectCandidates("monit", {"config", "set"}, {"--port", "--cache-ttl"});
  const auto pending = complete("monit", {"config", "set", "--port"});
  EXPECT_TRUE(pending.candidates.empty());
  EXPECT_EQ(pending.path, CompletionPath::None);
  expectCandidates("monit", {"config", "set", "--port", "9000"}, {"--cache-ttl"});
}

TEST(ConsoleCompletion, AdministrativeAndStorageNestedGrammars)
{
  struct Example {
    const char* command;
    std::vector<std::string> preceding;
    std::vector<std::string> expected;
  };
  const std::vector<Example> examples{
      {"access", {"ban"}, {"user", "group", "host", "domain"}},
      {"access", {"set"}, {"redirect", "stall", "limit"}},
      {"accounting", {"report"}, {"-f"}},
      {"archive", {"transfers"}, {"all", "put", "get", "purge"}},
      {"attr", {"get"}, {"sys.acl", "user.acl", "sys.forced.space"}},
      {"convert", {"config"}, {"list", "set"}},
      {"daemon", {"sss"}, {"recreate"}},
      {"devices", {"ls"}, {"-l", "-m", "--refresh"}},
      {"file", {"layout", "/eos/test", "-checksum"}, {"adler32", "sha256"}},
      {"file", {"touch", "/eos/test"}, {"-a", "-n", "-0", "-l", "-u"}},
      {"fs", {"boot"}, {"--syncdisk", "--syncmgm"}},
      {"fs", {"config", "1"}, {"configstatus=rw", "drain=on", "headroom="}},
      {"fsck", {"config"}, {"collect", "repair", "repair-category"}},
      {"fusex", {"ls"}, {"-l", "-f", "-k", "-m"}},
      {"geosched", {"updater"}, {"pause", "resume"}},
      {"io", {"shaping", "ls"}, {"--apps", "--users", "--groups", "--window"}},
      {"io", {"shaping", "config", "set", "--detail"}, {"aggregate", "fs"}},
      {"io", {"shaping", "policy"}, {"ls", "set", "rm"}},
      {"node", {"set", "host.example:1095"}, {"on", "off"}},
      {"quota", {"ls"}, {"--summary", "--recursive", "--uid", "--gid", "--path"}},
      {"quota", {"set"}, {"--uid", "--gid", "--volume", "--inodes", "--path"}},
      {"rclone", {"copy"}, {"--dryrun", "--atomic", "--delete"}},
      {"sched", {"configure", "type"}, {"geotree", "flat:roundrobin"}},
      {"scitoken", {"create", "--profile"}, {"wlcg", "scitokens1", "scitokens2"}},
      {"space", {"groupdrainer"}, {"status", "reset"}},
      {"space", {"config", "default"}, {"space.balancer=on", "space.quota=off"}},
      {"squash", {"pack", "/eos/test"}, {"-f"}},
      {"token", {"--permission"}, {"rx", "rwx", "rwx!d", "rwxq"}},
      {"vid", {"enable"}, {"krb5", "gsi", "sss", "unix"}}};
  for (const auto& example : examples) {
    SCOPED_TRACE(example.command);
    SCOPED_TRACE(::testing::PrintToString(example.preceding));
    expectCandidates(example.command, example.preceding, example.expected);
  }
}

TEST(ConsoleCompletion, SuggestedLiteralsDoNotRejectOpenEndedValidArguments)
{
  // Literal hints also coexist with freeform paths, identifiers and numbers.
  // Completion must not turn these hints into an execution-time enum validator.
  expectCandidates("ns", {"recompute_tree_size", "/eos/project"}, {"--depth"});
  expectCandidates("ns", {"recompute_tree_size", "cid:123"}, {"--depth"});
  expectCandidates("rtlog", {".", "10"}, {"debug", "err"});
  expectCandidates("fusex", {"evict", "12345678-abcd-4321-abcd-123456789abc"},
                   {"abort", "setlog", "stacktrace"});
}

TEST(ConsoleCompletion, LegacyMultiLetterOptionsRetainTheirRealSpellingAndArity)
{
  const auto find = complete("find");
  EXPECT_TRUE(contains(find.candidates, "-g"));
  EXPECT_TRUE(contains(find.candidates, "-gid"));
  // -gid is the value-taking legacy option; --gid is a distinct output flag.
  EXPECT_TRUE(contains(find.candidates, "--gid"));
  const auto pending = complete("find", {"-gid"});
  EXPECT_TRUE(pending.candidates.empty());
  EXPECT_EQ(pending.path, CompletionPath::None);
  const auto supplied = complete("find", {"-gid", "123"});
  EXPECT_EQ(supplied.path, CompletionPath::EosOrFile);
  EXPECT_TRUE(contains(supplied.candidates, "-g"));
  EXPECT_EQ(complete("find", {"-g"}).path, CompletionPath::EosOrFile);
  const auto drop = complete("file", {"drop", "/eos/file", "17"});
  EXPECT_TRUE(contains(drop.candidates, "-f"));
  EXPECT_EQ(complete("quota", {"ls", "-p"}).path, CompletionPath::EosDirectory);
  EXPECT_EQ(complete("fs", {"add", "uuid", "host.example:1095"}).path,
            CompletionPath::None);
}

TEST(ConsoleCompletion, RepeatableOptionsRemainAvailableAfterAValue)
{
  expectCandidates("token", {"--origin", "^https://first.example/.*$"}, {"--origin"});
  expectCandidates("token", {"--origin=^https://first.example/.*$"}, {"--origin"});
  expectCandidates("scitoken", {"create", "--claim", "sub=foo"}, {"--claim"});
  expectCandidates("scitoken", {"create", "--claim=sub=foo", "--claim", "aud=bar"},
                   {"--claim"});
}

TEST(ConsoleCompletion, DirectoryAndInlineOnlyOptionsMatchTheActualCommands)
{
  const auto install = complete("squash", {"install"});
  EXPECT_TRUE(contains(install.candidates, "--curl="));
  EXPECT_FALSE(contains(install.candidates, "--curl"));
  EXPECT_EQ(
      complete("squash", {"install", "--curl=https://example.invalid/pkg.tgz"}).path,
      CompletionPath::LocalDirectory);
  for (const auto* operation : {"new", "new-release"}) {
    EXPECT_EQ(complete("squash", {operation}).path, CompletionPath::LocalDirectory)
        << operation;
  }
  for (const auto* operation : {"pack", "unpack"}) {
    EXPECT_EQ(complete("squash", {operation}).path, CompletionPath::Local) << operation;
  }
  for (const auto* operation : {"copy", "sync"}) {
    EXPECT_EQ(complete("rclone", {operation}).path, CompletionPath::AnyDirectory);
    EXPECT_EQ(complete("rclone", {operation, "file:/tmp/source"}).path,
              CompletionPath::AnyDirectory);
  }
}

TEST(ConsoleCompletion, TypedCommandSchemasExposeFlagsAndRespectExclusions)
{
  expectCandidates("df", {}, {"-m", "-H", "-b", "--help"});
  const auto df = complete("df", {"-m"});
  EXPECT_FALSE(contains(df.candidates, "-H"));
  EXPECT_FALSE(contains(df.candidates, "-b"));
  expectCandidates("du", {}, {"-a", "-h", "-s", "--si", "--help"});
  expectCandidates("ls", {}, {"-l", "-a", "-h", "-N", "--no-globbing"});
  expectCandidates(
      "rm", {},
      {"-r", "--no-recycle-bin", "--no-confirmation", "--no-workflow", "--no-globbing"});
  expectCandidates("rm", {"-rf"}, {"--no-workflow", "--no-globbing"});
  expectCandidates("config", {"save", "demo"}, {"-f", "-c", "--comment"});
  const auto comment = complete("config", {"save", "demo", "--comment"});
  EXPECT_TRUE(comment.candidates.empty());
  EXPECT_EQ(comment.path, CompletionPath::None);
  expectCandidates("config", {"save", "demo", "--comment=a comment"}, {"-f"});
  expectCandidates("group", {"ls"}, {"-b", "--brief", "--io", "--IO"});
  const auto monitoring = complete("group", {"ls", "-m"});
  EXPECT_FALSE(contains(monitoring.candidates, "-l"));
  EXPECT_FALSE(contains(monitoring.candidates, "--io"));
}

TEST(ConsoleCompletion, PathsAreOfferedOnlyForPathArguments)
{
  EXPECT_EQ(complete("cd").path, CompletionPath::EosDirectory);
  EXPECT_EQ(complete("ls").path, CompletionPath::EosOrFile);
  EXPECT_EQ(complete("rm").path, CompletionPath::Eos);
  EXPECT_EQ(complete("chmod").path, CompletionPath::None);
  EXPECT_EQ(complete("chmod", {"755"}).path, CompletionPath::Eos);
  EXPECT_EQ(complete("chown").path, CompletionPath::None);
  EXPECT_EQ(complete("chown", {"1000"}).path, CompletionPath::Eos);
  EXPECT_EQ(complete("route", {"link"}).path, CompletionPath::EosDirectory);
  EXPECT_EQ(complete("route", {"link", "/eos/project/"}).path, CompletionPath::None);
  EXPECT_EQ(complete("evict", {"--fsid"}).path, CompletionPath::None);
  EXPECT_EQ(complete("config", {"save"}).path, CompletionPath::None);
  EXPECT_EQ(complete("fuse", {"mount"}).path, CompletionPath::LocalDirectory);
}

TEST(ConsoleCompletion, HelpAndAliasesFollowRegisteredCommandGrammars)
{
  const auto help = complete("help");
  const auto question = complete("?");
  EXPECT_EQ(help.candidates, question.candidates);
  for (const auto* command : CommandRegistry::instance().all()) {
    EXPECT_TRUE(contains(help.candidates, command->name())) << command->name();
  }
  EXPECT_TRUE(complete("help", {"ns"}).candidates.empty());
  EXPECT_EQ(complete("info").candidates, complete("file", {"info"}).candidates);
  EXPECT_EQ(complete("fileinfo").candidates, complete("file", {"info"}).candidates);
  EXPECT_EQ(complete("info").path, complete("file", {"info"}).path);
  EXPECT_EQ(complete("fileinfo").path, complete("file", {"info"}).path);
  EXPECT_EQ(complete("newfind").candidates, complete("find").candidates);
  EXPECT_EQ(complete("newfind").path, complete("find").path);
  EXPECT_EQ(complete("mv").path, CompletionPath::Eos);
}

TEST(ConsoleCompletion, UnknownSubcommandsAndOptionTerminatorDoNotSuggestGrammar)
{
  for (const auto* name : {"ns", "monit", "group", "config", "io"}) {
    const auto unknown = complete(name, {"no-such-subcommand"});
    EXPECT_TRUE(unknown.candidates.empty()) << name;
    EXPECT_EQ(unknown.path, CompletionPath::None) << name;
    const auto afterTerminator = complete(name, {"--"});
    EXPECT_TRUE(afterTerminator.candidates.empty()) << name;
  }
  const auto rm = complete("rm", {"--"});
  EXPECT_TRUE(rm.candidates.empty());
  EXPECT_EQ(rm.path, CompletionPath::Eos);
  const auto literal = complete("rm", {"--", "-filename"});
  EXPECT_TRUE(literal.candidates.empty());
  EXPECT_EQ(literal.path, CompletionPath::Eos);
}

TEST(ConsoleCompletion, CliMetadataCompletionNeverRunsCallbacksOrValidators)
{
  CLI::App app{"completion test", "test"};
  int callbacks = 0;
  int validations = 0;
  std::string value;
  app.callback([&]() { ++callbacks; });
  app.add_option("--value", value)
      ->check(CLI::Validator(
          [&](const std::string&) {
            ++validations;
            return std::string("not valid for execution");
          },
          "COMPLETION_MUST_NOT_VALIDATE"));
  auto* child = app.add_subcommand("child");
  child->callback([&]() { ++callbacks; });
  child->add_flag("--flag");
  for (const auto& arguments : std::vector<std::vector<std::string>>{
           {}, {"--value"}, {"--value", "invalid"}, {"child"}, {"child", "--flag"}}) {
    CompleteCliArguments(app, arguments);
  }
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(validations, 0);
  EXPECT_TRUE(value.empty());
  EXPECT_EQ(app.count_all(), 0u);
}

TEST(ConsoleCompletion, CliMetadataTracksOptionArityAliasesAndInlineValues)
{
  CLI::App app{"completion test", "test"};
  app.add_flag("-a,--all");
  app.add_flag("-b,--brief");
  auto* mode = app.add_option("-m,--mode");
  auto* pair = app.add_option("--pair")->expected(2);
  auto* path = app.add_option("path");
  const std::map<const CLI::Option*, std::vector<std::string>> values{
      {mode, {"fast", "safe"}}, {pair, {"first", "second"}}};
  const std::map<const CLI::Option*, CompletionPath> paths{{path, CompletionPath::Eos}};
  EXPECT_EQ(CompleteCliArguments(app, {"--mode"}, values, paths).candidates,
            (std::vector<std::string>{"fast", "safe"}));
  EXPECT_EQ(CompleteCliArguments(app, {"-abm"}, values, paths).candidates,
            (std::vector<std::string>{"fast", "safe"}));
  EXPECT_EQ(CompleteCliArguments(app, {"--pair", "one"}, values, paths).candidates,
            (std::vector<std::string>{"first", "second"}));
  EXPECT_EQ(CompleteCliArguments(app, {"--pair", "one"}, values, paths).path,
            CompletionPath::None);
  for (const auto& args : std::vector<std::vector<std::string>>{
           {"--mode=fast"}, {"-mfast"}, {"--pair", "one", "two"}, {"-ab"}}) {
    const auto completed = CompleteCliArguments(app, args, values, paths);
    EXPECT_EQ(completed.path, CompletionPath::Eos);
    EXPECT_TRUE(contains(completed.candidates, "--help"));
  }
  const auto terminated = CompleteCliArguments(app, {"--"}, values, paths);
  EXPECT_TRUE(terminated.candidates.empty());
  EXPECT_EQ(terminated.path, CompletionPath::Eos);
}

TEST(ConsoleCompletion, CliMetadataUsesExclusionsWithoutMutatingParserState)
{
  CLI::App app{"completion test", "test"};
  auto* alpha = app.add_flag("-a,--alpha");
  auto* beta = app.add_flag("-b,--beta");
  alpha->excludes(beta);
  auto* child = app.add_subcommand("child");
  child->add_flag("--leaf");
  const auto selected = CompleteCliArguments(app, {"--alpha"});
  EXPECT_FALSE(contains(selected.candidates, "--beta"));
  EXPECT_FALSE(contains(selected.candidates, "-b"));
  EXPECT_FALSE(contains(CompleteCliArguments(app, {"-b"}).candidates, "--alpha"));
  EXPECT_TRUE(contains(CompleteCliArguments(app, {}).candidates, "--beta"));
  EXPECT_FALSE(contains(CompleteCliArguments(app, {}).candidates, "--leaf"));
  EXPECT_TRUE(contains(CompleteCliArguments(app, {"child"}).candidates, "--leaf"));
  EXPECT_TRUE(CompleteCliArguments(app, {"unknown"}).candidates.empty());
  EXPECT_TRUE(CompleteCliArguments(app, {"--unknown"}).candidates.empty());
  EXPECT_TRUE(CompleteCliArguments(app, {"--"}).candidates.empty());
  EXPECT_EQ(alpha->count(), 0u);
  EXPECT_EQ(beta->count(), 0u);
}

TEST(ConsoleCompletion, AnonymousOptionGroupCardinalityRestrictsFurtherFlags)
{
  CLI::App app{"completion test", "test"};
  auto* choice = app.add_option_group("selection");
  choice->require_option(0, 1);
  choice->add_flag("-a,--alpha");
  choice->add_flag("-b,--beta");
  app.add_flag("--outside");
  EXPECT_TRUE(contains(CompleteCliArguments(app, {}).candidates, "--alpha"));
  EXPECT_TRUE(contains(CompleteCliArguments(app, {}).candidates, "--beta"));
  const auto selected = CompleteCliArguments(app, {"--alpha"});
  EXPECT_FALSE(contains(selected.candidates, "--beta"));
  EXPECT_FALSE(contains(selected.candidates, "-b"));
  EXPECT_TRUE(contains(selected.candidates, "--outside"));

  const auto io = complete("io", {"shaping", "ls", "--apps"});
  EXPECT_FALSE(contains(io.candidates, "--users"));
  EXPECT_FALSE(contains(io.candidates, "--groups"));
  EXPECT_FALSE(contains(io.candidates, "--nodes"));
  EXPECT_TRUE(contains(io.candidates, "--window"));
}

TEST(ConsoleCompletion, VariadicOptionValuesHandleInlineAndAttachedFormsIdentically)
{
  CLI::App app{"completion test", "test"};
  const auto* error = app.add_option("-e,--error")->expected(1, -1);
  app.add_flag("--json");
  const std::map<const CLI::Option*, std::vector<std::string>> values{
      {error, {"alpha", "beta", "gamma"}}};
  const auto separate = CompleteCliArguments(app, {"--error", "alpha", "beta"}, values);
  for (const auto* candidate : {"alpha", "beta", "gamma", "--json", "--help"}) {
    EXPECT_TRUE(contains(separate.candidates, candidate));
  }
  for (const auto& args : std::vector<std::vector<std::string>>{{"--error=alpha", "beta"},
                                                                {"-ealpha", "beta"}}) {
    EXPECT_EQ(CompleteCliArguments(app, args, values).candidates, separate.candidates);
    auto ended = args;
    ended.push_back("--json");
    const auto result = CompleteCliArguments(app, ended, values);
    EXPECT_TRUE(contains(result.candidates, "--help"));
    EXPECT_FALSE(contains(result.candidates, "gamma"));
  }
}

TEST(ConsoleCompletion, RecycleMonitoringFlagExcludesAllSubcommands)
{
  const auto status = complete("recycle", {"-m"});
  for (const auto* subcommand : {"ls", "purge", "restore", "config"}) {
    EXPECT_FALSE(contains(status.candidates, subcommand));
  }
}

namespace {
class ConsoleCompletionNoIpcTest : public ConsoleTestFixture {};
} // namespace

TEST_F(ConsoleCompletionNoIpcTest, StaticCompletionOfEveryCommandDoesNotContactMgm)
{
  for (const auto* command : CommandRegistry::instance().all()) {
    SCOPED_TRACE(command->name());
    command->completion({});
    command->complete({});
    eos_shell_completion_candidates({command->name()}, "--");
  }
  for (const auto& args :
       std::vector<std::vector<std::string>>{{"stat"},
                                             {"cache"},
                                             {"cache", "drop"},
                                             {"mutex", "--setblockedtime"},
                                             {"behaviour", "set"},
                                             {"tracker", "list", "--name"}}) {
    complete("ns", args);
  }
  std::lock_guard<std::mutex> lock(mutex);
  EXPECT_TRUE(requests.empty());
}

TEST_F(ConsoleCompletionNoIpcTest, ReadlineUsesSameNamespaceCompletionAsShell)
{
  // Readline's completion array starts with the common replacement prefix;
  // subsequent entries are the alternatives displayed by a second Tab.
  rl_initialize();
  const ScopedReadlineState readlineState;
  std::string line = "ns ";
  rl_line_buffer = line.data();
  char** matches = eos_console_completion("", 3, 3);
  ASSERT_NE(matches, nullptr);
  std::vector<std::string> alternatives;
  for (size_t i = 0; matches[i]; ++i) {
    alternatives.emplace_back(matches[i]);
    free(matches[i]);
  }
  free(matches);
  EXPECT_TRUE(contains(alternatives, "stat"));
  EXPECT_TRUE(contains(alternatives, "cache"));
  EXPECT_TRUE(contains(alternatives, "behaviour"));
  std::lock_guard<std::mutex> lock(mutex);
  EXPECT_TRUE(requests.empty());
}
