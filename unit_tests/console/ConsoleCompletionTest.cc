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

#include "console/CommandFramework.hh"
#include "console/ConsoleCompletion.hh"
#include "gtest/gtest.h"

#include <algorithm>

namespace {
bool
contains(const std::vector<std::string>& values, const std::string& needle)
{
  return std::find(values.begin(), values.end(), needle) != values.end();
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

TEST(ConsoleCompletion, GroupCommandUsesHelpTextForCompletion)
{
  EnsureNativeCommandRegistryInitialized();
  auto* cmd = CommandRegistry::instance().find("group");

  ASSERT_NE(cmd, nullptr);
  EXPECT_NE(cmd->helpText().find("on|drain|off"), std::string::npos);
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

  EXPECT_TRUE(contains(suggestions, "configstatus="));
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
            EosShellPathCompletionMode::None);
  EXPECT_EQ(eos_shell_path_completion_mode("cp", {}, "./local"),
            EosShellPathCompletionMode::None);
}

TEST(ConsoleCompletion, ResolveRootedPathInputForEmptyWordStartsAtRoot)
{
  std::string lookupDir;
  std::string displayPrefix;
  std::string basename;

  eos_shell_resolve_rooted_path_input("", lookupDir, displayPrefix, basename);

  EXPECT_EQ(lookupDir, "/eos/");
  EXPECT_EQ(displayPrefix, "/eos/");
  EXPECT_EQ(basename, "");
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
