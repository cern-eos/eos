// ----------------------------------------------------------------------
// File: ConsoleCommandHelpTest.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleCommandHelp.hh"
#include "console/ConsoleCompletion.hh"
#include "unit_tests/console/ConsoleTestFixture.hh"
#include "gtest/gtest.h"
#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

using namespace eos::console;

namespace {
std::string
Words(const std::string& text)
{
  std::istringstream input(text);
  std::string result, word;
  while (input >> word) {
    if (!result.empty()) {
      result += ' ';
    }
    result += word;
  }
  return result;
}

const std::string manual =
    "Usage: sample <operation>\n\n"
    "  sample group <operation>\n"
    "    Configure sample state.\n\n"
    "  sample group set <name> <value>\n"
    "    Set a value without discarding its original explanation.\n"
    "    --limit <count> : Maximum count.\n"
    "                     Zero disables the limit.\n\n"
    "  sample list [-m]\n"
    "    List every configured object.\n\n"
    "  EXAMPLES:\n"
    "    # Set a sample value\n"
    "    eos sample group set demo 42\n\n"
    "    # List all sample objects\n"
    "    eos sample list -m\n";

CommandHelpSpec
specification()
{
  return {{{{"group"}, "  sample group <", "Configure sample state."},
           {{"group", "set"}, "  sample group set ", "Set a value."},
           {{"group", "update"}, "  sample group set ", "Set a value (alias)."},
           {{"list"}, "  sample list ", "List objects."}},
          ShortHelpPolicy::Always,
          {{"--label", 1}},
          {}};
}

class RecordingCommand final : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "sample";
  }
  const char*
  description() const override
  {
    return "Manage sample objects.";
  }
  std::string
  helpText() const override
  {
    return manual;
  }
  bool
  requiresMgmArguments(const std::vector<std::string>&) const override
  {
    return true;
  }
  int
  run(const std::vector<std::string>& args, CommandContext&) override
  {
    ++executions;
    arguments = args;
    return 42;
  }
  CommandCompletionResult
  completion(const std::vector<std::string>&) const override
  {
    return {{"--original-option"}, CompletionPath::None};
  }
  void
  printHelp() const override
  {
    ADD_FAILURE() << "Original help must not execute";
  }
  int executions = 0;
  std::vector<std::string> arguments;
};

class ConsoleCommandHelpTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    savedRetc = global_retc;
    ConfigureColor(ColorMode::Never, false, false);
    ConfigureHelp(false, false);
  }
  void
  TearDown() override
  {
    global_retc = savedRetc;
    ConfigureColor(ColorMode::Auto, false, false);
    ConfigureHelp(false, false);
  }
  int savedRetc = 0;
};
} // namespace

TEST_F(ConsoleCommandHelpTest, ValidatesAuthoredAnchorsAndPermitsSharedAliasSections)
{
  auto spec = specification();
  std::string error;
  EXPECT_TRUE(ValidateCommandHelpSpec(manual, spec, &error)) << error;
  spec.topics.push_back({{"missing"}, "  sample missing ", "Missing operation."});
  EXPECT_FALSE(ValidateCommandHelpSpec(manual, spec, &error));
  EXPECT_NE(error.find("sample missing"), std::string::npos);
  spec = specification();
  spec.topics.push_back(spec.topics.front());
  EXPECT_FALSE(ValidateCommandHelpSpec(manual, spec, &error));
}

TEST_F(ConsoleCommandHelpTest, RootIndexAndNestedSectionsRetainOnlyTheirDeclaredTopics)
{
  const auto spec = specification();
  const auto index =
      BuildCommandHelpPage("sample", "Manage samples.", manual, spec).render(80);
  EXPECT_NE(index.find("OPERATIONS"), std::string::npos);
  EXPECT_NE(index.find("Configure sample state."), std::string::npos);
  EXPECT_EQ(index.find("--limit"), std::string::npos);
  const auto leaf =
      BuildCommandHelpPage("sample", "Manage samples.", manual, spec, {"group", "set"})
          .render(40);
  EXPECT_NE(leaf.find("--limit <count>"), std::string::npos);
  EXPECT_EQ(leaf.find("eos sample group set [options]"), std::string::npos);
  EXPECT_NE(Words(leaf).find("Zero disables the limit."), std::string::npos);
  EXPECT_NE(leaf.find("eos sample group set demo 42"), std::string::npos);
  EXPECT_EQ(leaf.find("List every configured object."), std::string::npos);
  EXPECT_EQ(leaf.find("eos sample list -m"), std::string::npos);
  const auto subtree =
      BuildCommandHelpPage("sample", "Manage samples.", manual, spec, {"group"}, true)
          .render(80);
  EXPECT_NE(subtree.find("Zero disables the limit."), std::string::npos);
  EXPECT_EQ(subtree.find("List every configured object."), std::string::npos);
  EXPECT_EQ(
      subtree.find("original explanation", subtree.find("original explanation") + 1),
      std::string::npos);
}

TEST_F(ConsoleCommandHelpTest, ExpandedManualKeepsEveryOriginalDescriptionAndExample)
{
  const auto text =
      BuildCommandHelpPage("sample", "Manage samples.", manual, specification(), {}, true)
          .render(80);
  for (const auto* detail :
       {"Configure sample state.", "original explanation.", "Maximum count.",
        "Zero disables the limit.", "List every configured object.",
        "eos sample group set demo 42", "eos sample list -m"}) {
    EXPECT_NE(text.find(detail), std::string::npos) << detail;
  }
}

TEST_F(ConsoleCommandHelpTest, BareCli11HeadingsRetainTheOwningCommand)
{
  const CommandHelpSpec spec{{{{"info"}, "info ", "Inspect a file."}}};
  const auto text =
      BuildCommandHelpPage("file", "File interface.", "info <path>\n  Inspect a file.\n",
                           spec, {"info"})
          .render(80);
  EXPECT_NE(text.find("eos file info <path>"), std::string::npos);
}

TEST_F(ConsoleCommandHelpTest, FocusedPagesKeepSharedOptionsAndExcludeBoundedAppendices)
{
  const std::string source =
      "Usage: sample [-m] operation\n"
      "  -m : Monitoring output.\n\n"
      "  sample list\n"
      "    List all objects.\n\n"
      "Remarks:\n"
      "  Shared administrator appendix with every original explanation.\n";
  const CommandHelpSpec spec{{{{"list"}, "  sample list", "List objects.", "Remarks:"}},
                             ShortHelpPolicy::Always,
                             {},
                             {},
                             "Options:\n  -m : Monitoring output.\n"};
  std::string error;
  ASSERT_TRUE(ValidateCommandHelpSpec(source, spec, &error)) << error;
  for (const auto& path : std::vector<std::vector<std::string>>{{}, {"list"}}) {
    const auto focused =
        BuildCommandHelpPage("sample", "Sample interface.", source, spec, path)
            .render(80);
    EXPECT_NE(focused.find("Monitoring output."), std::string::npos);
    EXPECT_EQ(focused.find("administrator appendix"), std::string::npos);
  }
  const auto full =
      BuildCommandHelpPage("sample", "Sample interface.", source, spec, {}, true)
          .render(80);
  EXPECT_NE(full.find("administrator appendix"), std::string::npos);
  auto invalid = spec;
  invalid.topics.front().endMarker = "Usage:";
  EXPECT_FALSE(ValidateCommandHelpSpec(source, invalid));
}

TEST_F(ConsoleCommandHelpTest, LegacyExamplesNormalizeOnlyTheDeclaredCommand)
{
  const std::string source =
      "Usage: sample group <name>\n\n"
      "  sample group <name>\n"
      "    Configure an object.\n\n"
      "  sample list\n"
      "    List objects.\n\n"
      "  EXAMPLES:\n"
      "    sample group first -> Configure the first sample object with its original "
      "description.\n"
      "    [eos] sample group second -> Configure another object.\n"
      "    $ eos sample group third\n"
      "  This explanation has enough words to require wrapping on a narrow terminal.\n"
      "  curl https://example.invalid/a/very/long/resource/path --output result\n"
      "    sample list -> List all objects.\n";
  const CommandHelpSpec spec{{{{"group"}, "  sample group ", "Configure objects."},
                              {{"list"}, "  sample list", "List objects."}}};
  const auto full =
      BuildCommandHelpPage("sample", "Sample interface.", source, spec, {}, true)
          .render(40);
  EXPECT_EQ(full.find(" -> "), std::string::npos);
  for (const auto* command : {"eos sample group first", "eos sample group second",
                              "eos sample group third", "eos sample list"}) {
    EXPECT_NE(full.find(command), std::string::npos) << command;
  }
  EXPECT_NE(full.find("# Configure another object."), std::string::npos);
  EXPECT_NE(
      full.find("curl https://example.invalid/a/very/long/resource/path --output result"),
      std::string::npos);
  EXPECT_EQ(
      full.find(
          "This explanation has enough words to require wrapping on a narrow terminal."),
      std::string::npos);
  const auto focused =
      BuildCommandHelpPage("sample", "Sample interface.", source, spec, {"group"})
          .render(80);
  EXPECT_NE(focused.find("eos sample group first"), std::string::npos);
  EXPECT_NE(focused.find("eos sample group second"), std::string::npos);
  EXPECT_NE(focused.find("eos sample group third"), std::string::npos);
  EXPECT_EQ(focused.find("eos sample list"), std::string::npos);
}

TEST_F(ConsoleCommandHelpTest, HelpBypassesExecutionAndForwardsOrdinaryArguments)
{
  auto original = std::make_unique<RecordingCommand>();
  auto* observed = original.get();
  auto command = WithCommandHelp(std::move(original), specification());
  CommandContext context{};
  for (const auto& args : std::vector<std::vector<std::string>>{
           {"group", "set", "--help"}, {"group", "--help-all"}, {"-h"}}) {
    EXPECT_FALSE(command->requiresMgmArguments(args));
    testing::internal::CaptureStderr();
    EXPECT_EQ(command->run(args, context), 0);
    const auto help = testing::internal::GetCapturedStderr();
    EXPECT_FALSE(help.empty());
    EXPECT_EQ(global_retc, 0);
    EXPECT_EQ(observed->executions, 0);
  }
  for (const auto& args :
       std::vector<std::vector<std::string>>{{"--", "--help"},
                                             {"--label", "--help"},
                                             {"--label", "--", "--help"},
                                             {"group", "set", "literal --help suffix"}}) {
    EXPECT_TRUE(command->requiresMgmArguments(args));
    EXPECT_EQ(command->run(args, context), 42);
    EXPECT_EQ(observed->arguments, args);
  }
  EXPECT_EQ(command->completion({}).candidates,
            (std::vector<std::string>{"--help", "--help-all", "--original-option"}));
  EXPECT_EQ(command->completion({"--label"}).candidates,
            (std::vector<std::string>{"--original-option"}));
  EXPECT_EQ(command->completion({"--"}).candidates,
            (std::vector<std::string>{"--original-option"}));
}

TEST_F(ConsoleCommandHelpTest, ShortHelpPoliciesPreserveDataOptions)
{
  for (const auto policy : {ShortHelpPolicy::Standalone, ShortHelpPolicy::Never}) {
    auto spec = specification();
    spec.shortHelp = policy;
    auto command = WithCommandHelp(std::make_unique<RecordingCommand>(), spec);
    EXPECT_EQ(command->requiresMgmArguments({"-h"}), policy == ShortHelpPolicy::Never);
    EXPECT_TRUE(command->requiresMgmArguments({"-h", "/eos/path"}));
    EXPECT_FALSE(command->requiresMgmArguments({"--help"}));
    EXPECT_FALSE(command->requiresMgmArguments({"--help-all"}));
    EXPECT_TRUE(command->requiresMgmArguments({"--", "-h"}));
  }
}

TEST_F(ConsoleCommandHelpTest, CompletionFiltersCurrentPrefixesAndPendingValues)
{
  EnsureNativeCommandRegistryInitialized();
  EXPECT_EQ(eos_shell_completion_candidates({"fs"}, "l"),
            (std::vector<std::string>{"ls"}));
  const auto config = eos_shell_completion_candidates({"config"}, "s");
  EXPECT_EQ(std::find(config.begin(), config.end(), "--help-all"), config.end());
  EXPECT_EQ(eos_shell_completion_candidates({"fs"}, "--help-a"),
            (std::vector<std::string>{"--help-all"}));
  for (const auto* prefix : {"", "--hel"}) {
    EXPECT_TRUE(
        eos_shell_completion_candidates({"fs", "add", "--manual"}, prefix).empty());
    EXPECT_TRUE(
        eos_shell_completion_candidates({"config", "save", "--comment"}, prefix).empty());
  }
  EXPECT_TRUE(eos_shell_completion_candidates({"fs", "--"}, "--hel").empty());
  const auto* fs = CommandRegistry::instance().find("fs");
  ASSERT_NE(fs, nullptr);
  EXPECT_TRUE(fs->completion({"no-such-subcommand"}).candidates.empty());
}

TEST_F(ConsoleCommandHelpTest, EveryEnabledCommandHasValidAuthoredMarkers)
{
  EnsureNativeCommandRegistryInitialized();
  std::size_t enabled = 0;
  for (const auto* command : CommandRegistry::instance().all()) {
    if (!HasStructuredCommandHelp(*command)) {
      continue;
    }
    ++enabled;
    std::string error;
    EXPECT_TRUE(ValidateCommandHelp(*command, &error))
        << command->name() << ": " << error;
  }
  EXPECT_GT(enabled, 0u);
}

TEST_F(ConsoleCommandHelpTest, FilesystemHelpRetainsConfigurationAndScopesExamples)
{
  EnsureNativeCommandRegistryInitialized();
  const auto* fs = CommandRegistry::instance().find("fs");
  ASSERT_NE(fs, nullptr);
  ASSERT_TRUE(HasStructuredCommandHelp(*fs));
  EXPECT_FALSE(fs->requiresMgmArguments({"config", "--help-all"}));
  EXPECT_FALSE(fs->requiresMgmArguments({"add", "--manual", "1", "--help"}));
  EXPECT_TRUE(fs->requiresMgmArguments({"config", "--", "--help"}));
  testing::internal::CaptureStderr();
  fs->printHelp({"config"});
  const auto config = testing::internal::GetCapturedStderr();
  EXPECT_NE(config.find("configstatus"), std::string::npos);
  EXPECT_NE(config.find("altxs_sync_interval"), std::string::npos);
  EXPECT_EQ(config.find("--syncdisk"), std::string::npos);
  testing::internal::CaptureStderr();
  fs->printHelp({"boot"});
  const auto boot = testing::internal::GetCapturedStderr();
  EXPECT_NE(boot.find("# Send a boot request"), std::string::npos);
  EXPECT_NE(boot.find("eos fs boot *"), std::string::npos);
  EXPECT_EQ(boot.find("eos fs mv 100"), std::string::npos);
}

namespace {
class ConsoleCommandHelpRequestTest : public ConsoleTestFixture {
protected:
  void
  checkHelp(const std::string& command, const std::vector<std::string>& arguments)
  {
    SCOPED_TRACE(command + " " + ::testing::PrintToString(arguments));
    const bool wasJson = json, wasSilent = silent, wasTiming = timing;
    const int wasDone = done;
    const std::string oldPwd = gPwd.c_str(), oldUri = serveruri.c_str();
    const std::string oldUser = user_role.c_str(), oldGroup = group_role.c_str();
    const std::string oldApp = app.c_str(), oldComment = global_comment.c_str();
    testing::internal::CaptureStderr();
    testing::internal::CaptureStdout();
    run(command, arguments);
    const auto output = testing::internal::GetCapturedStdout();
    const auto error = testing::internal::GetCapturedStderr();
    EXPECT_EQ(global_retc, 0) << output << error;
    EXPECT_FALSE(output.empty() && error.empty());
    {
      std::lock_guard<std::mutex> lock(mutex);
      EXPECT_TRUE(requests.empty());
    }
    EXPECT_EQ(json, wasJson);
    EXPECT_EQ(silent, wasSilent);
    EXPECT_EQ(timing, wasTiming);
    EXPECT_EQ(done, wasDone);
    EXPECT_EQ(std::string(gPwd.c_str()), oldPwd);
    EXPECT_EQ(std::string(serveruri.c_str()), oldUri);
    EXPECT_EQ(std::string(user_role.c_str()), oldUser);
    EXPECT_EQ(std::string(group_role.c_str()), oldGroup);
    EXPECT_EQ(std::string(app.c_str()), oldApp);
    EXPECT_EQ(std::string(global_comment.c_str()), oldComment);
  }
};
} // namespace

TEST_F(ConsoleCommandHelpRequestTest,
       EveryEnabledRootAndTopicIsLocalAndPreservesShellState)
{
  for (const auto* command : CommandRegistry::instance().all()) {
    if (!HasStructuredCommandHelp(*command)) {
      continue;
    }
    const std::string name = command->name();
    std::string error;
    ASSERT_TRUE(ValidateCommandHelp(*command, &error)) << name << ": " << error;
    checkHelp(name, {"--help"});
    checkHelp(name, {"--help-all"});
    checkHelp("help", {name});
    for (auto path : StructuredCommandHelpPaths(*command)) {
      auto direct = path;
      direct.emplace_back("--help");
      EXPECT_FALSE(command->requiresMgmArguments(direct));
      checkHelp(name, direct);
      path.insert(path.begin(), name);
      checkHelp("help", path);
    }
  }
}
