// ----------------------------------------------------------------------
// File: ConsoleHelpTest.cc
// ----------------------------------------------------------------------

#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include "console/ConsoleOptions.hh"
#include "gtest/gtest.h"
#include <CLI/CLI.hpp>
#include <clocale>
#include <cstdlib>
#include <fcntl.h>
#include <locale.h>
#include <map>
#include <optional>
#include <sstream>
#include <sys/ioctl.h>
#include <unistd.h>

using namespace eos::console;

namespace {
std::string
words(const std::string& text)
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

void
expectWidth(const std::string& text, size_t width)
{
  std::istringstream input(text);
  std::string line;
  while (std::getline(input, line)) {
    EXPECT_LE(HelpDisplayWidth(line), width) << line;
  }
}

class ConsoleHelpTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    for (const auto* name :
         {"EOS_HELP_WIDTH", "EOS_PAGER", "PAGER", "NO_COLOR", "TERM"}) {
      const char* value = getenv(name);
      saved[name] = value ? std::optional<std::string>(value) : std::nullopt;
      unsetenv(name);
    }
    setenv("TERM", "xterm", 1);
    ConfigureColor(ColorMode::Never, false, false);
    ConfigureHelp(false, false, true);
    master = posix_openpt(O_RDWR | O_NOCTTY);
    ASSERT_GE(master, 0);
    ASSERT_EQ(grantpt(master), 0);
    ASSERT_EQ(unlockpt(master), 0);
    const char* path = ptsname(master);
    ASSERT_NE(path, nullptr);
    terminal = open(path, O_RDWR | O_NOCTTY);
    ASSERT_GE(terminal, 0);
    redirected = tmpfile();
    ASSERT_NE(redirected, nullptr);
  }

  void
  TearDown() override
  {
    ConfigureHelp(false, false, true);
    ConfigureColor(ColorMode::Auto, false, false);
    for (const auto& [name, value] : saved) {
      if (value) {
        setenv(name.c_str(), value->c_str(), 1);
      } else {
        unsetenv(name.c_str());
      }
    }
    if (terminal >= 0) {
      close(terminal);
    }
    if (master >= 0) {
      close(master);
    }
    if (redirected) {
      fclose(redirected);
    }
  }

  void
  resize(unsigned short width)
  {
    winsize size{};
    size.ws_col = width;
    size.ws_row = 24;
    ASSERT_EQ(ioctl(terminal, TIOCSWINSZ, &size), 0);
  }

  int master = -1, terminal = -1;
  FILE* redirected = nullptr;
  std::map<std::string, std::optional<std::string>> saved;
};
} // namespace

TEST_F(ConsoleHelpTest, MeasuresUnicodeDisplayCellsWithoutChangingLocale)
{
  const std::string processLocale = std::setlocale(LC_CTYPE, nullptr);
  const auto threadLocale = uselocale(nullptr);
  EXPECT_EQ(HelpDisplayWidth("ASCII"), 5u);
  EXPECT_EQ(HelpDisplayWidth(u8"résumé"), 6u);
  EXPECT_EQ(HelpDisplayWidth(u8"目录"), 4u);
  EXPECT_EQ(HelpDisplayWidth("e\xcc\x81"), 1u);
  EXPECT_EQ(HelpDisplayWidth(u8"Ω目录 e\u0301"), 7u);
  EXPECT_EQ(HelpDisplayWidth(""), 0u);
  EXPECT_EQ(std::string(std::setlocale(LC_CTYPE, nullptr)), processLocale);
  EXPECT_EQ(uselocale(nullptr), threadLocale);
}

TEST_F(ConsoleHelpTest, WrapsParagraphsAtActualDisplayWidth)
{
  const std::string paragraph =
      u8"Inspect 目录 entries and résumé metadata without splitting e\u0301 accents. "
      "Descriptions wrap at words and use the same indentation on each line.";
  HelpPage page{{{HelpBlockKind::Paragraph, paragraph, {}}}};
  for (size_t width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto text = page.render(width);
    expectWidth(text, width);
    EXPECT_EQ(words(text), words(paragraph));
    EXPECT_EQ(text.substr(0, 2), "  ");
    EXPECT_NE(text.find(u8"目录"), std::string::npos);
    EXPECT_NE(text.find("e\xcc\x81"), std::string::npos);
  }
}

TEST_F(ConsoleHelpTest, UsesHangingIndentForWrappedUsage)
{
  const std::string usage =
      "eos demo operation <directory> [--recursive] [--owner <user>] "
      "[--group <group>] [--monitoring]";
  HelpPage page{{{HelpBlockKind::Usage, usage, {}}}};
  for (size_t width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto text = page.render(width);
    expectWidth(text, width);
    EXPECT_EQ(words(text), usage);
    const auto newline = text.find('\n');
    if (newline + 1 < text.size()) {
      EXPECT_EQ(text.substr(newline + 1, 4), "    ");
    }
  }
}

TEST_F(ConsoleHelpTest, StacksDefinitionsAtNarrowWidthsAndPreservesAllWords)
{
  const std::string description =
      "Choose the maximum number of entries kept in the directory cache. "
      "The active setting can be inspected with the statistics command.";
  HelpPage page{{{HelpBlockKind::Definition, "--maximum-entries <count>", description},
                 {HelpBlockKind::Definition, "-f", "Select the file cache."}}};
  for (size_t width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto text = page.render(width);
    expectWidth(text, width);
    EXPECT_EQ(words(text),
              "--maximum-entries <count> " + description + " -f Select the file cache.");
    EXPECT_EQ(text.find("\n\n"), std::string::npos);
    if (width == 40) {
      EXPECT_NE(text.find("--maximum-entries <count>\n    Choose"), std::string::npos);
      EXPECT_NE(text.find("  -f\n    Select"), std::string::npos);
    }
  }
}

TEST_F(ConsoleHelpTest, DoesNotBreakLongArgumentsUrlsOrCopyableExamples)
{
  const std::string url = "https://example.invalid/a/long/path/with/no/word/boundaries";
  const std::string example = "eos demo --endpoint '" + url + "' --name 'two words'";
  HelpPage page{{{HelpBlockKind::Paragraph, "See " + url + " for details.", {}},
                 {HelpBlockKind::Example, example, {}}}};
  const auto text = page.render(40);
  EXPECT_NE(text.find(url), std::string::npos);
  EXPECT_NE(text.find("  " + example + "\n"), std::string::npos);
  EXPECT_EQ(words(text), "See " + url + " for details. " + example);
}

TEST_F(ConsoleHelpTest, KeepsMultilineExamplesAndParagraphBoundaries)
{
  HelpPage page{{{HelpBlockKind::Section, "USAGE", {}},
                 {HelpBlockKind::Usage, "eos demo [options]", {}},
                 {HelpBlockKind::Section, "DETAILS", {}},
                 {HelpBlockKind::Paragraph, "First paragraph.\n\nSecond paragraph.", {}},
                 {HelpBlockKind::Section, "EXAMPLES", {}},
                 {HelpBlockKind::Example, "eos demo one\neos demo two", {}}}};
  const auto text = page.render(40);
  EXPECT_EQ(text.substr(0, 6), "USAGE\n");
  EXPECT_NE(text.find("\n\nDETAILS\n"), std::string::npos);
  EXPECT_NE(text.find("First paragraph.\n\n  Second paragraph."), std::string::npos);
  EXPECT_NE(text.find("  eos demo one\n  eos demo two\n"), std::string::npos);
  EXPECT_EQ(text.find("\n\n\n"), std::string::npos);
  ASSERT_FALSE(text.empty());
  EXPECT_EQ(text.back(), '\n');
  EXPECT_NE(text[text.size() - 2], '\n');
  EXPECT_TRUE(HelpPage{}.render(40).empty());
}

TEST_F(ConsoleHelpTest, DefaultStructuredWidthIsEighty)
{
  HelpPage page{{{HelpBlockKind::Paragraph,
                  "A paragraph that is deliberately long enough to wrap across lines "
                  "at the default width and preserves every input word.",
                  {}}}};
  EXPECT_EQ(page.render(0), page.render(80));
}

TEST_F(ConsoleHelpTest, TerminalSizeUsesDestinationDescriptorAndCapsWideTerminals)
{
  for (unsigned short width : {40, 60, 80, 120}) {
    resize(width);
    EXPECT_EQ(HelpWidth(terminal), width);
    EXPECT_EQ(HelpWidth(fileno(redirected)), 0u);
  }
  resize(240);
  EXPECT_EQ(HelpWidth(terminal), 120u);
  resize(0);
  EXPECT_EQ(HelpWidth(terminal), 80u);
}

TEST_F(ConsoleHelpTest, WidthOverrideIsValidatedAndDoesNotChangeRedirectedOutput)
{
  resize(60);
  for (const char* width : {"20", "40", "120", "200", "1000"}) {
    setenv("EOS_HELP_WIDTH", width, 1);
    EXPECT_EQ(HelpWidth(terminal), std::stoul(width));
    EXPECT_EQ(HelpWidth(fileno(redirected)), 0u);
  }
  for (const char* width :
       {"", "0", "19", "1001", "-1", "40x", " 40", "99999999999999999999"}) {
    setenv("EOS_HELP_WIDTH", width, 1);
    EXPECT_EQ(HelpWidth(terminal), 60u) << width;
  }
}

TEST_F(ConsoleHelpTest, LayoutDoesNotDependOnColorAndIsDisabledForMachineModes)
{
  resize(40);
  setenv("NO_COLOR", "1", 1);
  for (auto mode : {ColorMode::Auto, ColorMode::Always, ColorMode::Never}) {
    ConfigureColor(mode, false, false);
    EXPECT_EQ(HelpWidth(terminal), 40u);
  }
  for (const auto& modes :
       {std::pair<bool, bool>{true, false}, {false, true}, {true, true}}) {
    ConfigureHelp(modes.first, modes.second, true);
    EXPECT_EQ(HelpWidth(terminal), 0u);
  }
}

TEST_F(ConsoleHelpTest, JsonToggleRestoresTerminalLayoutWithoutOverridingBatch)
{
  resize(40);
  ConfigureHelp(false, false, true);
  EXPECT_EQ(HelpWidth(terminal), 40u);
  SetJsonHelpOutput(true);
  EXPECT_EQ(HelpWidth(terminal), 0u);
  SetJsonHelpOutput(false);
  EXPECT_EQ(HelpWidth(terminal), 40u);
  ConfigureHelp(true, false, true);
  SetJsonHelpOutput(true);
  SetJsonHelpOutput(false);
  EXPECT_EQ(HelpWidth(terminal), 0u);
}

TEST_F(ConsoleHelpTest, NoPagerIsAGlobalFlagAndPreservesCommandArguments)
{
  ConsoleOptions options;
  std::string error;
  ASSERT_TRUE(ParseConsoleOptions({"--no-pager", "--color=never", "ns", "--help"},
                                  options, error))
      << error;
  EXPECT_TRUE(options.noPager);
  EXPECT_EQ(options.command, (std::vector<std::string>{"ns", "--help"}));
  ASSERT_TRUE(ParseConsoleOptions({"ns", "--no-pager"}, options, error)) << error;
  EXPECT_FALSE(options.noPager);
  EXPECT_EQ(options.command, (std::vector<std::string>{"ns", "--no-pager"}));
}

TEST_F(ConsoleHelpTest, CliHelpWrapsWithoutMutatingSharedFormatterOrApplication)
{
  CLI::App app{"Inspect namespace metadata and display useful descriptive information "
               "for administrators and regular users.",
               "demo"};
  bool recursive = false;
  bool callback = false;
  app.callback([&] { callback = true; });
  app.add_flag(
      "-r,--recursive", recursive,
      "Visit all nested directories and include information about their entries.");
  auto* child =
      app.add_subcommand("inspect", "Inspect one selected entry and its metadata.");
  child->add_flag("--details", "Include every available property of the selected entry.");
  app.footer("Additional examples are available in the administrator documentation.");
  const auto formatter = app.get_formatter();
  const auto column = formatter->get_column_width();
  const auto right = formatter->get_right_column_width();
  const auto description = formatter->get_description_paragraph_width();
  const auto baseline = app.help("", CLI::AppFormatMode::All);
  EXPECT_EQ(LayoutCliHelp(app, CLI::AppFormatMode::All, 0), baseline);
  for (size_t width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto text = LayoutCliHelp(app, CLI::AppFormatMode::All, width);
    expectWidth(text, width);
    EXPECT_NE(text.find("--recursive"), std::string::npos);
    EXPECT_NE(text.find("--details"), std::string::npos);
    EXPECT_NE(
        words(text).find(
            "Visit all nested directories and include information about their entries."),
        std::string::npos);
    EXPECT_EQ(app.get_formatter(), formatter);
    EXPECT_EQ(formatter->get_column_width(), column);
    EXPECT_EQ(formatter->get_right_column_width(), right);
    EXPECT_EQ(formatter->get_description_paragraph_width(), description);
    EXPECT_EQ(app.help("", CLI::AppFormatMode::All), baseline);
    EXPECT_FALSE(recursive);
    EXPECT_FALSE(callback);
  }
}

TEST_F(ConsoleHelpTest, CustomCliFormatterRemainsVerbatimAtEveryWidth)
{
  CLI::App app{"Custom help", "demo"};
  const std::string custom = "CUSTOM\n  This deliberately long custom line retains its "
                             "established layout and whitespace.\n\n";
  const auto formatter = std::make_shared<CLI::FormatterLambda>(
      [custom](const CLI::App*, std::string, CLI::AppFormatMode) { return custom; });
  app.formatter(formatter);
  for (size_t width : {0, 40, 60, 80, 120}) {
    EXPECT_EQ(LayoutCliHelp(app, CLI::AppFormatMode::All, width), custom);
    EXPECT_EQ(app.get_formatter(), formatter);
  }
}
