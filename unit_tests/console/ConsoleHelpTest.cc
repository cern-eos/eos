// ----------------------------------------------------------------------
// File: ConsoleHelpTest.cc
// ----------------------------------------------------------------------

#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include "gtest/gtest.h"
#include <CLI/CLI.hpp>
#include <clocale>
#include <cstdlib>
#include <fcntl.h>
#include <locale.h>
#include <map>
#include <optional>
#include <regex>
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

std::string
plain(const std::string& text)
{
  static const std::regex escapes("\033\\[[0-?]*[ -/]*[@-~]|\033[()][0-2A-Z]");
  return std::regex_replace(text, escapes, "");
}

void
expectWidth(const std::string& text, size_t width)
{
  std::istringstream input(plain(text));
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
    for (const auto* name : {"NO_COLOR", "TERM"}) {
      const char* value = getenv(name);
      saved[name] = value ? std::optional<std::string>(value) : std::nullopt;
      unsetenv(name);
    }
    setenv("TERM", "xterm", 1);
    ConfigureColor(ColorMode::Never, false, false);
    ConfigureHelp(false, false);
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
    ConfigureHelp(false, false);
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
  EXPECT_EQ(HelpDisplayWidth("résumé"), 6u);
  EXPECT_EQ(HelpDisplayWidth("目录"), 4u);
  EXPECT_EQ(HelpDisplayWidth("e\xcc\x81"), 1u);
  EXPECT_EQ(HelpDisplayWidth("Ω目录 e\u0301"), 7u);
  EXPECT_EQ(HelpDisplayWidth(""), 0u);
  EXPECT_EQ(std::string(std::setlocale(LC_CTYPE, nullptr)), processLocale);
  EXPECT_EQ(uselocale(nullptr), threadLocale);
}

TEST_F(ConsoleHelpTest, WrapsParagraphsAtActualDisplayWidth)
{
  const std::string paragraph =
      "Inspect 目录 entries and résumé metadata without splitting e\u0301 accents. "
      "Descriptions wrap at words and use the same indentation on each line.";
  HelpPage page{{{HelpBlockKind::Paragraph, paragraph, {}}}};
  for (size_t width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto text = page.render(width);
    expectWidth(text, width);
    EXPECT_EQ(words(text), words(paragraph));
    EXPECT_EQ(text.substr(0, 2), "  ");
    EXPECT_NE(text.find("目录"), std::string::npos);
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
    ConfigureHelp(modes.first, modes.second);
    EXPECT_EQ(HelpWidth(terminal), 0u);
  }
}

TEST_F(ConsoleHelpTest, JsonToggleRestoresTerminalLayoutWithoutOverridingBatch)
{
  resize(40);
  ConfigureHelp(false, false);
  EXPECT_EQ(HelpWidth(terminal), 40u);
  SetJsonHelpOutput(true);
  EXPECT_EQ(HelpWidth(terminal), 0u);
  SetJsonHelpOutput(false);
  EXPECT_EQ(HelpWidth(terminal), 40u);
  ConfigureHelp(true, false);
  SetJsonHelpOutput(true);
  SetJsonHelpOutput(false);
  EXPECT_EQ(HelpWidth(terminal), 0u);
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

TEST_F(ConsoleHelpTest, TypedCommandDefinitionsDoNotDependOnTheLegacyVocabulary)
{
  struct RestoreCommands {
    std::vector<HelpCommand> commands = SetHelpCommands({{"clear", {}}, {"stat", {}}});
    ~RestoreCommands() { SetHelpCommands(std::move(commands)); }
  } restore;
  HelpPage page{{{HelpBlockKind::Section, "OPERATIONS", {}},
                 {HelpBlockKind::Definition, HelpCommandText("stat"),
                  "stat and clear remain ordinary prose here."},
                 {HelpBlockKind::Definition, HelpCommandText("unregistered-operation"),
                  "This command is absent from the global command registry."},
                 {HelpBlockKind::Definition, HelpCommandText("clear"),
                  "Clear the selected setting."}}};
  ConfigureColor(ColorMode::Always, false, false);
  for (const size_t width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto uncolored = page.render(width);
    const auto colored = page.render(width, terminal);
    EXPECT_EQ(plain(colored), uncolored);
    for (const auto* command : {"stat", "unregistered-operation", "clear"}) {
      EXPECT_NE(colored.find("  " + Colorize(command, TextStyle::Command, terminal)),
                std::string::npos)
          << command;
    }
    EXPECT_EQ(colored.find(Colorize("stat", TextStyle::Command, terminal) + " and"),
              std::string::npos);
    EXPECT_EQ(colored.find(Colorize("clear", TextStyle::Command, terminal) + " remain"),
              std::string::npos);
  }
}

TEST_F(ConsoleHelpTest, TypedSpansSurviveUnicodeWrappingAndDefinitionContinuations)
{
  HelpText description{"Inspect "};
  description.append("目录", TextStyle::Path);
  description.append(" and ");
  description.append("résumé", TextStyle::Identifier);
  description.append(" metadata, preserving ");
  description.append("e\xcc\x81", TextStyle::Identifier);
  description.append(" accents and the selected identifier ");
  description.append("disable_access_log", TextStyle::Identifier);
  description.append(" across every continuation line.");
  HelpText label = StyledHelpText("inspect", TextStyle::Command);
  label.append(" ");
  label.append(HelpSyntax("<directory> [--details]"));
  HelpPage page{{{HelpBlockKind::Section, "OPTIONS", {}},
                 {HelpBlockKind::Definition, label, description}}};
  ConfigureColor(ColorMode::Always, false, false);
  for (const size_t width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto baseline = page.render(width);
    const auto colored = page.render(width, terminal);
    EXPECT_EQ(plain(colored), baseline);
    if (width >= 40) {
      expectWidth(colored, width);
    }
    for (const auto& [token, style] : std::vector<std::pair<std::string, TextStyle>>{
             {"inspect", TextStyle::Command},
             {"<directory>", TextStyle::Argument},
             {"--details", TextStyle::Option},
             {"目录", TextStyle::Path},
             {"résumé", TextStyle::Identifier},
             {"e\xcc\x81", TextStyle::Identifier},
             {"disable_access_log", TextStyle::Identifier}}) {
      EXPECT_NE(colored.find(Colorize(token, style, terminal)), std::string::npos)
          << token;
    }
    EXPECT_EQ(colored.find(Colorize("metadata", TextStyle::Identifier, terminal)),
              std::string::npos);
  }
}

TEST_F(ConsoleHelpTest, SyntaxSeparatesPlaceholdersLiteralChoicesAndProse)
{
  HelpText usage = HelpCommandText("eos ns behaviour set");
  usage.append(" ");
  usage.append(HelpSyntax("<name> <on|off> [all]", {"on", "off", "all"}));
  HelpPage page{{{HelpBlockKind::Section, "USAGE", {}},
                 {HelpBlockKind::Usage, usage, {}},
                 {HelpBlockKind::Paragraph,
                  "The words on, off and all in this sentence stay ordinary prose.",
                  {}},
                 {HelpBlockKind::Definition, HelpSyntax("<name>|all", {"all"}),
                  "Select one setting or every setting."}}};
  ConfigureColor(ColorMode::Always, false, false);
  for (const size_t width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto colored = page.render(width, terminal);
    EXPECT_EQ(plain(colored), page.render(width));
    for (const auto* token : {"on", "off", "all"}) {
      EXPECT_NE(colored.find(Colorize(token, TextStyle::Literal, terminal)),
                std::string::npos)
          << token;
      EXPECT_EQ(colored.find(Colorize(token, TextStyle::Error, terminal)),
                std::string::npos);
    }
    EXPECT_NE(colored.find(Colorize("<name>", TextStyle::Argument, terminal)),
              std::string::npos);
    EXPECT_EQ(colored.find(Colorize("on,", TextStyle::Literal, terminal)),
              std::string::npos);
    EXPECT_EQ(colored.find(Colorize("off and", TextStyle::Literal, terminal)),
              std::string::npos);
  }
}

TEST_F(ConsoleHelpTest, MarkedReferencesKeepTheirRoleWhenAppendedToPlainProse)
{
  const std::string text = "Use disable_access_log for access-check logging.";
  HelpText note{text};
  note.mark(text.find("disable_access_log"), std::string("disable_access_log").size(),
            TextStyle::Identifier);
  HelpText paragraph{"Configuration: "};
  paragraph.append(note);
  paragraph.append(" See ");
  paragraph.append(StyledHelpText("https://example.invalid/eos", TextStyle::Path));
  paragraph.append(" for details.");
  HelpPage page{{{HelpBlockKind::Paragraph, paragraph, {}}}};
  ConfigureColor(ColorMode::Always, false, false);
  for (const size_t width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto colored = page.render(width, terminal);
    EXPECT_EQ(plain(colored), page.render(width));
    EXPECT_NE(
        colored.find(Colorize("disable_access_log", TextStyle::Identifier, terminal)),
        std::string::npos);
    EXPECT_NE(
        colored.find(Colorize("https://example.invalid/eos", TextStyle::Path, terminal)),
        std::string::npos);
  }
}

TEST_F(ConsoleHelpTest, MultiwordRolesResumeAfterEveryWrappedLine)
{
  const std::string marked = "alpha résumé 目录 metadata omega";
  HelpPage page{
      {{HelpBlockKind::Paragraph, StyledHelpText(marked, TextStyle::Identifier), {}}}};
  ConfigureColor(ColorMode::Always, false, false);
  const auto probe = Colorize("probe", TextStyle::Identifier, terminal);
  const auto start = probe.substr(0, probe.find("probe"));
  const auto reset = probe.substr(probe.find("probe") + 5);
  ASSERT_FALSE(start.empty());
  ASSERT_FALSE(reset.empty());
  for (const size_t width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    const auto colored = page.render(width, terminal);
    EXPECT_EQ(plain(colored), page.render(width));
    expectWidth(colored, width);
    for (const auto* word : {"alpha", "résumé", "目录", "metadata", "omega"}) {
      SCOPED_TRACE(word);
      const auto position = colored.find(word);
      ASSERT_NE(position, std::string::npos);
      const auto active = colored.rfind(start, position);
      ASSERT_NE(active, std::string::npos);
      EXPECT_EQ(colored.find(reset, active), colored.find(reset, position));
    }
  }
}

TEST_F(ConsoleHelpTest, SemanticRenderingHonorsPlainDestinationAndMachinePolicies)
{
  HelpPage page{{{HelpBlockKind::Section, "OPERATIONS", {}},
                 {HelpBlockKind::Definition, HelpCommandText("clear"),
                  StyledHelpText("disable_access_log", TextStyle::Identifier)}}};
  const auto baseline = page.render(40);
  ConfigureColor(ColorMode::Auto, false, false);
  EXPECT_EQ(page.render(40, fileno(redirected)), baseline);
  EXPECT_NE(page.render(40, terminal).find('\033'), std::string::npos);
  setenv("NO_COLOR", "1", 1);
  EXPECT_EQ(page.render(40, terminal), baseline);
  unsetenv("NO_COLOR");
  ConfigureColor(ColorMode::Never, false, false);
  EXPECT_EQ(page.render(40, terminal), baseline);
  for (const auto& [batch, json] :
       {std::pair{true, false}, std::pair{false, true}, std::pair{true, true}}) {
    ConfigureColor(ColorMode::Always, batch, json);
    EXPECT_EQ(page.render(40, terminal), baseline);
  }
  ConfigureColor(ColorMode::Always, false, false);
  EXPECT_EQ(page.render(40), baseline);
  EXPECT_EQ(plain(page.render(40, fileno(redirected))), baseline);
}

TEST_F(ConsoleHelpTest, Cli11SemanticLabelsUseEachAppsMetadataAtEveryDepth)
{
  CLI::App app{"stat and clear are ordinary words in this description.", "unregistered"};
  auto* stat = app.add_subcommand("stat", "Inspect namespace statistics.");
  stat->alias("s");
  stat->required();
  auto* nested = stat->add_subcommand("unregistered-child", "Inspect nested state.");
  nested->alias("u");
  nested->add_subcommand("clear", "Remove selected settings.");
  std::string value;
  nested->add_option("--name", value, "The required setting name.")->required();
  ConfigureColor(ColorMode::Always, false, false);
  for (const auto mode : {CLI::AppFormatMode::Normal, CLI::AppFormatMode::All}) {
    const auto page = BuildCliHelpPage(app, mode);
    ASSERT_TRUE(page);
    for (const size_t width : {20, 40, 60, 80, 120}) {
      SCOPED_TRACE(width);
      SCOPED_TRACE(static_cast<int>(mode));
      const auto colored = page->render(width, terminal);
      EXPECT_EQ(plain(colored), page->render(width));
      for (const auto* token : {"stat", "s"}) {
        EXPECT_NE(colored.find(Colorize(token, TextStyle::Command, terminal)),
                  std::string::npos)
            << token;
      }
      EXPECT_EQ(colored.find(Colorize("REQUIRED", TextStyle::Command, terminal)),
                std::string::npos);
      if (mode == CLI::AppFormatMode::All) {
        for (const auto* token : {"unregistered-child", "u", "clear"}) {
          EXPECT_NE(colored.find(Colorize(token, TextStyle::Command, terminal)),
                    std::string::npos)
              << token;
        }
        EXPECT_NE(colored.find(Colorize("--name", TextStyle::Option, terminal)),
                  std::string::npos);
      }
      EXPECT_EQ(colored.find(Colorize("stat", TextStyle::Command, terminal) + " and"),
                std::string::npos);
    }
  }
}

TEST_F(ConsoleHelpTest, CustomCli11FormatterDoesNotClaimSemanticMetadata)
{
  CLI::App app{"", "custom"};
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [](const CLI::App*, std::string, CLI::AppFormatMode) { return "CUSTOM HELP\n"; }));
  EXPECT_FALSE(BuildCliHelpPage(app, CLI::AppFormatMode::Normal));
  EXPECT_FALSE(BuildCliHelpPage(app, CLI::AppFormatMode::All));
}

TEST_F(ConsoleHelpTest, Cli11PositionalNamesDoNotOverwriteMatchingCommandNames)
{
  ConfigureColor(ColorMode::Always, false, false);
  for (const bool nested : {false, true}) {
    SCOPED_TRACE(nested);
    CLI::App app{"Inspect a file.", nested ? "eos" : "file"};
    auto* selected = nested ? app.add_subcommand("file", "Inspect a file.") : &app;
    std::string file;
    selected->add_option("file", file, "The selected file.")->required();
    selected->add_subcommand("inspect", "Inspect metadata.");
    selected->require_subcommand(0, nested ? 1 : 0);
    if (nested) {
      EXPECT_THROW(app.parse("file --help"), CLI::CallForHelp);
    }
    const auto page = BuildCliHelpPage(app, CLI::AppFormatMode::Normal);
    ASSERT_TRUE(page);
    for (const size_t width : {20, 40, 80, 120}) {
      SCOPED_TRACE(width);
      const auto colored = page->render(width, terminal);
      EXPECT_EQ(plain(colored), page->render(width));
      const auto heading = colored.find(Colorize("Usage:", TextStyle::Heading, terminal));
      ASSERT_NE(heading, std::string::npos);
      const auto start = colored.find('\n', heading) + 1;
      const auto synopsis = colored.substr(start, colored.find("\n\n", start) - start);
      EXPECT_NE(synopsis.find(Colorize("file", TextStyle::Command, terminal)),
                std::string::npos);
      EXPECT_NE(synopsis.find(Colorize("file", TextStyle::Argument, terminal)),
                std::string::npos);
      EXPECT_NE(synopsis.find(Colorize(nested ? "SUBCOMMAND" : "SUBCOMMANDS",
                                       TextStyle::Argument, terminal)),
                std::string::npos);
    }
  }
}

TEST_F(ConsoleHelpTest, Cli11NumericFlagMetadataIsDistinctFromNegativeValues)
{
  CLI::App app{"Choose a report limit.", "report"};
  app.allow_non_standard_option_names();
  for (const auto* name : {"-100", "-1000", "-10000"}) {
    app.add_flag(name, "Select a declared report limit.");
  }
  int limit = -1;
  app.add_option("--limit", limit, "Maximum entries.")->default_val(-1);
  ConfigureColor(ColorMode::Always, false, false);
  const auto page = BuildCliHelpPage(app, CLI::AppFormatMode::Normal);
  ASSERT_TRUE(page);
  for (const size_t width : {20, 40, 80, 120}) {
    SCOPED_TRACE(width);
    const auto colored = page->render(width, terminal);
    EXPECT_EQ(plain(colored), page->render(width));
    for (const auto* name : {"-100", "-1000", "-10000"}) {
      EXPECT_NE(colored.find(Colorize(name, TextStyle::Option, terminal)),
                std::string::npos)
          << name;
    }
    EXPECT_NE(colored.find(Colorize("-1", TextStyle::Literal, terminal)),
              std::string::npos);
    EXPECT_EQ(colored.find(Colorize("-1", TextStyle::Option, terminal)),
              std::string::npos);
  }
  const auto negative = HelpSyntax("--limit -1").render(terminal);
  EXPECT_NE(negative.find(Colorize("-1", TextStyle::Literal, terminal)),
            std::string::npos);
  EXPECT_EQ(negative.find(Colorize("-1", TextStyle::Option, terminal)),
            std::string::npos);
}
