// ----------------------------------------------------------------------
// File: ConsoleColorTest.cc
// ----------------------------------------------------------------------

#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include "console/ConsoleOptions.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include "gtest/gtest.h"
#include <algorithm>
#include <cstdlib>
#include <fcntl.h>
#include <optional>
#include <regex>
#include <unistd.h>
#include <utility>

using namespace eos::console;

namespace {
class ConsoleColorTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    save("TERM", term);
    save("NO_COLOR", noColor);
    setenv("TERM", "xterm", 1);
    unsetenv("NO_COLOR");
    ConfigureColor(ColorMode::Auto, false, false);
    commands = SetHelpCommands({{"demo", {{"nested", {{"leaf", {}}}}}},
                                {"ns",
                                 {{"stat", {}},
                                  {"master", {}},
                                  {"recompute_tree_size", {}},
                                  {"update_quotanode", {}},
                                  {"cache", {{"set", {}}, {"drop", {}}}}}},
                                {"vid", {{"set", {{"geotag", {}}}}}},
                                {"attr", {{"get", {}}, {"set", {}}}},
                                {"sched", {{"configure", {{"weight", {}}}}}}});
    master = posix_openpt(O_RDWR | O_NOCTTY);
    ASSERT_GE(master, 0);
    ASSERT_EQ(grantpt(master), 0);
    ASSERT_EQ(unlockpt(master), 0);
    const char* name = ptsname(master);
    ASSERT_NE(name, nullptr);
    terminal = open(name, O_RDWR | O_NOCTTY);
    ASSERT_GE(terminal, 0);
    file = tmpfile();
    ASSERT_NE(file, nullptr);
  }

  void
  TearDown() override
  {
    ConfigureColor(ColorMode::Auto, false, false);
    SetHelpCommands(std::move(commands));
    restore("TERM", term);
    restore("NO_COLOR", noColor);
    if (terminal >= 0) {
      close(terminal);
    }
    if (master >= 0) {
      close(master);
    }
    if (file) {
      fclose(file);
    }
  }

  static void
  save(const char* key, std::optional<std::string>& value)
  {
    if (const char* current = getenv(key)) {
      value = current;
    }
  }

  static void
  restore(const char* key, const std::optional<std::string>& value)
  {
    if (value) {
      setenv(key, value->c_str(), 1);
    } else {
      unsetenv(key);
    }
  }

  static std::string
  plain(const std::string& value)
  {
    static const std::regex escapes("\033\\[[0-?]*[ -/]*[@-~]|\033[()][0-2A-Z]");
    return std::regex_replace(value, escapes, "");
  }

  std::string
  help(CLI::App& app, CLI::AppFormatMode mode = CLI::AppFormatMode::All)
  {
    rewind(file);
    EXPECT_EQ(ftruncate(fileno(file), 0), 0);
    PrintCliHelp(app, mode, file);
    fflush(file);
    rewind(file);
    std::string result;
    char buffer[1024];
    size_t count;
    while ((count = fread(buffer, 1, sizeof(buffer), file))) {
      result.append(buffer, count);
    }
    return result;
  }

  std::string
  legacyHelp(std::string_view text)
  {
    rewind(file);
    EXPECT_EQ(ftruncate(fileno(file), 0), 0);
    PrintLegacyHelp(text, file);
    fflush(file);
    rewind(file);
    std::string result;
    char buffer[1024];
    size_t count;
    while ((count = fread(buffer, 1, sizeof(buffer), file))) {
      result.append(buffer, count);
    }
    return result;
  }

  int master = -1;
  int terminal = -1;
  std::FILE* file = nullptr;
  std::optional<std::string> term, noColor;
  std::vector<HelpCommand> commands;
};
} // namespace

TEST_F(ConsoleColorTest, AutomaticColorDependsOnTheDestination)
{
  EXPECT_TRUE(ColorEnabled(terminal));
  EXPECT_FALSE(ColorEnabled(fileno(file)));
  EXPECT_EQ(Colorize("error: failed", TextStyle::Error, fileno(file)), "error: failed");
  const auto colored = Colorize("error: failed", TextStyle::Error, terminal);
  EXPECT_NE(colored.find('\033'), std::string::npos);
  EXPECT_EQ(plain(colored), "error: failed");
  EXPECT_EQ(colored.find('\001'), std::string::npos);
  EXPECT_EQ(colored.find('\002'), std::string::npos);
}

TEST_F(ConsoleColorTest, MixedStreamsKeepLegacyServerRepliesPlain)
{
  class Probe : public ICmdHelper {
  public:
    explicit Probe(const GlobalOptions& options)
        : ICmdHelper(options)
    {
    }
    bool
    ParseCommand(const char*) override
    {
      return false;
    }
    bool
    plainReply() const
    {
      return mReq.dontcolor();
    }
  };
  const int savedOutput = dup(STDOUT_FILENO);
  ASSERT_GE(savedOutput, 0);
  const int savedError = dup(STDERR_FILENO);
  if (savedError < 0) {
    close(savedOutput);
    FAIL() << "Cannot save stderr";
  }
  int outputStatus = -1, errorStatus = -1;
  bool plainReply = false;
  {
    struct RestoreStreams {
      int output, error;
      ~RestoreStreams()
      {
        dup2(output, STDOUT_FILENO);
        dup2(error, STDERR_FILENO);
        close(output);
        close(error);
      }
    } restore{savedOutput, savedError};
    outputStatus = dup2(terminal, STDOUT_FILENO);
    errorStatus = dup2(fileno(file), STDERR_FILENO);
    if (outputStatus >= 0 && errorStatus >= 0) {
      GlobalOptions options;
      plainReply = Probe(options).plainReply();
    }
  }
  ASSERT_GE(outputStatus, 0);
  ASSERT_GE(errorStatus, 0);
  EXPECT_TRUE(ColorEnabled(terminal));
  EXPECT_TRUE(plainReply);
}

TEST_F(ConsoleColorTest, NoColorRequiresANonemptyValueIncludingZero)
{
  setenv("NO_COLOR", "", 1);
  EXPECT_TRUE(ColorEnabled(terminal));
  for (const char* value : {"0", "1", "false"}) {
    setenv("NO_COLOR", value, 1);
    EXPECT_FALSE(ColorEnabled(terminal)) << value;
  }
}

TEST_F(ConsoleColorTest, MissingDumbAndUnknownTerminalsArePlain)
{
  for (const char* value :
       {"", "dumb", "vt100", "xterm-direct", "eos-nonexistent-terminal-test"}) {
    setenv("TERM", value, 1);
    EXPECT_FALSE(ColorEnabled(terminal)) << value;
  }
  unsetenv("TERM");
  EXPECT_FALSE(ColorEnabled(terminal));
}

TEST_F(ConsoleColorTest, ExplicitAlwaysOverridesEnvironmentAndRedirection)
{
  ConfigureColor(ColorMode::Always, false, false);
  setenv("TERM", "dumb", 1);
  setenv("NO_COLOR", "1", 1);
  EXPECT_TRUE(ColorEnabled(fileno(file)));
  const auto output = Colorize("warning", TextStyle::Warning, fileno(file));
  EXPECT_EQ(output, "\033[1;33mwarning\033[0m");
  EXPECT_EQ(Colorize("", TextStyle::Error, fileno(file)), "");
}

TEST_F(ConsoleColorTest, NeverSuppressesAllStyling)
{
  ConfigureColor(ColorMode::Never, false, false);
  EXPECT_FALSE(ColorEnabled(terminal));
  EXPECT_EQ(Colorize("heading", TextStyle::Heading, terminal), "heading");
  EXPECT_EQ(ColorPrompt("EOS Console", TextStyle::Heading, terminal), "EOS Console");
}

TEST_F(ConsoleColorTest, BatchAndJsonOverrideForcedColor)
{
  ConfigureColor(ColorMode::Always, true, false);
  EXPECT_FALSE(ColorEnabled(terminal));
  ConfigureColor(ColorMode::Always, false, true);
  EXPECT_FALSE(ColorEnabled(terminal));
  SetJsonColorOutput(false);
  EXPECT_TRUE(ColorEnabled(terminal));
  ConfigureColor(ColorMode::Always, true, true);
  SetJsonColorOutput(false);
  EXPECT_FALSE(ColorEnabled(terminal));
}

TEST_F(ConsoleColorTest, MachineAndRawBytesArePreservedEvenWhenForced)
{
  ConfigureColor(ColorMode::Always, false, false);
  std::string bytes = "[failed]=ok";
  bytes.push_back('\0');
  bytes += "\033[31mraw\n";
  for (auto kind : {OutputKind::Machine, OutputKind::Raw}) {
    EXPECT_FALSE(ColorEnabled(terminal, kind));
    EXPECT_EQ(Colorize(bytes, TextStyle::Error, terminal, kind), bytes);
  }
}

TEST_F(ConsoleColorTest, ReadlineMarkersAppearOnlyInPrompts)
{
  ConfigureColor(ColorMode::Always, false, false);
  const auto prompt = ColorPrompt("EOS Console", TextStyle::Heading, terminal);
  EXPECT_EQ(std::count(prompt.begin(), prompt.end(), '\001'), 2);
  EXPECT_EQ(std::count(prompt.begin(), prompt.end(), '\002'), 2);
  auto visible = plain(prompt);
  visible.erase(std::remove(visible.begin(), visible.end(), '\001'), visible.end());
  visible.erase(std::remove(visible.begin(), visible.end(), '\002'), visible.end());
  EXPECT_EQ(visible, "EOS Console");
}

TEST_F(ConsoleColorTest, CliHelpPreservesLayoutAndDoesNotMutateTheFormatter)
{
  CLI::App app{"A long description used to check wrapping remains unchanged.", "demo"};
  std::string value;
  app.add_option("--some-long-option", value,
                 "A value with a sufficiently long description");
  app.add_subcommand("nested", "Nested operation")->add_flag("--enabled");
  const auto original = app.get_formatter();
  const auto baseline = app.help("", CLI::AppFormatMode::All);
  EXPECT_EQ(help(app), baseline);
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = help(app);
  EXPECT_NE(output.find('\033'), std::string::npos);
  EXPECT_EQ(plain(output), baseline);
  EXPECT_EQ(app.get_formatter(), original);
  EXPECT_EQ(app.help("", CLI::AppFormatMode::All), baseline);
}

TEST_F(ConsoleColorTest, CustomCliHelpAndMachineHelpRemainExact)
{
  CLI::App app{"", "demo"};
  const std::string text = "Usage: demo\n  --flag  unchanged text\n";
  app.formatter(std::make_shared<CLI::FormatterLambda>(
      [&](const CLI::App*, std::string, CLI::AppFormatMode) { return text; }));
  ConfigureColor(ColorMode::Always, true, false);
  EXPECT_EQ(help(app), text);
  ConfigureColor(ColorMode::Always, false, true);
  EXPECT_EQ(help(app), text);
}

TEST_F(ConsoleColorTest, CliHelpStylesSubcommandListsAliasesAndAnonymousGroups)
{
  CLI::App app{"Root description remains plain.", "demo"};
  auto* nested = app.add_subcommand("nested", "nested description stays plain.");
  nested->alias("n");
  nested->add_subcommand("grandchild", "Grandchild description stays plain.");
  app.add_option_group("Selection", "Grouped options stay plain.")
      ->add_subcommand("grouped", "Grouped description stays plain.");
  ConfigureColor(ColorMode::Always, false, false);
  for (const auto mode : {CLI::AppFormatMode::Normal, CLI::AppFormatMode::All}) {
    SCOPED_TRACE(static_cast<int>(mode));
    const auto baseline = app.help("", mode);
    const auto output = help(app, mode);
    EXPECT_EQ(plain(output), baseline);
    for (const char* command : {"nested", "n", "grouped"}) {
      EXPECT_NE(output.find(Colorize(command, TextStyle::Command, fileno(file))),
                std::string::npos)
          << command;
    }
    for (const char* description :
         {"nested description stays plain.", "Grouped options stay plain.",
          "Grouped description stays plain."}) {
      EXPECT_NE(output.find(description), std::string::npos) << description;
    }
    if (mode == CLI::AppFormatMode::All) {
      EXPECT_NE(output.find(Colorize("grandchild", TextStyle::Command, fileno(file))),
                std::string::npos);
      EXPECT_NE(output.find("\nnested description stays plain.\n"), std::string::npos);
      EXPECT_NE(output.find(" Grandchild description stays plain.\n"), std::string::npos);
    } else {
      EXPECT_EQ(output.find("grandchild"), std::string::npos);
    }
  }
}

TEST_F(ConsoleColorTest, LegacyHelpLoadsCommandVocabularyOnlyWhenStylingIsEnabled)
{
  struct RestoreMetadata {
    std::vector<HelpCommand> vocabulary = SetHelpCommands({});
    HelpCommandLoader loader = SetHelpCommandLoader(nullptr);
    ~RestoreMetadata()
    {
      SetHelpCommandLoader(loader);
      SetHelpCommands(std::move(vocabulary));
    }
  } restore;
  static size_t calls;
  calls = 0;
  SetHelpCommandLoader(+[]() -> std::vector<HelpCommand> {
    ++calls;
    return {{"lazy", {{"cached", {}}}}};
  });
  const std::string text = "Usage: lazy\n  lazy cached <value>\n";
  for (const auto policy : {ColorMode::Never, ColorMode::Auto}) {
    ConfigureColor(policy, false, false);
    EXPECT_EQ(legacyHelp(text), text);
    EXPECT_EQ(calls, 0u);
  }
  for (const auto [batch, json] :
       {std::pair{true, false}, std::pair{false, true}, std::pair{true, true}}) {
    ConfigureColor(ColorMode::Always, batch, json);
    EXPECT_EQ(legacyHelp(text), text);
    EXPECT_EQ(calls, 0u);
  }
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(calls, 1u);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find(Colorize("cached", TextStyle::Command, fileno(file))),
            std::string::npos);
  EXPECT_EQ(legacyHelp(text), output);
  EXPECT_EQ(calls, 1u);
}

TEST_F(ConsoleColorTest, ParsedNestedCliHelpStylesTheSelectedAppAndRestoresFormatters)
{
  CLI::App app{"Root operations", "demo"};
  auto* nested = app.add_subcommand("nested", "Nested operation");
  auto* leaf = nested->add_subcommand("leaf", "Selected operation");
  leaf->add_flag("--enabled", "Enable the selected operation");
  const auto rootFormatter = app.get_formatter();
  const auto nestedFormatter = nested->get_formatter();
  const auto leafFormatter = leaf->get_formatter();
  bool requested = false;
  try {
    app.parse("nested leaf --help");
  } catch (const CLI::CallForHelp&) {
    requested = true;
  }
  ASSERT_TRUE(requested);
  const auto baseline = app.help("", CLI::AppFormatMode::Normal);
  EXPECT_NE(baseline.find("demo nested leaf"), std::string::npos);
  EXPECT_NE(baseline.find("--enabled"), std::string::npos);
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = help(app, CLI::AppFormatMode::Normal);
  EXPECT_NE(output.find('\033'), std::string::npos);
  EXPECT_NE(output.find(Colorize("--enabled", TextStyle::Option, fileno(file))),
            std::string::npos);
  for (const char* command : {"demo", "nested", "leaf"}) {
    EXPECT_NE(output.find(Colorize(command, TextStyle::Command, fileno(file))),
              std::string::npos)
        << command;
  }
  EXPECT_EQ(plain(output), baseline);
  EXPECT_EQ(app.get_formatter(), rootFormatter);
  EXPECT_EQ(nested->get_formatter(), nestedFormatter);
  EXPECT_EQ(leaf->get_formatter(), leafFormatter);
  EXPECT_EQ(app.help("", CLI::AppFormatMode::Normal), baseline);
}

TEST_F(ConsoleColorTest, LegacyHelpDecoratesUsageHeadingsAndOptionNames)
{
  const std::string text = "\n \t\nUsage: demo <path> [--help]\n"
                           "\nOptions:\n"
                           "  -r, --recursive  preserve café and λ\n"
                           "  --name=<name>   value with spaces\n"
                           "\t--enabled       keep tabs\n"
                           "Examples:\n"
                           "  demo --name=example /eos/test";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find(Colorize("Usage:", TextStyle::Heading, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("demo", TextStyle::Command, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("<path>", TextStyle::Argument, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("Options:", TextStyle::Heading, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("--recursive", TextStyle::Option, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("-r", TextStyle::Option, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("--name", TextStyle::Option, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("--enabled", TextStyle::Option, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find("preserve café and λ"), std::string::npos);
  EXPECT_EQ(output.find('\001'), std::string::npos);
  EXPECT_EQ(output.find('\002'), std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyHelpPreservesEmptyAndUnterminatedLayouts)
{
  ConfigureColor(ColorMode::Always, false, false);
  for (const std::string text : {"", "\n", " \t\n\n", "Usage: ns", "\nUsage: ns\n",
                                 "\tUsage: ns\r\n  --help\r\n"}) {
    SCOPED_TRACE(text);
    const auto output = legacyHelp(text);
    EXPECT_EQ(plain(output), text);
    if (text.find_first_not_of(" \t\r\n") == std::string::npos) {
      EXPECT_EQ(output, text);
    } else {
      EXPECT_NE(output.find('\033'), std::string::npos);
    }
  }
}

TEST_F(ConsoleColorTest, LegacyOptionDefinitionsDoNotStyleDescriptionsAsHeadings)
{
  const std::string text = "Usage: report [--read]\n"
                           "options:\n"
                           "  --read  filter read reports only:\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find(Colorize("--read", TextStyle::Option, fileno(file)) +
                        "  filter read reports only:"),
            std::string::npos);
  EXPECT_EQ(output.find(Colorize(
                "  --read  filter read reports only:", TextStyle::Heading, fileno(file))),
            std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyNumericFlagsAreDistinctFromNegativeArguments)
{
  const std::string text = "Usage: demo\n"
                           "  touch [-0] <path>\n"
                           "  -100 : select one hundred items\n"
                           "  ns --purge -1\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find(Colorize("[", TextStyle::Syntax, fileno(file)) +
                        Colorize("-0", TextStyle::Option, fileno(file)) +
                        Colorize("]", TextStyle::Syntax, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("-100", TextStyle::Option, fileno(file)) +
                        " : select one hundred items"),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("--purge", TextStyle::Option, fileno(file)) + " -1\n"),
            std::string::npos);
  EXPECT_EQ(output.find(Colorize("-1", TextStyle::Option, fileno(file))),
            std::string::npos);
}

TEST_F(ConsoleColorTest, HelpRolesHaveDistinctCommandOptionAndArgumentStyles)
{
  ConfigureColor(ColorMode::Always, false, false);
  const auto command = Colorize("token", TextStyle::Command, fileno(file));
  const auto option = Colorize("token", TextStyle::Option, fileno(file));
  const auto argument = Colorize("token", TextStyle::Argument, fileno(file));
  EXPECT_NE(command, option);
  EXPECT_NE(command, argument);
  EXPECT_NE(option, argument);
  for (const auto& output : {command, option, argument}) {
    EXPECT_NE(output.find('\033'), std::string::npos);
    EXPECT_EQ(plain(output), "token");
  }
}

TEST_F(ConsoleColorTest, SemanticHelpRolesDistinguishLiteralsIdentifiersAndPlaceholders)
{
  ConfigureColor(ColorMode::Always, false, false);
  for (const char* term : {"xterm", "dumb"}) {
    SCOPED_TRACE(term);
    setenv("TERM", term, 1);
    const auto literal = Colorize("token", TextStyle::Literal, fileno(file));
    const auto identifier = Colorize("token", TextStyle::Identifier, fileno(file));
    const auto path = Colorize("token", TextStyle::Path, fileno(file));
    const auto argument = Colorize("token", TextStyle::Argument, fileno(file));
    EXPECT_NE(literal, identifier);
    EXPECT_NE(literal, argument);
    EXPECT_NE(identifier, argument);
    for (const auto& output : {literal, identifier, path, argument}) {
      EXPECT_NE(output.find('\033'), std::string::npos);
      EXPECT_EQ(plain(output), "token");
      EXPECT_EQ(output.find('\001'), std::string::npos);
      EXPECT_EQ(output.find('\002'), std::string::npos);
    }
  }
}

TEST_F(ConsoleColorTest, ShapingRolesSeparateEntityTypesReadAndWrite)
{
  ConfigureColor(ColorMode::Always, false, false);
  for (const char* term : {"xterm", "dumb"}) {
    SCOPED_TRACE(term);
    setenv("TERM", term, 1);
    const auto entity = Colorize("token", TextStyle::EntityType, fileno(file));
    const auto read = Colorize("token", TextStyle::Read, fileno(file));
    const auto write = Colorize("token", TextStyle::Write, fileno(file));
    EXPECT_NE(entity, read);
    EXPECT_NE(entity, write);
    EXPECT_NE(read, write);
    EXPECT_NE(read.find("\033[34m"), std::string::npos);
    EXPECT_NE(write.find("\033[35m"), std::string::npos);
    for (const auto& output : {entity, read, write}) {
      EXPECT_EQ(plain(output), "token");
    }
  }
}

TEST_F(ConsoleColorTest, SemanticHelpRolesRespectColorPoliciesAndRejectInvalidRoles)
{
  for (const auto style : {TextStyle::Literal, TextStyle::Identifier, TextStyle::Path,
                           TextStyle::EntityType, TextStyle::Read, TextStyle::Write}) {
    for (const auto policy : {ColorMode::Auto, ColorMode::Never}) {
      ConfigureColor(policy, false, false);
      EXPECT_EQ(Colorize("token", style, fileno(file)), "token");
    }
    for (const auto& [batch, json] :
         {std::pair{true, false}, std::pair{false, true}, std::pair{true, true}}) {
      ConfigureColor(ColorMode::Always, batch, json);
      EXPECT_EQ(Colorize("token", style, terminal), "token");
    }
  }
  ConfigureColor(ColorMode::Always, false, false);
  for (const auto style : {TextStyle::Count, static_cast<TextStyle>(-1)}) {
    EXPECT_EQ(Colorize("token", style, terminal), "token");
    EXPECT_EQ(ColorPrompt("token", style, terminal), "token");
  }
}

TEST_F(ConsoleColorTest, LegacyNamespaceHelpDistinguishesCommandPathsAndArguments)
{
  const std::string text =
      "Usage: ns [stat|cache|recompute_tree_size]\n"
      "  ns cache set|drop [-d|-f] [<max_num>] [<max_size>K|M|G...]\n"
      "    set        : update cache size for files or directories\n"
      "    drop       : drop cached file and/or directory entries\n"
      "  ns recompute_tree_size <path>|cid:<decimal_id>|cxid:<hex_id> [--depth <val>]\n"
      "    recompute the tree size of a directory and all its subdirectories\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find(Colorize("ns", TextStyle::Command, fileno(file)) + " " +
                        Colorize("cache", TextStyle::Command, fileno(file)) + " " +
                        Colorize("set", TextStyle::Command, fileno(file)) +
                        Colorize("|", TextStyle::Syntax, fileno(file)) +
                        Colorize("drop", TextStyle::Command, fileno(file))),
            std::string::npos);
  for (const char* command : {"set", "drop", "recompute_tree_size"}) {
    EXPECT_NE(output.find(Colorize(command, TextStyle::Command, fileno(file))),
              std::string::npos)
        << command;
  }
  EXPECT_NE(output.find("    " + Colorize("set", TextStyle::Command, fileno(file)) +
                        "        : update cache size for files or directories\n"),
            std::string::npos);
  EXPECT_NE(output.find("    " + Colorize("drop", TextStyle::Command, fileno(file)) +
                        "       : drop cached file and/or directory entries\n"),
            std::string::npos);
  for (const char* option : {"-d", "-f", "--depth"}) {
    EXPECT_NE(output.find(Colorize(option, TextStyle::Option, fileno(file))),
              std::string::npos)
        << option;
  }
  for (const char* argument :
       {"<path>", "<decimal_id>", "<hex_id>", "<val>", "<max_num>", "<max_size>"}) {
    EXPECT_NE(output.find(Colorize(argument, TextStyle::Argument, fileno(file))),
              std::string::npos)
        << argument;
  }
  EXPECT_NE(
      output.find("recompute the tree size of a directory and all its subdirectories"),
      std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyHelpPreservesMultiwordAndNestedPlaceholderSyntax)
{
  const std::string text = "Usage: demo <id of file to drop> <host[:port]>\n"
                           "  --name=<name>  preserve café, λ and 東京\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(
      output.find(Colorize("<id of file to drop>", TextStyle::Argument, fileno(file))),
      std::string::npos);
  EXPECT_NE(output.find(Colorize("<host[:port]>", TextStyle::Argument, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find(Colorize("--name", TextStyle::Option, fileno(file)) + "=" +
                        Colorize("<name>", TextStyle::Argument, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find("preserve café, λ and 東京"), std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyHelpStylesRootlessCommandPathsWithoutStylingTheirProse)
{
  const std::string text = "Usage: sched configure [OPTIONS]\n"
                           "  configure weight <space> <fsid> <weight>\n"
                           "    configure weight for fsid in space. NOT persisted\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find("  " + Colorize("configure", TextStyle::Command, fileno(file)) +
                        " " + Colorize("weight", TextStyle::Command, fileno(file)) + " " +
                        Colorize("<space>", TextStyle::Argument, fileno(file))),
            std::string::npos);
  EXPECT_NE(output.find("    configure weight for fsid in space. NOT persisted\n"),
            std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyHelpRecognizesSubcommandsAfterOptionalFlags)
{
  const std::string text =
      "Usage: attr [OPTIONS] get|set ...\n"
      "  attr [-r] [-V] get <key> <identifier>\n"
      "    Get attributes of path (-r recursive, -V only print value)\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find(Colorize("get", TextStyle::Command, fileno(file)) + " " +
                        Colorize("<key>", TextStyle::Argument, fileno(file))),
            std::string::npos);
  for (const char* option : {"-r", "-V"}) {
    EXPECT_NE(output.find(Colorize(option, TextStyle::Option, fileno(file))),
              std::string::npos)
        << option;
  }
}

TEST_F(ConsoleColorTest, LegacyHelpDecoratesOnlyTheCommandPrefixInProse)
{
  const std::string text = "Usage: ns [stat|cache]\n"
                           "  ns cache set|drop [<max_num>]\n"
                           "    ns stat command to see the current values.\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find("    " + Colorize("ns", TextStyle::Command, fileno(file)) + " " +
                        Colorize("stat", TextStyle::Command, fileno(file)) +
                        " command to see the current values.\n"),
            std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyHelpKeepsSentenceHeadingsPlainAndHighlightsOnlyNoteLabels)
{
  const std::string text =
      "Usage: ns master [<option>]\n"
      "    master/slave operations. Option can be:\n"
      "    Note: for project quotas the uid values needs to be specified\n";
  ConfigureColor(ColorMode::Always, false, false);
  const auto output = legacyHelp(text);
  EXPECT_EQ(plain(output), text);
  EXPECT_NE(output.find("    master/slave operations. Option can be:\n"),
            std::string::npos);
  EXPECT_NE(output.find("    " + Colorize("Note:", TextStyle::Warning, fileno(file)) +
                        " for project quotas the uid values needs to be specified\n"),
            std::string::npos);
}

TEST_F(ConsoleColorTest, LegacyHelpRespectsTheSamePlainAndMachinePolicies)
{
  const std::string text = "Usage: demo [--help]\nOptions:\n  --name=<name> café\n";
  EXPECT_EQ(legacyHelp(text), text);
  for (const auto policy : {ColorMode::Never, ColorMode::Auto}) {
    ConfigureColor(policy, false, false);
    EXPECT_EQ(legacyHelp(text), text);
  }
  for (const auto [batch, json] :
       {std::pair{true, false}, std::pair{false, true}, std::pair{true, true}}) {
    ConfigureColor(ColorMode::Always, batch, json);
    EXPECT_EQ(legacyHelp(text), text);
  }
}

TEST_F(ConsoleColorTest, GlobalColorOptionHasValidatedValues)
{
  ConsoleOptions options;
  std::string error;
  EXPECT_TRUE(ParseConsoleOptions({"--color=always", "help"}, options, error));
  EXPECT_EQ(options.color, ColorMode::Always);
  EXPECT_EQ(options.command, (std::vector<std::string>{"help"}));
  EXPECT_TRUE(ParseConsoleOptions({"--color", "never", "help"}, options, error));
  EXPECT_EQ(options.color, ColorMode::Never);
  EXPECT_TRUE(ParseConsoleOptions({"help", "--color=always"}, options, error));
  EXPECT_EQ(options.color, ColorMode::Auto);
  EXPECT_EQ(options.command, (std::vector<std::string>{"help", "--color=always"}));
  EXPECT_TRUE(
      ParseConsoleOptions({"--color=auto", "--color=never", "help"}, options, error));
  EXPECT_EQ(options.color, ColorMode::Never);
  for (const char* value : {"invalid", "0", "1", "true", "ALWAYS"}) {
    EXPECT_FALSE(
        ParseConsoleOptions({std::string("--color=") + value, "help"}, options, error));
  }
  EXPECT_FALSE(ParseConsoleOptions({"--color=never", "--unknown"}, options, error));
  EXPECT_EQ(options.color, ColorMode::Never);
  EXPECT_FALSE(ParseConsoleOptions({"--color"}, options, error));
}

TEST_F(ConsoleColorTest, StructuredUppercaseSectionHeadingsAreStyledAfterLayout)
{
  ConfigureColor(ColorMode::Always, false, false);
  const std::string text =
      "USAGE\n  eos ns [<operation>]\n\nOPERATIONS\n  cache  Change metadata caches.\n";
  const auto result = legacyHelp(text);
  EXPECT_EQ(plain(result), text);
  for (const char* heading : {"USAGE", "OPERATIONS"}) {
    EXPECT_NE(result.find(Colorize(heading, TextStyle::Heading, fileno(file))),
              std::string::npos);
  }
}

TEST_F(ConsoleColorTest, HelpOperationTablesKeepTheirContextWhenGlobalNamesCollide)
{
  SetHelpCommands(
      {{"ns",
        {{"stat", {}}, {"mutex", {}}, {"tracker", {}}, {"cache", {}}, {"behaviour", {}}}},
       {"stat", {}},
       {"tracker", {}}});
  const std::vector<std::pair<std::string, std::string>> operations{
      {"stat", "Read statistics."},
      {"mutex", "Inspect mutexes."},
      {"tracker", "Inspect trackers."},
      {"cache", "Adjust caches."},
      {"behaviour", "Inspect settings."}};
  HelpPage page;
  page.blocks = {{HelpBlockKind::Section, "USAGE", {}},
                 {HelpBlockKind::Usage, "eos ns [<operation>]", {}},
                 {HelpBlockKind::Section, "OPERATIONS", {}}};
  for (const auto& [operation, description] : operations) {
    page.blocks.push_back({HelpBlockKind::Definition, operation, description});
  }
  ConfigureColor(ColorMode::Always, false, false);
  for (const size_t width : {40, 120}) {
    SCOPED_TRACE(width);
    const auto text = page.render(width);
    const auto output = legacyHelp(text);
    EXPECT_EQ(plain(output), text);
    for (const auto& [operation, description] : operations) {
      EXPECT_NE(
          output.find("\n  " + Colorize(operation, TextStyle::Command, fileno(file))),
          std::string::npos)
          << operation;
      EXPECT_NE(output.find(description), std::string::npos) << description;
    }
  }
}

TEST_F(ConsoleColorTest, ResponsiveCli11HeadingsKeepSemanticStyling)
{
  CLI::App app{"Inspect metadata.", "demo"};
  app.add_flag("--details", "Include detailed information.");
  app.add_subcommand("inspect", "Inspect an entry.");
  const auto text = LayoutCliHelp(app, CLI::AppFormatMode::Normal, 40);
  ConfigureColor(ColorMode::Always, false, false);
  const auto result = legacyHelp(text);
  EXPECT_EQ(plain(result), text);
  for (const char* heading : {"Usage:", "OPTIONS:", "SUBCOMMANDS:"}) {
    ASSERT_NE(text.find(heading), std::string::npos);
    EXPECT_NE(result.find(Colorize(heading, TextStyle::Heading, fileno(file))),
              std::string::npos);
  }
}
