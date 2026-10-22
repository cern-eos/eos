// ----------------------------------------------------------------------
// File: ConsoleHelpProcessTest.cc
// ----------------------------------------------------------------------

#include "console/ConsoleHelp.hh"
#include "gtest/gtest.h"
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <optional>
#include <regex>
#include <signal.h>
#include <spawn.h>
#include <sstream>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <termios.h>
#include <thread>
#include <unistd.h>
#include <vector>

extern char** environ;

namespace {
std::string
styled(std::string_view text, eos::console::TextStyle style)
{
  struct Restore {
    std::optional<std::string> term;
    ~Restore()
    {
      if (term) {
        setenv("TERM", term->c_str(), 1);
      } else {
        unsetenv("TERM");
      }
      eos::console::ConfigureColor(eos::console::ColorMode::Auto, false, false);
    }
  } restore;
  if (const char* term = getenv("TERM")) {
    restore.term = term;
  }
  setenv("TERM", "xterm", 1);
  eos::console::ConfigureColor(eos::console::ColorMode::Always, false, false);
  return eos::console::Colorize(text, style, STDERR_FILENO);
}

std::string
styledCommand(std::string_view text)
{
  return styled(text, eos::console::TextStyle::Command);
}

class ConsoleHelpProcessTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    char path[] = "/tmp/eos-help-process-XXXXXX";
    ASSERT_NE(mkdtemp(path), nullptr);
    directory = path;
  }

  void
  TearDown() override
  {
    for (const auto& path : files) {
      unlink(path.c_str());
    }
    for (const char* name :
         {"stdout", "stderr", "pager-marker", "pager-arguments", "injected"}) {
      unlink((directory + "/" + name).c_str());
    }
    rmdir(directory.c_str());
  }

  int
  process(const std::vector<std::string>& args, unsigned short width = 80,
          bool outputTerminal = false, bool errorTerminal = false,
          bool inputTerminal = false,
          const std::map<std::string, std::string>& overrides = {},
          unsigned short rows = 24)
  {
    output.clear();
    error.clear();
    terminalOutput.clear();
    std::map<std::string, std::string> environment;
    for (char** entry = environ; *entry; ++entry) {
      const std::string value(*entry);
      const auto equal = value.find('=');
      environment[value.substr(0, equal)] = value.substr(equal + 1);
    }
    for (const auto* name : {"NO_COLOR", "EOSAUTHZ", "EOS_CONSOLE_DEBUG",
                             "EOS_HELP_WIDTH", "EOS_PAGER", "PAGER"}) {
      environment.erase(name);
    }
    environment["TERM"] = "xterm";
    environment["EOSAPP"] = "";
    environment["EOS_MGM_URL"] = "ipc://" + directory + "/unavailable-mgm";
    environment["EOS_PWD_FILE"] = "/dev/null";
    environment["EOS_HISTORY_FILE"] = "/dev/null";
    environment["INPUTRC"] = "/dev/null";
    environment["EOS_TEST_PAGER_MARKER"] = directory + "/pager-marker";
    environment["EOS_TEST_PAGER_ARGUMENTS"] = directory + "/pager-arguments";
    for (const auto& [key, value] : overrides) {
      environment[key] = value;
    }
    std::vector<std::string> environmentStrings;
    for (const auto& [key, value] : environment) {
      environmentStrings.push_back(key + "=" + value);
    }
    std::vector<char*> env;
    for (auto& value : environmentStrings) {
      env.push_back(value.data());
    }
    env.push_back(nullptr);
    const char* executable = access(EOS_CONSOLE_EXECUTABLE, X_OK) == 0
                                 ? EOS_CONSOLE_EXECUTABLE
                                 : EOS_CONSOLE_INSTALLED_EXECUTABLE;
    std::vector<std::string> arguments{executable};
    arguments.insert(arguments.end(), args.begin(), args.end());
    std::vector<char*> argv;
    for (auto& value : arguments) {
      argv.push_back(value.data());
    }
    argv.push_back(nullptr);
    struct Terminal {
      int master = -1, slave = -1;
      ~Terminal()
      {
        if (master >= 0) {
          close(master);
        }
        if (slave >= 0) {
          close(slave);
        }
      }
    } terminal;
    if (outputTerminal || errorTerminal || inputTerminal) {
      terminal.master = posix_openpt(O_RDWR | O_NOCTTY | O_NONBLOCK);
      if (terminal.master < 0 || grantpt(terminal.master) || unlockpt(terminal.master)) {
        ADD_FAILURE() << "Cannot allocate help test PTY";
        return -1;
      }
      const char* name = ptsname(terminal.master);
      if (!name || (terminal.slave = open(name, O_RDWR | O_NOCTTY)) < 0) {
        ADD_FAILURE() << "Cannot open help test PTY";
        return -1;
      }
      winsize size{};
      size.ws_col = width;
      size.ws_row = rows;
      if (ioctl(terminal.slave, TIOCSWINSZ, &size)) {
        ADD_FAILURE() << "Cannot size help test PTY";
        return -1;
      }
      termios attributes{};
      if (tcgetattr(terminal.slave, &attributes)) {
        ADD_FAILURE() << "Cannot read help test PTY attributes";
        return -1;
      }
      attributes.c_oflag &= ~ONLCR;
      if (tcsetattr(terminal.slave, TCSANOW, &attributes)) {
        ADD_FAILURE() << "Cannot configure help test PTY";
        return -1;
      }
    }
    posix_spawn_file_actions_t actions;
    EXPECT_EQ(posix_spawn_file_actions_init(&actions), 0);
    if (inputTerminal) {
      EXPECT_EQ(posix_spawn_file_actions_adddup2(&actions, terminal.slave, STDIN_FILENO),
                0);
    } else {
      EXPECT_EQ(posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null",
                                                 O_RDONLY, 0),
                0);
    }
    for (int descriptor : {STDOUT_FILENO, STDERR_FILENO}) {
      const bool tty = descriptor == STDOUT_FILENO ? outputTerminal : errorTerminal;
      if (tty) {
        EXPECT_EQ(posix_spawn_file_actions_adddup2(&actions, terminal.slave, descriptor),
                  0);
      } else {
        const auto path =
            directory + (descriptor == STDOUT_FILENO ? "/stdout" : "/stderr");
        EXPECT_EQ(posix_spawn_file_actions_addopen(&actions, descriptor, path.c_str(),
                                                   O_WRONLY | O_CREAT | O_TRUNC, 0600),
                  0);
      }
    }
    if (terminal.master >= 0) {
      EXPECT_EQ(posix_spawn_file_actions_addclose(&actions, terminal.master), 0);
      EXPECT_EQ(posix_spawn_file_actions_addclose(&actions, terminal.slave), 0);
    }
    pid_t child = -1;
    const int spawned =
        posix_spawn(&child, executable, &actions, nullptr, argv.data(), env.data());
    posix_spawn_file_actions_destroy(&actions);
    if (spawned) {
      ADD_FAILURE() << "Cannot spawn console: " << spawned;
      return -1;
    }
    auto drain = [&] {
      char data[4096];
      ssize_t count;
      while (terminal.master >= 0 &&
             (count = read(terminal.master, data, sizeof(data))) > 0) {
        terminalOutput.append(data, static_cast<size_t>(count));
      }
    };
    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    pid_t waited;
    while ((waited = waitpid(child, &status, WNOHANG)) == 0) {
      drain();
      if (std::chrono::steady_clock::now() >= deadline) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
        ADD_FAILURE() << "Console help test timed out";
        return -1;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    drain();
    output = outputTerminal ? "" : readFile(directory + "/stdout");
    error = errorTerminal ? "" : readFile(directory + "/stderr");
    if (waited != child || !WIFEXITED(status)) {
      ADD_FAILURE() << "Console did not exit normally";
      return -1;
    }
    return WEXITSTATUS(status);
  }

  static std::string
  readFile(const std::string& path)
  {
    std::ifstream stream(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(stream), {}};
  }

  static std::string
  plain(const std::string& text)
  {
    static const std::regex escapes("\033\\[[0-?]*[ -/]*[@-~]|\033[()][0-2A-Z]");
    return std::regex_replace(text, escapes, "");
  }

  static void
  expectWidth(const std::string& text, size_t width)
  {
    std::istringstream lines(plain(text));
    std::string line;
    while (std::getline(lines, line)) {
      EXPECT_LE(eos::console::HelpDisplayWidth(line), width) << line;
    }
  }

  static std::string
  operationSection(const std::string& text, const std::string& context = "")
  {
    std::istringstream lines(text);
    std::string line, result;
    bool foundContext = context.empty(), foundSection = false;
    while (std::getline(lines, line)) {
      const auto uncolored = plain(line);
      if (!foundContext) {
        foundContext = uncolored == context;
      } else if (!foundSection) {
        foundSection = uncolored == "OPERATIONS";
      } else {
        if (!uncolored.empty() && uncolored.front() != ' ') {
          break;
        }
        result += line + '\n';
      }
    }
    return result;
  }

  std::string
  pager(const std::string& body = "cat\n")
  {
    const auto path = directory + "/pager fixture";
    {
      std::ofstream script(path);
      script << "#!/bin/sh\n"
                "printf 'invoked\\n' > \"$EOS_TEST_PAGER_MARKER\"\n"
                "printf '<%s>\\n' \"$@\" > \"$EOS_TEST_PAGER_ARGUMENTS\"\n"
             << body;
    }
    EXPECT_EQ(chmod(path.c_str(), 0700), 0);
    files.push_back(path);
    return "\"" + path + "\" \"two words\"";
  }

  std::string directory, output, error, terminalOutput;
  std::vector<std::string> files;
};
} // namespace

TEST_F(ConsoleHelpProcessTest, NamespaceIndexFitsRealTerminalWidthsWithEveryColorPolicy)
{
  for (unsigned short width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    ASSERT_EQ(process({"--color=never", "ns", "--help"}, width, false, true), EINVAL);
    const auto baseline = terminalOutput;
    EXPECT_TRUE(output.empty());
    EXPECT_NE(baseline.find("OPERATIONS"), std::string::npos);
    EXPECT_NE(baseline.find("--help-all"), std::string::npos);
    EXPECT_EQ(baseline.find("master/slave operations"), std::string::npos);
    expectWidth(baseline, width);
    ASSERT_EQ(process({"--color=always", "ns", "--help"}, width, false, true), EINVAL);
    EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
    EXPECT_EQ(plain(terminalOutput), baseline);
    ASSERT_EQ(process({"ns", "--help"}, width, false, true, false, {{"NO_COLOR", "1"}}),
              EINVAL);
    EXPECT_EQ(terminalOutput, baseline);
  }
}

TEST_F(ConsoleHelpProcessTest, NamespaceIndexStylesEveryOperationWithItsCommandColor)
{
  for (unsigned short width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    ASSERT_EQ(process({"--color=never", "ns", "--help"}, width, false, true), EINVAL);
    const auto baseline = terminalOutput;
    for (const auto* mode : {"--color=auto", "--color=always"}) {
      SCOPED_TRACE(mode);
      ASSERT_EQ(process({mode, "ns", "--help"}, width, false, true), EINVAL);
      EXPECT_EQ(plain(terminalOutput), baseline);
      for (const auto* operation :
           {"stat", "mutex", "compact", "master", "recompute_tree_size",
            "recompute_quotanode", "update_quotanode", "cache", "drain", "reserve-ids",
            "benchmark", "tracker", "behaviour"}) {
        EXPECT_NE(terminalOutput.find("\n  " + styledCommand(operation)),
                  std::string::npos)
            << operation;
      }
    }
  }
}

TEST_F(ConsoleHelpProcessTest, NestedNamespaceOperationTablesStyleEveryChildInContext)
{
  const std::map<std::string, std::vector<std::string>> groups{
      {"compact", {"off", "on"}},
      {"cache", {"set", "drop", "drop-single-file", "drop-single-container"}},
      {"drain", {"list", "set"}},
      {"tracker", {"list", "clear"}},
      {"behaviour", {"list", "set", "get", "clear"}}};
  for (const unsigned short width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    for (const auto& [group, children] : groups) {
      SCOPED_TRACE(group);
      ASSERT_EQ(process({"--color=never", "ns", group, "--help"}, width, false, true),
                EINVAL);
      const auto baseline = terminalOutput;
      for (const auto* mode : {"--color=auto", "--color=always"}) {
        SCOPED_TRACE(mode);
        ASSERT_EQ(process({mode, "ns", group, "--help"}, width, false, true), EINVAL);
        EXPECT_EQ(plain(terminalOutput), baseline);
        const auto definitions = operationSection(terminalOutput);
        ASSERT_FALSE(definitions.empty());
        for (const auto& child : children) {
          EXPECT_NE(definitions.find("  " + styledCommand(child)), std::string::npos)
              << child;
        }
      }
      ASSERT_EQ(process({"ns", group, "--help"}, width, false, true, false,
                        {{"NO_COLOR", "1"}}),
                EINVAL);
      EXPECT_EQ(terminalOutput, baseline);
    }
    ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, width, false, true), EINVAL);
    const auto expanded = terminalOutput;
    for (const auto* mode : {"--color=auto", "--color=always"}) {
      SCOPED_TRACE(mode);
      ASSERT_EQ(process({mode, "ns", "--help-all"}, width, false, true), EINVAL);
      EXPECT_EQ(plain(terminalOutput), expanded);
      for (const auto& [group, children] : groups) {
        SCOPED_TRACE(group);
        const auto definitions = operationSection(terminalOutput, "eos ns " + group);
        ASSERT_FALSE(definitions.empty());
        for (const auto& child : children) {
          EXPECT_NE(definitions.find("  " + styledCommand(child)), std::string::npos)
              << child;
        }
      }
    }
  }
}

TEST_F(ConsoleHelpProcessTest, BehaviourHelpSeparatesConfigurationNamesAndLiteralValues)
{
  using eos::console::TextStyle;
  for (const unsigned short width : {20, 40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    ASSERT_EQ(
        process({"--color=never", "ns", "behaviour", "--help-all"}, width, false, true),
        EINVAL);
    const auto baseline = terminalOutput;
    ASSERT_EQ(
        process({"--color=always", "ns", "behaviour", "--help-all"}, width, false, true),
        EINVAL);
    EXPECT_EQ(plain(terminalOutput), baseline);
    for (const auto* identifier : {"rain_min_fsid_entry", "disable_access_log"}) {
      EXPECT_NE(terminalOutput.find(styled(identifier, TextStyle::Identifier)),
                std::string::npos)
          << identifier;
    }
    for (const auto* value : {"on", "off", "all"}) {
      EXPECT_NE(terminalOutput.find(styled(value, TextStyle::Literal)), std::string::npos)
          << value;
      EXPECT_EQ(terminalOutput.find(styled(value, TextStyle::Error)), std::string::npos)
          << value;
    }
    EXPECT_NE(terminalOutput.find(styled("<name>", TextStyle::Argument)),
              std::string::npos);
    const auto example = styledCommand("eos") + " " + styledCommand("ns") + " " +
                         styledCommand("behaviour") + " " + styledCommand("get") + " " +
                         styled("disable_access_log", TextStyle::Identifier);
    EXPECT_NE(terminalOutput.find("  " + example + "\n"), std::string::npos);
    EXPECT_EQ(terminalOutput.find(styled("Inspect", TextStyle::Command)),
              std::string::npos);
  }
}

TEST_F(ConsoleHelpProcessTest, ScopedNamespaceHelpContainsOnlyTheRequestedOperation)
{
  for (unsigned short width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    ASSERT_EQ(process({"--color=never", "ns", "cache", "--help"}, width, false, true),
              EINVAL);
    EXPECT_NE(terminalOutput.find("drop-single-container"), std::string::npos);
    EXPECT_EQ(terminalOutput.find("update_quotanode"), std::string::npos);
    EXPECT_EQ(terminalOutput.find("recompute_tree_size"), std::string::npos);
    expectWidth(terminalOutput, width);
    ASSERT_EQ(
        process({"--color=never", "ns", "cache", "set", "--help"}, width, false, true),
        EINVAL);
    const auto leaf = terminalOutput;
    EXPECT_NE(leaf.find("<max_num>"), std::string::npos);
    EXPECT_NE(leaf.find("EXAMPLES"), std::string::npos);
    EXPECT_NE(leaf.find("eos ns cache set -f 1000000"), std::string::npos);
    EXPECT_EQ(leaf.find("drop-single-container"), std::string::npos);
    expectWidth(leaf, width);
    ASSERT_EQ(
        process({"--color=never", "help", "ns", "cache", "set"}, width, false, true), 0);
    EXPECT_EQ(terminalOutput, leaf);
  }
}

TEST_F(ConsoleHelpProcessTest, EveryNamespaceHelpTopicIsAvailableWithoutAnMgm)
{
  for (const auto& path :
       std::vector<std::vector<std::string>>{{"stat"},
                                             {"mutex"},
                                             {"compact"},
                                             {"compact", "on"},
                                             {"compact", "off"},
                                             {"master"},
                                             {"recompute_tree_size"},
                                             {"recompute_quotanode"},
                                             {"update_quotanode"},
                                             {"cache"},
                                             {"cache", "set"},
                                             {"cache", "drop"},
                                             {"cache", "drop-single-file"},
                                             {"cache", "drop-single-container"},
                                             {"drain"},
                                             {"drain", "list"},
                                             {"drain", "set"},
                                             {"reserve-ids"},
                                             {"benchmark"},
                                             {"tracker"},
                                             {"tracker", "list"},
                                             {"tracker", "clear"},
                                             {"behaviour"},
                                             {"behaviour", "list"},
                                             {"behaviour", "get"},
                                             {"behaviour", "set"},
                                             {"behaviour", "clear"}}) {
    std::vector<std::string> args{"--color=never", "ns"};
    args.insert(args.end(), path.begin(), path.end());
    args.push_back("--help");
    SCOPED_TRACE(::testing::PrintToString(path));
    ASSERT_EQ(process(args, 60, false, true), EINVAL);
    EXPECT_EQ(terminalOutput.substr(0, 6), "USAGE\n");
    EXPECT_TRUE(output.empty());
    EXPECT_EQ(terminalOutput.find("error:"), std::string::npos);
  }
}

TEST_F(ConsoleHelpProcessTest, GlobalUsageAndCommandIndexFitRealTerminalWidths)
{
  for (unsigned short width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    ASSERT_EQ(process({"--color=never", "--help"}, width, false, true), 255);
    expectWidth(terminalOutput, width);
    EXPECT_NE(terminalOutput.find("EOS_HELP_WIDTH"), std::string::npos);
    EXPECT_NE(terminalOutput.find("--no-pager"), std::string::npos);
    ASSERT_EQ(process({"--color=never", "help"}, width, false, true), 0);
    expectWidth(terminalOutput, width);
    EXPECT_NE(terminalOutput.find("Available commands:"), std::string::npos);
    EXPECT_NE(terminalOutput.find("ns"), std::string::npos);
  }
}

TEST_F(ConsoleHelpProcessTest, ExpandedHelpIsExplicitAndRetainsEveryOperation)
{
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true), EINVAL);
  for (const auto* operation :
       {"mutex", "master", "update_quotanode", "reserve-ids", "benchmark",
        "drop-single-file", "drop-single-container", "behaviour"}) {
    EXPECT_NE(terminalOutput.find(operation), std::string::npos) << operation;
  }
  EXPECT_EQ(terminalOutput.find("\n\n\n"), std::string::npos);
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}), EINVAL);
  EXPECT_NE(error.find("ns update_quotanode"), std::string::npos);
  EXPECT_TRUE(output.empty());
}

TEST_F(ConsoleHelpProcessTest, RedirectedAndMachineHelpRetainsTheEstablishedManual)
{
  ASSERT_EQ(process({"--color=never", "ns", "--help"}), EINVAL);
  const auto baseline = error;
  EXPECT_NE(baseline.find("ns update_quotanode"), std::string::npos);
  EXPECT_NE(baseline.find("    master/slave operations. Option can be:\n"),
            std::string::npos);
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"--color=never", "ns", "cache", "set", "--help"}), EINVAL);
  EXPECT_EQ(error, baseline);
  for (const auto* mode : {"-b", "-j"}) {
    ASSERT_EQ(process({"--color=always", mode, "ns", "cache", "set", "--help"}, 40, false,
                      true),
              EINVAL);
    EXPECT_EQ(terminalOutput, baseline);
  }
  ASSERT_EQ(
      process({"ns", "--help"}, 40, false, false, false, {{"EOS_HELP_WIDTH", "40"}}),
      EINVAL);
  EXPECT_EQ(error, baseline);
  ASSERT_EQ(process({"ns", "--help"}, 40, true, false), EINVAL);
  EXPECT_EQ(error, baseline);
  EXPECT_TRUE(terminalOutput.empty());
  for (const auto* mode : {"-b", "-j"}) {
    ASSERT_EQ(process({"--color=always", mode, "ns", "--help"}, 40, false, true, false,
                      {{"EOS_HELP_WIDTH", "40"}}),
              EINVAL);
    EXPECT_EQ(terminalOutput, baseline);
  }
}

TEST_F(ConsoleHelpProcessTest, WidthOverrideChangesInteractiveLayoutWithoutChangingColor)
{
  ASSERT_EQ(process({"--color=never", "ns", "--help"}, 120, false, true, false,
                    {{"EOS_HELP_WIDTH", "40"}}),
            EINVAL);
  const auto overridden = terminalOutput;
  expectWidth(overridden, 40);
  ASSERT_EQ(process({"--color=never", "ns", "--help"}, 40, false, true), EINVAL);
  EXPECT_EQ(terminalOutput, overridden);
}

TEST_F(ConsoleHelpProcessTest, CompletionProtocolIsUnchangedAtNarrowWidths)
{
  ASSERT_EQ(process({"__complete", "ns", "cache", "d"}, 40, true, true, true,
                    {{"EOS_HELP_WIDTH", "20"}, {"EOS_PAGER", pager()}}, 3),
            0);
  EXPECT_EQ(terminalOutput, "drop\ndrop-single-container\ndrop-single-file\n");
  EXPECT_NE(access((directory + "/pager-marker").c_str(), F_OK), 0);
}

TEST_F(ConsoleHelpProcessTest, NewHelpOptionsAreDiscoverableThroughCompletion)
{
  ASSERT_EQ(process({"__complete", "--no-p"}), 0);
  EXPECT_EQ(output, "--no-pager\n");
  ASSERT_EQ(process({"__complete", "ns", "--help-a"}), 0);
  EXPECT_EQ(output, "--help-all\n");
  ASSERT_EQ(process({"__complete", "ns", "cache", "--help-a"}), 0);
  EXPECT_EQ(output, "--help-all\n");
}

TEST_F(ConsoleHelpProcessTest, OptInPagerReceivesCompleteHelpAndQuotedArguments)
{
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true), EINVAL);
  const auto baseline = terminalOutput;
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true, true,
                    {{"EOS_PAGER", pager()}}, 3),
            EINVAL);
  EXPECT_EQ(readFile(directory + "/pager-marker"), "invoked\n");
  EXPECT_EQ(readFile(directory + "/pager-arguments"), "<two words>\n");
  EXPECT_EQ(terminalOutput, baseline);
  EXPECT_TRUE(output.empty());
}

TEST_F(ConsoleHelpProcessTest, PagerRequiresInteractiveInputAndTheHelpDestination)
{
  const auto command = pager();
  for (const auto& streams :
       {std::pair<bool, bool>{false, true}, {true, false}, {false, false}}) {
    ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, streams.second,
                      streams.first, {{"EOS_PAGER", command}}, 3),
              EINVAL);
    EXPECT_NE(access((directory + "/pager-marker").c_str(), F_OK), 0);
  }
}

TEST_F(ConsoleHelpProcessTest, PagerIsDisabledForShortHelpBatchJsonAndExplicitOptOut)
{
  const auto command = pager();
  for (const auto& options :
       std::vector<std::vector<std::string>>{{"--no-pager"}, {"-b"}, {"-j"}}) {
    auto args = options;
    args.insert(args.end(), {"--color=never", "ns", "--help-all"});
    ASSERT_EQ(process(args, 60, false, true, true, {{"EOS_PAGER", command}}, 3), EINVAL);
    EXPECT_NE(access((directory + "/pager-marker").c_str(), F_OK), 0);
  }
  ASSERT_EQ(process({"--color=never", "ns", "cache", "set", "--help"}, 80, false, true,
                    true, {{"EOS_PAGER", command}}, 500),
            EINVAL);
  EXPECT_NE(access((directory + "/pager-marker").c_str(), F_OK), 0);
}

TEST_F(ConsoleHelpProcessTest, PagerCommandIsExecutedWithoutShellEvaluation)
{
  const auto malicious = pager() + " ; touch " + directory + "/injected";
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true, true,
                    {{"EOS_PAGER", malicious}}, 3),
            EINVAL);
  EXPECT_NE(access((directory + "/injected").c_str(), F_OK), 0);
  EXPECT_NE(terminalOutput.find("update_quotanode"), std::string::npos);
}

TEST_F(ConsoleHelpProcessTest, FailedPagerFallsBackToTheCompleteHelp)
{
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true), EINVAL);
  const auto baseline = terminalOutput;
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true, true,
                    {{"EOS_PAGER", directory + "/missing-pager"}}, 3),
            EINVAL);
  EXPECT_EQ(terminalOutput, baseline);
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, 60, false, true, true,
                    {{"EOS_PAGER", pager("exit 7\n")}}, 3),
            EINVAL);
  EXPECT_EQ(terminalOutput, baseline);
}

TEST_F(ConsoleHelpProcessTest, StandardCli11HelpFitsActualTerminalWidths)
{
  for (unsigned short width : {40, 60, 80, 120}) {
    SCOPED_TRACE(width);
    ASSERT_EQ(process({"--color=never", "help", "config"}, width, false, true), 0);
    const auto baseline = terminalOutput;
    expectWidth(baseline, width);
    EXPECT_NE(baseline.find("--comment"), std::string::npos);
    EXPECT_NE(baseline.find("REQUIRED"), std::string::npos);
    ASSERT_EQ(process({"--color=always", "help", "config"}, width, false, true), 0);
    EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
    EXPECT_EQ(plain(terminalOutput), baseline);
  }
}

TEST_F(ConsoleHelpProcessTest, IoNamespaceHelpIsScopedInRedirectedAndTerminalOutput)
{
  auto check = [](const std::string& text) {
    EXPECT_NE(text.find("io ns"), std::string::npos);
    EXPECT_NE(text.find("-10000"), std::string::npos);
    EXPECT_NE(text.find("Monitoring format"), std::string::npos);
    EXPECT_EQ(text.find("Print IO statistics"), std::string::npos);
    EXPECT_EQ(text.find("Traffic Shaping engine"), std::string::npos);
    const std::string synopsis = "io ns [OPTIONS]";
    const auto usage = text.find(synopsis);
    ASSERT_NE(usage, std::string::npos);
    EXPECT_EQ(text.find(synopsis, usage + 1), std::string::npos);
  };
  for (const auto* option : {"--help", "-h"}) {
    SCOPED_TRACE(option);
    ASSERT_EQ(process({"--color=never", "io", "ns", option}), 0);
    EXPECT_TRUE(error.empty()) << error;
    check(output);
    for (unsigned short width : {40, 80}) {
      SCOPED_TRACE(width);
      ASSERT_EQ(process({"--color=never", "io", "ns", option}, width, true), 0);
      EXPECT_TRUE(error.empty()) << error;
      const auto baseline = terminalOutput;
      check(baseline);
      expectWidth(baseline.substr(0, baseline.find("EXAMPLES:")), width);
      ASSERT_EQ(process({"--color=always", "io", "ns", option}, width, true), 0);
      EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
      EXPECT_EQ(plain(terminalOutput), baseline);
    }
  }
  ASSERT_EQ(process({"--color=never", "help", "io", "ns"}), 0);
  EXPECT_TRUE(output.empty()) << output;
  check(error);
}

TEST_F(ConsoleHelpProcessTest, IoLeafHelpBypassesRequirementsAndExcludesSiblings)
{
  for (const auto& topic : {std::string("policy"), std::string("config")}) {
    for (const auto* option : {"--help", "-h"}) {
      SCOPED_TRACE(topic);
      SCOPED_TRACE(option);
      ASSERT_EQ(process({"--color=never", "io", "shaping", topic, "set", option}), 0);
      EXPECT_TRUE(error.empty()) << error;
      EXPECT_NE(output.find("io shaping " + topic + " set"), std::string::npos);
      EXPECT_NE(output.find(topic == "policy" ? "--limit-read" : "--estimators-period"),
                std::string::npos);
      EXPECT_EQ(output.find(topic == "policy" ? "--estimators-period" : "--limit-read"),
                std::string::npos);
      EXPECT_EQ(output.find("Print IO statistics"), std::string::npos);
      const std::string synopsis = "io shaping " + topic + " set [OPTIONS]";
      const auto usage = output.find(synopsis);
      ASSERT_NE(usage, std::string::npos);
      EXPECT_EQ(output.find(synopsis, usage + 1), std::string::npos);
    }
  }
}

TEST_F(ConsoleHelpProcessTest, IoExamplesUseConsistentCopyableLines)
{
  auto check = [](const std::string& text) {
    EXPECT_EQ(text.find('\t'), std::string::npos);
    const auto examples = text.find("EXAMPLES:");
    ASSERT_NE(examples, std::string::npos);
    EXPECT_EQ(text.find("EXAMPLES:", examples + 1), std::string::npos);
    std::istringstream lines(text.substr(examples));
    std::string line;
    size_t commands = 0;
    while (std::getline(lines, line)) {
      const auto comment = line.find_first_not_of(' ');
      if (comment == std::string::npos || line[comment] != '#') {
        continue;
      }
      ASSERT_TRUE(std::getline(lines, line));
      ASSERT_EQ(line.find_first_not_of(' '), comment) << line;
      EXPECT_EQ(line.compare(comment, 7, "eos io "), 0) << line;
      ++commands;
    }
    EXPECT_GT(commands, 0u);
  };
  for (const auto& path : std::vector<std::vector<std::string>>{
           {"stat"}, {"ns"}, {"shaping"}, {"shaping", "policy"}, {"shaping", "config"}}) {
    SCOPED_TRACE(::testing::PrintToString(path));
    std::vector<std::string> args{"--color=never", "io"};
    args.insert(args.end(), path.begin(), path.end());
    args.push_back("--help");
    ASSERT_EQ(process(args), 0);
    EXPECT_TRUE(error.empty()) << error;
    check(output);
    for (unsigned short width : {40, 80}) {
      SCOPED_TRACE(width);
      ASSERT_EQ(process(args, width, true), 0);
      const auto baseline = terminalOutput;
      check(baseline);
      args.front() = "--color=always";
      ASSERT_EQ(process(args, width, true), 0);
      EXPECT_EQ(plain(terminalOutput), baseline);
      args.front() = "--color=never";
    }
  }
}

TEST_F(ConsoleHelpProcessTest, LsHelpKeepsItsSynopsisInBatchAndRedirectedOutput)
{
  for (const char* flag : {"-h", "--help"}) {
    for (const auto& prefix : std::vector<std::vector<std::string>>{
             {"--color=never"}, {"--color=never", "-b", "root://localhost"}}) {
      auto args = prefix;
      args.insert(args.end(), {"ls", flag});
      ASSERT_EQ(process(args), 0);
      EXPECT_TRUE(output.empty());
      EXPECT_NE(error.find("ls [OPTIONS] [path...]"), std::string::npos);
      EXPECT_NE(error.find("--no-globbing"), std::string::npos);
    }
  }
}
