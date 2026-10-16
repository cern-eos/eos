// ----------------------------------------------------------------------
// File: ConsoleColorProcessTest.cc
// ----------------------------------------------------------------------

#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "gtest/gtest.h"
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <future>
#include <json/json.h>
#include <map>
#include <optional>
#include <regex>
#include <set>
#include <signal.h>
#include <spawn.h>
#include <sstream>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <termios.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>

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

class ColorTestMgm {
public:
  ColorTestMgm(const std::string& path, std::string reply)
      : mPath(path)
  {
    std::promise<void> ready;
    auto bound = ready.get_future();
    mThread =
        std::thread([this, reply = std::move(reply), ready = std::move(ready)]() mutable {
          bool started = false;
          try {
            zmq::context_t context(1);
            zmq::socket_t socket(context, ZMQ_REP);
            socket.set(zmq::sockopt::linger, 0);
            socket.set(zmq::sockopt::rcvtimeo, 50);
            socket.bind("ipc://" + mPath);
            ready.set_value();
            started = true;
            while (!mStop) {
              zmq::message_t request;
              if (socket.recv(request, zmq::recv_flags::none)) {
                ++mRequests;
                socket.send(zmq::buffer(reply), zmq::send_flags::none);
              }
            }
          } catch (...) {
            if (!started) {
              ready.set_exception(std::current_exception());
            } else {
              mError = std::current_exception();
            }
          }
        });
    try {
      bound.get();
    } catch (...) {
      mThread.join();
      throw;
    }
  }

  ~ColorTestMgm()
  {
    mStop = true;
    mThread.join();
    unlink(mPath.c_str());
    if (mError) {
      EXPECT_NO_THROW(std::rethrow_exception(mError));
    }
  }

  size_t
  requests() const
  {
    return mRequests.load();
  }

private:
  std::string mPath;
  std::atomic<bool> mStop{false};
  std::atomic<size_t> mRequests{0};
  std::thread mThread;
  std::exception_ptr mError;
};

class ConsoleColorProcessTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    char path[] = "/tmp/eos-color-process-XXXXXX";
    ASSERT_NE(mkdtemp(path), nullptr);
    directory = path;
  }

  void
  TearDown() override
  {
    for (const char* name : {"stdout", "stderr", "stdin"}) {
      unlink((directory + "/" + name).c_str());
    }
    rmdir(directory.c_str());
  }

  int
  process(const std::vector<std::string>& args, bool stdoutTerminal = false,
          bool stderrTerminal = false,
          const std::map<std::string, std::string>& overrides = {},
          const std::string& input = {})
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
    environment.erase("NO_COLOR");
    environment.erase("EOSAUTHZ");
    environment.erase("EOS_CONSOLE_DEBUG");
    environment["TERM"] = "xterm";
    environment["EOSAPP"] = "";
    environment["EOS_MGM_URL"] = "ipc://" + directory + "/unavailable-mgm";
    environment["EOS_PWD_FILE"] = "/dev/null";
    environment["EOS_HISTORY_FILE"] = "/dev/null";
    environment["INPUTRC"] = "/dev/null";
    // Readline measures redirected stdin, so give it the same size as the PTY.
    environment["COLUMNS"] = "10000";
    environment["LINES"] = "40";
    for (const auto& [key, value] : overrides) {
      environment[key] = value;
    }
    std::vector<std::string> envStrings;
    for (const auto& [key, value] : environment) {
      envStrings.push_back(key + "=" + value);
    }
    std::vector<char*> env;
    for (auto& value : envStrings) {
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
    const std::string inputPath = directory + "/stdin";
    {
      std::ofstream stream(inputPath);
      stream << input;
    }
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
    if (stdoutTerminal || stderrTerminal) {
      terminal.master = posix_openpt(O_RDWR | O_NOCTTY | O_NONBLOCK);
      if (terminal.master < 0 || grantpt(terminal.master) || unlockpt(terminal.master)) {
        ADD_FAILURE() << "Cannot allocate color test PTY";
        return -1;
      }
      const char* name = ptsname(terminal.master);
      if (!name || (terminal.slave = open(name, O_RDWR | O_NOCTTY)) < 0) {
        ADD_FAILURE() << "Cannot open color test PTY";
        return -1;
      }
      winsize dimensions{};
      dimensions.ws_row = 40;
      dimensions.ws_col = 10000;
      if (ioctl(terminal.slave, TIOCSWINSZ, &dimensions)) {
        ADD_FAILURE() << "Cannot size color test PTY";
        return -1;
      }
      termios attributes{};
      if (tcgetattr(terminal.slave, &attributes)) {
        ADD_FAILURE() << "Cannot read color test PTY attributes";
        return -1;
      }
      attributes.c_oflag &= ~ONLCR;
      if (tcsetattr(terminal.slave, TCSANOW, &attributes)) {
        ADD_FAILURE() << "Cannot configure color test PTY";
        return -1;
      }
    }
    posix_spawn_file_actions_t actions;
    EXPECT_EQ(posix_spawn_file_actions_init(&actions), 0);
    EXPECT_EQ(posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, inputPath.c_str(),
                                               O_RDONLY, 0),
              0);
    for (int descriptor : {STDOUT_FILENO, STDERR_FILENO}) {
      const bool tty = descriptor == STDOUT_FILENO ? stdoutTerminal : stderrTerminal;
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
        ADD_FAILURE() << "Console color test timed out";
        return -1;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    drain();
    output = stdoutTerminal ? "" : readFile(directory + "/stdout");
    error = stderrTerminal ? "" : readFile(directory + "/stderr");
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

  std::string directory, output, error, terminalOutput;
};
} // namespace

TEST_F(ConsoleColorProcessTest, RedirectedHelpIsExactUnlessExplicitlyForced)
{
  ASSERT_EQ(process({"--color=never", "help", "config"}), 0);
  const auto baseline = error;
  EXPECT_NE(baseline.find("--comment"), std::string::npos);
  ASSERT_EQ(process({"help", "config"}), 0);
  EXPECT_EQ(error, baseline);
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"--color=always", "help", "config"}), 0);
  EXPECT_NE(error.find('\033'), std::string::npos);
  EXPECT_EQ(plain(error), baseline);
}

TEST_F(ConsoleColorProcessTest, NamespaceHelpHonorsColorWithoutChangingItsContract)
{
  using eos::console::TextStyle;
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=&mgm.proc.stderr=&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  ASSERT_EQ(process({"--color=never", "ns", "--help"}, false, false, environment),
            EINVAL);
  const auto baseline = error;
  ASSERT_FALSE(baseline.empty());
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"--color=always", "ns", "--help"}, false, false, environment),
            EINVAL);
  EXPECT_NE(error.find('\033'), std::string::npos);
  EXPECT_EQ(plain(error), baseline);
  EXPECT_NE(error.find(styled("ns", TextStyle::Command) + " " +
                       styled("cache", TextStyle::Command) + " " +
                       styled("set", TextStyle::Command) +
                       styled("|", TextStyle::Syntax) +
                       styled("drop", TextStyle::Command)),
            std::string::npos);
  for (const char* command : {"master", "recompute_tree_size", "update_quotanode"}) {
    EXPECT_NE(error.find(styled(command, TextStyle::Command)), std::string::npos)
        << command;
  }
  for (const char* argument : {"<master_hostname>", "<path>", "<decimal_id>", "<hex_id>",
                               "<max_num>", "<id of file to drop>"}) {
    EXPECT_NE(error.find(styled(argument, TextStyle::Argument)), std::string::npos)
        << argument;
  }
  EXPECT_NE(error.find(styled("--depth", TextStyle::Option)), std::string::npos);
  EXPECT_NE(error.find("    master/slave operations. Option can be:\n"),
            std::string::npos);
  EXPECT_NE(error.find(styled("Note:", TextStyle::Warning) +
                       " for project quotas the uid values needs to be specified\n"),
            std::string::npos);
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"ns", "--help"}, false, true, environment), EINVAL);
  EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
  const auto interactiveHelp = plain(terminalOutput);
  EXPECT_NE(interactiveHelp.find("--help-all"), std::string::npos);
  EXPECT_NE(interactiveHelp.find("cache"), std::string::npos);
  auto noColor = environment;
  noColor["NO_COLOR"] = "1";
  ASSERT_EQ(process({"ns", "--help"}, false, true, noColor), EINVAL);
  EXPECT_EQ(terminalOutput, interactiveHelp);
  for (const char* mode : {"-b", "-j"}) {
    ASSERT_EQ(process({"--color=always", mode, "ns", "--help"}, false, true, environment),
              EINVAL);
    EXPECT_EQ(terminalOutput, baseline);
  }
  EXPECT_EQ(mgm.requests(), 0u);
}

TEST_F(ConsoleColorProcessTest, EveryRegisteredHelpTargetPreservesPlainAndMachineOutput)
{
  EnsureNativeCommandRegistryInitialized();
  // These commands intentionally have no detailed printHelp implementation.
  // In particular, `help license` must not print the license or execute a command.
  const std::set<std::string> emptyHelp{".q",      "daemon", "exit",  "json",
                                        "license", "pwd",    "quit",  "silent",
                                        "stat",    "status", "timing"};
  std::set<std::string> observedEmpty;
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=&mgm.proc.stderr=&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  for (const auto* command : CommandRegistry::instance().all()) {
    const std::string name = command->name();
    SCOPED_TRACE(name);
    ASSERT_EQ(process({"--color=never", "help", name}, false, false, environment), 0);
    const auto plainOutput = output;
    const auto plainError = error;
    const bool empty = plainOutput.empty() && plainError.empty();
    if (empty) {
      observedEmpty.insert(name);
    }
    ASSERT_EQ(process({"--color=always", "help", name}, false, false, environment), 0);
    EXPECT_EQ(plain(output), plainOutput);
    EXPECT_EQ(plain(error), plainError);
    if (empty) {
      EXPECT_TRUE(output.empty());
      EXPECT_TRUE(error.empty());
    } else {
      EXPECT_NE((output + error).find('\033'), std::string::npos)
          << "Nonempty help must use the shared styling policy";
    }
    for (const char* mode : {"-b", "-j"}) {
      SCOPED_TRACE(mode);
      ASSERT_EQ(
          process({"--color=always", mode, "help", name}, false, false, environment), 0);
      EXPECT_EQ(output, plainOutput);
      EXPECT_EQ(error, plainError);
    }
  }
  EXPECT_EQ(observedEmpty, emptyHelp);
  EXPECT_EQ(mgm.requests(), 0u);
}

TEST_F(ConsoleColorProcessTest, DirectHelpSpellingsCoverEverySafeCommandWithoutExecution)
{
  EnsureNativeCommandRegistryInitialized();
  // These are control commands or external launchers, not help grammars.
  // Query their printHelp through `help <command>` in the registry audit above.
  const std::set<std::string> controls{".q",     "?",       "exit",   "help",
                                       "json",   "license", "pwd",    "quit",
                                       "silent", "status",  "timing", "tui"};
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=&mgm.proc.stderr=&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  for (const auto* command : CommandRegistry::instance().all()) {
    const std::string name = command->name();
    if (controls.count(name)) {
      continue;
    }
    SCOPED_TRACE(name);
    for (const char* option : {"--help", "-h"}) {
      SCOPED_TRACE(option);
      const auto status =
          process({"--color=never", name, option}, false, false, environment);
      ASSERT_TRUE(status == 0 || status == EINVAL) << status;
      const auto plainOutput = output;
      const auto plainError = error;
      ASSERT_FALSE(plainOutput.empty() && plainError.empty());
      ASSERT_EQ(process({"--color=always", name, option}, false, false, environment),
                status);
      EXPECT_EQ(plain(output), plainOutput);
      EXPECT_EQ(plain(error), plainError);
      EXPECT_NE((output + error).find('\033'), std::string::npos);
    }
  }
  EXPECT_EQ(mgm.requests(), 0u);
}

TEST_F(ConsoleColorProcessTest, AutomaticHelpUsesStderrIndependentlyOfStdoutAndStdin)
{
  ASSERT_EQ(process({"help", "config"}, false, true), 0);
  EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
  ASSERT_EQ(process({"help", "config"}, true, false), 0);
  EXPECT_EQ(error.find('\033'), std::string::npos);
  EXPECT_TRUE(terminalOutput.empty());
}

TEST_F(ConsoleColorProcessTest, BatchJsonNoColorAndDumbTerminalDisableStyling)
{
  for (const auto& args : std::vector<std::vector<std::string>>{
           {"--color=always", "-b", "help", "config"},
           {"--color=always", "-j", "help", "config"}}) {
    ASSERT_EQ(process(args, true, true), 0);
    EXPECT_EQ(terminalOutput.find('\033'), std::string::npos);
  }
  for (const auto& environment : std::vector<std::map<std::string, std::string>>{
           {{"NO_COLOR", "1"}},
           {{"NO_COLOR", "0"}},
           {{"TERM", "dumb"}},
           {{"TERM", ""}},
           {{"TERM", "eos-nonexistent-terminal-test"}}}) {
    ASSERT_EQ(process({"help", "config"}, true, true, environment), 0);
    EXPECT_EQ(terminalOutput.find('\033'), std::string::npos);
  }
}

TEST_F(ConsoleColorProcessTest, ExplicitAlwaysOverridesNoColorForHumanHelp)
{
  ASSERT_EQ(process({"--color=always", "help", "config"}, false, false,
                    {{"NO_COLOR", "1"}, {"TERM", "dumb"}}),
            0);
  EXPECT_NE(error.find('\033'), std::string::npos);
}

TEST_F(ConsoleColorProcessTest, JsonToggleRestoresHumanStylingButDoesNotOverrideBatch)
{
  ASSERT_EQ(process({"--color=never", "help", "config"}), 0);
  const auto help = error;
  const std::string commands = "json\nhelp config\njson\nhelp config\nquit\n";
  ASSERT_EQ(process({"--color=always"}, true, false, {}, commands), 0);
  const std::string jsonHelp = "json=1\n" + help;
  ASSERT_GE(error.size(), jsonHelp.size());
  EXPECT_EQ(error.substr(0, jsonHelp.size()), jsonHelp);
  const auto humanHelp = error.substr(jsonHelp.size());
  EXPECT_NE(humanHelp.find('\033'), std::string::npos);
  EXPECT_EQ(plain(humanHelp), "json=0\n" + help);
  ASSERT_EQ(process({"--color=always", "-b"}, true, true, {}, commands), 0);
  EXPECT_EQ(terminalOutput.find('\033'), std::string::npos);
}

TEST_F(ConsoleColorProcessTest, RootCommandListPreservesTextAndAlignment)
{
  ASSERT_EQ(process({"--color=never", "help"}), 0);
  const auto baseline = error;
  ASSERT_EQ(process({"--color=always", "help"}), 0);
  EXPECT_NE(error.find('\033'), std::string::npos);
  EXPECT_EQ(plain(error), baseline);
}

TEST_F(ConsoleColorProcessTest, GlobalUsageKeepsItsTextAndExitStatus)
{
  ASSERT_EQ(process({"--color=never", "--help"}), 255);
  const auto baseline = error;
  ASSERT_EQ(process({"--color=always", "--help"}), 255);
  EXPECT_NE(error.find('\033'), std::string::npos);
  EXPECT_EQ(plain(error), baseline);
}

TEST_F(ConsoleColorProcessTest, CompletionProtocolRemainsPlainOnATerminal)
{
  ASSERT_EQ(process({"__complete", "--color", "always", "config", "s"}, true, true), 0);
  EXPECT_EQ(terminalOutput, "save\n");
  ASSERT_EQ(process({"__complete", "--color", "a"}, true, true), 0);
  EXPECT_EQ(terminalOutput, "always\nauto\n");
}

TEST_F(ConsoleColorProcessTest, InvalidColorReturnsErrorWithoutContactingMgm)
{
  EXPECT_EQ(process({"--color=purple", "help"}), EINVAL);
  EXPECT_NE(error.find("error:"), std::string::npos);
  EXPECT_EQ(error.find('\033'), std::string::npos);
}

TEST_F(ConsoleColorProcessTest, GlobalSyntaxErrorsRemainPlainBeforeAcceptingAPolicy)
{
  for (const auto& arguments : std::vector<std::vector<std::string>>{
           {"--color=never", "--comment", "", "help"},
           {"-j", "--comment", "", "help"},
           {"-b", "--color=always", "--comment", "", "help"},
           {"--color=always", "--color=purple", "help"}}) {
    ASSERT_EQ(process(arguments, true, true), EINVAL);
    EXPECT_NE(terminalOutput.find("error:"), std::string::npos);
    EXPECT_EQ(terminalOutput.find('\033'), std::string::npos);
  }
}

TEST_F(ConsoleColorProcessTest, LocalMetadataErrorsHonorTheAcceptedColorPolicy)
{
  const std::map<std::string, std::string> environment{{"EOSAPP", "invalid&app"}};
  ASSERT_EQ(process({"--color=never", "pwd"}, false, false, environment), EINVAL);
  const auto baseline = error;
  EXPECT_EQ(baseline, "error: EOSAPP cannot contain '&' or control characters\n");
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"--color=always", "pwd"}, false, false, environment), EINVAL);
  EXPECT_EQ(error, styled("error:", eos::console::TextStyle::Error) +
                       " EOSAPP cannot contain '&' or control characters\n");
  EXPECT_EQ(plain(error), baseline);
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"pwd"}, false, true, environment), EINVAL);
  EXPECT_EQ(terminalOutput, styled("error:", eos::console::TextStyle::Error) +
                                " EOSAPP cannot contain '&' or control characters\n");
  EXPECT_EQ(plain(terminalOutput), baseline);
  ASSERT_EQ(process({"pwd"}, false, false, environment), EINVAL);
  EXPECT_EQ(error, baseline);
  auto noColor = environment;
  noColor["NO_COLOR"] = "1";
  ASSERT_EQ(process({"pwd"}, false, true, noColor), EINVAL);
  EXPECT_EQ(terminalOutput, baseline);
  for (const auto& args :
       std::vector<std::vector<std::string>>{{"--color=never", "pwd"},
                                             {"--color=always", "-b", "pwd"},
                                             {"--color=always", "-j", "pwd"}}) {
    ASSERT_EQ(process(args, false, true, environment), EINVAL);
    EXPECT_EQ(terminalOutput, baseline);
    EXPECT_TRUE(output.empty());
  }
}

TEST_F(ConsoleColorProcessTest, JsonResponseIsParseableOnATerminalEvenWhenForced)
{
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout={\"status\":\"ok\",\"items\":[]}\n"
                   "&mgm.proc.stderr=&mgm.proc.retc=0");
  ASSERT_EQ(process({"--color=always", "-j", "ns", "stat"}, true, true,
                    {{"EOS_MGM_URL", "ipc://" + directory + "/mgm"}}),
            0);
  EXPECT_EQ(terminalOutput, "{\"status\":\"ok\",\"items\":[]}\n");
  Json::CharReaderBuilder reader;
  Json::Value value;
  std::string errors;
  std::istringstream stream(terminalOutput);
  EXPECT_TRUE(Json::parseFromStream(reader, stream, &value, &errors)) << errors;
  EXPECT_EQ(value["status"].asString(), "ok");
}

TEST_F(ConsoleColorProcessTest, ErrorColorPreservesMessageStreamAndExitStatus)
{
  ColorTestMgm mgm(
      directory + "/mgm",
      "mgm.proc.stdout=&mgm.proc.stderr=error: fixture failure&mgm.proc.retc=5");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  ASSERT_EQ(process({"--color=never", "ns", "stat"}, false, false, environment), EIO);
  const auto baseline = error;
  EXPECT_EQ(baseline, "error: fixture failure\n");
  ASSERT_EQ(process({"--color=always", "ns", "stat"}, false, false, environment), EIO);
  EXPECT_TRUE(output.empty());
  EXPECT_NE(error.find('\033'), std::string::npos);
  EXPECT_EQ(plain(error), baseline);
}

TEST_F(ConsoleColorProcessTest, ArbitraryServerTextIsNotRewrittenForColor)
{
  const std::string text = "[failed] filename=ok master-rw\n";
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=" + text + "&mgm.proc.stderr=&mgm.proc.retc=0");
  ASSERT_EQ(process({"--color=always", "who"}, false, false,
                    {{"EOS_MGM_URL", "ipc://" + directory + "/mgm"}}),
            0);
  EXPECT_EQ(output, text);
  EXPECT_TRUE(error.empty());
}

TEST_F(ConsoleColorProcessTest, PromptUsesReadlineMarkersWithoutLeakingThem)
{
  ASSERT_EQ(process({"--color=always"}, true, false, {}, "quit\n"), 0);
  EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
  EXPECT_EQ(terminalOutput.find('\001'), std::string::npos);
  EXPECT_EQ(terminalOutput.find('\002'), std::string::npos);
  ASSERT_EQ(process({"--color=never"}, true, false, {}, "quit\n"), 0);
  EXPECT_EQ(terminalOutput.find('\033'), std::string::npos);
}

TEST_F(ConsoleColorProcessTest, LongPromptKeepsBoundedTextAndCompleteColorReset)
{
  const std::string endpoint =
      "root://127.0.0.1/" + std::string(6000, 'x') + "/prompt-end";
  for (const char* locale : {"C", "C.UTF-8"}) {
    SCOPED_TRACE(locale);
    ASSERT_EQ(process({"--color=always"}, true, false,
                      {{"EOS_MGM_URL", endpoint}, {"LC_ALL", locale}}, "quit\n"),
              0);
    const std::string text = plain(terminalOutput);
    const auto start = text.find("root://127.0.0.1/");
    ASSERT_NE(start, std::string::npos);
    const auto end = text.find("/prompt-end", start);
    ASSERT_NE(end, std::string::npos);
    const std::string display =
        text.substr(start, end + std::string("/prompt-end").size() - start);
    EXPECT_NE(display.find("..."), std::string::npos);
    EXPECT_LT(text.size(), 4096u);
    EXPECT_NE(terminalOutput.find(styled(display, eos::console::TextStyle::Command)),
              std::string::npos);
    EXPECT_EQ(terminalOutput.find('\001'), std::string::npos);
    EXPECT_EQ(terminalOutput.find('\002'), std::string::npos);
  }
}
