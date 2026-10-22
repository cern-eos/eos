// ----------------------------------------------------------------------
// File: ConsoleColorProcessTest.cc
// ----------------------------------------------------------------------

#include "common/SymKeys.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleCommandHelp.hh"
#include "proto/ConsoleRequest.pb.h"
#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <future>
#include <json/json.h>
#include <map>
#include <mutex>
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
                {
                  std::lock_guard<std::mutex> lock(mMutex);
                  mWireRequests.emplace_back(static_cast<const char*>(request.data()),
                                             request.size());
                }
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

  eos::console::RequestProto
  request(size_t index) const
  {
    std::string wire;
    {
      std::lock_guard<std::mutex> lock(mMutex);
      EXPECT_LT(index, mWireRequests.size());
      if (index < mWireRequests.size()) {
        wire = mWireRequests[index];
      }
    }
    eos::console::RequestProto proto;
    const auto query = wire.find('?');
    EXPECT_NE(query, std::string::npos) << wire;
    if (query == std::string::npos) {
      return proto;
    }
    XrdOucEnv env(wire.substr(query + 1).c_str());
    const char* encoded = env.Get("mgm.cmd.proto");
    EXPECT_NE(encoded, nullptr) << wire;
    if (encoded) {
      std::string bytes;
      EXPECT_TRUE(eos::common::SymKey::Base64Decode(encoded, bytes));
      EXPECT_TRUE(proto.ParseFromString(bytes));
    }
    return proto;
  }

private:
  std::string mPath;
  mutable std::mutex mMutex;
  std::vector<std::string> mWireRequests;
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
  ASSERT_EQ(process({"--color=never", "ns", "--help-all"}, false, false, environment), 0);
  const auto baseline = error;
  ASSERT_FALSE(baseline.empty());
  EXPECT_TRUE(output.empty());
  ASSERT_EQ(process({"--color=always", "ns", "--help-all"}, false, false, environment),
            0);
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
  ASSERT_EQ(process({"ns", "--help"}, false, true, environment), 0);
  EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
  const auto interactiveHelp = plain(terminalOutput);
  EXPECT_NE(interactiveHelp.find("--help"), std::string::npos);
  EXPECT_NE(interactiveHelp.find("cache"), std::string::npos);
  auto noColor = environment;
  noColor["NO_COLOR"] = "1";
  ASSERT_EQ(process({"ns", "--help"}, false, true, noColor), 0);
  EXPECT_EQ(terminalOutput, interactiveHelp);
  ASSERT_EQ(process({"--color=never", "ns", "--help"}, false, false, environment), 0);
  const auto plainIndex = error;
  for (const char* mode : {"-b", "-j"}) {
    ASSERT_EQ(process({"--color=always", mode, "ns", "--help"}, false, true, environment),
              0);
    EXPECT_EQ(terminalOutput, plainIndex);
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
      EXPECT_TRUE(emptyHelp.count(name));
      EXPECT_FALSE(eos::console::HasStructuredCommandHelp(*command));
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
  EXPECT_LE(observedEmpty.size(), emptyHelp.size());
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

TEST_F(ConsoleColorProcessTest, IoShapingExplicitJsonKeepsAllFourReplyContracts)
{
  const std::string payload = "[ {\"type\":\"fixture\",\"id\":\"byte-exact-json\"} ]\n";
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=" + payload +
                       "&mgm.proc.stderr=fixture note&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  for (const auto& command : std::vector<std::vector<std::string>>{
           {"ls"}, {"policy", "ls"}, {"pressure", "ls"}, {"config", "ls"}}) {
    auto args = std::vector<std::string>{"--color=always", "io", "shaping"};
    args.insert(args.end(), command.begin(), command.end());
    args.push_back("--json");
    const auto index = mgm.requests();
    ASSERT_EQ(process(args, false, false, environment), 0);
    EXPECT_EQ(output, payload);
    EXPECT_EQ(error, styled("fixture note\n", eos::console::TextStyle::Error));
    EXPECT_EQ(mgm.requests(), index + 1);
    const auto request = mgm.request(index);
    const auto& shaping = request.io().shaping();
    if (command.front() == "ls") {
      EXPECT_TRUE(shaping.list().json_output());
      EXPECT_FALSE(shaping.list().resolve_ids());
    } else if (command.front() == "policy") {
      EXPECT_TRUE(shaping.policy().list().json_output());
    } else if (command.front() == "pressure") {
      EXPECT_TRUE(shaping.pressure().list().json_output());
    } else {
      EXPECT_TRUE(shaping.config().list().json_output());
    }
    ASSERT_EQ(process(args, true, false, environment), 0);
    EXPECT_EQ(terminalOutput, payload);
    EXPECT_EQ(error, styled("fixture note\n", eos::console::TextStyle::Error));
    EXPECT_EQ(mgm.requests(), index + 2);
  }
}

TEST_F(ConsoleColorProcessTest, GlobalJsonAndMonitoringIoKeepTheirOriginalBytes)
{
  const std::string payload = "fixture.key=123 fixture.value=unchanged\n";
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=" + payload +
                       "&mgm.proc.stderr=fixture note&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  for (const auto& command :
       std::vector<std::vector<std::string>>{{"-j", "io", "shaping", "ls"},
                                             {"-j", "io", "shaping", "policy", "ls"},
                                             {"-j", "io", "shaping", "pressure", "ls"},
                                             {"-j", "io", "shaping", "config", "ls"},
                                             {"io", "stat", "-m"},
                                             {"io", "ns", "-m"},
                                             {"io", "stat"}}) {
    auto args = std::vector<std::string>{"--color=always"};
    args.insert(args.end(), command.begin(), command.end());
    const auto index = mgm.requests();
    ASSERT_EQ(process(args, false, false, environment), 0);
    EXPECT_EQ(output, payload);
    EXPECT_EQ(error, command.front() == "-j"
                         ? "fixture note\n"
                         : styled("fixture note\n", eos::console::TextStyle::Error));
    EXPECT_EQ(mgm.requests(), index + 1);
    const auto request = mgm.request(index);
    if (command.front() == "-j") {
      EXPECT_EQ(request.format(), eos::console::RequestProto::JSON);
      EXPECT_TRUE(request.dontcolor());
    }
  }
  for (const char* operation : {"stat", "ns"}) {
    ASSERT_EQ(
        process({"--color=always", "io", operation, "-m"}, true, false, environment), 0);
    EXPECT_EQ(terminalOutput, payload);
    EXPECT_EQ(error, styled("fixture note\n", eos::console::TextStyle::Error));
  }
}

TEST_F(ConsoleColorProcessTest, HumanIoShapingUsesOneJsonRequestAndPreservesSelections)
{
  const std::string payload =
      "[{\"type\":\"uid\",\"id\":\"1001\",\"name\":\"resolved-user-marker\","
      "\"window_sec\":300,\"read_rate_bps\":12000000.0,\"write_rate_bps\":0.0,"
      "\"read_iops\":4.0,\"write_iops\":0.0}]\n";
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=" + payload + "&mgm.proc.stderr=&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  for (const char* resolution : {"--resolve-ids", "--no-resolve-ids"}) {
    const auto index = mgm.requests();
    ASSERT_EQ(process({"--color=never", "io", "shaping", "ls", "--users", "--sys",
                       "--window", "300", resolution},
                      false, false, environment),
              0);
    EXPECT_NE(output.find("1001"), std::string::npos);
    EXPECT_NE(output.find("resolved-user-marker"), std::string::npos);
    EXPECT_NE(output.find("MB/s"), std::string::npos);
    EXPECT_TRUE(error.empty());
    EXPECT_EQ(mgm.requests(), index + 1);
    const auto request = mgm.request(index);
    EXPECT_TRUE(request.dontcolor());
    EXPECT_EQ(request.format(), eos::console::RequestProto::DEFAULT);
    const auto& list = request.io().shaping().list();
    EXPECT_TRUE(list.json_output());
    EXPECT_TRUE(list.show_users());
    EXPECT_FALSE(list.show_apps());
    EXPECT_TRUE(list.system_stats());
    EXPECT_EQ(list.time_window_seconds(), 300u);
    EXPECT_TRUE(list.has_resolve_ids());
    EXPECT_EQ(list.resolve_ids(), std::string(resolution) == "--resolve-ids");
  }
}

TEST_F(ConsoleColorProcessTest, IoShapingHumanColorFollowsTheDestinationPolicy)
{
  const std::string payload =
      "[{\"type\":\"app\",\"id\":\"color-app-marker\",\"window_sec\":60,"
      "\"read_rate_bps\":12000000.0,\"write_rate_bps\":0.0,"
      "\"read_iops\":4.0,\"write_iops\":0.0}]\n";
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=" + payload + "&mgm.proc.stderr=&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  ASSERT_EQ(process({"io", "shaping", "ls"}, false, false, environment), 0);
  const auto baseline = output;
  EXPECT_EQ(baseline.find('\033'), std::string::npos);
  ASSERT_EQ(process({"--color=always", "io", "shaping", "ls"}, false, false, environment),
            0);
  EXPECT_NE(output.find('\033'), std::string::npos);
  EXPECT_EQ(plain(output), baseline);
  const auto expectRoles = [](const std::string& text) {
    using eos::console::TextStyle;
    EXPECT_NE(text.find(styled("Application", TextStyle::EntityType)), std::string::npos);
    EXPECT_NE(text.find(styled("Read", TextStyle::Read)), std::string::npos);
    EXPECT_NE(text.find(styled("Write", TextStyle::Write)), std::string::npos);
    EXPECT_NE(text.find(styled("12.00 MB/s", TextStyle::Read)), std::string::npos);
    EXPECT_NE(text.find(styled("0.00 B/s", TextStyle::Write)), std::string::npos);
  };
  expectRoles(output);
  ASSERT_EQ(process({"io", "shaping", "ls"}, true, false, environment), 0);
  EXPECT_NE(terminalOutput.find('\033'), std::string::npos);
  EXPECT_EQ(plain(terminalOutput), baseline);
  expectRoles(terminalOutput);
  for (const auto& override : std::vector<std::map<std::string, std::string>>{
           {{"NO_COLOR", "1"}}, {{"TERM", "dumb"}}}) {
    auto options = environment;
    options.insert(override.begin(), override.end());
    ASSERT_EQ(process({"io", "shaping", "ls"}, true, false, options), 0);
    EXPECT_EQ(terminalOutput, baseline);
  }
  ASSERT_EQ(
      process({"--color=always", "-b", "io", "shaping", "ls"}, true, false, environment),
      0);
  EXPECT_EQ(terminalOutput, baseline);
}

TEST_F(ConsoleColorProcessTest, IoShapingInvalidRepliesAndFailuresAreNotRetried)
{
  for (const auto& payload :
       std::vector<std::string>{"legacy server table\n", "{malformed json\n",
                                "[{\"type\":\"future-schema\",\"new-field\":123}]\n"}) {
    ColorTestMgm mgm(directory + "/mgm",
                     "mgm.proc.stdout=" + payload + "&mgm.proc.stderr=&mgm.proc.retc=0");
    ASSERT_EQ(process({"--color=always", "io", "shaping", "ls"}, false, false,
                      {{"EOS_MGM_URL", "ipc://" + directory + "/mgm"}}),
              0);
    EXPECT_EQ(output, payload);
    EXPECT_TRUE(error.empty());
    EXPECT_EQ(mgm.requests(), 1u);
  }
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=raw failure details&mgm.proc.stderr=error: fixture "
                   "failure&mgm.proc.retc=5");
  ASSERT_EQ(process({"--color=always", "io", "shaping", "ls"}, false, false,
                    {{"EOS_MGM_URL", "ipc://" + directory + "/mgm"}}),
            EIO);
  EXPECT_EQ(output, "raw failure details\n");
  EXPECT_EQ(plain(error), "error: fixture failure\n");
  EXPECT_EQ(mgm.requests(), 1u);
}

TEST_F(ConsoleColorProcessTest, IoShapingMutationsSendOnlyTheRequestedOperation)
{
  ColorTestMgm mgm(directory + "/mgm",
                   "mgm.proc.stdout=success: fixture operation complete"
                   "&mgm.proc.stderr=&mgm.proc.retc=0");
  const std::map<std::string, std::string> environment{
      {"EOS_MGM_URL", "ipc://" + directory + "/mgm"}};
  const std::vector<std::vector<std::string>> operations{
      {"enable"},
      {"disable"},
      {"policy", "set", "--app", "fixture", "--limit-read", "1M"},
      {"policy", "rm", "--uid", "1001"},
      {"config", "set", "--limits", "disabled"}};
  for (size_t index = 0; index < operations.size(); ++index) {
    auto args = std::vector<std::string>{"--color=always", "io", "shaping"};
    args.insert(args.end(), operations[index].begin(), operations[index].end());
    ASSERT_EQ(process(args, false, false, environment), 0);
    EXPECT_EQ(plain(output), "success: fixture operation complete\n");
    EXPECT_TRUE(error.empty());
    EXPECT_EQ(mgm.requests(), index + 1);
    const auto request = mgm.request(index);
    const auto& shaping = request.io().shaping();
    EXPECT_FALSE(shaping.has_list());
    EXPECT_FALSE(shaping.has_pressure());
    if (index == 0) {
      EXPECT_TRUE(shaping.has_enable());
    }
    if (index == 1) {
      EXPECT_TRUE(shaping.has_disable());
    }
    if (index == 2) {
      EXPECT_TRUE(shaping.policy().has_set());
    }
    if (index == 3) {
      EXPECT_TRUE(shaping.policy().has_remove());
    }
    if (index == 4) {
      EXPECT_TRUE(shaping.config().has_set());
    }
  }
}
