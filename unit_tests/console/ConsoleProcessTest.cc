//------------------------------------------------------------------------------
//! @file ConsoleProcessTest.cc
//! @brief Exercise the real console entry point without an EOS instance.
//------------------------------------------------------------------------------

#include "unit_tests/console/ConsoleTestFixture.hh"
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <set>
#include <signal.h>
#include <spawn.h>
#include <sstream>
#include <sys/wait.h>

extern char** environ;

namespace {
class ConsoleProcessTest : public ConsoleTestFixture {
protected:
  int
  process(const std::vector<std::string>& arguments,
          const std::map<std::string, std::string>& overrides = {},
          const std::string& program = {})
  {
    {
      std::lock_guard<std::mutex> lock(mutex);
      requests.clear();
    }
    std::map<std::string, std::string> environment;
    for (char** entry = environ; *entry; ++entry) {
      const std::string value(*entry);
      const auto separator = value.find('=');
      environment[value.substr(0, separator)] = value.substr(separator + 1);
    }
    environment.erase("EOSAUTHZ");
    environment.erase("EOS_CONSOLE_DEBUG");
    environment["EOS_MGM_URL"] = gGlobalOpts.mMgmUri;
    environment["EOSAPP"] = "";
    environment["EOS_PWD_FILE"] = tempDir + "/pwd";
    environment["EOS_HISTORY_FILE"] = tempDir + "/history";
    for (const auto& [key, value] : overrides) {
      environment[key] = value;
    }
    std::vector<std::string> envStrings;
    for (const auto& [key, value] : environment) {
      envStrings.push_back(key + "=" + value);
    }
    std::vector<char*> env;
    for (auto& entry : envStrings) {
      env.push_back(entry.data());
    }
    env.push_back(nullptr);
    const char* executable = program.empty() ? consoleExecutable() : program.c_str();
    std::vector<std::string> words{executable};
    words.insert(words.end(), arguments.begin(), arguments.end());
    std::vector<char*> argv;
    for (auto& word : words) {
      argv.push_back(word.data());
    }
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    EXPECT_EQ(posix_spawn_file_actions_init(&actions), 0);
    EXPECT_EQ(posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null",
                                               O_RDONLY, 0),
              0);
    const std::string output = tempDir + "/process-output";
    EXPECT_EQ(posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, output.c_str(),
                                               O_WRONLY | O_CREAT | O_TRUNC, 0600),
              0);
    EXPECT_EQ(posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO),
              0);
    pid_t child = 0;
    const int spawned =
        posix_spawn(&child, executable, &actions, nullptr, argv.data(), env.data());
    posix_spawn_file_actions_destroy(&actions);
    EXPECT_EQ(spawned, 0);
    if (spawned) {
      return -1;
    }
    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    pid_t waited = 0;
    while ((waited = waitpid(child, &status, WNOHANG)) == 0) {
      if (std::chrono::steady_clock::now() >= deadline) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
        ADD_FAILURE() << "console process timed out";
        return -1;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(waited, child);
    if (waited != child) {
      return -1;
    }
    std::ifstream captured(output);
    lastOutput.assign(std::istreambuf_iterator<char>(captured), {});
    unlink(output.c_str());
    EXPECT_TRUE(WIFEXITED(status));
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  }

  const char*
  consoleExecutable() const
  {
    return access(EOS_CONSOLE_EXECUTABLE, X_OK) == 0 ? EOS_CONSOLE_EXECUTABLE
                                                     : EOS_CONSOLE_INSTALLED_EXECUTABLE;
  }

  int
  bashCompletion(const std::string& line, const std::vector<std::string>& words,
                 size_t currentWord)
  {
    const char* completionScript = access(EOS_CONSOLE_COMPLETION_SCRIPT, R_OK) == 0
                                       ? EOS_CONSOLE_COMPLETION_SCRIPT
                                       : "/etc/bash_completion.d/eos";
    if (access(completionScript, R_OK) != 0) {
      ADD_FAILURE() << "EOS Bash completion script is unavailable: "
                    << EOS_CONSOLE_COMPLETION_SCRIPT << " or " << completionScript;
      return -1;
    }
    // The shell program is fixed. Command lines and completion words are data
    // passed through argv; evaluating them would invalidate the quoting test.
    static const std::string script = R"bash(
unset _EOS_BASH_COMPLETION_LOADED
source "$1" || exit
eos_binary=$2
cd -- "$3" || exit
COMP_LINE=$4
COMP_POINT=${#COMP_LINE}
COMP_CWORD=$5
COMP_WORDBREAKS=$' \t\n"\'@><=;|&(:'
shift 5
COMP_WORDS=("$@")
eos() { "$eos_binary" "$@"; }
_eos
if ((${#COMPREPLY[@]})); then
  printf '%s\n' "${COMPREPLY[@]}"
fi
)bash";
    std::vector<std::string> arguments{"--noprofile",
                                       "--norc",
                                       "-c",
                                       script,
                                       "eos-completion-test",
                                       completionScript,
                                       consoleExecutable(),
                                       tempDir,
                                       line,
                                       std::to_string(currentWord)};
    arguments.insert(arguments.end(), words.begin(), words.end());
    return process(arguments, {}, "/bin/bash");
  }

  void
  noRequests()
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_TRUE(requests.empty());
  }

  std::set<std::string>
  outputLines() const
  {
    std::istringstream output(lastOutput);
    std::set<std::string> lines;
    std::string line;
    while (std::getline(output, line)) {
      lines.insert(line);
    }
    return lines;
  }
  std::string lastOutput;
};
} // namespace

TEST_F(ConsoleProcessTest, UnknownChildrenAndTerminatedOptionsDoNotFallBackToFlags)
{
  for (const auto* name : {"ns", "monit", "group", "config"}) {
    SCOPED_TRACE(name);
    EXPECT_EQ(process({"__complete", name, "unknown-child", ""}), 0);
    EXPECT_TRUE(lastOutput.empty()) << lastOutput;
    noRequests();
    EXPECT_EQ(process({"__complete", name, "--", "--"}), 0);
    EXPECT_TRUE(lastOutput.empty()) << lastOutput;
    noRequests();
  }
}

TEST_F(ConsoleProcessTest, BashHandlesLocalFileSchemeAndQuotedOrEscapedSpaces)
{
  const std::string name = "alpha file";
  const auto path = tempDir + "/" + name;
  {
    std::ofstream file(path);
    ASSERT_TRUE(file.good());
  }
  const auto prefix = tempDir + "/al";
  EXPECT_EQ(
      bashCompletion("eos ls file:" + prefix, {"eos", "ls", "file", ":", prefix}, 4), 0)
      << lastOutput;
  EXPECT_EQ(outputLines(), (std::set<std::string>{path}));
  noRequests();
  for (const auto& line : {"eos cp './alpha f", "eos cp ./alpha\\ f"}) {
    EXPECT_EQ(bashCompletion(line, {"eos", "cp", "./alpha f"}, 2), 0) << lastOutput;
    EXPECT_EQ(outputLines(), (std::set<std::string>{"./" + name}));
    noRequests();
  }
  unlink(path.c_str());
}
