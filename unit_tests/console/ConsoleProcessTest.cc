//------------------------------------------------------------------------------
//! @file ConsoleProcessTest.cc
//! @brief Exercise the real console entry point without an EOS instance.
//------------------------------------------------------------------------------

#include "common/StringConversion.hh"
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
#include <sys/stat.h>
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

TEST_F(ConsoleProcessTest, MissingGlobalValuesFailWithoutCrashOrRequest)
{
  for (const auto& args : std::vector<std::vector<std::string>>{{"-a"},
                                                                {"--app"},
                                                                {"-r"},
                                                                {"-r", "0"},
                                                                {"--role", "0"},
                                                                {"--unknown"},
                                                                {"-b", "evict", ""}}) {
    SCOPED_TRACE(::testing::PrintToString(args));
    EXPECT_EQ(process(args), EINVAL);
    noRequests();
  }
}

TEST_F(ConsoleProcessTest, InheritedUnsafeApplicationFailsBeforeDirectCopyTransport)
{
  for (const auto* value : {"x&eos.ruid=0", "x\nvalue"}) {
    EXPECT_EQ(process({"-b", "cp", "--help"}, {{"EOSAPP", value}}), EINVAL);
    noRequests();
    EXPECT_NE(lastOutput.find("EOSAPP"), std::string::npos);
  }
}

TEST_F(ConsoleProcessTest, GlobalOptionsAndOriginalArgvReachProtobufUnchanged)
{
  const std::string path = "/eos/O'Brien \"quotes\" \\backslash & literal";
  const std::string comment = "audit \"quote\" & x=y";
  ASSERT_EQ(process({"-j", "-a", "console test", "-b", "-r", "123", "456", "register",
                     path, "attr=user.key=a=b&c", "--comment", comment}),
            ENOENT)
      << lastOutput;
  const auto result = protobuf(true);
  EXPECT_EQ(result.record().path(), path);
  EXPECT_EQ(result.record().attr().at("user.key"), "a=b&c");
  EXPECT_EQ(result.comment(), comment);
  EXPECT_EQ(result.format(), eos::console::RequestProto::JSON);
  const std::string wire = request();
  EXPECT_NE(wire.find("&eos.ruid=123"), std::string::npos);
  EXPECT_NE(wire.find("&eos.rgid=456"), std::string::npos);
}

TEST_F(ConsoleProcessTest, AttrOriginalArgvReachesCgiWithoutInventedEscapes)
{
  for (const std::string& value :
       {"value\"quote", "\"leading", "\"", "value\\\"quote", "tail\\", "two words"}) {
    SCOPED_TRACE(value);
    ASSERT_EQ(
        process({"-b", "attr", "set", "user.attr.json4=" + value, "/eos/attribute"}),
        ENOENT)
        << lastOutput;
    const auto wire = request();
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    ASSERT_NE(env.Get("mgm.attr.value"), nullptr);
    std::string decoded;
    EXPECT_TRUE(eos::common::SymKey::DeBase64(env.Get("mgm.attr.value"), decoded));
    EXPECT_EQ(decoded, value);
  }
}

TEST_F(ConsoleProcessTest, ScriptSkipsBlankLinesAndRejectsNulBeforeDispatch)
{
  const auto path = tempDir + "/commands.eosh";
  {
    std::ofstream script(path, std::ios::binary);
    script << "\n  \t\nregister /eos/script\n";
  }
  EXPECT_EQ(process({"-b", path}), ENOENT);
  EXPECT_EQ(protobuf(true).record().path(), "/eos/script");
  {
    std::ofstream script(path, std::ios::binary | std::ios::trunc);
    std::string line = "register /eos/truncated";
    line.push_back('\0');
    line += " invalid\n";
    script.write(line.data(), line.size());
  }
  EXPECT_EQ(process({"-b", path}), EINVAL);
  noRequests();
  unlink(path.c_str());
}

TEST_F(ConsoleProcessTest, ScriptReadFailureReturnsError)
{
  const auto path = tempDir + "/directory.eosh";
  ASSERT_EQ(mkdir(path.c_str(), 0700), 0);
  EXPECT_EQ(process({"-b", path}), EIO);
  noRequests();
  rmdir(path.c_str());
}

TEST_F(ConsoleProcessTest, LocalListingPreservesArgvWithoutMgmPreflight)
{
  const auto path = tempDir + "/O'Brien \"quotes\" \\backslash";
  ASSERT_EQ(mkdir(path.c_str(), 0700), 0);
  EXPECT_EQ(process({"-b", "ls", "file:" + path}, {{"EOS_MGM_URL", "invalid://offline"}}),
            0);
  noRequests();
  rmdir(path.c_str());
}

TEST_F(ConsoleProcessTest, HelpAndShellCompletionDoNotContactMgm)
{
  EXPECT_EQ(process({"-b", "-r", "0", "0", "-a", "test", "-j", "evict", "--help"}),
            EINVAL);
  noRequests();
  EXPECT_EQ(process({"__complete", "eo"}), 0);
  noRequests();
}

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

TEST_F(ConsoleProcessTest, HealthHelpDoesNotContactMgm)
{
  for (const auto* option : {"--help", "-h"}) {
    SCOPED_TRACE(option);
    EXPECT_EQ(process({"-b", "health", option}), 0) << lastOutput;
    EXPECT_NE(lastOutput.find("Usage: eos health"), std::string::npos);
    noRequests();
  }
}

TEST_F(ConsoleProcessTest, MalformedRootListingReturnsAnErrorWithoutCrashing)
{
  for (const auto* path : {"root:malformed", "root:/malformed"}) {
    SCOPED_TRACE(path);
    EXPECT_EQ(process({"-b", "ls", path}), EINVAL) << lastOutput;
    noRequests();
  }
}

TEST_F(ConsoleProcessTest, AttributeValuesPreserveExecutableArguments)
{
  responseRetc = 0;
  for (const std::string& value :
       {std::string("value\"quote"), std::string("back\\slash"),
        std::string("value\\\"quote"), std::string("\"leading"),
        std::string("trailing\""), std::string("\"quoted\""), std::string("two words"),
        std::string(""), std::string("apostrophe's"), std::string("tab\tvalue")}) {
    SCOPED_TRACE(value);
    ASSERT_EQ(
        process({"-b", "attr", "set", "user.attr.value=" + value, "/eos/test/file"}), 0)
        << lastOutput;
    const auto wire = request();
    XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
    ASSERT_STREQ(env.Get("mgm.cmd"), "attr");
    ASSERT_STREQ(env.Get("mgm.subcmd"), "set");
    ASSERT_STREQ(env.Get("mgm.attr.key"), "user.attr.value");
    ASSERT_STREQ(env.Get("mgm.path"), "/eos/test/file");
    ASSERT_STREQ(env.Get("mgm.enc"), "b64");
    ASSERT_NE(env.Get("mgm.attr.value"), nullptr);
    std::string decoded;
    ASSERT_TRUE(eos::common::SymKey::DeBase64(env.Get("mgm.attr.value"), decoded));
    EXPECT_EQ(decoded, value);
  }
}

TEST_F(ConsoleProcessTest, AttributeCommentTextRemainsLiteralValueData)
{
  responseRetc = 0;
  const std::string value = "literal --comment \"quoted\" &mgm.option=p";
  ASSERT_EQ(process({"-b", "attr", "set", "user.attr.value=" + value, "/eos/test/file"}),
            0)
      << lastOutput;
  const auto wire = request();
  XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
  ASSERT_NE(env.Get("mgm.attr.value"), nullptr);
  std::string decoded;
  ASSERT_TRUE(eos::common::SymKey::DeBase64(env.Get("mgm.attr.value"), decoded));
  EXPECT_EQ(decoded, value);
  EXPECT_EQ(env.Get("mgm.comment"), nullptr);
  EXPECT_EQ(env.Get("mgm.option"), nullptr);
}

TEST_F(ConsoleProcessTest, ExecutableCommentPreservesLiteralValueAndCannotInjectOptions)
{
  responseRetc = 0;
  const std::string value = "value\\\"quote";
  const std::string comment = "audit \"quote\"\\line&mgm.option=p&eos.ruid=0";
  ASSERT_EQ(process({"-b", "attr", "set", "user.attr.value=" + value, "/eos/test/file",
                     "--comment", comment}),
            0)
      << lastOutput;
  const auto wire = request();
  XrdOucEnv env(wire.substr(wire.find('?') + 1).c_str());
  ASSERT_NE(env.Get("mgm.attr.value"), nullptr);
  std::string decoded;
  ASSERT_TRUE(eos::common::SymKey::DeBase64(env.Get("mgm.attr.value"), decoded));
  EXPECT_EQ(decoded, value);
  ASSERT_NE(env.Get("mgm.comment"), nullptr);
  EXPECT_EQ(eos::common::StringConversion::curl_unescaped(env.Get("mgm.comment")),
            comment);
  EXPECT_EQ(env.Get("mgm.option"), nullptr);
  EXPECT_EQ(env.Get("eos.ruid"), nullptr);
}

TEST_F(ConsoleProcessTest, MissingOrEmptyExecutableCommentIsRejectedLocally)
{
  for (const auto& arguments : std::vector<std::vector<std::string>>{
           {"-b", "attr", "set", "user.attr.value=value", "/eos/test/file", "--comment"},
           {"-b", "attr", "set", "user.attr.value=value", "/eos/test/file", "--comment",
            ""}}) {
    SCOPED_TRACE(::testing::PrintToString(arguments));
    EXPECT_EQ(process(arguments), EINVAL) << lastOutput;
    noRequests();
  }
}

TEST_F(ConsoleProcessTest, ProtobufPathsAndCommentsPreserveExecutableArguments)
{
  responseRetc = 0;
  const std::string path = "/eos/test/a b'c\"d\\e";
  const std::string comment = "audit \"quote\"\\line&mgm.option=p";
  ASSERT_EQ(process({"-b", "evict", path, "--comment", comment}), 0) << lastOutput;
  const auto proto = protobuf(false);
  ASSERT_TRUE(proto.has_evict());
  ASSERT_EQ(proto.evict().file_size(), 1);
  EXPECT_EQ(proto.evict().file(0).path(), path);
  EXPECT_EQ(proto.comment(), comment);
}

TEST_F(ConsoleProcessTest, CommentOptionAfterTerminatorRemainsAPositionalArgument)
{
  responseRetc = 0;
  ASSERT_EQ(process({"-b", "evict", "--", "/eos/test/one", "--comment", "/eos/test/two"}),
            0)
      << lastOutput;
  const auto proto = protobuf(false);
  ASSERT_TRUE(proto.has_evict());
  ASSERT_EQ(proto.evict().file_size(), 3);
  EXPECT_EQ(proto.evict().file(0).path(), "/eos/test/one");
  const auto& literalOption = proto.evict().file(1).path();
  const std::string suffix = "/--comment";
  ASSERT_GE(literalOption.size(), suffix.size());
  EXPECT_EQ(literalOption.substr(literalOption.size() - suffix.size()), suffix);
  EXPECT_EQ(proto.evict().file(2).path(), "/eos/test/two");
  EXPECT_TRUE(proto.comment().empty());
}

TEST_F(ConsoleProcessTest, LongPromptsRemainSafeInWideAndNarrowTerminals)
{
  const std::string endpoint =
      "root://127.0.0.1/" + std::string(65536, 'a') + "prompt-end";
  for (const auto* columns : {"4096", "2"}) {
    SCOPED_TRACE(columns);
    EXPECT_EQ(
        process({"-b"},
                {{"EOS_MGM_URL", endpoint}, {"COLUMNS", columns}, {"TERM", "dumb"}}),
        0)
        << lastOutput;
    if (std::string(columns) == "4096") {
      EXPECT_NE(lastOutput.find("EOS Console [root://127.0.0.1/"), std::string::npos);
      EXPECT_NE(lastOutput.find("..."), std::string::npos);
      EXPECT_NE(lastOutput.find("prompt-end] |/> "), std::string::npos);
      EXPECT_LT(lastOutput.size(), 4096u);
    }
    noRequests();
  }
}

TEST_F(ConsoleProcessTest, OrdinaryPromptsKeepTheirExistingDisplay)
{
  const std::string endpoint = "root://127.0.0.1/ordinary";
  EXPECT_EQ(
      process({"-b"}, {{"EOS_MGM_URL", endpoint}, {"COLUMNS", "4096"}, {"TERM", "dumb"}}),
      0)
      << lastOutput;
  EXPECT_NE(lastOutput.find("EOS Console [" + endpoint + "] |/> "), std::string::npos);
  noRequests();
}
