//------------------------------------------------------------------------------
//! @file ConsoleCompletion.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "ConsoleCompletion.hh"
#include "CommandFramework.hh"
#include "ConsoleMain.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucTokenizer.hh>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <memory>
#include <readline/history.h>
#include <readline/readline.h>
#include <string.h>
#include <vector>

namespace {
std::vector<std::string> gWordCompletionCandidates;

bool starts_with(const std::string& value, const std::string& prefix);
std::vector<std::string> dedupe_and_sort(std::vector<std::string> candidates);

struct CompletionLine {
  std::vector<std::string> preceding;
  std::string word;
  bool started = false;
};

CompletionLine
tokenize_completion_line(const std::string& input)
{
  CompletionLine result;
  char quote = 0;
  for (size_t i = 0; i < input.size(); ++i) {
    const char current = input[i];
    if (current == '\\' && quote != '\'') {
      if (i + 1 < input.size() &&
          (quote == 0 || input[i + 1] == '"' || input[i + 1] == '\\')) {
        result.word.push_back(input[++i]);
      } else {
        result.word.push_back(current);
      }
      result.started = true;
    } else if ((current == '\'' || current == '"') && (quote == 0 || quote == current)) {
      quote = quote ? 0 : current;
      result.started = true;
    } else if (!quote && std::isspace(static_cast<unsigned char>(current))) {
      if (result.started) {
        result.preceding.push_back(std::move(result.word));
      }
      result.word.clear();
      result.started = false;
    } else {
      result.word.push_back(current);
      result.started = true;
    }
  }
  return result;
}

bool
starts_with(const std::string& value, const std::string& prefix)
{
  return value.compare(0, prefix.size(), prefix) == 0;
}

std::vector<std::string>
dedupe_and_sort(std::vector<std::string> candidates)
{
  std::sort(candidates.begin(), candidates.end());
  candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());
  return candidates;
}

bool
ends_with(const std::string& value, const std::string& suffix)
{
  if (suffix.size() > value.size()) {
    return false;
  }

  return value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

std::vector<std::string>
filter_candidates(const std::vector<std::string>& candidates, const std::string& prefix)
{
  std::vector<std::string> filtered;

  for (const auto& candidate : candidates) {
    if (!candidate.empty() && starts_with(candidate, prefix)) {
      filtered.push_back(candidate);
    }
  }

  return dedupe_and_sort(std::move(filtered));
}

std::vector<std::string>
global_option_candidates()
{
  return {"--app", "--batch", "--help", "--json", "--role", "--version", "-a",
          "-b",    "-h",      "-j",     "-r",     "-s",     "-v"};
}

size_t
find_command_token_index(const std::vector<std::string>& tokens)
{
  for (size_t i = 0; i < tokens.size(); ++i) {
    const auto& token = tokens[i];

    if (token.empty()) {
      continue;
    }

    if (token == "-a" || token == "--app") {
      ++i;
      continue;
    }

    if (token == "-r" || token == "--role") {
      i += 2;
      continue;
    }

    if (token == "-b" || token == "--batch" || token == "-h" || token == "--help" ||
        token == "-j" || token == "--json" || token == "-s" || token == "-v" ||
        token == "--version") {
      continue;
    }

    if (token.rfind("root://", 0) == 0 || token.rfind("ipc://", 0) == 0) {
      continue;
    }

    if (!token.empty() && token[0] == '-') {
      continue;
    }

    return i;
  }

  return tokens.size();
}

bool
is_path_identifier_token(const std::string& token)
{
  return token.rfind("fid:", 0) == 0 || token.rfind("fxid:", 0) == 0 ||
         token.rfind("cid:", 0) == 0 || token.rfind("cxid:", 0) == 0 ||
         token.rfind("pid:", 0) == 0 || token.rfind("pxid:", 0) == 0;
}

bool
is_eos_path_prefix(const std::string& token)
{
  return token == "/eos" || token.rfind("/eos/", 0) == 0;
}

struct CompletionClientState {
  XrdOucString serverUri;
  XrdOucString userRole;
  XrdOucString groupRole;
  bool jsonOutput;
};

class ScopedCompletionClientState {
public:
  ScopedCompletionClientState()
      : mSaved{serveruri, user_role, group_role, json}
  {
  }

  ~ScopedCompletionClientState()
  {
    serveruri = mSaved.serverUri;
    user_role = mSaved.userRole;
    group_role = mSaved.groupRole;
    json = mSaved.jsonOutput;
  }

private:
  CompletionClientState mSaved;
};

void
apply_completion_client_context(const std::vector<std::string>& tokens)
{
  for (size_t i = 0; i < tokens.size(); ++i) {
    const auto& token = tokens[i];

    if ((token == "-r" || token == "--role") && i + 2 < tokens.size()) {
      user_role = tokens[i + 1].c_str();
      group_role = tokens[i + 2].c_str();
      i += 2;
      continue;
    }

    if (token.rfind("root://", 0) == 0) {
      serveruri = token.c_str();
      continue;
    }

    if (token.rfind("ipc://", 0) == 0) {
      if (token == "ipc://") {
        serveruri = "ipc:///var/eos/md/.admin_socket:1094";
      } else {
        serveruri = token.c_str();
      }
      continue;
    }
  }

  json = false;
}

std::vector<std::string>
decode_env_lines(XrdOucEnv* env, const char* key)
{
  std::vector<std::string> lines;

  if (!env) {
    return lines;
  }

  XrdOucString value = env->Get(key);

  if (!value.length()) {
    return lines;
  }

  if (value.beginswith("base64:")) {
    XrdOucString decoded;
    eos::common::SymKey::DeBase64(value, decoded);
    value = decoded;
  } else {
    eos::common::StringConversion::UnSeal(value);
  }

  XrdOucTokenizer tokenizer((char*)value.c_str());
  const char* line = nullptr;

  while ((line = tokenizer.GetLine())) {
    std::string candidate = line;

    if (!candidate.empty() && candidate.back() == '\n') {
      candidate.pop_back();
    }

    if (!candidate.empty()) {
      lines.push_back(std::move(candidate));
    }
  }

  return lines;
}

std::vector<std::string>
list_remote_path_candidates(const std::vector<std::string>& tokens,
                            const std::string& lookupDir,
                            const std::string& displayPrefix, const std::string& basename,
                            EosShellPathCompletionMode mode)
{
  ScopedCompletionClientState stateGuard;
  apply_completion_client_context(tokens);

  XrdOucString escaped =
      eos::common::StringConversion::curl_escaped(lookupDir.c_str()).c_str();
  XrdOucString in = "mgm.cmd=ls&mgm.path=";
  in += escaped;
  in += "&eos.encodepath=1&mgm.option=-F";

  std::unique_ptr<XrdOucEnv> env(client_command(in, false, nullptr));
  auto entries = decode_env_lines(env.get(), "mgm.proc.stdout");
  CommandEnv = nullptr;
  std::vector<std::string> candidates;

  for (const auto& entry : entries) {
    const bool isDir = !entry.empty() && entry.back() == '/';

    if (mode == EosShellPathCompletionMode::Directories && !isDir) {
      continue;
    }

    if (!basename.empty() && !starts_with(entry, basename)) {
      continue;
    }

    candidates.push_back(displayPrefix + entry);
  }

  return dedupe_and_sort(std::move(candidates));
}
} // namespace

//------------------------------------------------------------------------------
// Tokenize the command prefix before the cursor while honoring simple quoting.
//------------------------------------------------------------------------------
std::vector<std::string>
eos_completion_tokenize_prefix(const std::string& input)
{
  auto result = tokenize_completion_line(input);
  if (result.started) {
    result.preceding.push_back(std::move(result.word));
  }
  return result.preceding;
}

EosShellPathCompletionMode
eos_shell_path_completion_mode(const std::string& commandName,
                               const std::vector<std::string>& args,
                               const std::string& currentWord)
{
  EnsureNativeCommandRegistryInitialized();
  const auto* command = CommandRegistry::instance().find(commandName);
  if (!command || (!currentWord.empty() && currentWord.front() == '-') ||
      is_path_identifier_token(currentWord) ||
      currentWord.find("://") != std::string::npos || currentWord.rfind("as3:", 0) == 0) {
    return EosShellPathCompletionMode::None;
  }
  const auto kind = command->completion(args).path;
  if (kind == CompletionPath::EosDirectory ||
      (kind == CompletionPath::AnyDirectory &&
       (currentWord.empty() || is_eos_path_prefix(currentWord)))) {
    return EosShellPathCompletionMode::Directories;
  }
  if (kind == CompletionPath::Eos ||
      (kind == CompletionPath::EosOrFile && !starts_with(currentWord, "file:")) ||
      (kind == CompletionPath::Any &&
       (currentWord.empty() || is_eos_path_prefix(currentWord)))) {
    return EosShellPathCompletionMode::Any;
  }
  return EosShellPathCompletionMode::None;
}

void
eos_shell_resolve_rooted_path_input(const std::string& currentWord,
                                    std::string& lookupDir, std::string& displayPrefix,
                                    std::string& basename)
{
  if (currentWord.empty()) {
    lookupDir = abspath(".");
    if (!ends_with(lookupDir, "/")) {
      lookupDir += "/";
    }
    displayPrefix.clear();
    basename.clear();
    return;
  }

  if (currentWord == "/eos") {
    lookupDir = "/";
    displayPrefix = "/";
    basename = "eos";
    return;
  }

  eos_path_split(currentWord, displayPrefix, basename);

  if (currentWord.back() == '/') {
    basename.clear();
    lookupDir = abspath(currentWord.c_str());
  } else if (!displayPrefix.empty()) {
    lookupDir = abspath(displayPrefix.c_str());
  } else {
    lookupDir = abspath(".");
  }

  if (lookupDir.empty()) {
    lookupDir = "/eos/";
  }

  if (!ends_with(lookupDir, "/")) {
    lookupDir += "/";
  }
}

namespace {
std::vector<std::string>
local_path_candidates(const std::string& word, bool directories, bool fileScheme)
{
  std::string input = word;
  std::string scheme;
  if (fileScheme && starts_with(input, "file:")) {
    scheme = "file:";
    input.erase(0, scheme.size());
  }
  std::string display, basename;
  eos_path_split(input, display, basename);
  std::string lookup = display.empty() ? "." : display;
  if (starts_with(lookup, "~/")) {
    const auto* home = getenv("HOME");
    if (!home) {
      return {};
    }
    lookup.replace(0, 1, home);
  }
  std::error_code error;
  std::vector<std::string> result;
  const std::filesystem::directory_iterator end;
  for (std::filesystem::directory_iterator it(lookup, error); !error && it != end;
       it.increment(error)) {
    const auto name = it->path().filename().string();
    if (!starts_with(name, basename) || (!name.empty() && name.front() == '.' &&
                                         (basename.empty() || basename.front() != '.'))) {
      continue;
    }
    std::error_code typeError;
    const bool directory = it->is_directory(typeError);
    if (directories && (!directory || typeError)) {
      continue;
    }
    result.push_back(scheme + display + name + (directory ? "/" : ""));
  }
  return dedupe_and_sort(std::move(result));
}

std::vector<std::string>
completion_candidates(const std::vector<std::string>& precedingTokens,
                      const std::string& currentWord, bool readline)
{
  EnsureNativeCommandRegistryInitialized();
  const auto commandIndex = find_command_token_index(precedingTokens);
  if (commandIndex >= precedingTokens.size()) {
    if (!precedingTokens.empty()) {
      // A global option value is pending; it is not a command position.
      for (size_t i = 0; i < precedingTokens.size(); ++i) {
        const auto& token = precedingTokens[i];
        const size_t required = token == "-r" || token == "--role"  ? 2
                                : token == "-a" || token == "--app" ? 1
                                                                    : 0;
        if (required && i + required >= precedingTokens.size()) {
          return {};
        }
        i += required;
      }
    }
    std::vector<std::string> candidates = global_option_candidates();
    for (const auto* command : CommandRegistry::instance().all()) {
      candidates.emplace_back(command->name());
    }
    return filter_candidates(candidates, currentWord);
  }

  const auto& commandName = precedingTokens[commandIndex];
  const auto* command = CommandRegistry::instance().find(commandName);
  if (!command) {
    return {};
  }
  std::vector<std::string> args(precedingTokens.begin() + commandIndex + 1,
                                precedingTokens.end());
  std::string word = currentWord;
  std::string valuePrefix;
  const auto equals = word.find('=');
  if (!word.empty() && word.front() == '-' && equals != std::string::npos) {
    args.push_back(word.substr(0, equals));
    valuePrefix = word.substr(0, equals + 1);
    word.erase(0, equals + 1);
  }
  const auto context = command->completion(args);
  auto candidates = filter_candidates(context.candidates, word);
  if (context.path != CompletionPath::None && (word.empty() || word.front() != '-') &&
      !is_path_identifier_token(word) && word.find("://") == std::string::npos &&
      !starts_with(word, "as3:")) {
    const bool localKind = context.path == CompletionPath::Local ||
                           context.path == CompletionPath::LocalDirectory;
    const bool mixed = context.path == CompletionPath::Any ||
                       context.path == CompletionPath::AnyDirectory;
    const bool fileScheme =
        context.path == CompletionPath::EosOrFile || context.path == CompletionPath::Any;
    const bool explicitLocal = fileScheme && starts_with(word, "file:");
    const bool mixedLocal = mixed && !is_eos_path_prefix(word);
    if (explicitLocal || localKind || mixedLocal) {
      auto paths =
          local_path_candidates(word,
                                context.path == CompletionPath::LocalDirectory ||
                                    context.path == CompletionPath::AnyDirectory ||
                                    context.path == CompletionPath::EosDirectory,
                                fileScheme);
      candidates.insert(candidates.end(), paths.begin(), paths.end());
    }
    const bool remote = !explicitLocal && !localKind &&
                        (!mixed || word.empty() || is_eos_path_prefix(word));
    if (remote) {
      const bool directories = context.path == CompletionPath::EosDirectory ||
                               context.path == CompletionPath::AnyDirectory;
      if (readline) {
        for (int state = 0;; ++state) {
          std::unique_ptr<char, decltype(&free)> entry(
              eos_entry_generator(word.c_str(), state, directories), &free);
          if (!entry) {
            break;
          }
          candidates.emplace_back(entry.get());
        }
      } else {
        std::string lookupDir, displayPrefix, basename;
        eos_shell_resolve_rooted_path_input(word, lookupDir, displayPrefix, basename);
        auto paths = list_remote_path_candidates(
            precedingTokens, lookupDir, displayPrefix, basename,
            directories ? EosShellPathCompletionMode::Directories
                        : EosShellPathCompletionMode::Any);
        candidates.insert(candidates.end(), paths.begin(), paths.end());
      }
    }
  }
  candidates = dedupe_and_sort(std::move(candidates));
  if (!valuePrefix.empty()) {
    for (auto& candidate : candidates) {
      candidate.insert(0, valuePrefix);
    }
  }
  return candidates;
}
} // namespace

std::vector<std::string>
eos_shell_completion_candidates(const std::vector<std::string>& precedingTokens,
                                const std::string& currentWord)
{
  return completion_candidates(precedingTokens, currentWord, false);
}

//------------------------------------------------------------------------------
// Helper function to extract the dirname and base name from a absolute or
// relative path. For example:
// "/a/b/c/d"  -> dirname: "/a/b/c/"   and basename: "d"
// "/a/b/c/d/" -> dirname: "/a/b/c/d/" and basename: ""
// "x/y/z"     -> dirname: "x/y/"      and basename: "z"
// "x/y/z/"    -> dirname: "x/y/z/"    and basename: ""
// ""          -> dirname: ""          and basename: ""
//------------------------------------------------------------------------------
void eos_path_split(const std::string& input, std::string& dirname,
                    std::string& basename)
{
  if (input.empty()) {
    dirname.clear();
    basename.clear();
    return;
  }

  size_t pos = input.rfind('/');

  if (pos == std::string::npos) {
    dirname.clear();
    basename = input;
    return;
  }

  dirname = input.substr(0, pos + 1);
  basename = input.substr(pos + 1);
  return;
}

char*
eos_completion_quote(char* text, int matchType, char* quotePointer)
{
  (void)matchType;
  const char quote = quotePointer ? *quotePointer : '\0';
  std::string result;
  for (const char current : std::string(text ? text : "")) {
    if (quote == '\'' && current == '\'') {
      result += "'\\''";
    } else {
      if ((quote == '"' && (current == '"' || current == '\\')) ||
          (quote == '\0' && (current == '\'' || current == '"' || current == '\\' ||
                             std::isspace(static_cast<unsigned char>(current))))) {
        result.push_back('\\');
      }
      result.push_back(current);
    }
  }
  return strdup(result.c_str());
}

//------------------------------------------------------------------------------
// EOS console custom completion function to be used by the readline library
// to provide autocompletion.
//------------------------------------------------------------------------------
char** eos_console_completion(const char* text, int start, int end)
{
  rl_attempted_completion_over = 1;
  EnsureNativeCommandRegistryInitialized();
  if (start == 0) {
    rl_completion_append_character = ' ';
    return rl_completion_matches(text, eos_command_generator);
  }
  const auto line =
      tokenize_completion_line(std::string(rl_line_buffer, rl_line_buffer + end));
  const auto prefix =
      tokenize_completion_line(std::string(rl_line_buffer, rl_line_buffer + start));
  gWordCompletionCandidates = completion_candidates(line.preceding, line.word, true);
  const auto& preserved = prefix.word;
  gWordCompletionCandidates.erase(std::remove_if(gWordCompletionCandidates.begin(),
                                                 gWordCompletionCandidates.end(),
                                                 [&](const std::string& word) {
                                                   return !starts_with(word, preserved);
                                                 }),
                                  gWordCompletionCandidates.end());
  for (auto& candidate : gWordCompletionCandidates) {
    candidate.erase(0, preserved.size());
  }
  rl_filename_completion_desired = 1;
  rl_completion_append_character = ' ';
  for (const auto& candidate : gWordCompletionCandidates) {
    if (!candidate.empty() && (candidate.back() == '/' || candidate.back() == '=')) {
      rl_completion_append_character = '\0';
      break;
    }
  }
  const auto fragment = line.word.substr(preserved.size());
  return rl_completion_matches(fragment.c_str(), eos_word_generator);
}

//------------------------------------------------------------------------------
// EOS entry (files/directories) generator
//------------------------------------------------------------------------------
char* eos_entry_generator(const char* text, int state, bool only_dirs)
{
  static size_t index;
  static std::vector<std::string> entries;

  // If this is a new word to complete, initialize now. This includes
  // saving the length of TEXT for efficiency, and initializing the index
  // variable to 0.
  if (!state) {
    index = 0;
    entries.clear();
    std::string inarg = text;
    std::string dirname;
    std::string basename;
    eos_path_split(inarg, dirname, basename);

    if (dirname.empty()) {
      inarg = gPwd.c_str();
    } else {
      if (dirname.at(0) == '/') {
        inarg = dirname;
      } else {
        inarg = gPwd.c_str() + dirname;
      }
    }

    const bool oldsilent = silent;
    const int oldRetc = global_retc;
    silent = true;
    rstdout = "";
    EnsureNativeCommandRegistryInitialized();
    auto* command = CommandRegistry::instance().find("ls");
    bool listed = false;
    if (command) {
      CommandContext context{};
      context.globalOpts = &gGlobalOpts;
      context.json = json;
      context.silent = true;
      context.clientCommand = &client_command;
      context.outputResult = &output_result;
      command->run({"-F", "--", inarg}, context);
      listed = global_retc == 0;
    }
    silent = oldsilent;
    global_retc = oldRetc;

    if (listed && rstdout.c_str()) {
      XrdOucTokenizer subtokenizer((char*) rstdout.c_str());

      do {
        XrdOucString entry = subtokenizer.GetLine();

        if (entry.length()) {
          if (entry.endswith('\n')) {
            entry.erase(entry.length() - 1);
          }

          if (only_dirs && !entry.endswith('/')) {
            continue;
          }

          if (rl_completion_type == 63) { // ? - list possible completions
            // When listing completions we need to return the basename of the
            // candidates
            if (basename.empty() ||
                ((strncmp(basename.c_str(), entry.c_str(), basename.length()) == 0) &&
                 // Exclude exact matches
                 (basename.length() < (size_t)entry.length()))) {
              entries.push_back(entry.c_str());
            }
          } else if (rl_completion_type == 9) { // TAB - do standard completion
            // When doing the standard completion we need to return the full path
            // as given by the user initially (i.e. not including the pwd deduction).
            if (basename.empty() ||
                (strncmp(basename.c_str(), entry.c_str(), basename.length()) == 0)) {
              std::string add_path = dirname;
              add_path += entry.c_str();
              entries.push_back(add_path.c_str());
            }
          }
        } else {
          break;
        }
      } while (1);
    }
  }

  if (index < entries.size()) {
    return strdup(entries[index++].c_str());
  }

  return ((char*) 0);
}

//------------------------------------------------------------------------------
// EOS directories generator - similar to the above
//------------------------------------------------------------------------------
char* eos_dir_generator(const char* text, int state)
{
  return eos_entry_generator(text, state, true);
}

//------------------------------------------------------------------------------
// EOS files and directories generator - similar to the above
//------------------------------------------------------------------------------
char* eos_all_generator(const char* text, int state)
{
  return eos_entry_generator(text, state);
}

//------------------------------------------------------------------------------
// Generator function for command completion - similar to the above.
//------------------------------------------------------------------------------
char* eos_command_generator(const char* text, int state)
{
  static size_t index;
  static std::vector<std::string> completions;

  // If this is a new word to complete, initialize now.  This includes
  // saving the length of TEXT for efficiency, and initializing the index
  // variable to 0.
  if (!state) {
    int len = strlen(text);
    completions.clear();
    index = 0;

    // Prefer registry over static commands array
    auto& all = CommandRegistry::instance().all();
    for (auto* c : all) {
      const char* name = c->name();
      if (strncmp(name, text, len) == 0) completions.push_back(name);
    }
  } else {
    ++index;
  }

  if (index < completions.size()) {
    return strdup(completions[index].c_str());
  }

  return ((char*) 0);
}

//------------------------------------------------------------------------------
// Generator function for static word completion lists returned by commands.
//------------------------------------------------------------------------------
char*
eos_word_generator(const char* text, int state)
{
  static size_t index;
  static std::vector<std::string> completions;

  if (!state) {
    index = 0;
    completions.clear();
    const int len = strlen(text);

    for (const auto& candidate : gWordCompletionCandidates) {
      if (strncmp(candidate.c_str(), text, len) == 0) {
        completions.push_back(candidate);
      }
    }
  } else {
    ++index;
  }

  if (index < completions.size()) {
    return strdup(completions[index].c_str());
  }

  return ((char*)0);
}
