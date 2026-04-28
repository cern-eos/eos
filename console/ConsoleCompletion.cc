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
#include <limits>
#include <map>
#include <memory>
#include <readline/history.h>
#include <readline/readline.h>
#include <regex>
#include <set>
#include <sstream>
#include <string.h>
#include <unordered_set>
#include <vector>

extern int com_ls(char*);

namespace {
std::vector<std::string> gWordCompletionCandidates;

bool starts_with(const std::string& value, const std::string& prefix);
std::vector<std::string> dedupe_and_sort(std::vector<std::string> candidates);

struct HelpCompletionLeaf {
  size_t optionActivationPositionalCount = std::numeric_limits<size_t>::max();
  std::map<size_t, std::set<std::string>> literalsAtPositionalCount;
  std::set<std::string> options;
};

using HelpCompletionSpec = std::map<std::vector<std::string>, HelpCompletionLeaf>;

std::string
trim_copy(const std::string& value)
{
  const auto begin = value.find_first_not_of(" \t\r\n");

  if (begin == std::string::npos) {
    return {};
  }

  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

bool
is_command_word(const std::string& token)
{
  if (token.empty()) {
    return false;
  }

  for (const char c : token) {
    const auto uch = static_cast<unsigned char>(c);

    if (!std::isalnum(uch) && c != '-' && c != '_') {
      return false;
    }
  }

  return true;
}

bool
allows_path_extension_token(const std::string& token)
{
  return !token.empty() && (token[0] == '<' || token[0] == '[' || token[0] == '-' ||
                            token == ":" || token.find('|') != std::string::npos);
}

std::vector<std::string>
split_words(const std::string& value)
{
  std::istringstream iss(value);
  std::vector<std::string> words;
  std::string word;

  while (iss >> word) {
    words.push_back(word);
  }

  return words;
}

std::vector<std::string>
extract_option_tokens(const std::string& text)
{
  static const std::regex kOptionRegex(
      R"((--[A-Za-z0-9][A-Za-z0-9-]*|-{1}[A-Za-z0-9][A-Za-z0-9-]*))");
  std::vector<std::string> options;

  for (std::sregex_iterator it(text.begin(), text.end(), kOptionRegex), end; it != end;
       ++it) {
    if (it->position() > 0) {
      const char prev = text[it->position() - 1];

      if (std::isalnum(static_cast<unsigned char>(prev)) || prev == '<' || prev == '_' ||
          prev == '.') {
        continue;
      }
    }

    options.push_back(it->str());
  }

  return dedupe_and_sort(std::move(options));
}

std::string
strip_choice_token(std::string token)
{
  while (!token.empty() &&
         (token.front() == '[' || token.front() == '(' || token.front() == '<')) {
    token.erase(token.begin());
  }

  while (!token.empty() &&
         (token.back() == ']' || token.back() == ')' || token.back() == '>' ||
          token.back() == ',' || token.back() == ':' || token.back() == ';' ||
          token.back() == '.')) {
    token.pop_back();
  }

  return trim_copy(token);
}

std::vector<std::string>
extract_literal_choices(const std::string& token)
{
  if (token.find('|') == std::string::npos || token.find("--") != std::string::npos ||
      token.find('<') != std::string::npos) {
    return {};
  }

  std::vector<std::string> choices;
  std::stringstream ss(token);
  std::string part;

  while (std::getline(ss, part, '|')) {
    part = strip_choice_token(part);

    if (!part.empty() && part[0] != '-' && is_command_word(part)) {
      choices.push_back(part);
    }
  }

  return dedupe_and_sort(std::move(choices));
}

bool
parse_absolute_help_path(const std::string& commandName, const std::string& line,
                         std::vector<std::string>& path, std::string& rest)
{
  const std::string prefix = commandName + " ";

  if (!starts_with(line, prefix)) {
    return false;
  }

  const auto words = split_words(line.substr(prefix.size()));

  if (words.empty() || !is_command_word(words[0])) {
    return false;
  }

  path.clear();
  path.push_back(words[0]);

  for (size_t i = 1; i < words.size(); ++i) {
    const auto& token = words[i];

    if (!is_command_word(token)) {
      break;
    }

    if (i + 1 >= words.size() || !allows_path_extension_token(words[i + 1])) {
      break;
    }

    path.push_back(token);
  }

  size_t offset = prefix.size();

  for (const auto& segment : path) {
    offset += segment.size();

    if (offset < line.size() && line[offset] == ' ') {
      ++offset;
    }
  }

  rest = offset < line.size() ? trim_copy(line.substr(offset)) : std::string{};
  return true;
}

bool
parse_rootless_file_help_path(const std::string& commandName, const std::string& rawLine,
                              const std::string& line, std::vector<std::string>& path,
                              std::string& rest)
{
  if (commandName != "file" || rawLine != line || line.empty() ||
      !std::islower(line[0]) || starts_with(line, "Usage:") || starts_with(line, "'")) {
    return false;
  }

  const auto words = split_words(line);

  if (words.empty() || !is_command_word(words[0])) {
    return false;
  }

  path = {words[0]};
  rest = words.size() > 1 ? trim_copy(line.substr(words[0].size())) : std::string{};
  return true;
}

size_t
leading_indent(const std::string& line)
{
  size_t indent = 0;

  for (const char c : line) {
    if (c == ' ') {
      ++indent;
    } else if (c == '\t') {
      indent += 8 - (indent % 8);
    } else {
      break;
    }
  }

  return indent;
}

bool
parse_relative_help_path(const std::vector<std::string>& currentParent,
                         const std::string& line, std::vector<std::string>& path,
                         std::string& rest)
{
  if (currentParent.empty() || line.empty() || !std::isalpha(line[0]) ||
      starts_with(line, "usage:") || starts_with(line, "Usage:") ||
      starts_with(line, "SUBCOMMANDS") || starts_with(line, "EXAMPLES") ||
      starts_with(line, "TAPE ") || starts_with(line, "eos ") || starts_with(line, "#")) {
    return false;
  }

  std::smatch actionMatch;
  static const std::regex kActionRegex(R"(^action '([A-Za-z0-9_-]+)')");

  if (std::regex_search(line, actionMatch, kActionRegex)) {
    path = currentParent;
    path.push_back(actionMatch[1].str());
    rest = trim_copy(line.substr(actionMatch.position() + actionMatch.length()));
    return true;
  }

  const auto words = split_words(line);

  if (words.empty() || !is_command_word(words[0]) || words[0] == "Note") {
    return false;
  }

  path = currentParent;
  path.push_back(words[0]);
  rest = words.size() > 1 ? trim_copy(line.substr(words[0].size())) : std::string{};
  return true;
}

void
attach_primary_help_line(HelpCompletionLeaf& leaf, const std::string& rest)
{
  size_t leadingRequiredPositionals = 0;
  const auto tokens = split_words(rest);

  for (const auto& token : tokens) {
    if (token.empty() || token == ":") {
      continue;
    }

    const auto options = extract_option_tokens(token);

    if (!options.empty()) {
      leaf.options.insert(options.begin(), options.end());

      if (leaf.optionActivationPositionalCount == std::numeric_limits<size_t>::max()) {
        leaf.optionActivationPositionalCount = leadingRequiredPositionals;
      }

      continue;
    }

    if (token[0] == '<' || token.find('<') != std::string::npos) {
      if (token[0] != '[') {
        ++leadingRequiredPositionals;
      }
      continue;
    }

    const auto choices = extract_literal_choices(token);

    if (!choices.empty()) {
      auto& slot = leaf.literalsAtPositionalCount[leadingRequiredPositionals];
      slot.insert(choices.begin(), choices.end());
    }
  }
}

bool
attach_help_key_literal(const std::vector<std::string>& path, const std::string& line,
                        HelpCompletionLeaf& leaf)
{
  if (path.empty() || path.back() != "config" || line.empty() || line[0] == '-') {
    return false;
  }

  const auto eq = line.find('=');

  if (eq == std::string::npos) {
    return false;
  }

  std::string key = trim_copy(line.substr(0, eq + 1));

  if (key.empty() || !std::isalpha(static_cast<unsigned char>(key[0]))) {
    return false;
  }

  leaf.literalsAtPositionalCount[1].insert(key);

  const auto options = extract_option_tokens(line);
  leaf.options.insert(options.begin(), options.end());
  return true;
}

bool
is_non_completion_help_line(const std::string& line)
{
  return starts_with(line, "eos ") || starts_with(line, "#");
}

HelpCompletionSpec
parse_help_completion_spec(const std::string& commandName, const std::string& helpText)
{
  HelpCompletionSpec spec;
  std::istringstream lines(helpText);
  std::string line;
  std::vector<std::string> currentLeaf;
  std::map<size_t, std::vector<std::string>> parentByIndent;

  while (std::getline(lines, line)) {
    const size_t indent = leading_indent(line);
    const auto trimmed = trim_copy(line);

    if (trimmed.empty()) {
      continue;
    }

    std::vector<std::string> path;
    std::string rest;
    const bool matchedAbsolute =
        parse_absolute_help_path(commandName, trimmed, path, rest);
    const bool matchedRootless =
        !matchedAbsolute &&
        parse_rootless_file_help_path(commandName, line, trimmed, path, rest);
    std::vector<std::string> relativeParent;
    auto parentIt = parentByIndent.lower_bound(indent);

    if (parentIt != parentByIndent.begin()) {
      --parentIt;
      relativeParent = parentIt->second;
    }

    const bool matchedRelative =
        !matchedAbsolute && !matchedRootless &&
        parse_relative_help_path(relativeParent, trimmed, path, rest);

    if (matchedAbsolute || matchedRootless || matchedRelative) {
      auto& leaf = spec[path];
      attach_primary_help_line(leaf, rest);
      currentLeaf = path;
      parentByIndent.erase(parentByIndent.lower_bound(indent), parentByIndent.end());

      if (trimmed.find("[subcommand]") != std::string::npos ||
          trimmed.find("[action]") != std::string::npos) {
        parentByIndent[indent] = path;
      }

      continue;
    }

    if (currentLeaf.empty()) {
      continue;
    }

    if (is_non_completion_help_line(trimmed)) {
      continue;
    }

    auto& leaf = spec[currentLeaf];

    if (attach_help_key_literal(currentLeaf, trimmed, leaf)) {
      continue;
    }

    const auto options = extract_option_tokens(trimmed);

    if (!options.empty()) {
      leaf.options.insert(options.begin(), options.end());

      if (leaf.optionActivationPositionalCount == std::numeric_limits<size_t>::max()) {
        leaf.optionActivationPositionalCount = 0;
      }
    }
  }

  return spec;
}

const HelpCompletionSpec&
cached_help_completion_spec(const std::string& commandName, const std::string& helpText)
{
  static std::map<std::string, HelpCompletionSpec> cache;
  const std::string key = commandName + "\n" + helpText;
  auto it = cache.find(key);

  if (it == cache.end()) {
    it = cache.emplace(key, parse_help_completion_spec(commandName, helpText)).first;
  }

  return it->second;
}

size_t
count_non_option_arguments(const std::vector<std::string>& args)
{
  size_t count = 0;

  for (const auto& arg : args) {
    if (arg.empty() || arg[0] == '-') {
      continue;
    }

    ++count;
  }

  return count;
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
looks_like_non_eos_path(const std::string& token)
{
  return token.rfind("./", 0) == 0 || token.rfind("../", 0) == 0 ||
         token.rfind("~/", 0) == 0 || token.rfind("file:", 0) == 0 ||
         token.rfind("root://", 0) == 0 || token.rfind("as3:", 0) == 0;
}

bool
is_eos_path_prefix(const std::string& token)
{
  return token == "/eos" || token.rfind("/eos/", 0) == 0;
}

bool
should_complete_file_subcommand_paths(const std::vector<std::string>& args)
{
  if (args.empty()) {
    return false;
  }

  static const std::unordered_set<std::string> kPathSubcommands = {"adjustreplica",
                                                                   "check",
                                                                   "convert",
                                                                   "copy",
                                                                   "drop",
                                                                   "info",
                                                                   "layout",
                                                                   "move",
                                                                   "purge",
                                                                   "rename",
                                                                   "rename_with_symlink",
                                                                   "replicate",
                                                                   "share",
                                                                   "symlink",
                                                                   "tag",
                                                                   "touch",
                                                                   "verify",
                                                                   "version",
                                                                   "versions",
                                                                   "workflow"};

  return kPathSubcommands.find(args[0]) != kPathSubcommands.end();
}

bool
is_path_capable_command_name(const std::string& commandName)
{
  static const std::unordered_set<std::string> kPathCommands = {
      "acl", "attr",     "cat",   "cd",   "chmod", "chown", "cp",
      "du",  "fileinfo", "find",  "info", "ln",    "ls",    "mkdir",
      "mv",  "rm",       "rmdir", "stat", "touch"};

  return kPathCommands.find(commandName) != kPathCommands.end();
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
  std::vector<std::string> tokens;
  std::string cur;
  bool in_double = false;
  bool in_single = false;

  for (size_t i = 0; i < input.size(); ++i) {
    const char c = input[i];

    if (c == '\\' && !in_single && i + 1 < input.size()) {
      cur.push_back(input[i + 1]);
      ++i;
      continue;
    }

    if (c == '"' && !in_single) {
      in_double = !in_double;
      continue;
    }

    if (c == '\'' && !in_double) {
      in_single = !in_single;
      continue;
    }

    if (isspace(static_cast<unsigned char>(c)) && !in_double && !in_single) {
      if (!cur.empty()) {
        tokens.push_back(cur);
        cur.clear();
      }
      continue;
    }

    cur.push_back(c);
  }

  if (!cur.empty()) {
    tokens.push_back(cur);
  }

  return tokens;
}

EosShellPathCompletionMode
eos_shell_path_completion_mode(const std::string& commandName,
                               const std::vector<std::string>& args,
                               const std::string& currentWord)
{
  if ((!currentWord.empty() && currentWord[0] == '-') ||
      is_path_identifier_token(currentWord) || looks_like_non_eos_path(currentWord)) {
    return EosShellPathCompletionMode::None;
  }

  if (!currentWord.empty() && !is_eos_path_prefix(currentWord)) {
    return EosShellPathCompletionMode::None;
  }

  if (commandName == "cd" || commandName == "mkdir" || commandName == "rmdir") {
    return EosShellPathCompletionMode::Directories;
  }

  if (is_path_capable_command_name(commandName)) {
    return EosShellPathCompletionMode::Any;
  }

  if (commandName == "file" && should_complete_file_subcommand_paths(args)) {
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
    lookupDir = "/eos/";
    displayPrefix = "/eos/";
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
    lookupDir = currentWord;
  } else if (!displayPrefix.empty()) {
    lookupDir = displayPrefix;
  } else {
    lookupDir = "/eos/";
  }

  if (lookupDir.empty()) {
    lookupDir = "/eos/";
  }

  if (!ends_with(lookupDir, "/")) {
    lookupDir += "/";
  }
}

std::vector<std::string>
eos_shell_completion_candidates(const std::vector<std::string>& precedingTokens,
                                const std::string& currentWord)
{
  EnsureNativeCommandRegistryInitialized();

  const auto commandIndex = find_command_token_index(precedingTokens);

  if (commandIndex >= precedingTokens.size()) {
    if (!currentWord.empty() && currentWord[0] == '-') {
      return filter_candidates(global_option_candidates(), currentWord);
    }

    std::vector<std::string> candidates = global_option_candidates();

    for (const auto* command : CommandRegistry::instance().all()) {
      if (command && command->name()) {
        candidates.emplace_back(command->name());
      }
    }

    return filter_candidates(candidates, currentWord);
  }

  const auto& commandName = precedingTokens[commandIndex];
  IConsoleCommand* icmd = CommandRegistry::instance().find(commandName);

  if (!icmd) {
    return {};
  }

  std::vector<std::string> args(precedingTokens.begin() + commandIndex + 1,
                                precedingTokens.end());
  const auto pathMode = eos_shell_path_completion_mode(commandName, args, currentWord);

  if (pathMode != EosShellPathCompletionMode::None) {
    std::string lookupDir;
    std::string displayPrefix;
    std::string basename;
    eos_shell_resolve_rooted_path_input(currentWord, lookupDir, displayPrefix, basename);

    auto pathCandidates = list_remote_path_candidates(precedingTokens, lookupDir,
                                                      displayPrefix, basename, pathMode);

    if (!pathCandidates.empty()) {
      return pathCandidates;
    }
  }

  return filter_candidates(icmd->complete(args), currentWord);
}

std::vector<std::string>
eos_help_completion_candidates(const std::string& commandName,
                               const std::string& helpText,
                               const std::vector<std::string>& args)
{
  if (helpText.empty()) {
    return {};
  }

  const auto& spec = cached_help_completion_spec(commandName, helpText);
  std::set<std::string> pathSuggestions;
  size_t bestMatchLength = 0;
  std::vector<const HelpCompletionLeaf*> bestLeaves;

  for (const auto& entry : spec) {
    const auto& path = entry.first;
    const auto& leaf = entry.second;

    if (args.size() < path.size() && std::equal(args.begin(), args.end(), path.begin())) {
      pathSuggestions.insert(path[args.size()]);
    }

    if (args.size() >= path.size() &&
        std::equal(path.begin(), path.end(), args.begin())) {
      if (path.size() > bestMatchLength) {
        bestMatchLength = path.size();
        bestLeaves.clear();
      }

      if (path.size() == bestMatchLength) {
        bestLeaves.push_back(&leaf);
      }
    }
  }

  if (!pathSuggestions.empty()) {
    return dedupe_and_sort(
        std::vector<std::string>(pathSuggestions.begin(), pathSuggestions.end()));
  }

  if (bestLeaves.empty()) {
    return {};
  }

  const std::vector<std::string> remaining(args.begin() + bestMatchLength, args.end());
  const size_t positionalCount = count_non_option_arguments(remaining);
  std::set<std::string> literalSuggestions;
  std::set<std::string> optionSuggestions;

  for (const auto* leaf : bestLeaves) {
    auto literalIt = leaf->literalsAtPositionalCount.find(positionalCount);

    if (literalIt != leaf->literalsAtPositionalCount.end()) {
      literalSuggestions.insert(literalIt->second.begin(), literalIt->second.end());
    }
  }

  if (!literalSuggestions.empty()) {
    return dedupe_and_sort(
        std::vector<std::string>(literalSuggestions.begin(), literalSuggestions.end()));
  }

  for (const auto* leaf : bestLeaves) {
    if (leaf->optionActivationPositionalCount != std::numeric_limits<size_t>::max() &&
        positionalCount >= leaf->optionActivationPositionalCount) {
      optionSuggestions.insert(leaf->options.begin(), leaf->options.end());
    }
  }

  return dedupe_and_sort(
      std::vector<std::string>(optionSuggestions.begin(), optionSuggestions.end()));
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

//------------------------------------------------------------------------------
// EOS console custom completion function to be used by the readline library
// to provide autocompletion.
//------------------------------------------------------------------------------
char** eos_console_completion(const char* text, int start, int end)
{
  char** matches;
  matches = (char**) 0;
  // Disable filename completion if our generator finds no matches
  rl_attempted_completion_over = 1;

  EnsureNativeCommandRegistryInitialized();

  // If this word is at the start of the line, then it is a command
  // to complete.  Otherwise it is the name of a file in the current
  // directory.
  if (start == 0) {
    rl_completion_append_character = ' ';
    matches = rl_completion_matches(text, eos_command_generator);
    return matches;
  }

  std::string prefix(rl_line_buffer, rl_line_buffer + start);
  const auto tokens = eos_completion_tokenize_prefix(prefix);

  if (!tokens.empty()) {
    IConsoleCommand* icmd = CommandRegistry::instance().find(tokens.front());

    if (icmd) {
      std::vector<std::string> args(tokens.begin() + 1, tokens.end());
      auto candidates = icmd->complete(args);

      if (!candidates.empty()) {
        std::unordered_set<std::string> seen;
        gWordCompletionCandidates.clear();

        for (const auto& candidate : candidates) {
          if (candidate.empty()) {
            continue;
          }

          if (seen.insert(candidate).second) {
            gWordCompletionCandidates.push_back(candidate);
          }
        }

        std::sort(gWordCompletionCandidates.begin(), gWordCompletionCandidates.end());
        rl_completion_append_character = ' ';
        matches = rl_completion_matches(text, eos_word_generator);

        if (matches) {
          return matches;
        }
      }
    }
  }

  XrdOucString cmd = rl_line_buffer;

  if (cmd.beginswith("mkdir ") ||
      cmd.beginswith("rmdir ") ||
      cmd.beginswith("find ") ||
      cmd.beginswith("cd ") ||
      cmd.beginswith("chown ") ||
      cmd.beginswith("chmod ") ||
      cmd.beginswith("attr ") ||
      cmd.beginswith("acl ")) {
    // dir completion
    rl_completion_append_character = '\0';
    matches = rl_completion_matches(text, eos_dir_generator);
  }

  if (cmd.beginswith("rm ") ||
      cmd.beginswith("ls ") ||
      cmd.beginswith("fileinfo ") ||
      cmd.beginswith("cp ")) {
    // dir/file completion
    rl_completion_append_character = '\0';
    matches = rl_completion_matches(text, eos_all_generator);
  }

  return (matches);
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

    bool oldsilent = silent;
    silent = true;
    XrdOucString comarg = "-F ";
    comarg += inarg.c_str();
    char buffer[4096];
    sprintf(buffer, "%s", comarg.c_str());
    com_ls((char*) buffer);
    silent = oldsilent;

    if (rstdout.c_str()) {
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
