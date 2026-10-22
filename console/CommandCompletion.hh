#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace CLI {
class App;
class Option;
} // namespace CLI

enum class CompletionPath {
  None,
  Eos,
  EosDirectory,
  EosOrFile,
  Local,
  LocalDirectory,
  Any,
  AnyDirectory,
};

struct CommandCompletionResult {
  std::vector<std::string> candidates;
  CompletionPath path = CompletionPath::None;
};

// Completion-only schemas for commands whose execution grammar is still manual.
// Parsing a partial command must never invoke its execution callbacks.
struct CommandCompletionSchema {
  explicit CommandCompletionSchema(const std::string& name);
  ~CommandCompletionSchema();
  std::unique_ptr<CLI::App> app;
  std::map<const CLI::Option*, std::vector<std::string>> values;
  std::map<const CLI::Option*, CompletionPath> paths;
  std::map<const CLI::Option*, std::vector<std::string>> spellings;

  CLI::App& sub(CLI::App& parent, const std::string& name);
  CLI::Option* flag(CLI::App& command, const std::string& names);
  CLI::Option* value(CLI::App& command, const std::string& names,
                     const std::vector<std::string>& choices = {},
                     CompletionPath path = CompletionPath::None);
  CLI::Option* argument(CLI::App& command, const std::string& name,
                        const std::vector<std::string>& choices = {},
                        CompletionPath path = CompletionPath::None, bool repeat = false);
};

CommandCompletionResult CompleteCliArguments(
    const CLI::App& app, const std::vector<std::string>& args,
    const std::map<const CLI::Option*, std::vector<std::string>>& values = {},
    const std::map<const CLI::Option*, CompletionPath>& paths = {},
    const std::map<const CLI::Option*, std::vector<std::string>>& spellings = {});

CommandCompletionResult CompleteRegisteredCommand(const std::string& name,
                                                  const std::vector<std::string>& args);
bool HasCommandCompletionSchema(const std::string& name);

bool ConfigureAdminCompletion(CommandCompletionSchema& schema, const std::string& name);
bool ConfigureStorageCompletion(CommandCompletionSchema& schema, const std::string& name);
