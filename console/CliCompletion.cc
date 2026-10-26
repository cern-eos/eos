#include "console/CommandCompletion.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <limits>
#include <set>
#include <sstream>

CommandCompletionSchema::CommandCompletionSchema(const std::string& name)
    : app(std::make_unique<CLI::App>("", name))
{
  app->allow_non_standard_option_names();
}

CommandCompletionSchema::~CommandCompletionSchema() = default;

CLI::App&
CommandCompletionSchema::sub(CLI::App& parent, const std::string& name)
{
  auto* command = parent.add_subcommand(name);
  command->allow_non_standard_option_names();
  return *command;
}

namespace {
CLI::Option*
addCompletionOption(CommandCompletionSchema& schema, CLI::App& command,
                    const std::string& names, bool flag)
{
  CLI::Option* option = nullptr;
  try {
    option = flag ? command.add_flag(names) : command.add_option(names);
  } catch (const CLI::OptionAlreadyAdded&) {
    // Legacy commands permit overlapping spellings such as -g and -gid.
    // Keep their real spellings separate from CLI11's unambiguous storage keys.
    if (names.empty() || names.front() != '-') {
      throw;
    }
    const auto key = "--__eos_completion_" + std::to_string(schema.spellings.size());
    option = flag ? command.add_flag(key) : command.add_option(key);
  }
  if (!names.empty() && names.front() == '-') {
    std::istringstream input(names);
    std::string spelling;
    while (std::getline(input, spelling, ',')) {
      if (!spelling.empty() && spelling.front() == '-') {
        schema.spellings[option].push_back(spelling);
      }
    }
  }
  return option;
}
} // namespace

CLI::Option*
CommandCompletionSchema::flag(CLI::App& command, const std::string& names)
{
  return addCompletionOption(*this, command, names, true);
}

CLI::Option*
CommandCompletionSchema::value(CLI::App& command, const std::string& names,
                               const std::vector<std::string>& choices,
                               CompletionPath path)
{
  auto* option = addCompletionOption(*this, command, names, false)->expected(1);
  if (!choices.empty()) {
    values[option] = choices;
  }
  if (path != CompletionPath::None) {
    paths[option] = path;
  }
  return option;
}

CLI::Option*
CommandCompletionSchema::argument(CLI::App& command, const std::string& name,
                                  const std::vector<std::string>& choices,
                                  CompletionPath path, bool repeat)
{
  auto* option = value(command, name, choices, path);
  if (repeat) {
    option->expected(1, -1);
  }
  return option;
}

namespace {
using Values = std::map<const CLI::Option*, std::vector<std::string>>;
using Paths = std::map<const CLI::Option*, CompletionPath>;

std::vector<const CLI::Option*>
options(const CLI::App& command)
{
  auto result = command.get_options();
  for (const auto* child : command.get_subcommands([](const CLI::App* app) {
         return app->get_name().empty() && !app->get_disabled();
       })) {
    const auto nested = options(*child);
    for (const auto* option : nested) {
      if (std::find(result.begin(), result.end(), option) == result.end()) {
        result.push_back(option);
      }
    }
  }
  return result;
}

bool
accepts(const CLI::Option* option, const std::string& token, const Values& values)
{
  if (token.empty()) {
    return false;
  }
  const auto found = values.find(option);
  if (found == values.end()) {
    return true;
  }
  for (const auto& choice : found->second) {
    if (token == choice || (!choice.empty() && choice.back() == '=' &&
                            token.compare(0, choice.size(), choice) == 0)) {
      return true;
    }
  }
  return false;
}

CommandCompletionResult
valueResult(const CLI::Option* option, const Values& values, const Paths& paths)
{
  CommandCompletionResult result;
  if (const auto found = values.find(option); found != values.end()) {
    result.candidates = found->second;
  }
  if (const auto found = paths.find(option); found != paths.end()) {
    result.path = found->second;
  }
  return result;
}

const CLI::Option*
findOption(const CLI::App& command, const std::string& name, const Values& spellings)
{
  for (const auto* option : options(command)) {
    const auto custom = spellings.find(option);
    if (custom != spellings.end()
            ? std::any_of(custom->second.begin(), custom->second.end(),
                          [&](const std::string& spelling) {
                            return spelling == name ||
                                   (!spelling.empty() && spelling.back() == '=' &&
                                    spelling.substr(0, spelling.size() - 1) == name);
                          })
            : option->check_name(name)) {
      return option;
    }
  }
  return nullptr;
}

bool
groupAllows(const CLI::App& command, const CLI::Option* candidate,
            const std::set<const CLI::Option*>& used)
{
  const auto members = options(command);
  if (candidate &&
      std::find(members.begin(), members.end(), candidate) == members.end()) {
    return true;
  }
  const auto maximum = command.get_require_option_max();
  const auto count =
      std::count_if(members.begin(), members.end(),
                    [&](const CLI::Option* option) { return used.count(option) != 0; });
  if (maximum &&
      (static_cast<size_t>(count) > maximum ||
       (candidate && !used.count(candidate) && static_cast<size_t>(count) >= maximum))) {
    return false;
  }
  for (const auto* child : command.get_subcommands([](const CLI::App* app) {
         return app->get_name().empty() && !app->get_disabled();
       })) {
    if (!groupAllows(*child, candidate, used)) {
      return false;
    }
  }
  return true;
}

} // namespace

CommandCompletionResult
CompleteCliArguments(const CLI::App& app, const std::vector<std::string>& args,
                     const Values& values, const Paths& paths, const Values& spellings)
{
  const CLI::App* command = &app;
  const CLI::Option* pending = nullptr;
  int pendingCount = 0;
  bool positionalOnly = false;
  std::map<const CLI::Option*, int> counts;
  std::set<const CLI::Option*> used;
  auto nextPositional = [&]() -> const CLI::Option* {
    for (const auto* option : options(*command)) {
      if (option->get_positional() && counts[option] < option->get_items_expected_max()) {
        return option;
      }
    }
    return nullptr;
  };
  auto consumeValue = [&](const CLI::Option* option, const std::string& token) {
    if (token.empty()) {
      return false;
    }
    ++counts[option];
    used.insert(option);
    return true;
  };
  for (const auto& token : args) {
    if (pending) {
      const auto optionName = token.substr(0, token.find('='));
      if (pendingCount >= pending->get_items_expected_min() &&
          (token == "--" || findOption(*command, optionName, spellings) ||
           command->get_subcommand_no_throw(token))) {
        pending = nullptr;
      } else {
        if (!consumeValue(pending, token)) {
          return {};
        }
        if (++pendingCount >= pending->get_items_expected_max()) {
          pending = nullptr;
        }
        continue;
      }
    }
    if (!positionalOnly && token == "--") {
      positionalOnly = true;
      continue;
    }
    const auto* positional = nextPositional();
    const auto literal = values.find(positional);
    const bool positionalLiteral =
        literal != values.end() && accepts(positional, token, values);
    if (!positionalOnly && !positionalLiteral && token.size() > 1 && token[0] == '-') {
      const auto equals = token.find('=');
      const std::string name = token.substr(0, equals);
      const CLI::Option* option = findOption(*command, name, spellings);
      if (option) {
        used.insert(option);
        if (option->get_items_expected_max() == 0) {
          if (equals != std::string::npos && option->get_disable_flag_override()) {
            return {};
          }
        } else if (equals != std::string::npos) {
          if (!consumeValue(option, token.substr(equals + 1))) {
            return {};
          }
          if (option->get_items_expected_max() > 1) {
            pending = option;
            pendingCount = 1;
          }
        } else {
          pending = option;
          pendingCount = 0;
        }
        continue;
      }
      if (token[1] == '-') {
        return {};
      }
      for (size_t i = 1; i < token.size(); ++i) {
        option = findOption(*command, "-" + token.substr(i, 1), spellings);
        if (!option) {
          return {};
        }
        used.insert(option);
        if (option->get_items_expected_max() != 0) {
          if (i + 1 < token.size()) {
            if (!consumeValue(option, token.substr(i + 1))) {
              return {};
            }
            pendingCount = 1;
          } else {
            pendingCount = 0;
          }
          if (pendingCount < option->get_items_expected_max()) {
            pending = option;
          }
          break;
        }
      }
      continue;
    }
    if (!positionalOnly) {
      const auto* child = command->get_subcommand_no_throw(token);
      if (child && !child->get_disabled()) {
        command = child;
        continue;
      }
    }
    if (!positional || !consumeValue(positional, token)) {
      return {};
    }
  }
  if (!groupAllows(*command, nullptr, used)) {
    return {};
  }
  for (const auto* option : used) {
    for (const auto* excluded : option->get_excludes()) {
      if (used.count(excluded)) {
        return {};
      }
    }
  }
  if (pending && pendingCount < pending->get_items_expected_min()) {
    return valueResult(pending, values, paths);
  }
  CommandCompletionResult result;
  if (pending) {
    result = valueResult(pending, values, paths);
  } else if (const auto* positional = nextPositional()) {
    result = valueResult(positional, values, paths);
  }
  if (!positionalOnly) {
    for (const auto* child : command->get_subcommands([](const CLI::App* candidate) {
           return !candidate->get_disabled() && !candidate->get_name().empty() &&
                  !candidate->get_group().empty();
         })) {
      result.candidates.push_back(child->get_name());
      for (const auto& alias : child->get_aliases()) {
        result.candidates.push_back(alias);
      }
    }
    for (const auto* option : options(*command)) {
      if (option->get_group().empty() ||
          (used.count(option) &&
           option->get_multi_option_policy() != CLI::MultiOptionPolicy::TakeAll)) {
        continue;
      }
      bool excluded = false;
      for (const auto* selected : used) {
        if (selected->get_excludes().count(const_cast<CLI::Option*>(option)) ||
            option->get_excludes().count(const_cast<CLI::Option*>(selected))) {
          excluded = true;
          break;
        }
      }
      if (excluded || !groupAllows(*command, option, used)) {
        continue;
      }
      if (const auto custom = spellings.find(option); custom != spellings.end()) {
        result.candidates.insert(result.candidates.end(), custom->second.begin(),
                                 custom->second.end());
        continue;
      }
      for (const auto& name : option->get_snames()) {
        result.candidates.push_back("-" + name);
      }
      for (const auto& name : option->get_lnames()) {
        result.candidates.push_back("--" + name);
      }
    }
  }
  std::sort(result.candidates.begin(), result.candidates.end());
  result.candidates.erase(std::unique(result.candidates.begin(), result.candidates.end()),
                          result.candidates.end());
  return result;
}
