#include "console/CommandCompletion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleColor.hh"
#include <CLI/CLI.hpp>
#include <set>

namespace {
bool
configureBasic(CommandCompletionSchema& schema, const std::string& name)
{
  auto& app = *schema.app;
  if (name == "help" || name == "?") {
    app.set_help_flag();
    std::vector<std::string> names;
    for (const auto* command : CommandRegistry::instance().all()) {
      names.emplace_back(command->name());
    }
    schema.argument(app, "command", names);
    return true;
  }
  static const std::set<std::string> noArguments{".q",     "exit",   "quit", "json",
                                                 "silent", "timing", "clear"};
  if (noArguments.count(name)) {
    app.set_help_flag();
    return true;
  }
  if (name == "tui") {
    schema.argument(app, "command", {}, CompletionPath::None, true);
    return true;
  }
  return false;
}

bool
configure(CommandCompletionSchema& schema, const std::string& name)
{
  return configureBasic(schema, name) || ConfigureAdminCompletion(schema, name) ||
         ConfigureStorageCompletion(schema, name);
}

const std::map<std::string, std::vector<std::string>>&
aliases()
{
  static const std::map<std::string, std::vector<std::string>> result{
      {"info", {"file", "info"}},
      {"fileinfo", {"file", "info"}},
      {"mv", {"file", "rename"}},
      {"touch", {"file", "touch"}},
      {"newfind", {"find"}}};
  return result;
}

std::vector<eos::console::HelpCommand>
helpChildren(const CLI::App& app)
{
  std::vector<eos::console::HelpCommand> result;
  for (const auto* child : app.get_subcommands(
           [](const CLI::App* command) { return !command->get_disabled(); })) {
    auto children = helpChildren(*child);
    if (child->get_name().empty()) {
      result.insert(result.end(), children.begin(), children.end());
      continue;
    }
    result.push_back({child->get_name(), children});
    for (const auto& alias : child->get_aliases()) {
      result.push_back({alias, children});
    }
  }
  return result;
}
} // namespace

CommandCompletionResult
CompleteRegisteredCommand(const std::string& name, const std::vector<std::string>& args)
{
  if (const auto found = aliases().find(name); found != aliases().end()) {
    const auto& target = found->second;
    auto forwarded = std::vector<std::string>(target.begin() + 1, target.end());
    forwarded.insert(forwarded.end(), args.begin(), args.end());
    return CompleteRegisteredCommand(target.front(), forwarded);
  }
  CommandCompletionSchema schema(name);
  if (!configure(schema, name)) {
    return {};
  }
  return CompleteCliArguments(*schema.app, args, schema.values, schema.paths,
                              schema.spellings);
}

bool
HasCommandCompletionSchema(const std::string& name)
{
  static const std::set<std::string> nativeSchemas{
      "cd",    "chmod", "chown",  "config",  "df",       "du", "evict",
      "group", "ls",    "member", "recycle", "register", "rm", "route"};
  if (nativeSchemas.count(name) || aliases().count(name)) {
    return true;
  }
  CommandCompletionSchema schema(name);
  return configure(schema, name);
}

namespace {
std::vector<eos::console::HelpCommand>
BuildCommandHelpMetadata()
{
  std::vector<eos::console::HelpCommand> commands;
  for (const auto* command : CommandRegistry::instance().all()) {
    const std::string name = command->name();
    const auto alias = aliases().find(name);
    CommandCompletionSchema schema(alias == aliases().end() ? name
                                                            : alias->second.front());
    eos::console::HelpCommand help{name, {}};
    // These schemas contain declarations only. Do not call command completion or
    // parse here: native command callbacks may validate or contact the instance.
    if (configure(schema, schema.app->get_name())) {
      const CLI::App* target = schema.app.get();
      if (alias != aliases().end()) {
        for (auto part = alias->second.begin() + 1; part != alias->second.end(); ++part) {
          target = target->get_subcommand_no_throw(*part);
          if (!target) {
            break;
          }
        }
      }
      if (target) {
        help.children = helpChildren(*target);
      }
    }
    commands.push_back(std::move(help));
  }
  return commands;
}
} // namespace

void
RegisterCommandHelpMetadata()
{
  eos::console::SetHelpCommandLoader(BuildCommandHelpMetadata);
}
