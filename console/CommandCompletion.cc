#include "console/CommandCompletion.hh"
#include "console/CommandFramework.hh"
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
