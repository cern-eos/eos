// ----------------------------------------------------------------------
// File: ConsoleCommandHelp.hh
// ----------------------------------------------------------------------

#pragma once

#include "console/ConsoleHelp.hh"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

class IConsoleCommand;

namespace eos::console {

enum class ShortHelpPolicy { Always, Standalone, Never };

struct CommandHelpTopic {
  std::vector<std::string> path;
  // An authored, unique line prefix in the existing manual. Empty for a group.
  std::string marker;
  std::string summary;
  // Optional unique boundary before a shared appendix or unrelated section.
  std::string endMarker;
};

struct CommandHelpSpec {
  std::vector<CommandHelpTopic> topics;
  ShortHelpPolicy shortHelp = ShortHelpPolicy::Always;
  // Supplement callback-free completion metadata for execution-only schemas.
  std::map<std::string, std::size_t> valueOptions;
  // Optional authored manual for commands without a helpText() implementation.
  std::string manual;
  // Authored shared options/notes needed on root and focused pages.
  std::string common;
};

bool ValidateCommandHelpSpec(std::string_view manual, const CommandHelpSpec& spec,
                             std::string* error = nullptr);
HelpPage BuildCommandHelpPage(std::string_view name, std::string_view description,
                              std::string_view manual, const CommandHelpSpec& spec,
                              const std::vector<std::string>& path = {},
                              bool all = false);
std::unique_ptr<IConsoleCommand> WithCommandHelp(std::unique_ptr<IConsoleCommand> command,
                                                 CommandHelpSpec spec = {});
bool HasStructuredCommandHelp(const IConsoleCommand& command);
std::vector<std::vector<std::string>>
StructuredCommandHelpPaths(const IConsoleCommand& command);
bool ValidateCommandHelp(const IConsoleCommand& command, std::string* error = nullptr);

} // namespace eos::console
