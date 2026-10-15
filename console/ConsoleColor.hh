// ----------------------------------------------------------------------
// File: ConsoleColor.hh
// ----------------------------------------------------------------------

#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

namespace CLI {
class App;
enum class AppFormatMode : std::uint8_t;
} // namespace CLI

namespace eos::console {

enum class ColorMode { Auto, Always, Never };
enum class OutputKind { Human, Machine, Raw };
enum class TextStyle {
  Heading,
  Command,
  Option,
  Warning,
  Error,
  Success,
  Argument,
  Syntax
};

struct HelpCommand {
  std::string name;
  std::vector<HelpCommand> children;
};

// Copy command names only; helpers never depend on the console registry/parser.
// Return the previous vocabulary so scoped users can restore it.
std::vector<HelpCommand> SetHelpCommands(std::vector<HelpCommand> commands);
using HelpCommandLoader = std::vector<HelpCommand> (*)();
HelpCommandLoader SetHelpCommandLoader(HelpCommandLoader loader);

// One policy for local rendering and the existing server dontcolor field.
void ConfigureColor(ColorMode mode, bool batch, bool json);
void SetJsonColorOutput(bool json);
bool ShouldColor(ColorMode mode, bool terminal, bool capable, bool noColor, bool batch,
                 bool machine);
bool ColorEnabled(int descriptor, OutputKind kind = OutputKind::Human);
bool ServerColorEnabled();
std::string Colorize(std::string_view text, TextStyle style, int descriptor,
                     OutputKind kind = OutputKind::Human);
std::string ColorPrompt(std::string_view text, TextStyle style, int descriptor);
// Render local help after layout; never apply this to command/server data.
void PrintLegacyHelp(std::string_view text, std::FILE* stream = stderr);
void PrintCliHelp(CLI::App& app, CLI::AppFormatMode mode, std::FILE* stream = stderr);

} // namespace eos::console
