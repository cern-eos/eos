// ----------------------------------------------------------------------
// File: ConsoleHelp.hh
// ----------------------------------------------------------------------

#pragma once

#include "console/ConsoleColor.hh"

#include <cstddef>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

namespace eos::console {

enum class HelpBlockKind { Section, Paragraph, Usage, Definition, Example };

struct HelpBlock {
  HelpBlockKind kind;
  std::string text;
  std::string description;
};

class HelpPage {
public:
  std::vector<HelpBlock> blocks;
  std::string render(std::size_t width) const;
};

// Measure terminal cells, independently of the process's current locale.
std::size_t HelpDisplayWidth(std::string_view text);
// A zero width preserves the original formatter, including its byte layout.
std::string LayoutCliHelp(CLI::App& app, CLI::AppFormatMode mode, std::size_t width);

void ConfigureHelp(bool batch, bool json, bool noPager = false);
void SetJsonHelpOutput(bool json);
std::size_t HelpWidth(int descriptor);
void WriteHelpOutput(std::string_view text, std::FILE* stream = stderr);
void PrintHelpPage(const HelpPage& page, std::FILE* stream = stderr);

} // namespace eos::console
