// ----------------------------------------------------------------------
// File: ConsoleHelp.hh
// ----------------------------------------------------------------------

#pragma once

#include "console/ConsoleColor.hh"

#include <cstddef>
#include <cstdio>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace eos::console {

enum class HelpBlockKind { Section, Paragraph, Usage, Definition, Example };

struct HelpSpan {
  std::size_t offset;
  std::size_t length;
  TextStyle style;
};

struct HelpText {
  HelpText() = default;
  HelpText(const char* value)
      : text(value)
  {
  }
  HelpText(std::string value)
      : text(std::move(value))
  {
  }
  HelpText(std::string_view value)
      : text(value)
  {
  }

  operator std::string_view() const { return text; }
  HelpText& append(std::string_view value, std::optional<TextStyle> style = std::nullopt);
  HelpText&
  append(const char* value, std::optional<TextStyle> style = std::nullopt)
  {
    return append(std::string_view(value), style);
  }
  HelpText&
  append(const std::string& value, std::optional<TextStyle> style = std::nullopt)
  {
    return append(std::string_view(value), style);
  }
  HelpText& append(const HelpText& value);
  HelpText& mark(std::size_t offset, std::size_t length, TextStyle style);
  HelpText slice(std::size_t offset, std::size_t length = std::string::npos) const;
  std::string render(int descriptor) const;

  std::string text;
  std::vector<HelpSpan> spans;
};

HelpText StyledHelpText(std::string_view text, TextStyle style);
HelpText HelpCommandText(std::string_view command);
// Tokenize declared usage/argument syntax, never arbitrary prose or command data.
HelpText HelpSyntax(std::string_view text,
                    std::initializer_list<std::string_view> literalChoices = {});

struct HelpBlock {
  HelpBlockKind kind;
  HelpText text;
  HelpText description;
};

class HelpPage {
public:
  std::vector<HelpBlock> blocks;
  std::string render(std::size_t width,
                     std::optional<int> descriptor = std::nullopt) const;
};

// Measure terminal cells, independently of the process's current locale.
std::size_t HelpDisplayWidth(std::string_view text);
// A zero width preserves the original formatter, including its byte layout.
std::string LayoutCliHelp(CLI::App& app, CLI::AppFormatMode mode, std::size_t width);
std::optional<HelpPage> BuildCliHelpPage(CLI::App& app, CLI::AppFormatMode mode);

void ConfigureHelp(bool batch, bool json);
void SetJsonHelpOutput(bool json);
std::size_t HelpWidth(int descriptor);
void PrintHelpPage(const HelpPage& page, std::FILE* stream = stderr);

} // namespace eos::console
