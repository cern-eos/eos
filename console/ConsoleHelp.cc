// ----------------------------------------------------------------------
// File: ConsoleHelp.cc
// ----------------------------------------------------------------------

#include "console/ConsoleHelp.hh"

#include <CLI/CLI.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <locale.h>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>
#include <wchar.h>

namespace eos::console {
namespace {

class HelpLocale {
public:
  HelpLocale()
  {
    for (const auto* name : {"C.UTF-8", "en_US.UTF-8", "UTF-8"}) {
      locale = newlocale(LC_CTYPE_MASK, name, nullptr);
      if (locale) {
        break;
      }
    }
  }

  ~HelpLocale()
  {
    if (locale) {
      freelocale(locale);
    }
  }

  locale_t locale = nullptr;
};

class ScopedHelpLocale {
public:
  ScopedHelpLocale()
  {
    static const HelpLocale utf8;
    if (utf8.locale) {
      previous = uselocale(utf8.locale);
    }
  }

  ~ScopedHelpLocale()
  {
    if (previous) {
      uselocale(previous);
    }
  }

private:
  locale_t previous = nullptr;
};

bool
IsSpace(char ch)
{
  return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
}

std::string_view
Trim(std::string_view text)
{
  while (!text.empty() && IsSpace(text.front())) {
    text.remove_prefix(1);
  }
  while (!text.empty() && IsSpace(text.back())) {
    text.remove_suffix(1);
  }
  return text;
}

std::uint32_t
NextCodepoint(std::string_view text, std::size_t& offset)
{
  const auto first = static_cast<unsigned char>(text[offset++]);
  if (first < 0x80) {
    return first;
  }
  const auto length = first >= 0xc2 && first <= 0xdf   ? 2U
                      : first >= 0xe0 && first <= 0xef ? 3U
                      : first >= 0xf0 && first <= 0xf4 ? 4U
                                                       : 0U;
  if (length == 0 || offset + length - 1 > text.size()) {
    return 0xfffd;
  }
  std::uint32_t value = first & (0x7fU >> length);
  for (std::size_t index = 0; index < length - 1; ++index) {
    const auto next = static_cast<unsigned char>(text[offset + index]);
    if ((next & 0xc0) != 0x80) {
      return 0xfffd;
    }
    value = (value << 6) | (next & 0x3f);
  }
  if ((length == 3 && value < 0x800) || (length == 4 && value < 0x10000) ||
      value > 0x10ffff || (value >= 0xd800 && value <= 0xdfff)) {
    return 0xfffd;
  }
  offset += length - 1;
  return value;
}

int
FallbackWidth(std::uint32_t value)
{
  // These common zero-width and wide blocks also cover minimal systems without
  // an installed UTF-8 locale. Normal installations use libc's Unicode tables.
  if ((value >= 0x0300 && value <= 0x036f) || (value >= 0x1ab0 && value <= 0x1aff) ||
      (value >= 0x1dc0 && value <= 0x1dff) || (value >= 0x200b && value <= 0x200f) ||
      (value >= 0x202a && value <= 0x202e) || (value >= 0x2060 && value <= 0x206f) ||
      (value >= 0x20d0 && value <= 0x20ff) || (value >= 0xfe00 && value <= 0xfe0f) ||
      (value >= 0xfe20 && value <= 0xfe2f) || (value >= 0xe0100 && value <= 0xe01ef)) {
    return 0;
  }
  if ((value >= 0x1100 && value <= 0x115f) || value == 0x2329 || value == 0x232a ||
      (value >= 0x2e80 && value <= 0xa4cf && value != 0x303f) ||
      (value >= 0xac00 && value <= 0xd7a3) || (value >= 0xf900 && value <= 0xfaff) ||
      (value >= 0xfe10 && value <= 0xfe19) || (value >= 0xfe30 && value <= 0xfe6f) ||
      (value >= 0xff00 && value <= 0xff60) || (value >= 0xffe0 && value <= 0xffe6) ||
      (value >= 0x1f300 && value <= 0x1faff) || (value >= 0x20000 && value <= 0x3fffd)) {
    return 2;
  }
  return value < 0x20 || (value >= 0x7f && value < 0xa0) ? 0 : 1;
}

std::vector<std::string_view>
Words(std::string_view text)
{
  std::vector<std::string_view> words;
  std::size_t offset = 0;
  while (offset < text.size()) {
    while (offset < text.size() && IsSpace(text[offset])) {
      ++offset;
    }
    const auto start = offset;
    while (offset < text.size() && !IsSpace(text[offset])) {
      // Keep argument placeholders such as <decimal id> copyable.
      if (text[offset] == '<') {
        const auto end = text.find('>', offset + 1);
        if (end != std::string_view::npos &&
            text.substr(offset, end - offset).find('\n') == std::string_view::npos) {
          offset = end + 1;
          continue;
        }
      }
      ++offset;
    }
    if (offset != start) {
      words.push_back(text.substr(start, offset - start));
    }
  }
  return words;
}

using HelpLines = std::vector<HelpText>;

HelpText
TrimHelp(const HelpText& text)
{
  const auto trimmed = Trim(text.text);
  return text.slice(static_cast<std::size_t>(trimmed.data() - text.text.data()),
                    trimmed.size());
}

void
BlankLine(HelpLines& lines)
{
  if (!lines.empty() && !lines.back().text.empty()) {
    lines.emplace_back();
  }
}

void
Wrap(HelpLines& lines, const HelpText& text, std::size_t width, HelpText firstIndent,
     const HelpText& nextIndent)
{
  const auto words = Words(text.text);
  if (words.empty()) {
    return;
  }
  auto line = std::move(firstIndent);
  auto cells = HelpDisplayWidth(line.text);
  bool hasWord = false;
  for (const auto word : words) {
    const auto wordCells = HelpDisplayWidth(word);
    if (hasWord && cells + 1 + wordCells > width) {
      lines.push_back(std::move(line));
      line = nextIndent;
      cells = HelpDisplayWidth(line.text);
      hasWord = false;
    }
    if (hasWord) {
      line.append(" ");
      ++cells;
    }
    line.append(text.slice(static_cast<std::size_t>(word.data() - text.text.data()),
                           word.size()));
    cells += wordCells;
    hasWord = true;
  }
  lines.push_back(std::move(line));
}

void
Paragraphs(HelpLines& lines, const HelpText& text, std::size_t width,
           const HelpText& indent, const HelpText& firstIndent = {})
{
  const std::string_view view = text.text;
  std::size_t start = 0;
  std::size_t offset = 0;
  bool first = true;
  while (offset < view.size()) {
    if (view[offset] != '\n') {
      ++offset;
      continue;
    }
    auto next = offset + 1;
    while (next < view.size() &&
           (view[next] == ' ' || view[next] == '\t' || view[next] == '\r')) {
      ++next;
    }
    if (next < view.size() && view[next] == '\n') {
      const auto paragraph = TrimHelp(text.slice(start, offset - start));
      if (!paragraph.text.empty()) {
        Wrap(lines, paragraph, width,
             first && !firstIndent.text.empty() ? firstIndent : indent, indent);
        first = false;
      }
      BlankLine(lines);
      offset = next + 1;
      start = offset;
    } else {
      ++offset;
    }
  }
  Wrap(lines, text.slice(start), width,
       first && !firstIndent.text.empty() ? firstIndent : indent, indent);
}

void
LiteralLines(HelpLines& lines, const HelpText& text)
{
  std::size_t offset = 0;
  while (offset < text.text.size()) {
    const auto end = text.text.find('\n', offset);
    const auto line =
        text.slice(offset, end == std::string_view::npos ? end : end - offset);
    if (Trim(line.text).empty()) {
      BlankLine(lines);
    } else {
      HelpText indented("  ");
      indented.append(line);
      lines.push_back(std::move(indented));
    }
    if (end == std::string_view::npos) {
      break;
    }
    offset = end + 1;
  }
}

bool
IsDefaultFormatter(const CLI::App* app)
{
  const auto formatter = app->get_formatter();
  const auto* formatterPointer = formatter.get();
  if (!formatterPointer || typeid(*formatterPointer) != typeid(CLI::Formatter)) {
    return false;
  }
  for (const auto* child : app->get_subcommands({})) {
    if (!IsDefaultFormatter(child)) {
      return false;
    }
  }
  return true;
}

class HelpFormatter : public CLI::Formatter {
public:
  explicit HelpFormatter(const CLI::Formatter& formatter)
      : CLI::Formatter(formatter)
  {
  }

  void
  append(HelpPage& page, const CLI::App* app, const std::string& name,
         CLI::AppFormatMode mode, bool expanded = false) const
  {
    const auto descriptionText = make_description(app);
    const auto description = Trim(descriptionText);
    if (!description.empty()) {
      page.blocks.push_back(
          {is_description_paragraph_formatting_enabled() ? HelpBlockKind::Paragraph
                                                         : HelpBlockKind::Example,
           is_description_paragraph_formatting_enabled() ? std::string(description)
                                                         : descriptionText,
           {}});
    }
    if (!expanded) {
      page.blocks.push_back({HelpBlockKind::Section, get_label("Usage") + ":", {}});
      auto usage = std::string(Trim(make_usage(app, name)));
      const auto prefix = get_label("Usage") + ":";
      if (usage.compare(0, prefix.size(), prefix) == 0) {
        usage = std::string(Trim(std::string_view(usage).substr(prefix.size())));
      }
      HelpText synopsis;
      auto arguments = HelpSyntax(usage);
      if (!name.empty() && usage.compare(0, name.size(), name) == 0 &&
          (usage.size() == name.size() || IsSpace(usage[name.size()]))) {
        synopsis.append(HelpCommandText(name));
        arguments = HelpSyntax(std::string_view(usage).substr(name.size()));
      }
      for (const auto* option : app->get_options({})) {
        if (option->get_positional()) {
          markValue(arguments, make_option_name(option, true), TextStyle::Argument);
        }
      }
      synopsis.append(arguments);
      page.blocks.push_back({HelpBlockKind::Usage, std::move(synopsis), {}});
    }

    const auto positionals = app->get_options([](const CLI::Option* option) {
      return !option->get_group().empty() && option->get_positional();
    });
    appendOptions(page, get_label("POSITIONALS"), positionals, true);

    for (const auto& group : app->get_groups()) {
      if (group.empty()) {
        continue;
      }
      const auto options = app->get_options([&](const CLI::Option* option) {
        return option->get_group() == group && option->nonpositional() &&
               (!expanded ||
                (option != app->get_help_ptr() && option != app->get_help_all_ptr()));
      });
      appendOptions(page, group, options, false);
    }

    std::vector<std::string> groups;
    for (const auto* child : app->get_subcommands({})) {
      if (child->get_name().empty()) {
        if (!child->get_group().empty() && child->get_group().front() != '+') {
          page.blocks.push_back({HelpBlockKind::Section, child->get_group() + ":", {}});
          const auto childFormatter =
              std::static_pointer_cast<CLI::Formatter>(child->get_formatter());
          HelpFormatter(*childFormatter).append(page, child, name, mode, true);
        }
        continue;
      }
      const auto& group = child->get_group();
      const auto sameGroup = [&](const std::string& other) {
        return CLI::detail::to_lower(other) == CLI::detail::to_lower(group);
      };
      if (!group.empty() && std::none_of(groups.begin(), groups.end(), sameGroup)) {
        groups.push_back(group);
      }
    }
    for (const auto& group : groups) {
      page.blocks.push_back({HelpBlockKind::Section, group + ":", {}});
      for (const auto* child : app->get_subcommands({})) {
        if (child->get_name().empty() ||
            CLI::detail::to_lower(child->get_group()) != CLI::detail::to_lower(group)) {
          continue;
        }
        if (mode == CLI::AppFormatMode::All) {
          page.blocks.push_back(
              {HelpBlockKind::Section,
               HelpCommandText(name + " " + child->get_display_name(true)),
               {}});
          const auto childFormatter =
              std::static_pointer_cast<CLI::Formatter>(child->get_formatter());
          HelpFormatter(*childFormatter)
              .append(page, child, name + " " + child->get_name(), mode, true);
        } else {
          auto label = HelpCommandText(child->get_display_name(true));
          if (child->get_required()) {
            label.append(" ").append(get_label("REQUIRED"), TextStyle::Warning);
          }
          page.blocks.push_back(
              {HelpBlockKind::Definition, std::move(label), child->get_description()});
        }
      }
    }

    const auto footerText = make_footer(app);
    const auto footer = Trim(footerText);
    if (!footer.empty()) {
      const auto* parent = app->get_parent();
      if (!expanded || !parent ||
          std::string(footer) != std::string(Trim(make_footer(parent)))) {
        page.blocks.push_back(
            {is_footer_paragraph_formatting_enabled() ? HelpBlockKind::Paragraph
                                                      : HelpBlockKind::Example,
             is_footer_paragraph_formatting_enabled() ? std::string(footer) : footerText,
             {}});
      }
    }
  }

private:
  static void
  markValue(HelpText& text, std::string_view value, TextStyle style)
  {
    if (value.empty()) {
      return;
    }
    const auto word = [](unsigned char ch) {
      return std::isalnum(ch) || ch == '_' || ch == '-';
    };
    for (std::size_t offset = 0;
         (offset = text.text.find(value, offset)) != std::string::npos;
         offset += value.size()) {
      if ((offset == 0 || !word(text.text[offset - 1])) &&
          (offset + value.size() == text.text.size() ||
           !word(text.text[offset + value.size()]))) {
        text.mark(offset, value.size(), style);
      }
    }
  }

  void
  appendOptions(HelpPage& page, const std::string& group,
                const std::vector<const CLI::Option*>& options, bool positional) const
  {
    if (options.empty()) {
      return;
    }
    page.blocks.push_back({HelpBlockKind::Section, group + ":", {}});
    for (const auto* option : options) {
      auto label =
          positional ? StyledHelpText(make_option_name(option, true), TextStyle::Argument)
                     : HelpSyntax(make_option_name(option, false));
      if (!positional) {
        for (const auto& name : option->get_snames()) {
          markValue(label, "-" + name, TextStyle::Option);
        }
        for (const auto& name : option->get_lnames()) {
          markValue(label, "--" + name, TextStyle::Option);
        }
      }
      auto attributes = HelpSyntax(make_option_opts(option));
      const auto type = get_label(option->get_type_name());
      const auto typeEnd = type.find_first_of(": [{");
      markValue(attributes, std::string_view(type).substr(0, typeEnd),
                TextStyle::Argument);
      markValue(attributes, option->get_default_str(), TextStyle::Literal);
      markValue(attributes, get_label("REQUIRED"), TextStyle::Warning);
      markValue(attributes, option->get_envname(), TextStyle::Identifier);
      for (const auto* dependency : option->get_needs()) {
        markValue(attributes, dependency->get_name(), TextStyle::Option);
      }
      for (const auto* excluded : option->get_excludes()) {
        markValue(attributes, excluded->get_name(), TextStyle::Option);
      }
      label.append(attributes);
      page.blocks.push_back(
          {HelpBlockKind::Definition, std::move(label), make_option_desc(option)});
    }
  }
};

} // namespace

HelpText&
HelpText::append(std::string_view value, std::optional<TextStyle> style)
{
  const auto offset = text.size();
  text.append(value);
  if (style && !value.empty()) {
    spans.push_back({offset, value.size(), *style});
  }
  return *this;
}

HelpText&
HelpText::append(const HelpText& value)
{
  if (&value == this) {
    return append(HelpText(value));
  }
  const auto offset = text.size();
  text += value.text;
  for (const auto& span : value.spans) {
    spans.push_back({offset + span.offset, span.length, span.style});
  }
  return *this;
}

HelpText&
HelpText::mark(std::size_t offset, std::size_t length, TextStyle style)
{
  if (offset > text.size() || length > text.size() - offset) {
    throw std::out_of_range("Help style span exceeds its text");
  }
  if (length) {
    spans.push_back({offset, length, style});
  }
  return *this;
}

HelpText
HelpText::slice(std::size_t offset, std::size_t length) const
{
  HelpText result(text.substr(offset, length));
  const auto end = offset + result.text.size();
  for (const auto& span : spans) {
    if (span.offset >= text.size()) {
      continue;
    }
    const auto from = std::max(offset, span.offset);
    const auto to = span.offset + std::min(span.length, text.size() - span.offset);
    if (from < std::min(end, to)) {
      result.spans.push_back({from - offset, std::min(end, to) - from, span.style});
    }
  }
  return result;
}

std::string
HelpText::render(int descriptor) const
{
  if (spans.empty() || !ColorEnabled(descriptor)) {
    return text;
  }
  std::vector<std::optional<TextStyle>> styles(text.size());
  for (const auto& span : spans) {
    if (span.offset < text.size()) {
      std::fill_n(styles.begin() + span.offset,
                  std::min(span.length, text.size() - span.offset), span.style);
    }
  }
  std::string result;
  for (std::size_t begin = 0; begin < text.size();) {
    auto end = begin + 1;
    while (end < text.size() && styles[end] == styles[begin]) {
      ++end;
    }
    const std::string_view part(text.data() + begin, end - begin);
    result +=
        styles[begin] ? Colorize(part, *styles[begin], descriptor) : std::string(part);
    begin = end;
  }
  return result;
}

HelpText
StyledHelpText(std::string_view text, TextStyle style)
{
  HelpText result;
  result.append(text, style);
  return result;
}

HelpText
HelpCommandText(std::string_view command)
{
  HelpText result;
  for (std::size_t begin = 0; begin < command.size();) {
    const bool space = IsSpace(command[begin]);
    const bool comma = command[begin] == ',';
    auto end = begin + 1;
    while (end < command.size() && IsSpace(command[end]) == space &&
           (command[end] == ',') == comma) {
      ++end;
    }
    result.append(command.substr(begin, end - begin),
                  space ? std::nullopt
                        : std::optional(comma ? TextStyle::Syntax : TextStyle::Command));
    begin = end;
  }
  return result;
}

HelpText
HelpSyntax(std::string_view text, std::initializer_list<std::string_view> literalChoices)
{
  HelpText result;
  const auto literal = [&](std::string_view value) {
    return std::find(literalChoices.begin(), literalChoices.end(), value) !=
           literalChoices.end();
  };
  const std::string_view punctuation("[]{}|(),=!");
  for (std::size_t begin = 0; begin < text.size();) {
    if (IsSpace(text[begin])) {
      result.append(text.substr(begin++, 1));
      continue;
    }
    if (punctuation.find(text[begin]) != std::string_view::npos) {
      result.append(text.substr(begin++, 1), TextStyle::Syntax);
      continue;
    }
    if (text[begin] == '<') {
      const auto close = text.find('>', begin + 1);
      if (close != std::string_view::npos) {
        const auto inside = text.substr(begin + 1, close - begin - 1);
        bool choices = !inside.empty();
        for (std::size_t part = 0; part < inside.size();) {
          const auto bar = inside.find('|', part);
          const auto end = bar == std::string_view::npos ? inside.size() : bar;
          choices = choices && literal(inside.substr(part, end - part));
          part = end + 1;
        }
        if (choices) {
          result.append("<", TextStyle::Syntax);
          for (std::size_t part = 0; part < inside.size();) {
            const auto bar = inside.find('|', part);
            const auto end = bar == std::string_view::npos ? inside.size() : bar;
            result.append(inside.substr(part, end - part), TextStyle::Literal);
            if (bar != std::string_view::npos) {
              result.append("|", TextStyle::Syntax);
            }
            part = end + 1;
          }
          result.append(">", TextStyle::Syntax);
        } else {
          result.append(text.substr(begin, close + 1 - begin), TextStyle::Argument);
        }
        begin = close + 1;
        continue;
      }
    }
    auto end = begin + 1;
    while (end < text.size() && !IsSpace(text[end]) && text[end] != '<' &&
           punctuation.find(text[end]) == std::string_view::npos) {
      ++end;
    }
    const auto word = text.substr(begin, end - begin);
    const bool option = word.front() == '-' &&
                        std::any_of(word.begin(), word.end(), [](unsigned char value) {
                          return std::isalpha(value) || value == '?';
                        });
    const auto style = option ? TextStyle::Option
                       : word.front() == '/' || word.substr(0, 2) == "./" ||
                               word.substr(0, 3) == "../" ||
                               word.find("://") != std::string_view::npos
                           ? TextStyle::Path
                       : word.back() == ':' ? TextStyle::Identifier
                       : word == "options" || word == "OPTIONS" || word == "SUBCOMMAND" ||
                               word == "SUBCOMMANDS"
                           ? TextStyle::Argument
                       : word == "..." ? TextStyle::Syntax
                                       : TextStyle::Literal;
    result.append(word, style);
    begin = end;
  }
  return result;
}

std::size_t
HelpDisplayWidth(std::string_view text)
{
  const ScopedHelpLocale locale;
  std::size_t cells = 0;
  std::size_t offset = 0;
  while (offset < text.size()) {
    // Width is useful for inspecting rendered help too; SGR adds no cells.
    if (text[offset] == '\033' && offset + 1 < text.size() && text[offset + 1] == '[') {
      offset += 2;
      while (offset < text.size()) {
        const auto ch = static_cast<unsigned char>(text[offset++]);
        if (ch >= 0x40 && ch <= 0x7e) {
          break;
        }
      }
      continue;
    }
    const auto value = NextCodepoint(text, offset);
    if (value == '\t') {
      cells += 8 - cells % 8;
    } else {
      const auto width = wcwidth(static_cast<wchar_t>(value));
      cells += width >= 0 ? width : FallbackWidth(value);
    }
  }
  return cells;
}

std::string
HelpPage::render(std::size_t width, std::optional<int> descriptor) const
{
  width = width == 0 ? 80 : width;
  HelpLines lines;
  auto previous = HelpBlockKind::Section;
  for (const auto& block : blocks) {
    if (Trim(block.text).empty()) {
      continue;
    }
    switch (block.kind) {
    case HelpBlockKind::Section:
      BlankLine(lines);
      Wrap(lines,
           block.text.spans.empty() ? StyledHelpText(block.text.text, TextStyle::Heading)
                                    : block.text,
           width, "", "");
      break;
    case HelpBlockKind::Paragraph:
      if (previous != HelpBlockKind::Section) {
        BlankLine(lines);
      }
      Paragraphs(lines, block.text, width, "  ");
      break;
    case HelpBlockKind::Usage:
      if (previous != HelpBlockKind::Section && previous != HelpBlockKind::Usage) {
        BlankLine(lines);
      }
      Wrap(lines, block.text, width, "  ", "    ");
      break;
    case HelpBlockKind::Definition: {
      if (previous != HelpBlockKind::Section && previous != HelpBlockKind::Definition) {
        BlankLine(lines);
      }
      const auto label = TrimHelp(block.text);
      const auto labelWidth = HelpDisplayWidth(label.text) + 2;
      const auto column = std::clamp<std::size_t>(width / 3, 20, 32);
      if (Trim(block.description).empty()) {
        Wrap(lines, label, width, "  ", "    ");
      } else if (width < 60 || labelWidth + 2 > column) {
        Wrap(lines, label, width, "  ", "    ");
        Paragraphs(lines, block.description, width, "    ");
      } else {
        HelpText firstIndent("  ");
        firstIndent.append(label).append(std::string(column - labelWidth, ' '));
        Paragraphs(lines, block.description, width, std::string(column, ' '),
                   firstIndent);
      }
      break;
    }
    case HelpBlockKind::Example:
      if (previous != HelpBlockKind::Section && previous != HelpBlockKind::Example) {
        BlankLine(lines);
      }
      LiteralLines(lines, block.text);
      break;
    }
    previous = block.kind;
  }
  while (!lines.empty() && lines.back().text.empty()) {
    lines.pop_back();
  }
  std::string output;
  for (const auto& line : lines) {
    output += descriptor ? line.render(*descriptor) : line.text;
    output += '\n';
  }
  return output;
}

std::string
LayoutCliHelp(CLI::App& app, CLI::AppFormatMode mode, std::size_t width)
{
  if (width) {
    if (const auto page = BuildCliHelpPage(app, mode)) {
      return page->render(width);
    }
  }
  return app.help("", mode);
}

std::optional<HelpPage>
BuildCliHelpPage(CLI::App& app, CLI::AppFormatMode mode)
{
  const CLI::App* selected = &app;
  auto name = app.get_name();
  while (!selected->get_subcommands().empty()) {
    selected = selected->get_subcommands().back();
    if (!name.empty()) {
      name += ' ';
    }
    name += selected->get_name();
  }
  if (!IsDefaultFormatter(selected)) {
    return std::nullopt;
  }
  const auto formatter =
      std::static_pointer_cast<CLI::Formatter>(selected->get_formatter());
  HelpFormatter responsive(*formatter);
  HelpPage page;
  responsive.append(page, selected, name, mode, mode == CLI::AppFormatMode::Sub);
  return page;
}

} // namespace eos::console
