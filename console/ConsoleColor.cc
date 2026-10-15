// ----------------------------------------------------------------------
// File: ConsoleColor.cc
// ----------------------------------------------------------------------

#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <array>
#include <cctype>
#include <cstdlib>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <term.h>
#include <unistd.h>

namespace eos::console {
namespace {
ColorMode colorMode = ColorMode::Auto;
bool configured = false;
bool batchOutput = false;
bool jsonOutput = false;
HelpCommandLoader helpCommandLoader = nullptr;

std::vector<HelpCommand>&
helpCommands()
{
  static std::vector<HelpCommand> commands;
  return commands;
}

struct Palette {
  std::array<std::string, 8> styles;
  std::string reset;
  bool capable = false;
};

std::string
capability(const char* name)
{
  const char* value = tigetstr(const_cast<char*>(name));
  return value && value != reinterpret_cast<char*>(-1) ? value : "";
}

Palette
loadPalette(const std::string& terminal, int descriptor)
{
  Palette result;
  if (terminal.empty() || terminal == "dumb") {
    return result;
  }
  TERMINAL* previous = cur_term;
  int error = 0;
  if (setupterm(const_cast<char*>(terminal.c_str()), descriptor, &error) == 0 &&
      error == 1) {
    const auto foreground = capability("setaf");
    result.reset = capability("sgr0");
    const int availableColors = tigetnum(const_cast<char*>("colors"));
    // Direct-color capabilities can interpret small indexes as near-black RGB.
    if (availableColors >= 8 && availableColors <= 256 && !foreground.empty() &&
        !result.reset.empty()) {
      const std::array<long, 7> colorIndexes{6, 2, 6, 3, 1, 2, 3};
      for (size_t i = 0; i < colorIndexes.size(); ++i) {
        const char* sequence =
            tparm(const_cast<char*>(foreground.c_str()), colorIndexes[i]);
        if (!sequence) {
          break;
        }
        result.styles[i] = sequence;
      }
      const auto bold = capability("bold");
      result.styles[0] = bold + result.styles[0];
      result.styles[1] = bold + result.styles[1];
      result.styles[7] = capability("dim");
      result.capable =
          std::all_of(result.styles.begin(), result.styles.begin() + 7,
                      [](const std::string& value) {
                        return !value.empty() && value.find("$<") == std::string::npos;
                      }) &&
          result.reset.find("$<") == std::string::npos;
      if (result.styles[7].find("$<") != std::string::npos) {
        result.styles[7].clear();
      }
    }
  }
  TERMINAL* created = cur_term;
  set_curterm(previous);
  if (created && created != previous) {
    del_curterm(created);
  }
  return result;
}

Palette
palette(int descriptor)
{
  const char* value = getenv("TERM");
  const std::string terminal = value ? value : "";
  static std::mutex mutex;
  static std::map<std::string, Palette> cache;
  std::lock_guard<std::mutex> lock(mutex);
  auto it = cache.find(terminal);
  if (it == cache.end()) {
    it = cache.emplace(terminal, loadPalette(terminal, descriptor)).first;
  }
  auto result = it->second;
  if (!result.capable && colorMode == ColorMode::Always) {
    // Explicit forcing also supports pagers when TERM is unavailable.
    result.styles = {"\033[1;36m", "\033[1;32m", "\033[36m", "\033[33m",
                     "\033[31m",   "\033[32m",   "\033[33m", "\033[2m"};
    result.reset = "\033[0m";
  }
  return result;
}

bool
helpWordCharacter(char value)
{
  return std::isalnum(static_cast<unsigned char>(value)) || value == '-' ||
         value == '_' || value == '?';
}

size_t
skipHelpSpace(std::string_view line, size_t position)
{
  while (position < line.size() &&
         std::isspace(static_cast<unsigned char>(line[position]))) {
    ++position;
  }
  return position;
}

bool
commandCharacter(char value)
{
  return helpWordCharacter(value) || value == '.';
}

using HelpLevels = std::vector<const std::vector<HelpCommand>*>;
using HelpStyles = std::vector<std::optional<TextStyle>>;

bool
markHelpCommand(std::string_view line, size_t& position, const HelpLevels& levels,
                std::vector<const HelpCommand*>& matched, HelpStyles& styles)
{
  const size_t begin = position;
  size_t end = begin;
  while (end < line.size() && (commandCharacter(line[end]) || line[end] == '|')) {
    ++end;
  }
  if (end == begin) {
    return false;
  }
  matched.clear();
  for (size_t part = begin; part < end;) {
    const auto separator = line.find('|', part);
    const auto stop =
        std::min(separator == std::string_view::npos ? end : separator, end);
    const auto word = line.substr(part, stop - part);
    const HelpCommand* found = nullptr;
    for (const auto* level : levels) {
      const auto item =
          std::find_if(level->begin(), level->end(),
                       [&](const HelpCommand& command) { return command.name == word; });
      if (item != level->end()) {
        found = &*item;
        break;
      }
    }
    if (!found) {
      return false;
    }
    matched.push_back(found);
    part = stop + 1;
  }
  for (size_t i = begin; i < end; ++i) {
    styles[i] = line[i] == '|' ? TextStyle::Syntax : TextStyle::Command;
  }
  position = end;
  return true;
}

std::string
colorHelpText(std::string_view text, int descriptor,
              const std::set<std::string>& options = {},
              std::vector<HelpCommand> roots = {})
{
  if (!ColorEnabled(descriptor)) {
    return std::string(text);
  }
  if (roots.empty()) {
    auto& vocabulary = helpCommands();
    if (vocabulary.empty() && helpCommandLoader) {
      vocabulary = helpCommandLoader();
    }
    roots = vocabulary;
  }
  // Standalone/custom help can name an application outside the console registry.
  for (size_t begin = 0; begin < text.size();) {
    const auto end = text.find('\n', begin);
    auto line = text.substr(begin, end == std::string_view::npos ? text.size() - begin
                                                                 : end - begin);
    const auto first = skipHelpSpace(line, 0);
    if (line.substr(first, 6) == "Usage:" || line.substr(first, 6) == "usage:" ||
        line.substr(first, 6) == "USAGE:") {
      auto word = skipHelpSpace(line, first + 6);
      auto stop = word;
      while (stop < line.size() && commandCharacter(line[stop])) {
        ++stop;
      }
      const std::string name(line.substr(word, stop - word));
      if (!name.empty() && name != "eos" &&
          std::none_of(roots.begin(), roots.end(),
                       [&](const HelpCommand& root) { return root.name == name; })) {
        roots.push_back({name, {}});
      }
    }
    if (end == std::string_view::npos) {
      break;
    }
    begin = end + 1;
  }
  roots.push_back({"eos", roots});
  const HelpCommand* page = nullptr;
  std::vector<const HelpCommand*> sections;
  HelpLevels definitions;
  size_t commandIndent = std::string_view::npos;
  bool firstLine = true;
  std::string result;
  for (size_t begin = 0; begin < text.size();) {
    const auto end = text.find('\n', begin);
    const auto line = text.substr(
        begin, end == std::string_view::npos ? text.size() - begin : end - begin);
    const auto first = skipHelpSpace(line, 0);
    const auto last = line.find_last_not_of(" \t\r");
    HelpStyles styles(line.size());
    auto mark = [&](size_t from, size_t to, TextStyle style) {
      std::fill(styles.begin() + from, styles.begin() + to, style);
    };
    size_t syntax = first;
    bool labelled = false;
    for (const auto label :
         {"Usage:", "usage:", "USAGE:", "Run with:", "Example:", "example:"}) {
      const std::string_view prefix(label);
      if (line.substr(first, prefix.size()) == prefix) {
        mark(first, first + prefix.size(), TextStyle::Heading);
        syntax = skipHelpSpace(line, first + prefix.size());
        labelled = true;
        break;
      }
    }
    if (first < line.size() && !labelled && line[last] == ':' && line[first] != '-') {
      // Sentence-like descriptions ending in a colon are not section headings.
      std::string title(line.substr(first, last - first));
      std::transform(title.begin(), title.end(), title.begin(),
                     [](unsigned char value) { return std::tolower(value); });
      const bool shortTitle = title.size() <= 48 &&
                              title.find_first_of("./<>=?!") == std::string::npos &&
                              std::count(title.begin(), title.end(), ' ') <= 2;
      if (first == 0 || shortTitle) {
        mark(first, last + 1, TextStyle::Heading);
      }
    }
    if (first == 0 && line.size() <= 48 &&
        std::any_of(line.begin(), line.end(),
                    [](unsigned char value) { return value >= 'A' && value <= 'Z'; }) &&
        std::all_of(line.begin(), line.end(), [](unsigned char value) {
          return (value >= 'A' && value <= 'Z') || value == ' ';
        })) {
      mark(0, line.size(), TextStyle::Heading);
    }
    for (const auto label : {"Note:", "Warning:", "Deprecated:"}) {
      const std::string_view prefix(label);
      if (line.substr(first, prefix.size()) == prefix) {
        mark(first, first + prefix.size(), TextStyle::Warning);
      }
    }
    for (size_t i = 0; i < line.size();) {
      if (line[i] == '<') {
        const auto close = line.find('>', i + 1);
        if (close != std::string_view::npos && close > i + 1 &&
            line.find('<', i + 1) > close) {
          mark(i, close + 1, TextStyle::Argument);
          i = close + 1;
          continue;
        }
      }
      if (line[i] == '[' || line[i] == ']' || line[i] == '|') {
        styles[i] = TextStyle::Syntax;
      }
      const bool boundary =
          i == 0 || std::isspace(static_cast<unsigned char>(line[i - 1])) ||
          std::string_view("[({|,;/\"'").find(line[i - 1]) != std::string_view::npos;
      if (line[i] == '-' && boundary) {
        size_t stop = i + 1;
        bool letter = false;
        while (stop < line.size() && helpWordCharacter(line[stop])) {
          letter = letter || std::isalpha(static_cast<unsigned char>(line[stop])) ||
                   line[stop] == '?';
          ++stop;
        }
        const std::string option(line.substr(i, stop - i));
        const bool numericOption =
            option.size() > 1 &&
            std::all_of(option.begin() + 1, option.end(),
                        [](unsigned char value) { return std::isdigit(value); }) &&
            (i == first || (i > 0 && std::string_view("[|,").find(line[i - 1]) !=
                                         std::string_view::npos));
        if (letter || numericOption || option == "--" || options.count(option)) {
          mark(i, stop, TextStyle::Option);
          i = stop;
          continue;
        }
      }
      ++i;
    }
    HelpLevels levels{&roots};
    std::vector<const HelpCommand*> matched;
    size_t position = syntax;
    bool command = markHelpCommand(line, position, levels, matched, styles);
    bool rootless = false;
    size_t sectionDepth = 0;
    if (!command && page) {
      // Rootless synopsis rows and aligned definitions use the current grammar.
      const bool definition = line.find(" :", syntax) != std::string_view::npos;
      const auto wordEnd = line.find_first_of(" \t,", syntax);
      const bool tableRow = wordEnd == std::string_view::npos || line[wordEnd] == ',' ||
                            line[wordEnd] == '\t' ||
                            (wordEnd + 1 < line.size() && line[wordEnd + 1] == ' ');
      for (size_t depth = sections.size() + 1; depth > 0; --depth) {
        sectionDepth = depth - 1;
        const auto* context = sectionDepth ? sections[sectionDepth - 1] : page;
        levels = definition && !definitions.empty() ? definitions
                                                    : HelpLevels{&context->children};
        auto trial = styles;
        position = syntax;
        if (!markHelpCommand(line, position, levels, matched, trial)) {
          continue;
        }
        bool synopsis = false;
        if (labelled || first <= commandIndent) {
          synopsis = line.find_first_of("[<", syntax) != std::string_view::npos;
          if (!synopsis) {
            HelpLevels children;
            for (const auto* item : matched) {
              children.push_back(&item->children);
            }
            auto next = skipHelpSpace(line, position);
            std::vector<const HelpCommand*> child;
            synopsis = markHelpCommand(line, next, children, child, trial);
          }
        }
        if (tableRow || definition || synopsis) {
          styles.swap(trial);
          command = rootless = true;
          break;
        }
      }
    }
    if (command) {
      const bool synopsis = labelled || first <= commandIndent;
      HelpLevels parent = levels;
      const HelpCommand* root = matched.front();
      if (root->name != "eos" && levels.front() == &roots &&
          (!page || labelled || synopsis)) {
        page = root;
        sections.clear();
      }
      const HelpCommand* selected = root;
      while (true) {
        parent = levels;
        size_t aliasPosition = skipHelpSpace(line, position);
        while (aliasPosition < line.size() && line[aliasPosition] == ',') {
          const auto comma = aliasPosition;
          aliasPosition = skipHelpSpace(line, comma + 1);
          std::vector<const HelpCommand*> aliases;
          if (!markHelpCommand(line, aliasPosition, parent, aliases, styles)) {
            break;
          }
          styles[comma] = TextStyle::Syntax;
          matched.insert(matched.end(), aliases.begin(), aliases.end());
          position = aliasPosition;
          aliasPosition = skipHelpSpace(line, position);
        }
        levels.clear();
        for (const auto* item : matched) {
          levels.push_back(&item->children);
        }
        selected = matched.front();
        size_t next = skipHelpSpace(line, position);
        // Global flags and optional flags can precede a documented subcommand.
        while (next < line.size() && (line[next] == '[' || line[next] == '-')) {
          if (line[next] == '[') {
            const auto close = line.find(']', next + 1);
            if (close == std::string_view::npos) {
              break;
            }
            next = skipHelpSpace(line, close + 1);
          } else {
            while (next < line.size() && helpWordCharacter(line[next])) {
              ++next;
            }
            next = skipHelpSpace(line, next);
          }
        }
        if (!markHelpCommand(line, next, levels, matched, styles)) {
          break;
        }
        if (root->name == "eos" && matched.front()->name != "eos" && synopsis) {
          page = matched.front();
          root = page;
        }
        position = next;
      }
      if (rootless && skipHelpSpace(line, position) == line.size()) {
        sections.resize(sectionDepth);
        sections.push_back(selected);
      }
      if (synopsis) {
        definitions = parent;
        if (!labelled) {
          commandIndent = std::min(commandIndent, first);
        }
      }
    }
    if (firstLine && first < line.size() && !command && !labelled &&
        std::none_of(
            styles.begin(), styles.end(),
            [](const auto& style) { return style.has_value(); })) {
      mark(first, line.size(), TextStyle::Heading);
    }
    for (size_t i = 0; i < line.size();) {
      size_t stop = i + 1;
      while (stop < line.size() && styles[stop] == styles[i]) {
        ++stop;
      }
      const auto part = line.substr(i, stop - i);
      result += styles[i] ? Colorize(part, *styles[i], descriptor) : std::string(part);
      i = stop;
    }
    if (first < line.size()) {
      firstLine = false;
    }
    if (end == std::string_view::npos) {
      break;
    }
    result += '\n';
    begin = end + 1;
  }
  return result;
}

void
collectHelpWords(const CLI::App& app, std::set<std::string>& options,
                 HelpCommand& command)
{
  command.name = app.get_name();
  for (const auto* option : app.get_options()) {
    for (const auto& name : option->get_snames()) {
      options.insert("-" + name);
    }
    for (const auto& name : option->get_lnames()) {
      options.insert("--" + name);
    }
  }
  for (const auto* child : app.get_subcommands([](const CLI::App*) { return true; })) {
    HelpCommand nested;
    collectHelpWords(*child, options, nested);
    if (nested.name.empty()) {
      command.children.insert(command.children.end(), nested.children.begin(),
                              nested.children.end());
    } else {
      for (const auto& alias : child->get_aliases()) {
        command.children.push_back({alias, nested.children});
      }
      command.children.push_back(std::move(nested));
    }
  }
}
} // namespace

std::vector<HelpCommand>
SetHelpCommands(std::vector<HelpCommand> commands)
{
  helpCommands().swap(commands);
  return commands;
}

HelpCommandLoader
SetHelpCommandLoader(HelpCommandLoader loader)
{
  std::swap(loader, helpCommandLoader);
  helpCommands().clear();
  return loader;
}

void
ConfigureColor(ColorMode mode, bool batch, bool json)
{
  configured = true;
  colorMode = mode;
  batchOutput = batch;
  jsonOutput = json;
}

void
SetJsonColorOutput(bool json)
{
  jsonOutput = json;
}

bool
ShouldColor(ColorMode mode, bool terminal, bool capable, bool noColor, bool batch,
            bool machine)
{
  if (batch || machine || mode == ColorMode::Never) {
    return false;
  }
  return mode == ColorMode::Always || (terminal && capable && !noColor);
}

bool
ColorEnabled(int descriptor, OutputKind kind)
{
  if (!configured) {
    return false;
  }
  const char* noColor = getenv("NO_COLOR");
  if (!ShouldColor(colorMode, isatty(descriptor), true, noColor && *noColor, batchOutput,
                   jsonOutput || kind != OutputKind::Human)) {
    return false;
  }
  return colorMode == ColorMode::Always || palette(descriptor).capable;
}

bool
ServerColorEnabled()
{
  // Shared helpers also run in servers, where console styling is not initialized.
  if (!configured) {
    return isatty(STDOUT_FILENO) && isatty(STDERR_FILENO);
  }
  // The legacy protocol has one switch covering both output streams.
  return ColorEnabled(STDOUT_FILENO) && ColorEnabled(STDERR_FILENO);
}

std::string
Colorize(std::string_view text, TextStyle style, int descriptor, OutputKind kind)
{
  if (text.empty() || !ColorEnabled(descriptor, kind)) {
    return std::string(text);
  }
  const auto selected = palette(descriptor);
  if (selected.styles[static_cast<size_t>(style)].empty()) {
    return std::string(text);
  }
  return selected.styles[static_cast<size_t>(style)] + std::string(text) + selected.reset;
}

std::string
ColorPrompt(std::string_view text, TextStyle style, int descriptor)
{
  if (!ColorEnabled(descriptor) || text.empty()) {
    return std::string(text);
  }
  const auto selected = palette(descriptor);
  if (selected.styles[static_cast<size_t>(style)].empty()) {
    return std::string(text);
  }
  return "\001" + selected.styles[static_cast<size_t>(style)] + "\002" +
         std::string(text) + "\001" + selected.reset + "\002";
}

void
PrintLegacyHelp(std::string_view text, std::FILE* stream)
{
  if (!text.empty()) {
    const auto help = colorHelpText(text, fileno(stream));
    WriteHelpOutput(help, stream);
  }
}

void
PrintCliHelp(CLI::App& app, CLI::AppFormatMode mode, std::FILE* stream)
{
  // CLI11 selects parsed subcommands and finishes layout before decoration.
  // Keeping formatters untouched also covers custom child formatters.
  const auto text = LayoutCliHelp(app, mode, HelpWidth(fileno(stream)));
  std::set<std::string> options;
  HelpCommand command;
  if (ColorEnabled(fileno(stream))) {
    collectHelpWords(app, options, command);
  }
  const auto help = colorHelpText(text, fileno(stream), options, {std::move(command)});
  WriteHelpOutput(help, stream);
}

} // namespace eos::console
