// ----------------------------------------------------------------------
// File: ConsoleCommandHelp.cc
// ----------------------------------------------------------------------

#include "console/ConsoleCommandHelp.hh"
#include "console/CommandCompletion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <cctype>
#include <optional>
#include <set>
#include <sstream>

namespace eos::console {
namespace {
std::string_view
Trim(std::string_view value)
{
  const auto first = value.find_first_not_of(" \t\r\n");
  return first == std::string_view::npos
             ? std::string_view{}
             : value.substr(first, value.find_last_not_of(" \t\r\n") - first + 1);
}

std::string
Join(const std::vector<std::string>& path)
{
  std::string result;
  for (const auto& part : path) {
    if (!result.empty()) {
      result += ' ';
    }
    result += part;
  }
  return result;
}

bool
Prefix(const std::vector<std::string>& prefix, const std::vector<std::string>& path)
{
  return prefix.size() <= path.size() &&
         std::equal(prefix.begin(), prefix.end(), path.begin());
}

struct ManualLine {
  std::size_t offset;
  std::string_view text;
};

std::vector<ManualLine>
Lines(std::string_view text)
{
  std::vector<ManualLine> result;
  for (std::size_t offset = 0; offset < text.size();) {
    const auto end = text.find('\n', offset);
    result.push_back(
        {offset,
         text.substr(offset, end == std::string_view::npos ? end : end - offset)});
    offset = end == std::string_view::npos ? text.size() : end + 1;
  }
  return result;
}

std::vector<std::size_t>
MarkerOffsets(std::string_view manual, std::string_view marker)
{
  std::vector<std::size_t> result;
  for (const auto& line : Lines(manual)) {
    if (line.text.substr(0, marker.size()) == marker) {
      result.push_back(line.offset);
    }
  }
  return result;
}

bool
ExamplesHeading(std::string_view text)
{
  auto heading = std::string(Trim(text));
  std::transform(heading.begin(), heading.end(), heading.begin(),
                 [](unsigned char ch) { return std::tolower(ch); });
  return heading == "examples:" || heading == "examples" || heading == "example:" ||
         heading == "example";
}

struct ExampleLine {
  std::string command;
  std::string description;
};

std::optional<ExampleLine>
NormalizeExample(std::string_view text, std::string_view name)
{
  text = Trim(text);
  if (text.substr(0, 2) == "$ ") {
    text = Trim(text.substr(2));
  }
  if (text.substr(0, 6) == "[eos] ") {
    text = Trim(text.substr(6));
  } else if (text.substr(0, 4) == "eos ") {
    text = Trim(text.substr(4));
  }
  if (text.substr(0, name.size()) != name ||
      (text.size() != name.size() && text[name.size()] != ' ')) {
    return std::nullopt;
  }
  ExampleLine result;
  const auto arrow = text.find(" -> ");
  if (arrow != std::string_view::npos) {
    result.description = "# " + std::string(Trim(text.substr(arrow + 4)));
    text = Trim(text.substr(0, arrow));
  }
  result.command = "eos " + std::string(text);
  return result;
}

bool
ExampleProse(std::string_view line)
{
  if (line.empty() || line.front() == '#') {
    return true;
  }
  // Display classification only: shell snippets and unknown executable lines
  // remain literal. This never supplies command grammar or accepted operands.
  return line.rfind("Note:", 0) == 0 || line.rfind("NOTE:", 0) == 0 ||
         line.rfind("where ", 0) == 0 || line.rfind("e.g.", 0) == 0 ||
         (std::isupper(static_cast<unsigned char>(line.front())) &&
          line.find_first_of("|><`$\\") == std::string_view::npos &&
          std::count(line.begin(), line.end(), ' ') >= 4);
}

void
AppendExample(HelpPage& page, const ExampleLine& example)
{
  if (!example.description.empty()) {
    page.blocks.push_back({HelpBlockKind::Paragraph, example.description, {}});
  }
  page.blocks.push_back({HelpBlockKind::Example, example.command, {}});
}

void
AppendFormatted(HelpPage& page, std::string_view text, std::string_view name,
                const CommandHelpSpec& spec)
{
  std::string paragraph;
  std::size_t definitionIndent = std::string::npos;
  bool examples = false;
  auto flush = [&] {
    if (!paragraph.empty()) {
      page.blocks.push_back({HelpBlockKind::Paragraph, paragraph, {}});
      paragraph.clear();
    }
  };
  for (const auto& entry : Lines(text)) {
    const auto line = Trim(entry.text);
    if (line.empty()) {
      flush();
      definitionIndent = std::string::npos;
      continue;
    }
    const auto topic =
        std::find_if(spec.topics.begin(), spec.topics.end(), [&](const auto& candidate) {
          return !candidate.marker.empty() &&
                 entry.text.substr(0, candidate.marker.size()) == candidate.marker;
        });
    if (topic != spec.topics.end()) {
      flush();
      examples = false;
      definitionIndent = std::string::npos;
      page.blocks.push_back(
          {HelpBlockKind::Section,
           HelpCommandText("eos " + std::string(name) + " " + Join(topic->path)),
           {}});
      auto synopsis = std::string(line);
      if (synopsis.rfind("Usage:", 0) == 0 || synopsis.rfind("usage:", 0) == 0) {
        synopsis = std::string(Trim(std::string_view(synopsis).substr(6)));
      }
      if (synopsis.rfind("eos ", 0) != 0) {
        const auto owner = std::string(name);
        if (synopsis != owner && synopsis.rfind(owner + " ", 0) != 0) {
          synopsis = owner + " " + synopsis;
        }
        synopsis = "eos " + synopsis;
      }
      page.blocks.push_back({HelpBlockKind::Usage, HelpSyntax(synopsis), {}});
      continue;
    }
    if (ExamplesHeading(line)) {
      flush();
      examples = true;
      definitionIndent = std::string::npos;
      page.blocks.push_back({HelpBlockKind::Section, "EXAMPLES", {}});
      continue;
    }
    if (const auto example = NormalizeExample(line, name)) {
      flush();
      AppendExample(page, *example);
      continue;
    }
    if (line.front() == '#' || (examples && ExampleProse(line))) {
      flush();
      page.blocks.push_back({HelpBlockKind::Paragraph, std::string(line), {}});
      continue;
    }
    if (examples) {
      flush();
      page.blocks.push_back({HelpBlockKind::Example, std::string(line), {}});
      continue;
    }
    if (line == "Options:" || line == "OPTIONS" || line == "OPTIONS:" ||
        line == "Usage:" || line == "USAGE" || line == "USAGE:") {
      flush();
      definitionIndent = std::string::npos;
      page.blocks.push_back({HelpBlockKind::Section, std::string(line), {}});
      continue;
    }
    if (line.rfind("Usage:", 0) == 0 || line.rfind("usage:", 0) == 0) {
      flush();
      page.blocks.push_back({HelpBlockKind::Section, "USAGE", {}});
      page.blocks.push_back({HelpBlockKind::Usage, HelpSyntax(Trim(line.substr(6))), {}});
      continue;
    }
    const auto separator = line.find(" : ");
    const auto indent = entry.text.find_first_not_of(" \t");
    if (separator != std::string_view::npos) {
      flush();
      page.blocks.push_back({HelpBlockKind::Definition,
                             HelpSyntax(Trim(line.substr(0, separator))),
                             std::string(Trim(line.substr(separator + 3)))});
      definitionIndent = indent;
    } else if (definitionIndent != std::string::npos && indent > definitionIndent &&
               !page.blocks.empty() &&
               page.blocks.back().kind == HelpBlockKind::Definition) {
      page.blocks.back().description.append(" ").append(line);
    } else {
      definitionIndent = std::string::npos;
      if (!paragraph.empty()) {
        paragraph += ' ';
      }
      paragraph += line;
    }
  }
  flush();
}

std::size_t
GlobalExamplesOffset(std::string_view manual, const CommandHelpSpec& spec)
{
  std::size_t lastTopic = 0;
  for (const auto& topic : spec.topics) {
    if (!topic.marker.empty()) {
      const auto offsets = MarkerOffsets(manual, topic.marker);
      if (offsets.size() == 1) {
        lastTopic = std::max(lastTopic, offsets.front());
      }
    }
  }
  for (const auto& line : Lines(manual)) {
    if (line.offset > lastTopic && ExamplesHeading(line.text)) {
      return line.offset;
    }
  }
  return manual.size();
}

void
AppendExamples(HelpPage& page, std::string_view text, std::string_view name,
               const std::vector<std::string>& path)
{
  const auto prefix = "eos " + std::string(name) + (path.empty() ? "" : " " + Join(path));
  std::vector<std::string> descriptions;
  bool heading = false;
  for (const auto& entry : Lines(text)) {
    const auto line = Trim(entry.text);
    if (line.empty() || ExamplesHeading(line)) {
      continue;
    }
    const auto example = NormalizeExample(line, name);
    if (!example) {
      if (ExampleProse(line)) {
        descriptions.emplace_back(line);
      } else {
        descriptions.clear();
      }
      continue;
    }
    if (example->command.substr(0, prefix.size()) == prefix &&
        (example->command.size() == prefix.size() ||
         example->command[prefix.size()] == ' ')) {
      if (!heading) {
        page.blocks.push_back({HelpBlockKind::Section, "EXAMPLES", {}});
        heading = true;
      }
      for (const auto& description : descriptions) {
        page.blocks.push_back({HelpBlockKind::Paragraph, description, {}});
      }
      AppendExample(page, *example);
    }
    descriptions.clear();
  }
}

const CLI::Option*
FindOption(const CLI::App* command, const std::string& name,
           const CommandCompletionSchema* schema)
{
  if (!command) {
    return nullptr;
  }
  for (const auto* option : command->get_options({})) {
    const auto spelling = schema->spellings.find(option);
    if (spelling != schema->spellings.end()) {
      if (std::find(spelling->second.begin(), spelling->second.end(), name) !=
          spelling->second.end()) {
        return option;
      }
    } else if (option->check_name(name)) {
      return option;
    }
  }
  for (const auto* child : command->get_subcommands({})) {
    if (child->get_name().empty()) {
      if (const auto* option = FindOption(child, name, schema)) {
        return option;
      }
    }
  }
  return nullptr;
}

struct HelpRequest {
  bool requested = false;
  bool all = false;
  bool options = true;
  std::vector<std::string> path;
};

HelpRequest
InspectArguments(const std::string& name, const std::vector<std::string>& args,
                 const CommandHelpSpec& spec)
{
  HelpRequest result;
  auto schema = BuildRegisteredCompletionSchema(name);
  const CLI::App* command = schema ? schema->app.get() : nullptr;
  if (schema) {
    for (const auto& part : schema->prefix) {
      command = command ? command->get_subcommand_no_throw(part) : nullptr;
    }
  }
  int pendingMinimum = 0, pendingMaximum = 0;
  bool selecting = true;
  for (const auto& token : args) {
    if (token == "--") {
      result.options = false;
      break;
    }
    if (pendingMinimum > 0) {
      --pendingMinimum;
      --pendingMaximum;
      continue;
    }
    const bool shortHelp =
        token == "-h" &&
        (spec.shortHelp == ShortHelpPolicy::Always ||
         (spec.shortHelp == ShortHelpPolicy::Standalone && args.size() == 1));
    if (token == "--help" || token == "--help-all" || shortHelp) {
      result.requested = true;
      result.all = result.all || token == "--help-all";
      pendingMaximum = 0;
      continue;
    }
    const auto equals = token.find('=');
    const auto optionName = token.substr(0, equals);
    const auto* option = FindOption(command, optionName, schema.get());
    if (pendingMaximum > 0 && !option && (token.empty() || token.front() != '-')) {
      --pendingMaximum;
      continue;
    }
    pendingMaximum = 0;
    if (option) {
      pendingMinimum = option->get_items_expected_min();
      pendingMaximum = option->get_items_expected_max();
      if (equals != std::string::npos) {
        pendingMinimum = std::max(0, pendingMinimum - 1);
        pendingMaximum = std::max(0, pendingMaximum - 1);
      }
      continue;
    }
    if (const auto configured = spec.valueOptions.find(optionName);
        configured != spec.valueOptions.end()) {
      pendingMinimum = static_cast<int>(configured->second);
      if (equals != std::string::npos) {
        pendingMinimum = std::max(0, pendingMinimum - 1);
      }
      pendingMaximum = pendingMinimum;
      continue;
    }
    if (token.size() > 1 && token.front() == '-') {
      for (std::size_t index = 1; token[1] != '-' && index < token.size(); ++index) {
        const auto shortName = "-" + token.substr(index, 1);
        const auto* member = FindOption(command, shortName, schema.get());
        const auto configured = spec.valueOptions.find(shortName);
        const int arity = member ? member->get_items_expected_min()
                          : configured == spec.valueOptions.end()
                              ? 0
                              : static_cast<int>(configured->second);
        if (arity > 0) {
          pendingMinimum = std::max(0, arity - (index + 1 < token.size() ? 1 : 0));
          pendingMaximum = pendingMinimum;
          break;
        }
      }
      continue;
    }
    auto path = result.path;
    path.push_back(token);
    if (selecting &&
        std::any_of(spec.topics.begin(), spec.topics.end(),
                    [&](const auto& topic) { return Prefix(path, topic.path); })) {
      result.path = std::move(path);
      command = command ? command->get_subcommand_no_throw(token) : nullptr;
    } else {
      selecting = false;
    }
  }
  result.options = result.options && pendingMinimum == 0;
  return result;
}

class CommandWithHelp final : public IConsoleCommand {
public:
  CommandWithHelp(std::unique_ptr<IConsoleCommand> command, CommandHelpSpec spec)
      : mCommand(std::move(command))
      , mSpec(std::move(spec))
  {
  }
  const char*
  name() const override
  {
    return mCommand->name();
  }
  const char*
  description() const override
  {
    return mCommand->description();
  }
  std::string
  helpText() const override
  {
    if (!mSpec.manual.empty()) {
      return mSpec.manual;
    }
    const auto manual = mCommand->helpText();
    return manual.empty()
               ? "Usage: " + std::string(name()) + "\n\n" + description() + "\n"
               : manual;
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    std::vector<std::string> arguments;
    if (!TokenizeArguments(args.c_str(), arguments)) {
      return mCommand->requiresMgm(args);
    }
    return requiresMgmArguments(arguments);
  }
  bool
  requiresMgmArguments(const std::vector<std::string>& args) const override
  {
    return !InspectArguments(name(), args, mSpec).requested &&
           mCommand->requiresMgmArguments(args);
  }
  int
  run(const std::vector<std::string>& args, CommandContext& context) override
  {
    const auto request = InspectArguments(name(), args, mSpec);
    if (request.requested) {
      print(request);
      global_retc = 0;
      return 0;
    }
    return mCommand->run(args, context);
  }
  CommandCompletionResult
  completion(const std::vector<std::string>& args) const override
  {
    auto result = mCommand->completion(args);
    const bool optionContext = std::any_of(
        result.candidates.begin(), result.candidates.end(), [](const auto& candidate) {
          return !candidate.empty() && candidate.front() == '-';
        });
    if (optionContext && InspectArguments(name(), args, mSpec).options) {
      result.candidates.push_back("--help");
      result.candidates.push_back("--help-all");
      std::sort(result.candidates.begin(), result.candidates.end());
      result.candidates.erase(
          std::unique(result.candidates.begin(), result.candidates.end()),
          result.candidates.end());
    }
    return result;
  }
  std::vector<std::string>
  complete(const std::vector<std::string>& args) const override
  {
    return completion(args).candidates;
  }
  std::vector<std::vector<std::string>>
  paths() const
  {
    std::vector<std::vector<std::string>> result;
    for (const auto& topic : mSpec.topics) {
      result.push_back(topic.path);
    }
    return result;
  }
  void
  printHelp() const override
  {
    print({});
  }
  void
  printHelp(const std::vector<std::string>& args) const override
  {
    print(InspectArguments(name(), args, mSpec));
  }
  bool
  validate(std::string* error) const
  {
    return ValidateCommandHelpSpec(helpText(), mSpec, error);
  }

private:
  void
  print(const HelpRequest& request) const
  {
    PrintHelpPage(BuildCommandHelpPage(name(), description(), helpText(), mSpec,
                                       request.path, request.all));
  }
  std::unique_ptr<IConsoleCommand> mCommand;
  CommandHelpSpec mSpec;
};
} // namespace

bool
ValidateCommandHelpSpec(std::string_view manual, const CommandHelpSpec& spec,
                        std::string* error)
{
  if (error) {
    error->clear();
  }
  std::set<std::vector<std::string>> paths;
  for (const auto& topic : spec.topics) {
    if (!paths.insert(topic.path).second) {
      if (error) {
        *error = "duplicate help topic: " + Join(topic.path);
      }
      return false;
    }
    if (!topic.marker.empty() && MarkerOffsets(manual, topic.marker).size() != 1) {
      if (error) {
        *error = "help marker must identify exactly one line: " + topic.marker;
      }
      return false;
    }
    if (!topic.endMarker.empty()) {
      const auto end = MarkerOffsets(manual, topic.endMarker);
      const auto start = MarkerOffsets(manual, topic.marker);
      if (topic.marker.empty() || end.size() != 1 || end.front() <= start.front()) {
        if (error) {
          *error = "help end marker must follow its section: " + topic.endMarker;
        }
        return false;
      }
    }
  }
  return true;
}

HelpPage
BuildCommandHelpPage(std::string_view name, std::string_view description,
                     std::string_view manual, const CommandHelpSpec& spec,
                     const std::vector<std::string>& path, bool all)
{
  HelpPage page;
  if (!ValidateCommandHelpSpec(manual, spec) || spec.topics.empty() ||
      (path.empty() && all)) {
    AppendFormatted(page, manual, name, spec);
    return page;
  }
  const auto selected =
      std::find_if(spec.topics.begin(), spec.topics.end(),
                   [&](const auto& topic) { return topic.path == path; });
  if (selected == spec.topics.end() || selected->marker.empty()) {
    page.blocks.push_back({HelpBlockKind::Section, "USAGE", {}});
    auto command = "eos " + std::string(name);
    if (!path.empty()) {
      command += " " + Join(path);
    }
    page.blocks.push_back({HelpBlockKind::Usage,
                           HelpCommandText(command).append(HelpSyntax(" [options]")),
                           {}});
  }
  page.blocks.push_back(
      {HelpBlockKind::Paragraph,
       selected == spec.topics.end() ? std::string(description) : selected->summary,
       {}});
  if (!spec.common.empty()) {
    AppendFormatted(page, spec.common, name, spec);
  }
  const auto examples = GlobalExamplesOffset(manual, spec);
  std::vector<std::size_t> boundaries{examples};
  for (const auto& topic : spec.topics) {
    if (!topic.marker.empty()) {
      boundaries.push_back(MarkerOffsets(manual, topic.marker).front());
    }
  }
  std::sort(boundaries.begin(), boundaries.end());
  const CommandHelpTopic* inherited = nullptr;
  if (!all && selected != spec.topics.end() && selected->marker.empty()) {
    for (const auto& topic : spec.topics) {
      if (!topic.marker.empty() && Prefix(topic.path, path) &&
          (!inherited || topic.path.size() > inherited->path.size())) {
        inherited = &topic;
      }
    }
  }
  std::set<std::size_t> sections;
  for (const auto& topic : spec.topics) {
    if (!topic.marker.empty() && ((!all && (topic.path == path || &topic == inherited)) ||
                                  (all && Prefix(path, topic.path)))) {
      sections.insert(MarkerOffsets(manual, topic.marker).front());
    }
  }
  for (const auto start : sections) {
    const auto next = std::upper_bound(boundaries.begin(), boundaries.end(), start);
    auto end = next == boundaries.end() ? examples : *next;
    for (const auto& topic : spec.topics) {
      if (!topic.marker.empty() && !topic.endMarker.empty() &&
          MarkerOffsets(manual, topic.marker).front() == start) {
        end = std::min(end, MarkerOffsets(manual, topic.endMarker).front());
      }
    }
    AppendFormatted(page, manual.substr(start, end - start), name, spec);
  }
  bool operations = false;
  for (const auto& topic : spec.topics) {
    if (topic.path.size() != path.size() + 1 || !Prefix(path, topic.path)) {
      continue;
    }
    if (!operations) {
      page.blocks.push_back({HelpBlockKind::Section, "OPERATIONS", {}});
      operations = true;
    }
    page.blocks.push_back(
        {HelpBlockKind::Definition, HelpCommandText(topic.path.back()), topic.summary});
  }
  if (examples < manual.size()) {
    AppendExamples(page, manual.substr(examples), name, path);
  }
  page.blocks.push_back({HelpBlockKind::Section, "LEARN MORE", {}});
  page.blocks.push_back({HelpBlockKind::Paragraph,
                         HelpText("Run '")
                             .append(HelpCommandText("eos " + std::string(name)))
                             .append(HelpSyntax(" --help-all"))
                             .append("' for the complete manual."),
                         {}});
  return page;
}

std::unique_ptr<IConsoleCommand>
WithCommandHelp(std::unique_ptr<IConsoleCommand> command, CommandHelpSpec spec)
{
  return std::make_unique<CommandWithHelp>(std::move(command), std::move(spec));
}

bool
HasStructuredCommandHelp(const IConsoleCommand& command)
{
  return dynamic_cast<const CommandWithHelp*>(&command) != nullptr;
}

std::vector<std::vector<std::string>>
StructuredCommandHelpPaths(const IConsoleCommand& command)
{
  const auto* wrapped = dynamic_cast<const CommandWithHelp*>(&command);
  return wrapped ? wrapped->paths() : std::vector<std::vector<std::string>>{};
}

bool
ValidateCommandHelp(const IConsoleCommand& command, std::string* error)
{
  const auto* wrapped = dynamic_cast<const CommandWithHelp*>(&command);
  return wrapped && wrapped->validate(error);
}
} // namespace eos::console
