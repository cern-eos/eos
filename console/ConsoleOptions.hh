#pragma once

#include "console/ConsoleCli.hh"
#include "console/ConsoleMetadata.hh"

namespace eos::console {

struct ConsoleOptions {
  bool batch = false;
  bool json = false;
  bool help = false;
  bool version = false;
  bool status = false;
  std::vector<std::string> role;
  std::string application;
  std::string endpoint;
  std::vector<std::string> command;
};

inline bool
ExtractConsoleComment(std::vector<std::string>& arguments, std::string& comment,
                      std::string& error)
{
  comment.clear();
  error.clear();
  std::vector<std::string> remaining;
  bool literal = false;
  bool found = false;
  for (size_t i = 0; i < arguments.size(); ++i) {
    const auto& argument = arguments[i];
    if (argument == "--") {
      literal = true;
    }
    if (!literal && argument == "--comment") {
      if (found || i + 1 == arguments.size() || arguments[i + 1].empty()) {
        error = "--comment requires one nonempty value and may only be specified once";
        return false;
      }
      found = true;
      comment = arguments[++i];
    } else {
      remaining.push_back(argument);
    }
  }
  arguments = std::move(remaining);
  return true;
}

inline bool
ParseConsoleOptions(const std::vector<std::string>& arguments, ConsoleOptions& options,
                    std::string& error)
{
  options = ConsoleOptions{};
  error.clear();
  CLI::App app{"EOS console", "eos"};
  app.set_help_flag("");
  app.prefix_command(CLI::PrefixCommandMode::PositionalOnly);
  app.add_flag("-b,--batch", options.batch);
  app.add_flag("-j,--json", options.json);
  app.add_flag("-h,--help", options.help);
  app.add_flag("-v,--version", options.version);
  app.add_flag("-s", options.status);
  app.add_option("-r,--role", options.role, "user role and group role")
      ->expected(2)
      ->allow_extra_args(false)
      ->check(CLI::Validator{
          [](const std::string& value) {
            return !value.empty() && value.front() != '-' && IsSafeCgiMetadata(value)
                       ? std::string{}
                       : std::string("role must be a nonempty id or mapping name");
          },
          "ROLE"});
  app.add_option("-a,--app", options.application)
      ->check(NonEmptyArgument)
      ->check(CLI::Validator{
          [](const std::string& value) {
            return IsSafeCgiMetadata(value)
                       ? std::string{}
                       : std::string("app cannot contain '&' or control characters");
          },
          "APP"});
  std::vector<std::string> reversed(arguments.rbegin(), arguments.rend());
  try {
    app.parse(reversed);
  } catch (const CLI::ParseError& failure) {
    error = failure.what();
    return false;
  }
  auto remaining = app.remaining();
  if (!remaining.empty() && remaining.front() == "--") {
    remaining.erase(remaining.begin());
  } else if (!remaining.empty() && remaining.front().size() > 1 &&
             remaining.front().front() == '-') {
    error = "unknown global option: " + remaining.front();
    return false;
  }
  if (!remaining.empty() && (remaining.front().rfind("root://", 0) == 0 ||
                             remaining.front().rfind("ipc://", 0) == 0)) {
    options.endpoint = remaining.front();
    remaining.erase(remaining.begin());
  }
  if (options.status && !remaining.empty()) {
    error = "-s selects the status command and cannot be combined with another command";
    return false;
  }
  options.command = std::move(remaining);
  return true;
}

} // namespace eos::console
