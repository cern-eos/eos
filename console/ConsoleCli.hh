#pragma once

#include "console/ConsoleColor.hh"

#include <CLI/CLI.hpp>
#include <cctype>
#include <charconv>
#include <iostream>
#include <string>
#include <vector>

namespace eos::console {

inline bool
TokenizeArguments(const char* arguments, std::vector<std::string>& result,
                  std::string* error = nullptr)
{
  result.clear();
  if (error) {
    error->clear();
  }
  enum class Quote { None, Single, Double };
  Quote quote = Quote::None;
  std::string token;
  bool started = false;
  const std::string input = arguments ? arguments : "";
  auto reject = [&](const char* reason) {
    result.clear();
    if (error) {
      *error = reason;
    }
    return false;
  };
  for (size_t i = 0; i < input.size(); ++i) {
    const char current = input[i];
    if (current == '\\' && quote != Quote::Single) {
      if (i + 1 == input.size()) {
        return reject("trailing escape in command line");
      }
      const char next = input[i + 1];
      if (quote == Quote::None || next == '"' || next == '\\') {
        token.push_back(next);
        ++i;
      } else {
        token.push_back(current);
      }
      started = true;
    } else if (current == '\'' && quote != Quote::Double) {
      quote = quote == Quote::Single ? Quote::None : Quote::Single;
      started = true;
    } else if (current == '"' && quote != Quote::Single) {
      quote = quote == Quote::Double ? Quote::None : Quote::Double;
      started = true;
    } else if (quote == Quote::None &&
               std::isspace(static_cast<unsigned char>(current))) {
      if (started) {
        result.emplace_back(std::move(token));
        token.clear();
        started = false;
      }
    } else {
      token.push_back(current);
      started = true;
    }
  }
  if (quote != Quote::None) {
    return reject("unterminated quote in command line");
  }
  if (started) {
    result.emplace_back(std::move(token));
  }
  return true;
}

inline std::vector<std::string>
TokenizeArguments(const char* arguments)
{
  std::vector<std::string> result;
  TokenizeArguments(arguments, result);
  return result;
}

inline bool
ParseCliArguments(CLI::App& app, const std::vector<std::string>& arguments)
{
  // CLI11 consumes its vector from the back. Keep the caller's argv intact.
  std::vector<std::string> reversed(arguments.rbegin(), arguments.rend());
  try {
    app.parse(reversed);
    return true;
  } catch (const CLI::ParseError& error) {
    if (error.get_exit_code() != 0) {
      std::cerr << Colorize("error:", TextStyle::Error, fileno(stderr)) << " "
                << error.what() << '\n';
    }
    return false;
  }
}

inline const CLI::Validator NonEmptyArgument{
    [](const std::string& value) {
      return value.empty() ? std::string("value must not be empty") : std::string{};
    },
    "NONEMPTY"};

inline const CLI::Validator DecimalNumber{
    [](std::string& value) {
      uint64_t number = 0;
      const auto result =
          std::from_chars(value.data(), value.data() + value.size(), number);
      if (value.empty() || result.ec != std::errc{} ||
          result.ptr != value.data() + value.size()) {
        return std::string("expected an unsigned decimal integer");
      }
      value = std::to_string(number);
      return std::string{};
    },
    "DECIMAL"};

inline const CLI::Validator SignedDecimalNumber{
    [](std::string& value) {
      int64_t number = 0;
      const auto result =
          std::from_chars(value.data(), value.data() + value.size(), number);
      if (value.empty() || result.ec != std::errc{} ||
          result.ptr != value.data() + value.size()) {
        return std::string("expected a signed decimal integer");
      }
      value = std::to_string(number);
      return std::string{};
    },
    "SIGNED_DECIMAL"};

} // namespace eos::console
