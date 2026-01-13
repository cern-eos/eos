// ----------------------------------------------------------------------
// File: ConsoleArgParser.hh
// ----------------------------------------------------------------------

#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>

class ConsoleArgParser {
public:
  struct OptionSpec {
    std::string longName;
    char shortName = '\0';
    bool requiresValue = false;
    bool allowMultiple = false;
    std::string valueName;
    std::string description;
    std::string defaultValue;
  };

  struct ParseResult {
    std::unordered_map<std::string, std::vector<std::string>> optionToValues;
    std::vector<std::string> positionals;
    std::vector<std::string> unknownTokens;
    std::vector<std::string> errors;

    bool has(const std::string& name) const;
    bool flag(const std::string& name) const; // present without a required value
    std::string value(const std::string& name, const std::string& fallback = "") const;
    std::vector<std::string> values(const std::string& name) const;
  };

  ConsoleArgParser();

  ConsoleArgParser& setProgramName(const std::string& nm);
  ConsoleArgParser& setDescription(const std::string& desc);
  ConsoleArgParser& allowCombinedShortOptions(bool allow);
  ConsoleArgParser& allowAttachedValue(bool allow);
  ConsoleArgParser& acceptBareAssignments(bool accept); // e.g. key=value without dashes
  ConsoleArgParser& collectUnknownTokens(bool collect);

  ConsoleArgParser& addOption(const OptionSpec& spec);

  ParseResult parse(const std::vector<std::string>& args) const;

  std::string help() const;

private:
  struct InternalSpec {
    OptionSpec spec;
  };

  const InternalSpec* findByLong(const std::string& nm) const;
  const InternalSpec* findByShort(char c) const;

  std::string mProgramName;
  std::string mDescription;
  bool mAllowCombinedShorts = true;
  bool mAllowAttachedValue = true;
  bool mAcceptBareAssignments = true;
  bool mCollectUnknownTokens = true;

  std::vector<InternalSpec> mSpecs;
  std::unordered_map<std::string, size_t> mLongToIndex;
  std::unordered_map<char, size_t> mShortToIndex;
};


