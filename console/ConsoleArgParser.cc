// ----------------------------------------------------------------------
// File: ConsoleArgParser.cc
// ----------------------------------------------------------------------

#include "console/ConsoleArgParser.hh"
#include <sstream>

namespace {
static bool starts_with(const std::string& s, const char* pfx) {
  size_t n = 0; while (pfx[n]) ++n; if (s.size() < n) return false; return s.compare(0, n, pfx) == 0;
}
}

ConsoleArgParser::ConsoleArgParser() = default;

namespace {
static inline std::string dequoteToken(const std::string& in) {
  if (in.size() >= 2) {
    char a = in.front();
    char b = in.back();
    if ((a == '"' && b == '"') || (a == '\'' && b == '\'')) {
      // avoid "" and '' edge-case returning empty meaningful token
      return in.substr(1, in.size() - 2);
    }
  }
  return in;
}
}

ConsoleArgParser& ConsoleArgParser::setProgramName(const std::string& nm) {
  mProgramName = nm; return *this;
}

ConsoleArgParser& ConsoleArgParser::setDescription(const std::string& desc) {
  mDescription = desc; return *this;
}

ConsoleArgParser& ConsoleArgParser::allowCombinedShortOptions(bool allow) {
  mAllowCombinedShorts = allow; return *this;
}

ConsoleArgParser& ConsoleArgParser::allowAttachedValue(bool allow) {
  mAllowAttachedValue = allow; return *this;
}

ConsoleArgParser& ConsoleArgParser::acceptBareAssignments(bool accept) {
  mAcceptBareAssignments = accept; return *this;
}

ConsoleArgParser& ConsoleArgParser::collectUnknownTokens(bool collect) {
  mCollectUnknownTokens = collect; return *this;
}

ConsoleArgParser& ConsoleArgParser::addOption(const OptionSpec& spec) {
  InternalSpec is; is.spec = spec;
  size_t idx = mSpecs.size();
  mSpecs.push_back(is);
  if (!spec.longName.empty()) mLongToIndex.emplace(spec.longName, idx);
  if (spec.shortName) mShortToIndex.emplace(spec.shortName, idx);
  return *this;
}

const ConsoleArgParser::InternalSpec* ConsoleArgParser::findByLong(const std::string& nm) const {
  auto it = mLongToIndex.find(nm);
  if (it == mLongToIndex.end()) return nullptr;
  return &mSpecs[it->second];
}

const ConsoleArgParser::InternalSpec* ConsoleArgParser::findByShort(char c) const {
  auto it = mShortToIndex.find(c);
  if (it == mShortToIndex.end()) return nullptr;
  return &mSpecs[it->second];
}

bool ConsoleArgParser::ParseResult::has(const std::string& name) const {
  return optionToValues.find(name) != optionToValues.end();
}

bool ConsoleArgParser::ParseResult::flag(const std::string& name) const {
  auto it = optionToValues.find(name);
  if (it == optionToValues.end()) return false;
  return it->second.empty();
}

std::string ConsoleArgParser::ParseResult::value(const std::string& name, const std::string& fallback) const {
  auto it = optionToValues.find(name);
  if (it == optionToValues.end() || it->second.empty()) return fallback;
  return it->second.back();
}

std::vector<std::string> ConsoleArgParser::ParseResult::values(const std::string& name) const {
  auto it = optionToValues.find(name);
  if (it == optionToValues.end()) return {};
  return it->second;
}

ConsoleArgParser::ParseResult ConsoleArgParser::parse(const std::vector<std::string>& args) const {
  ParseResult r;
  bool onlyPositionals = false;

  auto add_value = [&](const InternalSpec* spec, const std::string& v) {
    if (!spec) return;
    auto& vec = r.optionToValues[spec->spec.longName.empty() ? std::string(1, spec->spec.shortName) : spec->spec.longName];
    vec.push_back(v);
  };

  auto set_flag = [&](const InternalSpec* spec){ add_value(spec, std::string()); };

  for (size_t i = 0; i < args.size(); ++i) {
    std::string tok = dequoteToken(args[i]);
    if (onlyPositionals) { r.positionals.push_back(tok); continue; }

    if (tok == "--") { onlyPositionals = true; continue; }

    // Long option: --opt or --opt=value
    if (starts_with(tok, "--")) {
      std::string nameval = dequoteToken(tok.substr(2));
      std::string name, val;
      size_t eq = nameval.find('=');
      if (eq == std::string::npos) name = nameval; else { name = nameval.substr(0, eq); val = dequoteToken(nameval.substr(eq+1)); }
      auto* s = findByLong(name);
      if (!s) { if (mCollectUnknownTokens) r.unknownTokens.push_back(tok); else r.errors.push_back("Unknown option: " + tok); continue; }
      if (s->spec.requiresValue) {
        if (!val.empty()) { add_value(s, val); }
        else {
          if (i+1 < args.size()) { add_value(s, dequoteToken(args[++i])); }
          else { r.errors.push_back("Missing value for option --" + name); }
        }
      } else {
        set_flag(s);
      }
      continue;
    }

    // Short option(s): -a -abc -oValue -o Value
    if (tok.size() >= 2 && tok[0] == '-' && tok[1] != '-') {
      // Combined or single
      if (mAllowCombinedShorts && tok.size() > 2) {
        // iterate every char after '-'
        for (size_t k = 1; k < tok.size(); ++k) {
          char c = tok[k];
          auto* s = findByShort(c);
          if (!s) { if (mCollectUnknownTokens) r.unknownTokens.push_back(std::string("-") + c); else r.errors.push_back(std::string("Unknown option: -") + c); continue; }
          if (s->spec.requiresValue) {
            std::string val;
            if (mAllowAttachedValue && k+1 < tok.size()) {
              val = tok.substr(k+1);
              add_value(s, val);
              break; // rest belongs to value
            } else if (i+1 < args.size()) {
              add_value(s, dequoteToken(args[++i]));
            } else {
              r.errors.push_back(std::string("Missing value for option -") + c);
            }
          } else {
            set_flag(s);
          }
        }
      } else {
        // Single short option like -o or -oValue
        char c = tok.size() > 1 ? tok[1] : '\0';
        auto* s = findByShort(c);
        if (!s) { if (mCollectUnknownTokens) r.unknownTokens.push_back(tok); else r.errors.push_back("Unknown option: " + tok); continue; }
        if (s->spec.requiresValue) {
          std::string val;
          if (mAllowAttachedValue && tok.size() > 2) val = tok.substr(2);
          else if (i+1 < args.size()) val = dequoteToken(args[++i]);
          else r.errors.push_back(std::string("Missing value for option -") + c);
          if (!val.empty()) add_value(s, val);
        } else {
          set_flag(s);
        }
      }
      continue;
    }

    // Bare assignment key=value (legacy style)
    if (mAcceptBareAssignments) {
      size_t eq = tok.find('=');
      if (eq != std::string::npos && eq > 0) {
        std::string key = dequoteToken(tok.substr(0, eq));
        std::string val = dequoteToken(tok.substr(eq+1));
        // if there is a spec matching the long name, map it; otherwise keep as positional assignment
        if (auto* s = findByLong(key)) {
          add_value(s, val);
        } else {
          r.positionals.push_back(tok);
        }
        continue;
      }
    }

    // Positional
    r.positionals.push_back(tok);
  }

  // Apply defaults
  for (const auto& ins : mSpecs) {
    const auto& spec = ins.spec;
    if (!spec.defaultValue.empty() && r.optionToValues.find(spec.longName) == r.optionToValues.end()) {
      r.optionToValues[spec.longName].push_back(spec.defaultValue);
    }
  }

  return r;
}

std::string ConsoleArgParser::help() const {
  std::ostringstream oss;
  if (!mProgramName.empty()) oss << "Usage: " << mProgramName << " [options] [--] [args...]\n";
  if (!mDescription.empty()) oss << mDescription << "\n\n";
  oss << "Options:\n";
  for (const auto& ins : mSpecs) {
    const auto& s = ins.spec;
    oss << "  ";
    if (s.shortName) oss << '-' << s.shortName << (s.longName.empty() ? "" : ", ");
    if (!s.longName.empty()) oss << "--" << s.longName;
    if (s.requiresValue) oss << ' ' << (s.valueName.empty() ? "<value>" : s.valueName);
    if (!s.description.empty()) oss << "\t" << s.description;
    if (!s.defaultValue.empty()) oss << " (default: " << s.defaultValue << ")";
    oss << "\n";
  }
  return oss.str();
}


