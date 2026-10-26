//------------------------------------------------------------------------------
//! @file RecycleHelper.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "console/commands/helpers/RecycleHelper.hh"
#include "console/ConsoleCli.hh"
#include "console/commands/helpers/PathIdentifier.hh"
#include <array>
#include <charconv>
#include <cmath>
#include <limits>
#include <map>

namespace {
bool
ParseDecimal(const std::string& value, uint64_t& result)
{
  if (value.empty()) {
    return false;
  }
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
  return parsed.ec == std::errc{} && parsed.ptr == value.data() + value.size();
}

CLI::Validator
UnsignedDecimal(uint64_t maximum = std::numeric_limits<uint64_t>::max(),
                uint64_t minimum = 0)
{
  return CLI::Validator{
      [maximum, minimum](std::string& value) {
        uint64_t parsed = 0;
        if (!ParseDecimal(value, parsed) || parsed < minimum || parsed > maximum) {
          return std::string("expected an unsigned decimal integer in range");
        }
        value = std::to_string(parsed);
        return std::string{};
      },
      "UNSIGNED"};
}

CLI::Validator
ScaledDecimal(std::map<std::string, uint64_t> units, uint64_t maximum,
              uint64_t minimum = 0)
{
  return CLI::Validator{
      [units = std::move(units), maximum, minimum](std::string& value) {
        size_t cursor = 0;
        std::string digits;
        while (cursor < value.size() && value[cursor] >= '0' && value[cursor] <= '9') {
          digits.push_back(value[cursor++]);
        }
        size_t fractional_digits = 0;
        if (cursor < value.size() && value[cursor] == '.') {
          ++cursor;
          const auto begin = cursor;
          while (cursor < value.size() && value[cursor] >= '0' && value[cursor] <= '9') {
            digits.push_back(value[cursor++]);
          }
          fractional_digits = cursor - begin;
          if (fractional_digits == 0) {
            return std::string("fraction requires digits after the decimal point");
          }
        }
        if (digits.empty()) {
          return std::string("expected an unsigned decimal value with an optional unit");
        }
        std::string suffix = value.substr(cursor);
        std::transform(suffix.begin(), suffix.end(), suffix.begin(), [](unsigned char c) {
          return static_cast<char>(std::tolower(c));
        });
        const auto unit = units.find(suffix);
        if (unit == units.end()) {
          return std::string("unknown unit");
        }
        // Multiply decimal digits before dividing by the decimal scale. Keeping
        // the product as digits avoids floating rounding and intermediate overflow.
        std::string product;
        uint64_t carry = 0;
        for (auto digit = digits.rbegin(); digit != digits.rend(); ++digit) {
          const uint64_t part =
              static_cast<unsigned>(*digit - '0') * unit->second + carry;
          product.push_back(static_cast<char>('0' + part % 10));
          carry = part / 10;
        }
        while (carry) {
          product.push_back(static_cast<char>('0' + carry % 10));
          carry /= 10;
        }
        while (product.size() <= fractional_digits) {
          product.push_back('0');
        }
        if (product.find_first_not_of('0', 0) < fractional_digits) {
          return std::string("value must convert to a whole number of base units");
        }
        product.erase(0, fractional_digits);
        while (product.size() > 1 && product.back() == '0') {
          product.pop_back();
        }
        std::reverse(product.begin(), product.end());
        uint64_t number = 0;
        if (!ParseDecimal(product, number) || number < minimum || number > maximum) {
          return std::string("converted value is out of range");
        }
        value = std::to_string(number);
        return std::string{};
      },
      "DECIMAL_WITH_UNIT"};
}

std::map<std::string, uint64_t>
SizeUnits()
{
  std::map<std::string, uint64_t> units{{"", 1}, {"b", 1}};
  uint64_t decimal = 1, binary = 1;
  for (char prefix : std::string("kmgtpe")) {
    decimal *= 1000;
    binary *= 1024;
    const std::string name(1, prefix);
    units[name] = decimal;
    units[name + "b"] = decimal;
    units[name + "i"] = binary;
    units[name + "ib"] = binary;
  }
  return units;
}

CLI::Validator
RecycleDate(bool allow_index)
{
  return CLI::Validator{
      [allow_index](const std::string& value) {
        std::array<uint64_t, 4> fields{};
        size_t count = 0;
        size_t begin = 0;
        do {
          const auto end = value.find('/', begin);
          if (count == fields.size() ||
              !ParseDecimal(
                  value.substr(begin, end == std::string::npos ? end : end - begin),
                  fields[count++])) {
            return std::string("expected year[/month[/day[/index]]]");
          }
          if (end == std::string::npos) {
            break;
          }
          begin = end + 1;
        } while (true);
        if ((!allow_index && count > 3) || fields[0] == 0 || fields[0] > 9999 ||
            (count > 1 && (fields[1] == 0 || fields[1] > 12))) {
          return std::string("invalid recycle date");
        }
        if (count > 2) {
          constexpr std::array<unsigned, 12> days{31, 28, 31, 30, 31, 30,
                                                  31, 31, 30, 31, 30, 31};
          unsigned maximum = days[fields[1] - 1];
          if (fields[1] == 2 && fields[0] % 4 == 0 &&
              (fields[0] % 100 != 0 || fields[0] % 400 == 0)) {
            ++maximum;
          }
          if (fields[2] == 0 || fields[2] > maximum) {
            return std::string("invalid day in recycle date");
          }
        }
        return std::string{};
      },
      allow_index ? "YEAR[/MONTH[/DAY[/INDEX]]]" : "YEAR[/MONTH[/DAY]]"};
}

const CLI::Validator AbsolutePath{[](const std::string& value) {
                                    return !value.empty() && value.front() == '/'
                                               ? std::string{}
                                               : std::string("path must be absolute");
                                  },
                                  "ABSOLUTE_PATH"};

const CLI::Validator RecycleSubtree{
    [](const std::string& value) {
      if (!value.empty() && value.front() == '/') {
        return std::string{};
      }
      const auto identifier = eos::console::ParsePathIdentifier(value);
      if (identifier.value != 0 &&
          (identifier.type == eos::console::IdentifierType::File ||
           identifier.type == eos::console::IdentifierType::Container)) {
        return std::string{};
      }
      return std::string(
          "expected an absolute path or a nonzero file/container identifier");
    },
    "ABSOLUTE_PATH_OR_IDENTIFIER"};

void
AddSelection(CLI::App& app, bool& all, bool& uid, std::string& rid)
{
  auto* selection = app.add_option_group("Selection");
  selection->require_option(0, 1);
  selection->add_flag("--all", all, "entries for all users");
  selection->add_flag("--uid", uid, "entries for the current user");
  selection->add_option("--rid", rid, "recycle project id")->check(UnsignedDecimal());
}
} // namespace

bool
RecycleHelper::ParseCommand(const char* arg)
{
  std::vector<std::string> arguments;
  return eos::console::TokenizeArguments(arg, arguments) && ParseArguments(arguments);
}

bool
RecycleHelper::ParseArguments(const std::vector<std::string>& args)
{
  return ParseOrCompleteArguments(args, nullptr);
}

CommandCompletionResult
RecycleHelper::CompleteArguments(const std::vector<std::string>& args)
{
  CommandCompletionResult result;
  ParseOrCompleteArguments(args, &result);
  return result;
}

bool
RecycleHelper::ParseOrCompleteArguments(const std::vector<std::string>& args,
                                        CommandCompletionResult* completion)
{
  mReq.clear_recycle();
  CLI::App app{"Recycle bin functionality", "recycle"};
  app.set_help_flag("-h,--help");
  app.require_subcommand(0, 1);
  bool status_monitor = false;
  auto* status_option = app.add_flag("-m", status_monitor, "monitoring status");

  auto* list = app.add_subcommand("ls", "List recycled entries");
  bool list_monitor = false, list_numeric = false, list_all = false, list_uid = false;
  std::string list_rid, list_path, list_date;
  int32_t list_limit = 0;
  list->excludes(status_option);
  list->add_flag("-m", list_monitor, "monitoring format");
  list->add_flag("-n", list_numeric, "numeric user and group ids");
  AddSelection(*list, list_all, list_uid, list_rid);
  list->add_option("--project", list_path, "path belonging to a recycle project")
      ->check(AbsolutePath);
  auto* date_option =
      list->add_option("date", list_date, "recycle date")->check(RecycleDate(true));
  list->add_option("limit", list_limit, "maximum number of entries")
      ->transform(UnsignedDecimal(std::numeric_limits<int32_t>::max(), 1))
      ->needs(date_option);

  auto* purge = app.add_subcommand("purge", "Permanently remove recycled entries");
  bool purge_all = false, purge_uid = false;
  std::string purge_rid, purge_date, purge_key;
  purge->excludes(status_option);
  AddSelection(*purge, purge_all, purge_uid, purge_rid);
  auto* purge_entry = purge->add_option_group("Entry");
  purge_entry->require_option(1, 1);
  auto* purge_date_option = purge_entry->add_option("date", purge_date, "recycle date")
                                ->check(RecycleDate(false));
  auto* purge_key_option = purge_entry->add_option("-k", purge_key, "recycle key")
                               ->check(eos::console::NonEmptyArgument);
  purge_date_option->excludes(purge_key_option);

  auto* restore = app.add_subcommand("restore", "Restore a recycled entry");
  bool force_original = false, restore_versions = false, make_path = false;
  std::string restore_key;
  restore->excludes(status_option);
  restore->add_flag("-f,--force-original-name", force_original, "restore original name");
  restore->add_flag("-r,--restore-versions", restore_versions,
                    "restore previous versions");
  restore->add_flag("-p", make_path, "create missing parent directories");
  restore->add_option("key", restore_key, "recycle key")
      ->required()
      ->check(eos::console::NonEmptyArgument);

  auto* project = app.add_subcommand("project", "Set up a recycle project");
  std::string project_path, project_acl;
  project->excludes(status_option);
  project->add_option("--path", project_path, "project root path")
      ->required()
      ->check(AbsolutePath);
  project->add_option("--acl", project_acl, "project ACL")
      ->check(eos::console::NonEmptyArgument);

  auto* config = app.add_subcommand("config", "Configure recycle policies");
  config->excludes(status_option);
  config->require_option(1, 1);
  std::string add_bin, remove_bin, dry_run, enable, enforce;
  int32_t lifetime = 0;
  float ratio = 0;
  uint64_t size = 0, inodes = 0, collect_interval = 0, remove_interval = 0;
  bool dump = false;
  auto* add_bin_option =
      config->add_option("--add-bin", add_bin, "enable bin for subtree")
          ->check(RecycleSubtree);
  auto* remove_bin_option =
      config->add_option("--remove-bin", remove_bin, "disable bin for subtree")
          ->check(RecycleSubtree);
  auto* lifetime_option =
      config->add_option("--lifetime", lifetime, "retention duration")
          ->transform(ScaledDecimal({{"", 1},
                                     {"s", 1},
                                     {"min", 60},
                                     {"h", 3600},
                                     {"d", 86400},
                                     {"w", 7 * 86400},
                                     {"mo", 31 * 86400},
                                     {"y", 365 * 86400}},
                                    std::numeric_limits<int32_t>::max(), 60));
  auto* ratio_option =
      config->add_option("--ratio", ratio, "quota keep ratio")
          ->check(CLI::Range(0.0, 0.99))
          ->check(CLI::Validator{
              [](const std::string& value) {
                try {
                  size_t consumed = 0;
                  const double parsed = std::stod(value, &consumed);
                  if (consumed == value.size() && std::isfinite(parsed) && parsed > 0) {
                    return std::string{};
                  }
                } catch (const std::exception&) {
                }
                return std::string("ratio must be finite and greater than zero");
              },
              "FINITE_POSITIVE"});
  const auto size_value =
      ScaledDecimal(SizeUnits(), std::numeric_limits<uint64_t>::max());
  auto* size_option =
      config->add_option("--size", size, "volume quota")->transform(size_value);
  auto* inodes_option =
      config->add_option("--inodes", inodes, "inode quota")->transform(size_value);
  const auto interval_range = UnsignedDecimal(std::numeric_limits<int64_t>::max(), 10);
  auto* collect_option = config
                             ->add_option("--collect-interval", collect_interval,
                                          "collection interval in seconds")
                             ->transform(interval_range);
  auto* remove_option = config
                            ->add_option("--remove-interval", remove_interval,
                                         "removal interval in seconds")
                            ->transform(interval_range);
  auto* dry_run_option =
      config->add_option("--dry-run", dry_run, "disable actual removals")
          ->check(CLI::IsMember({"yes", "no"}));
  auto* enable_option = config->add_option("--enable", enable, "enable recycler")
                            ->check(CLI::IsMember({"on", "off"}));
  auto* enforce_option = config->add_option("--enforce", enforce, "enforce default bin")
                             ->check(CLI::IsMember({"on", "off"}));
  config->add_flag("--dump", dump, "show policy configuration")->disable_flag_override();

  if (completion) {
    *completion = CompleteCliArguments(
        app, args,
        {{dry_run_option, {"yes", "no"}},
         {enable_option, {"on", "off"}},
         {enforce_option, {"on", "off"}}},
        {{list->get_option("--project"), CompletionPath::EosDirectory},
         {project->get_option("--path"), CompletionPath::EosDirectory},
         {add_bin_option, CompletionPath::EosDirectory},
         {remove_bin_option, CompletionPath::EosDirectory}});
    bool statusOnly = false;
    for (const auto& arg : args) {
      if (app.get_subcommand_no_throw(arg)) {
        if (statusOnly) {
          *completion = {};
        }
        break;
      }
      if (arg.size() > 1 && arg.front() == '-' &&
          arg.find_first_not_of('m', 1) == std::string::npos) {
        statusOnly = true;
      }
    }
    if (statusOnly) {
      auto& words = completion->candidates;
      words.erase(std::remove_if(words.begin(), words.end(),
                                 [&](const std::string& word) {
                                   return app.get_subcommand_no_throw(word) != nullptr;
                                 }),
                  words.end());
    }
    return true;
  }

  if (!eos::console::ParseCliArguments(app, args)) {
    return false;
  }

  auto* recycle = mReq.mutable_recycle();
  if (*list || app.get_subcommands().empty()) {
    auto* result = recycle->mutable_ls();
    result->set_type(list_all || (args.empty()) ? eos::console::RecycleProto::ALL
                     : !list_rid.empty()        ? eos::console::RecycleProto::RID
                                                : eos::console::RecycleProto::UID);
    result->set_fulldetails(static_cast<bool>(*list));
    result->set_monitorfmt(list_monitor || status_monitor);
    result->set_numericids(list_numeric);
    result->set_recycleid(list_rid);
    result->set_path(list_path);
    result->set_date(list_date);
    result->set_maxentries(list_limit);
  } else if (*purge) {
    auto* result = recycle->mutable_purge();
    result->set_type(purge_all            ? eos::console::RecycleProto::ALL
                     : !purge_rid.empty() ? eos::console::RecycleProto::RID
                                          : eos::console::RecycleProto::UID);
    result->set_recycleid(purge_rid);
    result->set_date(purge_date);
    result->set_key(purge_key);
  } else if (*restore) {
    auto* result = recycle->mutable_restore();
    result->set_makepath(make_path);
    result->set_forceorigname(force_original);
    result->set_restoreversions(restore_versions);
    result->set_key(restore_key);
  } else if (*project) {
    auto* result = recycle->mutable_project();
    result->set_path(project_path);
    result->set_acl(project_acl);
  } else if (*config) {
    using Config = eos::console::RecycleProto::ConfigProto;
    auto* result = recycle->mutable_config();
    if (*add_bin_option || *remove_bin_option) {
      result->set_op(*add_bin_option ? Config::ADD_BIN : Config::RM_BIN);
      result->set_subtree(*add_bin_option ? add_bin : remove_bin);
    } else if (*lifetime_option) {
      result->set_op(Config::LIFETIME);
      result->set_lifetimesec(lifetime);
    } else if (*ratio_option) {
      result->set_op(Config::RATIO);
      result->set_ratio(ratio);
    } else if (*size_option || *inodes_option) {
      result->set_op(*size_option ? Config::SIZE : Config::INODES);
      result->set_size(*size_option ? size : inodes);
    } else if (*collect_option || *remove_option) {
      result->set_op(*collect_option ? Config::COLLECT_INTERVAL
                                     : Config::REMOVE_INTERVAL);
      result->set_size(*collect_option ? collect_interval : remove_interval);
    } else if (*dry_run_option || *enable_option || *enforce_option) {
      result->set_op(*dry_run_option  ? Config::DRY_RUN
                     : *enable_option ? Config::ENABLE
                                      : Config::ENFORCE);
      result->set_value(*dry_run_option ? dry_run : *enable_option ? enable : enforce);
    } else if (dump) {
      result->set_op(Config::DUMP);
    }
  }
  return true;
}
