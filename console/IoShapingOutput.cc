// ----------------------------------------------------------------------
// File: IoShapingOutput.cc
// ----------------------------------------------------------------------

#include "console/IoShapingOutput.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"

#include <json/json.h>

#include <algorithm>
#include <cmath>
#include <exception>
#include <iomanip>
#include <locale>
#include <memory>
#include <set>
#include <sstream>
#include <vector>

namespace eos::console {
namespace {

enum class Kind {
  Text,
  Identity,
  Number,
  Rate,
  Iops,
  Microseconds,
  Milliseconds,
  Seconds,
  Boolean,
  Pressure
};

struct Field {
  const char* key;
  const char* label;
  Kind kind = Kind::Text;
};

struct Cell {
  std::string text;
  std::optional<TextStyle> style;
};

using Fields = std::vector<Field>;
using Records = std::vector<const Json::Value*>;

// Keep server strings inert even with forced color and preserve printable UTF-8.
std::string
SafeText(std::string_view value)
{
  std::string result;
  constexpr char hex[] = "0123456789abcdef";
  for (std::size_t index = 0; index < value.size(); ++index) {
    const auto ch = static_cast<unsigned char>(value[index]);
    if (ch < 0x20 || ch == 0x7f) {
      result += "\\x";
      result += hex[ch >> 4];
      result += hex[ch & 15];
    } else if (ch == 0xc2 && index + 1 < value.size() &&
               static_cast<unsigned char>(value[index + 1]) >= 0x80 &&
               static_cast<unsigned char>(value[index + 1]) <= 0x9f) {
      const auto control = static_cast<unsigned char>(value[++index]);
      result += "\\u00";
      result += hex[control >> 4];
      result += hex[control & 15];
    } else if (ch >= 0xc2 && ch <= 0xf4) {
      const std::size_t length = ch < 0xe0 ? 2 : ch < 0xf0 ? 3 : 4;
      bool valid = index + length <= value.size();
      for (std::size_t offset = 1; valid && offset < length; ++offset) {
        valid = (static_cast<unsigned char>(value[index + offset]) & 0xc0) == 0x80;
      }
      if (valid) {
        result.append(value.substr(index, length));
        index += length - 1;
      } else {
        result += static_cast<char>(ch);
      }
    } else if (ch >= 0x80 && ch <= 0x9f) {
      result += "\\x";
      result += hex[ch >> 4];
      result += hex[ch & 15];
    } else {
      result += static_cast<char>(ch);
    }
  }
  return result;
}

std::string
CompactJson(const Json::Value& value)
{
  Json::StreamWriterBuilder writer;
  writer["indentation"] = "";
  return SafeText(Json::writeString(writer, value));
}

std::string
Fixed(double value, int precision)
{
  std::ostringstream stream;
  stream.imbue(std::locale::classic());
  stream << std::fixed << std::setprecision(precision) << value;
  return stream.str();
}

std::string
Rate(double value)
{
  constexpr const char* units[] = {"B/s", "kB/s", "MB/s", "GB/s", "TB/s", "PB/s"};
  std::size_t unit = 0;
  while (std::abs(value) >= 1000 && unit < 5) {
    value /= 1000;
    ++unit;
  }
  return Fixed(value, 2) + " " + units[unit];
}

std::string
Duration(double value, Kind kind)
{
  if (kind == Kind::Microseconds) {
    if (value < 1000) {
      return Fixed(value, 2) + " us";
    }
    value /= 1000;
  }
  if (kind == Kind::Milliseconds || kind == Kind::Microseconds) {
    if (value < 1000) {
      return Fixed(value, 2) + " ms";
    }
    value /= 1000;
  }
  return Fixed(value, 2) + " s";
}

bool
ValidField(const Json::Value& value, Kind kind)
{
  if (kind == Kind::Text || kind == Kind::Identity) {
    return value.isString() || (kind == Kind::Identity && value.isIntegral());
  }
  if (kind == Kind::Boolean) {
    return value.isBool();
  }
  return value.isNumeric() && std::isfinite(value.asDouble());
}

std::optional<TextStyle>
DirectionStyle(std::string_view key)
{
  for (std::size_t begin = 0; begin < key.size();) {
    const auto end = key.find('_', begin);
    const auto token = key.substr(
        begin, end == std::string_view::npos ? key.size() - begin : end - begin);
    if (token == "read") {
      return TextStyle::Read;
    }
    if (token == "write") {
      return TextStyle::Write;
    }
    if (end == std::string_view::npos) {
      break;
    }
    begin = end + 1;
  }
  return std::nullopt;
}

std::optional<TextStyle>
FieldDirection(const Field& field, const Json::Value* record = nullptr)
{
  if (const auto direction = DirectionStyle(field.key)) {
    return direction;
  }
  if (!record || field.kind == Kind::Text || field.kind == Kind::Identity ||
      field.kind == Kind::Boolean || !record->isMember("type") ||
      !(*record)["type"].isString() || !record->isMember("operation") ||
      !(*record)["operation"].isString()) {
    return std::nullopt;
  }
  const auto type = (*record)["type"].asString();
  const auto operation = (*record)["operation"].asString();
  if ((type == "node_controller_feedback" || type == "node_controller_cohort_app" ||
       type == "fst_actuator") &&
      (operation == "read" || operation == "write")) {
    return DirectionStyle(operation);
  }
  return std::nullopt;
}

std::optional<TextStyle>
LabelStyle(const Field& field, const Json::Value* record = nullptr)
{
  for (const auto label :
       {"Application", "UID", "User", "GID", "Group", "Node", "FSID", "Identity type"}) {
    if (std::string_view(field.label) == label) {
      return TextStyle::EntityType;
    }
  }
  return FieldDirection(field, record);
}

Cell
ValueCell(const Json::Value& record, const Field& field, bool declared = true)
{
  if (!record.isMember(field.key)) {
    return {"-", TextStyle::Syntax};
  }
  const auto& value = record[field.key];
  Cell result;
  if (declared) {
    result.style = FieldDirection(field, &record);
  }
  switch (field.kind) {
  case Kind::Text:
  case Kind::Identity:
    result.text = SafeText(value.asString());
    if (field.kind == Kind::Identity) {
      result.style = TextStyle::Identifier;
    } else if (declared && std::string_view(field.key) == "identity_type") {
      result.style = TextStyle::EntityType;
    } else if (declared && (std::string_view(field.key) == "operation" ||
                            std::string_view(field.key) == "cohort")) {
      if (result.text == "read" || result.text == "write") {
        result.style = DirectionStyle(result.text);
      }
    }
    break;
  case Kind::Boolean:
    result.text = value.asBool() ? "yes" : "no";
    result.style = value.asBool() ? TextStyle::Success : TextStyle::Syntax;
    if (std::string_view(field.key).find("enabled") != std::string_view::npos) {
      result.text = value.asBool() ? "enabled" : "disabled";
      result.style = value.asBool() ? TextStyle::Success : TextStyle::Warning;
    }
    if (value.asBool()) {
      const std::string_view key(field.key);
      if (key.find("pressure_active") != std::string_view::npos ||
          key.find("deficit_active") != std::string_view::npos ||
          key.find("throttling") != std::string_view::npos || key == "suppressed" ||
          key == "awaiting_response" ||
          key.find("pressured_") != std::string_view::npos) {
        result.style = TextStyle::Warning;
      }
    }
    break;
  case Kind::Rate:
    result.text = Rate(value.asDouble());
    break;
  case Kind::Iops:
    result.text = Fixed(value.asDouble(), 2);
    break;
  case Kind::Microseconds:
  case Kind::Milliseconds:
  case Kind::Seconds:
    result.text = Duration(value.asDouble(), field.kind);
    break;
  case Kind::Pressure:
    result.text = Fixed(value.asDouble(), 3);
    break;
  case Kind::Number:
    result.text = value.asString();
    break;
  }
  // Presence flags distinguish an unmeasured value from a measured zero.
  if (field.kind == Kind::Pressure) {
    const std::string presence = "has_" + std::string(field.key);
    if (record.isMember(presence) && !record[presence].asBool()) {
      result = {"-", TextStyle::Syntax};
    } else {
      std::string active;
      if (std::string_view(field.key) == "read_io_pressure") {
        active = "read_pressure_active";
      }
      if (std::string_view(field.key) == "write_io_pressure") {
        active = "write_pressure_active";
      }
      if (!active.empty() && record.isMember(active) && record[active].asBool()) {
        result.style = TextStyle::Warning;
      }
      if (std::string_view(field.key) == "node_io_pressure" &&
          ((record.isMember("read_pressure_active") &&
            record["read_pressure_active"].asBool()) ||
           (record.isMember("write_pressure_active") &&
            record["write_pressure_active"].asBool()))) {
        result.style = TextStyle::Warning;
      }
    }
  }
  if (std::string_view(field.key) == "phase" || std::string_view(field.key) == "cohort") {
    if (result.text == "suppressed" || result.text == "failed") {
      result.style = TextStyle::Warning;
    } else if (result.text == "active") {
      result.style = TextStyle::Success;
    }
  }
  return result;
}

std::string
Paint(const Cell& cell, int descriptor)
{
  return cell.style ? Colorize(cell.text, *cell.style, descriptor) : cell.text;
}

// Split at UTF-8 boundaries; the shared display measure also handles combining
// marks and wide characters independently of the process locale.
std::vector<std::string>
Wrap(std::string_view text, std::size_t width)
{
  std::vector<std::string> lines;
  width = std::max<std::size_t>(width, 2);
  while (!text.empty()) {
    std::size_t end = 0;
    std::size_t lastSpace = std::string_view::npos;
    while (end < text.size()) {
      std::size_t next = end + 1;
      while (next < text.size() &&
             (static_cast<unsigned char>(text[next]) & 0xc0) == 0x80) {
        ++next;
      }
      if (HelpDisplayWidth(text.substr(0, next)) > width && end != 0) {
        break;
      }
      if (text[end] == ' ') {
        lastSpace = end;
      }
      end = next;
    }
    if (end < text.size() && lastSpace != std::string_view::npos && lastSpace > 0) {
      end = lastSpace;
    }
    if (!end) {
      end = 1;
    }
    lines.emplace_back(text.substr(0, end));
    text.remove_prefix(end);
    // A single layout separator is not part of the wrapped value.
    if (!text.empty() && text.front() == ' ') {
      text.remove_prefix(1);
    }
  }
  if (lines.empty()) {
    lines.emplace_back();
  }
  return lines;
}

class View {
public:
  View(std::size_t width, int descriptor)
      : width(std::max<std::size_t>(width ? width : 100, 20))
      , descriptor(descriptor)
  {
  }

  void
  heading(std::string_view title)
  {
    if (!output.empty()) {
      output += '\n';
    }
    for (const auto& line : Wrap(title, width)) {
      output += Colorize(line, TextStyle::Heading, descriptor) + '\n';
    }
  }

  void
  note(std::string_view text)
  {
    for (const auto& line : Wrap(text, width)) {
      output += line + '\n';
    }
  }

  void
  detail(std::string_view label, const Cell& value,
         std::optional<TextStyle> style = std::nullopt)
  {
    const std::string cleanLabel = SafeText(label);
    const auto labelWidth = HelpDisplayWidth(cleanLabel) + 2;
    if (labelWidth + 8 <= width) {
      const auto lines = Wrap(value.text, width - labelWidth);
      for (std::size_t i = 0; i < lines.size(); ++i) {
        output += i ? std::string(labelWidth, ' ')
                    : Paint({cleanLabel + ":", style}, descriptor) + " ";
        output += Paint({lines[i], value.style}, descriptor) + '\n';
      }
    } else {
      for (const auto& line : Wrap(cleanLabel + ":", width)) {
        output += Paint({line, style}, descriptor) + '\n';
      }
      for (const auto& line : Wrap(value.text, width - 2)) {
        output += "  " + Paint({line, value.style}, descriptor) + '\n';
      }
    }
  }

  void
  table(const Fields& fields, const Records& records, bool policy = false)
  {
    if (records.empty()) {
      note("No entries.");
      return;
    }
    std::vector<std::size_t> widths;
    std::vector<std::vector<Cell>> cells;
    for (const auto& field : fields) {
      widths.push_back(HelpDisplayWidth(field.label));
    }
    for (const auto* record : records) {
      std::vector<Cell> row;
      for (std::size_t i = 0; i < fields.size(); ++i) {
        row.push_back(ValueCell(*record, fields[i]));
        if (policy && fields[i].kind == Kind::Rate && record->isMember(fields[i].key) &&
            (*record)[fields[i].key].asDouble() == 0) {
          const std::string_view key(fields[i].key);
          row.back() = {key.find("reservation_") == 0  ? "none"
                        : key.find("controller_") == 0 ? "no cap"
                                                       : "unlimited",
                        TextStyle::Syntax};
        }
        widths[i] = std::max(widths[i], HelpDisplayWidth(row.back().text));
      }
      cells.push_back(std::move(row));
    }
    std::size_t total = fields.empty() ? 0 : 2 * (fields.size() - 1);
    for (auto size : widths) {
      total += size;
    }
    if (total <= width) {
      for (std::size_t i = 0; i < fields.size(); ++i) {
        if (i) {
          output += "  ";
        }
        output +=
            Colorize(fields[i].label, LabelStyle(fields[i]).value_or(TextStyle::Heading),
                     descriptor);
        output += std::string(widths[i] - HelpDisplayWidth(fields[i].label), ' ');
      }
      output += '\n';
      for (std::size_t i = 0; i < widths.size(); ++i) {
        if (i) {
          output += "  ";
        }
        output += std::string(widths[i], '-');
      }
      output += '\n';
      for (const auto& row : cells) {
        for (std::size_t i = 0; i < row.size(); ++i) {
          if (i) {
            output += "  ";
          }
          const auto padding =
              std::string(widths[i] - HelpDisplayWidth(row[i].text), ' ');
          const bool numeric = fields[i].kind != Kind::Identity &&
                               fields[i].kind != Kind::Text &&
                               fields[i].kind != Kind::Boolean;
          if (numeric) {
            output += padding;
          }
          output += Paint(row[i], descriptor);
          if (!numeric) {
            output += padding;
          }
        }
        output += '\n';
      }
    } else {
      for (std::size_t row = 0; row < cells.size(); ++row) {
        if (row) {
          output += '\n';
        }
        for (std::size_t column = 0; column < fields.size(); ++column) {
          detail(fields[column].label, cells[row][column],
                 LabelStyle(fields[column], records[row]));
        }
      }
    }
  }

  void
  additional(const Json::Value& record, const Fields& consumed,
             const std::set<std::string>& ignored = {})
  {
    std::set<std::string> keys = ignored;
    keys.insert("type");
    for (const auto& field : consumed) {
      keys.insert(field.key);
    }
    bool title = false;
    for (const auto& key : record.getMemberNames()) {
      if (keys.count(key)) {
        continue;
      }
      if (!title) {
        const std::string identity =
            record.isMember("id")        ? SafeText(record["id"].asString())
            : record.isMember("node_id") ? SafeText(record["node_id"].asString())
                                         : "";
        heading(identity.empty() ? "Additional details"
                                 : "Additional details: " + identity);
        title = true;
      }
      detail(key, {record[key].isString() ? SafeText(record[key].asString())
                                          : CompactJson(record[key]),
                   std::nullopt});
    }
  }

  std::string output;
  std::size_t width;
  int descriptor;
};

const Fields rateFields = {{"read_rate_bps", "Read", Kind::Rate},
                           {"write_rate_bps", "Write", Kind::Rate},
                           {"read_iops", "Read IOPS", Kind::Iops},
                           {"write_iops", "Write IOPS", Kind::Iops}};

const Fields configFields = {
    {"enabled", "Engine", Kind::Boolean},
    {"estimators_update_period_ms", "Estimator update", Kind::Milliseconds},
    {"fst_io_policy_update_period_ms", "FST policy update", Kind::Milliseconds},
    {"fst_io_stats_reporting_period_ms", "FST reporting", Kind::Milliseconds},
    {"detail_level", "Detail level"},
    {"detail_auto_enabled", "Automatic detail", Kind::Boolean},
    {"detail_auto_low_cardinality", "Filesystem detail at or below", Kind::Number},
    {"detail_auto_high_cardinality", "Aggregate detail above", Kind::Number},
    {"limits_enabled", "Limits", Kind::Boolean},
    {"reservations_enabled", "Reservations", Kind::Boolean},
    {"controller_min_limit_bytes_per_sec", "Controller minimum limit", Kind::Rate},
    {"active_node_rate_threshold_bytes_per_sec", "Active node threshold", Kind::Rate},
    {"io_pressure_threshold", "IO pressure threshold", Kind::Pressure},
    {"max_delay_milliseconds", "Maximum legacy FST delay", Kind::Milliseconds},
    {"garbage_collection_idle_seconds", "Idle garbage collection", Kind::Seconds},
    {"system_stats_time_window_seconds", "System statistics window", Kind::Seconds}};

bool
Validate(const Json::Value& record, const Fields& fields)
{
  for (const auto& field : fields) {
    if (record.isMember(field.key) && !ValidField(record[field.key], field.kind)) {
      return false;
    }
  }
  return true;
}

Fields
RateIdentity(const std::string& type)
{
  if (type == "app") {
    return {{"id", "Application", Kind::Identity}};
  }
  if (type == "uid") {
    return {{"id", "UID", Kind::Identity}, {"name", "User", Kind::Identity}};
  }
  if (type == "gid") {
    return {{"id", "GID", Kind::Identity}, {"name", "Group", Kind::Identity}};
  }
  if (type == "node") {
    return {{"id", "Node", Kind::Identity}};
  }
  if (type == "fs") {
    return {{"node_id", "Node", Kind::Identity}, {"fsid", "FSID", Kind::Identity}};
  }
  if (type == "all") {
    return {{"node_id", "Node", Kind::Identity},    {"fsid", "FSID", Kind::Identity},
            {"app", "Application", Kind::Identity}, {"uid", "UID", Kind::Identity},
            {"user", "User", Kind::Identity},       {"gid", "GID", Kind::Identity},
            {"group", "Group", Kind::Identity}};
  }
  return {};
}

Fields
PressureFields(const std::string& type)
{
  Fields fields = {{"node_id", "Node", Kind::Identity}};
  if (type == "app_node_pressure") {
    fields.insert(fields.begin(), {"app", "Application", Kind::Identity});
    fields.insert(
        fields.end(),
        {{"read_rate_bps", "Read", Kind::Rate},
         {"write_rate_bps", "Write", Kind::Rate},
         {"node_io_pressure", "Node pressure", Kind::Pressure},
         {"read_io_pressure", "Read pressure", Kind::Pressure},
         {"write_io_pressure", "Write pressure", Kind::Pressure},
         {"global_read_rate_bps", "Global read", Kind::Rate},
         {"global_write_rate_bps", "Global write", Kind::Rate},
         {"reservation_read_bytes_per_sec", "Read reservation", Kind::Rate},
         {"reservation_write_bytes_per_sec", "Write reservation", Kind::Rate},
         {"effective_reservation_read_bytes_per_sec", "Effective read reservation",
          Kind::Rate},
         {"effective_reservation_write_bytes_per_sec", "Effective write reservation",
          Kind::Rate},
         {"node_controller_read_limit_bytes_per_sec", "Node read limit", Kind::Rate},
         {"node_controller_write_limit_bytes_per_sec", "Node write limit", Kind::Rate},
         {"read_reservation_deficit_bps", "Read deficit", Kind::Rate},
         {"write_reservation_deficit_bps", "Write deficit", Kind::Rate},
         {"read_pressure_active", "Read pressure active", Kind::Boolean},
         {"write_pressure_active", "Write pressure active", Kind::Boolean},
         {"read_reservation_deficit_active", "Read deficit active", Kind::Boolean},
         {"write_reservation_deficit_active", "Write deficit active", Kind::Boolean},
         {"read_triggers_competitor_throttling", "Read throttles competitors",
          Kind::Boolean},
         {"write_triggers_competitor_throttling", "Write throttles competitors",
          Kind::Boolean},
         {"node_has_pressured_read_reservation", "Node pressured read reservation",
          Kind::Boolean},
         {"node_has_pressured_write_reservation", "Node pressured write reservation",
          Kind::Boolean}});
  } else if (type == "node_controller_limit") {
    fields.insert(fields.end(), {{"app", "Application", Kind::Identity},
                                 {"read_bytes_per_sec", "Read limit", Kind::Rate},
                                 {"write_bytes_per_sec", "Write limit", Kind::Rate}});
  } else if (type == "node_controller_feedback") {
    fields.insert(
        fields.end(),
        {{"operation", "Operation"},
         {"phase", "Phase"},
         {"consecutive_deficit_samples", "Deficit samples", Kind::Number},
         {"protected_app_count", "Protected applications", Kind::Number},
         {"applied_reduction_bps", "Applied reduction", Kind::Rate},
         {"ineffective_probe_count", "Ineffective probes", Kind::Number},
         {"failed_protected_app_count", "Failed protected applications", Kind::Number},
         {"observed_protected_gain_bps", "Observed protected gain", Kind::Rate},
         {"response_ratio", "Response ratio", Kind::Number},
         {"awaiting_response", "Awaiting response", Kind::Boolean},
         {"suppressed", "Suppressed", Kind::Boolean},
         {"suppression_remaining_seconds", "Suppression remaining", Kind::Seconds}});
  } else if (type == "node_controller_cohort_app") {
    fields.insert(fields.end(),
                  {{"operation", "Operation"},
                   {"cohort", "Cohort"},
                   {"app", "Application", Kind::Identity},
                   {"target_bps", "Target", Kind::Rate},
                   {"baseline_rate_bps", "Baseline", Kind::Rate},
                   {"assigned_reduction_bps", "Assigned reduction", Kind::Rate},
                   {"rate_at_failure_bps", "Rate at failure", Kind::Rate}});
  } else if (type == "fst_actuator") {
    fields.insert(fields.end(),
                  {{"identity_type", "Identity type"},
                   {"identity", "Identity", Kind::Identity},
                   {"operation", "Operation"},
                   {"legacy_delay_microseconds", "Legacy delay", Kind::Microseconds},
                   {"assigned_rate_bytes_per_sec", "Assigned rate", Kind::Rate}});
  } else {
    return {};
  }
  return fields;
}

void
DropAbsent(Fields& fields, const Records& records)
{
  fields.erase(std::remove_if(fields.begin(), fields.end(),
                              [&](const Field& field) {
                                return std::none_of(records.begin(), records.end(),
                                                    [&](const Json::Value* record) {
                                                      return record->isMember(field.key);
                                                    });
                              }),
               fields.end());
}

bool
RenderRates(const IoProto_ShapingProto& request, const Json::Value& root, View& view)
{
  view.heading("IO shaping rates");
  unsigned int window = request.list().time_window_seconds();
  for (const auto& record : root) {
    if (record["type"] == "system") {
      continue;
    }
    if (record.isMember("window_sec")) {
      if (!record["window_sec"].isUInt() || !record["window_sec"].asUInt()) {
        return false;
      }
      window = record["window_sec"].asUInt();
      break;
    }
  }
  view.note(std::to_string(window ? window : 60) +
            " s simple moving average; rates use SI units.");
  if (root.empty()) {
    view.note("No rate samples.");
    if (request.list().show_fs() || request.list().show_all()) {
      view.note("Filesystem and stream samples require filesystem detail; check 'eos io "
                "shaping config ls'.");
    }
    return true;
  }
  for (const auto& type : {"app", "uid", "gid", "node", "fs", "all", "system"}) {
    Records records;
    for (const auto& record : root) {
      const auto current = record["type"].asString();
      if (RateIdentity(current).empty() && current != "system") {
        return false;
      }
      if (current == type) {
        records.push_back(&record);
      }
    }
    if (records.empty()) {
      continue;
    }
    if (std::string_view(type) == "system") {
      view.heading("Engine diagnostics");
      for (const auto* record : records) {
        for (const auto& group : {"loop", "cardinality", "other"}) {
          view.heading(std::string_view(group) == "loop" ? "Control loop timings"
                       : std::string_view(group) == "cardinality"
                           ? "Map cardinalities"
                           : "Collection and detail");
          for (const auto& key : record->getMemberNames()) {
            if (key == "id" || key == "type") {
              continue;
            }
            const bool loop = key.find("_loop_") != std::string::npos;
            const bool cardinality = key.find("_cardinality") != std::string::npos &&
                                     key.find("detail_auto_") != 0;
            if ((std::string_view(group) == "loop" && !loop) ||
                (std::string_view(group) == "cardinality" && !cardinality) ||
                (std::string_view(group) == "other" && (loop || cardinality))) {
              continue;
            }
            const auto& value = (*record)[key];
            Kind kind = value.isBool()     ? Kind::Boolean
                        : value.isString() ? Kind::Text
                                           : Kind::Number;
            if (loop) {
              kind = Kind::Microseconds;
            } else if (key == "system_stats_window_seconds") {
              kind = Kind::Seconds;
            }
            const Field field{key.c_str(), key.c_str(), kind};
            if (!ValidField(value, kind)) {
              return false;
            }
            std::string label = key;
            std::replace(label.begin(), label.end(), '_', ' ');
            view.detail(label, ValueCell(*record, field, false));
          }
        }
      }
      continue;
    }
    Fields fields = RateIdentity(type);
    fields.insert(fields.end(), rateFields.begin(), rateFields.end());
    if (std::string_view(type) == "app") {
      fields.insert(fields.end(),
                    {{"read_io_pressure", "Read pressure", Kind::Pressure},
                     {"write_io_pressure", "Write pressure", Kind::Pressure}});
    }
    DropAbsent(fields, records);
    for (const auto* record : records) {
      if (!Validate(*record, fields)) {
        return false;
      }
      for (const auto& field : rateFields) {
        if (!record->isMember(field.key)) {
          return false;
        }
      }
      if (std::string_view(type) == "fs" || std::string_view(type) == "all") {
        if (!record->isMember("node_id") || !record->isMember("fsid")) {
          return false;
        }
        if (std::string_view(type) == "all" &&
            (!record->isMember("app") || !record->isMember("uid") ||
             !record->isMember("gid"))) {
          return false;
        }
      } else if (!record->isMember("id")) {
        return false;
      }
      for (const auto& key : {"has_read_io_pressure", "has_write_io_pressure"}) {
        if (record->isMember(key) && !(*record)[key].isBool()) {
          return false;
        }
      }
      if (record->isMember("window_sec") &&
          (!(*record)["window_sec"].isUInt() ||
           (*record)["window_sec"].asUInt() != window)) {
        return false;
      }
    }
    view.heading(std::string_view(type) == "app"    ? "Applications"
                 : std::string_view(type) == "uid"  ? "Users"
                 : std::string_view(type) == "gid"  ? "Groups"
                 : std::string_view(type) == "node" ? "Storage nodes"
                 : std::string_view(type) == "fs"   ? "Filesystems"
                                                    : "Streams");
    view.table(fields, records);
    for (const auto* record : records) {
      view.additional(*record, fields,
                      {"window_sec", "has_read_io_pressure", "has_write_io_pressure"});
    }
  }
  return true;
}

bool
RenderPolicies(const IoProto_ShapingProto& request, const Json::Value& root, View& view)
{
  view.heading("IO shaping policies");
  view.note("Limits and reservations use SI bytes per second.");
  if (root.empty()) {
    view.note("No configured policies.");
  }
  for (const auto& type : {"app", "uid", "gid"}) {
    Records records;
    for (const auto& record : root) {
      const auto current = record["type"].asString();
      if (current != "app" && current != "uid" && current != "gid") {
        return false;
      }
      if (current == type) {
        records.push_back(&record);
      }
    }
    if (records.empty()) {
      continue;
    }
    Fields fields = {
        {"id",
         std::string_view(type) == "app"   ? "Application"
         : std::string_view(type) == "uid" ? "UID"
                                           : "GID",
         Kind::Identity},
        {"is_enabled", "Status", Kind::Boolean},
        {"limit_read_bytes_per_sec", "Read limit", Kind::Rate},
        {"limit_write_bytes_per_sec", "Write limit", Kind::Rate},
        {"reservation_read_bytes_per_sec", "Read reservation", Kind::Rate},
        {"reservation_write_bytes_per_sec", "Write reservation", Kind::Rate}};
    std::set<std::string> ignored;
    if (request.policy().list().show_controller_limits()) {
      fields.insert(fields.end(), {{"controller_limit_read_bytes_per_sec",
                                    "Controller read limit", Kind::Rate},
                                   {"controller_limit_write_bytes_per_sec",
                                    "Controller write limit", Kind::Rate}});
    } else {
      ignored = {"controller_limit_read_bytes_per_sec",
                 "controller_limit_write_bytes_per_sec"};
    }
    for (const auto* record : records) {
      if (!record->isMember("id") || !record->isMember("is_enabled") ||
          !Validate(*record, fields)) {
        return false;
      }
    }
    view.heading(std::string_view(type) == "app"   ? "Application policies"
                 : std::string_view(type) == "uid" ? "User policies"
                                                   : "Group policies");
    view.table(fields, records, true);
    for (const auto* record : records) {
      view.additional(*record, fields, ignored);
    }
  }
  return true;
}

bool
RenderPressure(const Json::Value& root, View& view)
{
  view.heading("IO shaping pressure");
  if (root.empty()) {
    view.note("No pressure or controller samples.");
  }
  for (const auto& type :
       {"app_node_pressure", "node_controller_limit", "node_controller_feedback",
        "node_controller_cohort_app", "fst_actuator"}) {
    Records records;
    for (const auto& record : root) {
      const auto current = record["type"].asString();
      if (PressureFields(current).empty()) {
        return false;
      }
      if (current == type) {
        records.push_back(&record);
      }
    }
    if (records.empty()) {
      continue;
    }
    auto fields = PressureFields(type);
    // Retain the pressure slots even when no direction has a sample.
    for (const auto* record : records) {
      if (!record->isMember("node_id") || !Validate(*record, fields)) {
        return false;
      }
      for (const auto& key :
           {"has_node_io_pressure", "has_read_io_pressure", "has_write_io_pressure"}) {
        if (record->isMember(key) && !(*record)[key].isBool()) {
          return false;
        }
      }
    }
    view.heading(
        std::string_view(type) == "app_node_pressure" ? "Reserved applications by node"
        : std::string_view(type) == "node_controller_limit"    ? "Node controller limits"
        : std::string_view(type) == "node_controller_feedback" ? "Controller feedback"
        : std::string_view(type) == "node_controller_cohort_app" ? "Controller cohorts"
                                                                 : "FST actuators");
    std::set<std::string> primaryKeys;
    if (std::string_view(type) == "app_node_pressure") {
      primaryKeys = {"app", "node_id", "read_rate_bps", "write_rate_bps",
                     "node_io_pressure"};
    } else if (std::string_view(type) == "node_controller_feedback") {
      primaryKeys = {"node_id", "operation", "phase", "protected_app_count",
                     "response_ratio"};
    } else if (std::string_view(type) == "node_controller_cohort_app") {
      primaryKeys = {"node_id", "app",        "operation",
                     "cohort",  "target_bps", "assigned_reduction_bps"};
    }
    Fields primary;
    for (const auto& field : fields) {
      if (primaryKeys.empty() || primaryKeys.count(field.key)) {
        primary.push_back(field);
      }
    }
    view.table(primary, records);
    for (const auto* record : records) {
      if (!primaryKeys.empty()) {
        const std::string identity =
            record->isMember("app") ? SafeText((*record)["app"].asString()) + " / " : "";
        view.heading("Details: " + identity + SafeText((*record)["node_id"].asString()));
        for (const auto& group : {"metrics", "signals"}) {
          bool title = false;
          for (const auto& field : fields) {
            if (primaryKeys.count(field.key)) {
              continue;
            }
            const bool signal =
                field.kind == Kind::Boolean || field.kind == Kind::Pressure;
            if ((std::string_view(group) == "signals") != signal) {
              continue;
            }
            if (!title) {
              view.heading(signal ? "Pressure and control signals"
                                  : "Reservations and controller metrics");
              title = true;
            }
            view.detail(field.label, ValueCell(*record, field),
                        LabelStyle(field, record));
          }
        }
      }
      view.additional(
          *record, fields,
          {"has_node_io_pressure", "has_read_io_pressure", "has_write_io_pressure"});
    }
  }
  return true;
}

bool
RenderConfig(const Json::Value& root, View& view)
{
  if (!root.isMember("enabled") || !Validate(root, configFields)) {
    return false;
  }
  view.heading("IO shaping configuration");
  const std::vector<std::pair<const char*, std::pair<std::size_t, std::size_t>>> groups =
      {{"Engine", {0, 1}},
       {"Collection and detail", {1, 8}},
       {"Enforcement and controller", {8, 14}},
       {"Maintenance and diagnostics", {14, 16}}};
  for (const auto& group : groups) {
    view.heading(group.first);
    for (std::size_t index = group.second.first; index < group.second.second; ++index) {
      const auto& field = configFields[index];
      if (root.isMember(field.key)) {
        view.detail(field.label, ValueCell(root, field), LabelStyle(field));
      }
    }
  }
  view.note("Automatic detail thresholds count node-state streams.");
  view.additional(root, configFields);
  return true;
}

} // namespace

bool
PrepareIoShapingHumanOutput(RequestProto& request, bool globalJson)
{
  if (globalJson || !request.has_io() || !request.io().has_shaping()) {
    return false;
  }
  auto* shaping = request.mutable_io()->mutable_shaping();
  if (shaping->has_list()) {
    if (shaping->list().json_output()) {
      return false;
    }
    shaping->mutable_list()->set_json_output(true);
  } else if (shaping->has_policy() && shaping->policy().has_list()) {
    if (shaping->policy().list().json_output()) {
      return false;
    }
    shaping->mutable_policy()->mutable_list()->set_json_output(true);
  } else if (shaping->has_pressure() && shaping->pressure().has_list()) {
    if (shaping->pressure().list().json_output()) {
      return false;
    }
    shaping->mutable_pressure()->mutable_list()->set_json_output(true);
  } else if (shaping->has_config() && shaping->config().has_list()) {
    if (shaping->config().list().json_output()) {
      return false;
    }
    shaping->mutable_config()->mutable_list()->set_json_output(true);
  } else {
    return false;
  }
  request.set_dontcolor(true);
  return true;
}

std::optional<std::string>
RenderIoShapingOutput(const IoProto_ShapingProto& request, std::string_view payload,
                      std::size_t width, int descriptor)
{
  try {
    if (payload.empty()) {
      return std::nullopt;
    }
    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    builder["allowComments"] = false;
    builder["allowTrailingCommas"] = false;
    builder["strictRoot"] = true;
    builder["failIfExtra"] = true;
    builder["rejectDupKeys"] = true;
    builder["allowSpecialFloats"] = false;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value root;
    std::string errors;
    if (!reader->parse(payload.data(), payload.data() + payload.size(), &root, &errors)) {
      return std::nullopt;
    }
    View view(width, descriptor);
    if (request.has_config() && request.config().has_list()) {
      if (!root.isObject() || !RenderConfig(root, view)) {
        return std::nullopt;
      }
    } else {
      if (!root.isArray()) {
        return std::nullopt;
      }
      for (const auto& record : root) {
        if (!record.isObject() || !record.isMember("type") ||
            !record["type"].isString()) {
          return std::nullopt;
        }
      }
      if (request.has_list()) {
        if (!RenderRates(request, root, view)) {
          return std::nullopt;
        }
      } else if (request.has_policy() && request.policy().has_list()) {
        if (!RenderPolicies(request, root, view)) {
          return std::nullopt;
        }
      } else if (request.has_pressure() && request.pressure().has_list()) {
        if (!RenderPressure(root, view)) {
          return std::nullopt;
        }
      } else {
        return std::nullopt;
      }
    }
    return view.output;
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

} // namespace eos::console
