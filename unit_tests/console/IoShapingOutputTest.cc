// ----------------------------------------------------------------------
// File: IoShapingOutputTest.cc
// ----------------------------------------------------------------------

#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include "console/IoShapingOutput.hh"
#include "proto/ConsoleRequest.pb.h"
#include "gtest/gtest.h"
#include <cstdlib>
#include <json/json.h>
#include <optional>
#include <regex>
#include <sstream>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace {
using namespace eos::console;

std::string
JsonText(const Json::Value& value)
{
  Json::StreamWriterBuilder writer;
  writer["indentation"] = "";
  return Json::writeString(writer, value);
}

class IoShapingOutputTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    if (const char* value = getenv("TERM")) {
      term = value;
    }
    setenv("TERM", "xterm", 1);
    ConfigureColor(ColorMode::Never, false, false);
  }

  void
  TearDown() override
  {
    if (term) {
      setenv("TERM", term->c_str(), 1);
    } else {
      unsetenv("TERM");
    }
    ConfigureColor(ColorMode::Auto, false, false);
  }

  static std::string
  Plain(const std::string& value)
  {
    static const std::regex escapes("\033\\[[0-?]*[ -/]*[@-~]|\033[()][0-2A-Z]");
    return std::regex_replace(value, escapes, "");
  }

  static void
  LinesFit(const std::string& text, size_t width)
  {
    std::istringstream stream(Plain(text));
    std::string line;
    while (std::getline(stream, line)) {
      EXPECT_LE(HelpDisplayWidth(line), width) << line;
    }
  }

  std::string
  Render(const IoProto_ShapingProto& request, const Json::Value& value,
         size_t width = 120)
  {
    const auto result =
        RenderIoShapingOutput(request, JsonText(value), width, STDOUT_FILENO);
    EXPECT_TRUE(result.has_value()) << JsonText(value);
    return result.value_or("");
  }

  std::optional<std::string> term;
};

Json::Value
Rates(const std::string& type, const std::string& identity)
{
  Json::Value entry(Json::objectValue);
  entry["type"] = type;
  entry["id"] = identity;
  entry["window_sec"] = 15;
  entry["read_rate_bps"] = 12000000.0;
  entry["write_rate_bps"] = 3500000.0;
  entry["read_iops"] = 12.0;
  entry["write_iops"] = 7.0;
  return entry;
}

Json::Value
Policy(const std::string& type, const std::string& identity)
{
  Json::Value entry(Json::objectValue);
  entry["type"] = type;
  entry["id"] = identity;
  entry["is_enabled"] = true;
  entry["limit_read_bytes_per_sec"] = Json::UInt64(12000000);
  entry["limit_write_bytes_per_sec"] = Json::UInt64(3500000);
  entry["reservation_read_bytes_per_sec"] = Json::UInt64(2000000);
  entry["reservation_write_bytes_per_sec"] = Json::UInt64(1000000);
  entry["controller_limit_read_bytes_per_sec"] = Json::UInt64(987654321);
  entry["controller_limit_write_bytes_per_sec"] = Json::UInt64(654321987);
  return entry;
}

TEST_F(IoShapingOutputTest, PreparationOnlyChangesHumanListTransport)
{
  for (int family = 0; family < 4; ++family) {
    SCOPED_TRACE(family);
    RequestProto request;
    request.set_comment("retain the request metadata");
    request.set_format(RequestProto::DEFAULT);
    auto* shaping = request.mutable_io()->mutable_shaping();
    if (family == 0) {
      auto* list = shaping->mutable_list();
      list->set_show_all(true);
      list->set_resolve_ids(true);
      list->set_system_stats(true);
      list->set_time_window_seconds(300);
    } else if (family == 1) {
      auto* list = shaping->mutable_policy()->mutable_list();
      list->set_filter_apps(true);
      list->set_filter_users(true);
      list->set_show_controller_limits(true);
    } else if (family == 2) {
      shaping->mutable_pressure()->mutable_list();
    } else {
      shaping->mutable_config()->mutable_list();
    }
    auto expected = request;
    expected.set_dontcolor(true);
    if (family == 0) {
      expected.mutable_io()->mutable_shaping()->mutable_list()->set_json_output(true);
    } else if (family == 1) {
      expected.mutable_io()
          ->mutable_shaping()
          ->mutable_policy()
          ->mutable_list()
          ->set_json_output(true);
    } else if (family == 2) {
      expected.mutable_io()
          ->mutable_shaping()
          ->mutable_pressure()
          ->mutable_list()
          ->set_json_output(true);
    } else {
      expected.mutable_io()
          ->mutable_shaping()
          ->mutable_config()
          ->mutable_list()
          ->set_json_output(true);
    }
    EXPECT_TRUE(PrepareIoShapingHumanOutput(request, false));
    EXPECT_EQ(request.SerializeAsString(), expected.SerializeAsString());
    const auto prepared = request.SerializeAsString();
    EXPECT_FALSE(PrepareIoShapingHumanOutput(request, false));
    EXPECT_EQ(request.SerializeAsString(), prepared);
  }
}

TEST_F(IoShapingOutputTest, MachineOutputAndMutationsAreUntouched)
{
  std::vector<RequestProto> requests(11);
  requests[0].mutable_io()->mutable_stat()->set_monitoring(true);
  requests[1].mutable_io()->mutable_ns()->set_monitoring(true);
  requests[2].mutable_ns()->mutable_stat();
  requests[3].mutable_io()->mutable_shaping()->mutable_enable();
  requests[4].mutable_io()->mutable_shaping()->mutable_disable();
  requests[5].mutable_io()->mutable_shaping()->mutable_policy()->mutable_set()->set_app(
      "x");
  requests[6]
      .mutable_io()
      ->mutable_shaping()
      ->mutable_policy()
      ->mutable_remove()
      ->set_uid(1);
  requests[7]
      .mutable_io()
      ->mutable_shaping()
      ->mutable_config()
      ->mutable_set()
      ->set_limits_enabled(false);
  requests[8].mutable_io()->mutable_shaping()->mutable_list()->set_json_output(true);
  requests[9]
      .mutable_io()
      ->mutable_shaping()
      ->mutable_pressure()
      ->mutable_list()
      ->set_json_output(true);
  requests[10]
      .mutable_io()
      ->mutable_shaping()
      ->mutable_policy()
      ->mutable_list()
      ->set_json_output(true);
  RequestProto config;
  config.mutable_io()
      ->mutable_shaping()
      ->mutable_config()
      ->mutable_list()
      ->set_json_output(true);
  requests.push_back(config);
  for (auto& request : requests) {
    const auto before = request.SerializeAsString();
    EXPECT_FALSE(PrepareIoShapingHumanOutput(request, false));
    EXPECT_EQ(request.SerializeAsString(), before);
  }
  RequestProto global;
  global.mutable_io()->mutable_shaping()->mutable_list()->set_show_users(true);
  const auto before = global.SerializeAsString();
  EXPECT_FALSE(PrepareIoShapingHumanOutput(global, true));
  EXPECT_EQ(global.SerializeAsString(), before);
}

TEST_F(IoShapingOutputTest, EveryRateGroupingKeepsItsIdentity)
{
  for (const char* type : {"app", "uid", "gid", "node", "fs", "all"}) {
    SCOPED_TRACE(type);
    IoProto_ShapingProto request;
    request.mutable_list()->set_time_window_seconds(15);
    Json::Value entry = Rates(type, "identity-marker");
    if (std::string(type) == "uid" || std::string(type) == "gid") {
      entry["id"] = "4294967294";
      entry["name"] = "resolved-identity-marker";
    } else if (std::string(type) == "fs" || std::string(type) == "all") {
      entry.removeMember("id");
      entry["node_id"] = "storage-node-marker";
      entry["fsid"] = Json::UInt64(18446744073709551615ULL);
      if (std::string(type) == "all") {
        entry["app"] = "application-marker";
        entry["uid"] = Json::UInt(4294967294U);
        entry["gid"] = Json::UInt(4294967293U);
        entry["user"] = "user-name-marker";
        entry["group"] = "group-name-marker";
      }
    }
    Json::Value rows(Json::arrayValue);
    rows.append(entry);
    const auto output = Render(request, rows, 200);
    EXPECT_NE(output.find("15"), std::string::npos);
    if (entry.isMember("id")) {
      EXPECT_NE(output.find(entry["id"].asString()), std::string::npos);
    }
    for (const char* key : {"name", "node_id", "app", "user", "group"}) {
      if (entry.isMember(key)) {
        EXPECT_NE(output.find(entry[key].asString()), std::string::npos) << key;
      }
    }
    if (entry.isMember("fsid")) {
      EXPECT_NE(output.find("18446744073709551615"), std::string::npos);
    }
    EXPECT_NE(output.find("MB/s"), std::string::npos);
  }
}

TEST_F(IoShapingOutputTest, PoliciesRespectControllerSelectionAndKeepAllTypes)
{
  IoProto_ShapingProto request;
  request.mutable_policy()->mutable_list();
  Json::Value rows(Json::arrayValue);
  for (const char* type : {"app", "uid", "gid"}) {
    rows.append(Policy(type, std::string(type) + "-policy-marker"));
  }
  auto output = Render(request, rows, 300);
  for (const char* type : {"app", "uid", "gid"}) {
    EXPECT_NE(output.find(std::string(type) + "-policy-marker"), std::string::npos);
  }
  EXPECT_EQ(output.find("987.65"), std::string::npos);
  EXPECT_EQ(output.find("654.32"), std::string::npos);
  request.mutable_policy()->mutable_list()->set_show_controller_limits(true);
  output = Render(request, rows, 300);
  EXPECT_NE(output.find("987.65"), std::string::npos);
  EXPECT_NE(output.find("654.32"), std::string::npos);

  auto zero = Policy("app", "zero-policy-marker");
  for (const char* field :
       {"limit_read_bytes_per_sec", "limit_write_bytes_per_sec",
        "reservation_read_bytes_per_sec", "reservation_write_bytes_per_sec",
        "controller_limit_read_bytes_per_sec", "controller_limit_write_bytes_per_sec"}) {
    zero[field] = Json::UInt64(0);
  }
  rows = Json::Value(Json::arrayValue);
  rows.append(zero);
  output = Render(request, rows, 80);
  for (const char* expected :
       {"Read limit: unlimited", "Write limit: unlimited", "Read reservation: none",
        "Write reservation: none", "Controller read limit: no cap",
        "Controller write limit: no cap"}) {
    EXPECT_NE(output.find(expected), std::string::npos) << expected;
  }
  EXPECT_EQ(output.find("0.00 B/s"), std::string::npos);
}

TEST_F(IoShapingOutputTest, OptionalPressureDistinguishesUnavailableFromZero)
{
  IoProto_ShapingProto request;
  request.mutable_list()->set_show_apps(true);
  Json::Value rows(Json::arrayValue);
  auto absent = Rates("app", "pressure-unavailable-marker");
  absent["has_read_io_pressure"] = false;
  absent["has_write_io_pressure"] = false;
  rows.append(absent);
  auto zero = Rates("app", "pressure-zero-marker");
  zero["has_read_io_pressure"] = true;
  zero["has_write_io_pressure"] = true;
  zero["read_io_pressure"] = 0.0;
  zero["write_io_pressure"] = 0.0;
  rows.append(zero);
  const auto output = Render(request, rows, 300);
  EXPECT_NE(output.find("pressure-unavailable-marker"), std::string::npos);
  EXPECT_NE(output.find("pressure-zero-marker"), std::string::npos);
  EXPECT_NE(output.find("0.000"), std::string::npos);
  EXPECT_NE(output.find("-"), std::string::npos);
}

TEST_F(IoShapingOutputTest, EveryPressureRecordTypeRetainsItsFields)
{
  IoProto_ShapingProto request;
  request.mutable_pressure()->mutable_list();
  Json::Value rows(Json::arrayValue);
  for (const char* type :
       {"app_node_pressure", "node_controller_limit", "node_controller_feedback",
        "node_controller_cohort_app", "fst_actuator"}) {
    Json::Value entry(Json::objectValue);
    entry["type"] = type;
    entry["node_id"] = std::string(type) + "-node-marker";
    if (std::string(type) == "app_node_pressure") {
      entry["app"] = "protected-app-marker";
      entry["has_node_io_pressure"] = false;
      entry["has_read_io_pressure"] = true;
      entry["read_io_pressure"] = 0.0;
      entry["has_write_io_pressure"] = false;
      entry["read_rate_bps"] = 1234.0;
      entry["write_rate_bps"] = 0.0;
      entry["read_triggers_competitor_throttling"] = true;
      entry["write_triggers_competitor_throttling"] = false;
    } else if (std::string(type) == "node_controller_limit") {
      entry["app"] = "competitor-app-marker";
      entry["read_bytes_per_sec"] = Json::UInt64(1048576);
      entry["write_bytes_per_sec"] = Json::UInt64(0);
    } else if (std::string(type) == "node_controller_feedback") {
      entry["operation"] = "read";
      entry["phase"] = "suppressed";
      entry["suppressed"] = true;
      entry["awaiting_response"] = false;
      entry["consecutive_deficit_samples"] = Json::UInt64(12345678901234567890ULL);
      entry["response_ratio"] = 0.125;
    } else if (std::string(type) == "node_controller_cohort_app") {
      entry["app"] = "failed-cohort-app-marker";
      entry["operation"] = "write";
      entry["cohort"] = "failed";
      entry["target_bps"] = 2000.0;
      entry["baseline_rate_bps"] = 4000.0;
      entry["assigned_reduction_bps"] = 1000.0;
      entry["rate_at_failure_bps"] = 3000.0;
    } else {
      entry["identity_type"] = "uid";
      entry["identity"] = "4294967294";
      entry["operation"] = "write";
      entry["legacy_delay_microseconds"] = Json::UInt64(12345678901234567890ULL);
      entry["assigned_rate_bytes_per_sec"] = Json::UInt64(1000000);
    }
    rows.append(entry);
  }
  const auto output = Render(request, rows, 300);
  for (const auto& entry : rows) {
    EXPECT_NE(output.find(entry["node_id"].asString()), std::string::npos);
    if (entry.isMember("app")) {
      EXPECT_NE(output.find(entry["app"].asString()), std::string::npos);
    }
  }
  EXPECT_NE(output.find("suppressed"), std::string::npos);
  EXPECT_NE(output.find("failed"), std::string::npos);
  EXPECT_NE(output.find("4294967294"), std::string::npos);
  EXPECT_NE(output.find("12345678901234567890"), std::string::npos);
}

TEST_F(IoShapingOutputTest, ConfigAndSystemStatsKeepExactLargeCounters)
{
  IoProto_ShapingProto request;
  request.mutable_config()->mutable_list();
  Json::Value config(Json::objectValue);
  config["enabled"] = true;
  config["detail_level"] = "fs";
  config["limits_enabled"] = false;
  config["reservations_enabled"] = true;
  config["estimators_update_period_ms"] = Json::UInt64(200);
  config["detail_auto_enabled"] = true;
  config["detail_auto_low_cardinality"] = Json::UInt64(12345678901234567890ULL);
  config["detail_auto_high_cardinality"] = Json::UInt64(18446744073709551615ULL);
  config["io_pressure_threshold"] = 0.125;
  const auto output = Render(request, config, 300);
  EXPECT_NE(output.find("12345678901234567890"), std::string::npos);
  EXPECT_NE(output.find("18446744073709551615"), std::string::npos);
  EXPECT_NE(output.find("0.125"), std::string::npos);
  request.mutable_list()->set_system_stats(true);
  Json::Value rows(Json::arrayValue);
  Json::Value system(Json::objectValue);
  system["type"] = "system";
  system["id"] = "engine_meta";
  system["estimators_loop_median_us"] = 1.25;
  system["detail_auto_indicator"] = "node_state_streams";
  system["node_states_cardinality"] = Json::UInt64(12345678901234567890ULL);
  system["published_fst_io_delay_configs_cardinality"] =
      Json::UInt64(18446744073709551615ULL);
  rows.append(system);
  const auto stats = Render(request, rows, 300);
  EXPECT_NE(stats.find("12345678901234567890"), std::string::npos);
  EXPECT_NE(stats.find("18446744073709551615"), std::string::npos);
  EXPECT_NE(stats.find("node_state_streams"), std::string::npos);
}

TEST_F(IoShapingOutputTest, ConfigurationPreservesEngineAndDetailStatesAcrossLayouts)
{
  IoProto_ShapingProto request;
  request.mutable_config()->mutable_list();
  for (const char* detail : {"aggregate", "fs"}) {
    SCOPED_TRACE(detail);
    for (const bool enabled : {false, true}) {
      SCOPED_TRACE(enabled);
      Json::Value config(Json::objectValue);
      config["enabled"] = enabled;
      config["detail_level"] = detail;
      const std::string state = enabled ? "enabled" : "disabled";
      const auto style = enabled ? TextStyle::Success : TextStyle::Warning;
      for (const size_t width : {120, 20}) {
        SCOPED_TRACE(width);
        ConfigureColor(ColorMode::Never, false, false);
        const auto baseline = Render(request, config, width);
        for (const auto mode : {ColorMode::Never, ColorMode::Always}) {
          SCOPED_TRACE(mode == ColorMode::Always ? "always" : "never");
          ConfigureColor(mode, false, false);
          const auto output = Render(request, config, width);
          const auto plain = Plain(output);
          EXPECT_EQ(plain, baseline);
          EXPECT_TRUE(std::regex_search(plain, std::regex("Engine:\\s+" + state + "\\b")))
              << plain;
          EXPECT_TRUE(std::regex_search(
              plain, std::regex("Detail level:\\s+" + std::string(detail) + "\\b")))
              << plain;
          EXPECT_NE(output.find(Colorize(state, style, STDOUT_FILENO)),
                    std::string::npos);
          EXPECT_EQ(output.find('\033') != std::string::npos, mode == ColorMode::Always);
          LinesFit(output, width);
        }
      }
    }
  }
}

TEST_F(IoShapingOutputTest, NarrowOutputKeepsLongIdentitiesAndEscapesTerminalControls)
{
  IoProto_ShapingProto request;
  request.mutable_list()->set_show_apps(true);
  const std::string identity =
      "utf8-caf\xc3\xa9-\xe7\x95\x8c-e\xcc\x81-" + std::string(80, 'x') + "-end-marker";
  Json::Value rows(Json::arrayValue);
  auto entry = Rates("app", identity);
  rows.append(entry);
  for (size_t width : {20u, 40u, 80u, 120u}) {
    SCOPED_TRACE(width);
    const auto output = Render(request, rows, width);
    EXPECT_EQ(output.find("..."), std::string::npos);
    EXPECT_EQ(output.find('\033'), std::string::npos);
    std::string compact;
    for (char byte : output) {
      if (byte != '\n' && byte != ' ') {
        compact += byte;
      }
    }
    EXPECT_NE(compact.find(identity), std::string::npos);
    LinesFit(output, width);
  }
  rows[0]["id"] = "bad\033[31mname\nnext\tcell\rcarriage\bbackspace";
  const auto sanitized = Render(request, rows, 300);
  EXPECT_EQ(sanitized.find('\033'), std::string::npos);
  EXPECT_EQ(sanitized.find('\t'), std::string::npos);
  EXPECT_EQ(sanitized.find('\r'), std::string::npos);
  EXPECT_EQ(sanitized.find('\b'), std::string::npos);
  EXPECT_NE(sanitized.find("bad"), std::string::npos);
  EXPECT_NE(sanitized.find("backspace"), std::string::npos);
}

TEST_F(IoShapingOutputTest, StylingNeverChangesTheDisplayedDataOrLayout)
{
  IoProto_ShapingProto request;
  request.mutable_policy()->mutable_list()->set_show_controller_limits(true);
  Json::Value rows(Json::arrayValue);
  rows.append(Policy("app", "policy-marker"));
  rows.append(Policy("uid", "1001"));
  rows[1]["is_enabled"] = false;
  const auto baseline = Render(request, rows, 120);
  ConfigureColor(ColorMode::Always, false, false);
  const auto styled = Render(request, rows, 120);
  EXPECT_NE(styled.find('\033'), std::string::npos);
  EXPECT_EQ(Plain(styled), baseline);
}

TEST_F(IoShapingOutputTest, EntityAndDirectionColorsSurviveAllRateLayouts)
{
  IoProto_ShapingProto request;
  request.mutable_list()->set_show_all(true);
  auto entry = Rates("all", "unused");
  entry.removeMember("id");
  entry["node_id"] = "storage-node";
  entry["fsid"] = Json::UInt64(1234);
  entry["app"] = "analysis";
  entry["uid"] = Json::UInt(1001);
  entry["user"] = "alice";
  entry["gid"] = Json::UInt(1002);
  entry["group"] = "research";
  Json::Value rows(Json::arrayValue);
  rows.append(entry);
  for (const size_t width : {300, 80, 20}) {
    SCOPED_TRACE(width);
    ConfigureColor(ColorMode::Never, false, false);
    const auto baseline = Render(request, rows, width);
    ConfigureColor(ColorMode::Always, false, false);
    const auto output = Render(request, rows, width);
    EXPECT_EQ(Plain(output), baseline);
    LinesFit(output, width);
    for (const char* label :
         {"Node", "FSID", "Application", "UID", "User", "GID", "Group"}) {
      const std::string text = std::string(label) + (width == 300 ? "" : ":");
      EXPECT_NE(output.find(Colorize(text, TextStyle::EntityType, STDOUT_FILENO)),
                std::string::npos)
          << text;
    }
    for (const auto& [label, value, style] :
         {std::tuple{"Read", "12.00 MB/s", TextStyle::Read},
          std::tuple{"Write", "3.50 MB/s", TextStyle::Write},
          std::tuple{"Read IOPS", "12.00", TextStyle::Read},
          std::tuple{"Write IOPS", "7.00", TextStyle::Write}}) {
      const std::string text = std::string(label) + (width == 300 ? "" : ":");
      EXPECT_NE(output.find(Colorize(text, style, STDOUT_FILENO)), std::string::npos)
          << text;
      EXPECT_NE(output.find(Colorize(value, style, STDOUT_FILENO)), std::string::npos)
          << value;
    }
  }
}

TEST_F(IoShapingOutputTest, DirectionColorsKeepPolicySemanticsAndWrappedLabels)
{
  IoProto_ShapingProto request;
  request.mutable_policy()->mutable_list()->set_show_controller_limits(true);
  Json::Value rows(Json::arrayValue);
  rows.append(Policy("app", "bounded"));
  auto zero = Policy("app", "unbounded");
  zero["is_enabled"] = false;
  for (const char* field :
       {"limit_read_bytes_per_sec", "limit_write_bytes_per_sec",
        "reservation_read_bytes_per_sec", "reservation_write_bytes_per_sec",
        "controller_limit_read_bytes_per_sec", "controller_limit_write_bytes_per_sec"}) {
    zero[field] = Json::UInt64(0);
  }
  rows.append(zero);
  for (const size_t width : {300, 80, 20}) {
    SCOPED_TRACE(width);
    ConfigureColor(ColorMode::Never, false, false);
    const auto baseline = Render(request, rows, width);
    ConfigureColor(ColorMode::Always, false, false);
    const auto output = Render(request, rows, width);
    EXPECT_EQ(Plain(output), baseline);
    LinesFit(output, width);
    if (width == 20) {
      EXPECT_NE(output.find(Colorize("12.00", TextStyle::Read, STDOUT_FILENO) + "\n" +
                            std::string(12, ' ') +
                            Colorize("MB/s", TextStyle::Read, STDOUT_FILENO)),
                std::string::npos);
    } else {
      EXPECT_NE(output.find(Colorize("12.00 MB/s", TextStyle::Read, STDOUT_FILENO)),
                std::string::npos);
    }
    EXPECT_NE(output.find(Colorize("3.50 MB/s", TextStyle::Write, STDOUT_FILENO)),
              std::string::npos);
    EXPECT_NE(output.find(Colorize("disabled", TextStyle::Warning, STDOUT_FILENO)),
              std::string::npos);
    for (const char* value : {"unlimited", "none", "no cap"}) {
      EXPECT_NE(output.find(Colorize(value, TextStyle::Syntax, STDOUT_FILENO)),
                std::string::npos)
          << value;
      EXPECT_EQ(output.find(Colorize(value, TextStyle::Read, STDOUT_FILENO)),
                std::string::npos)
          << value;
      EXPECT_EQ(output.find(Colorize(value, TextStyle::Write, STDOUT_FILENO)),
                std::string::npos)
          << value;
    }
    for (const auto& [direction, style] :
         {std::pair{"read", TextStyle::Read}, std::pair{"write", TextStyle::Write}}) {
      const std::string label = "Controller " + std::string(direction);
      if (width == 20) {
        EXPECT_NE(output.find(Colorize(label, style, STDOUT_FILENO)), std::string::npos);
        EXPECT_NE(output.find(Colorize("limit:", style, STDOUT_FILENO)),
                  std::string::npos);
      } else {
        const std::string text = label + " limit" + (width == 300 ? "" : ":");
        EXPECT_NE(output.find(Colorize(text, style, STDOUT_FILENO)), std::string::npos);
      }
    }
  }
}

TEST_F(IoShapingOutputTest, PressureKeepsWarningsAndStylesActuatorTypesAndOperations)
{
  IoProto_ShapingProto request;
  request.mutable_pressure()->mutable_list();
  Json::Value rows(Json::arrayValue);
  Json::Value pressure(Json::objectValue);
  pressure["type"] = "app_node_pressure";
  pressure["app"] = "reserved";
  pressure["node_id"] = "storage-node";
  pressure["has_read_io_pressure"] = true;
  pressure["read_io_pressure"] = 0.125;
  pressure["read_pressure_active"] = true;
  pressure["has_write_io_pressure"] = false;
  pressure["write_io_pressure"] = 0.0;
  rows.append(pressure);
  for (const char* type : {"app", "uid", "gid"}) {
    for (const char* operation : {"read", "write"}) {
      Json::Value actuator(Json::objectValue);
      actuator["type"] = "fst_actuator";
      actuator["node_id"] = "storage-node";
      actuator["identity_type"] = type;
      actuator["identity"] = std::string(type) + "-identity";
      actuator["operation"] = operation;
      actuator["assigned_rate_bytes_per_sec"] =
          Json::UInt64(std::string(operation) == "read" ? 1000000 : 2000000);
      rows.append(actuator);
    }
  }
  for (const size_t width : {300, 80, 20}) {
    SCOPED_TRACE(width);
    ConfigureColor(ColorMode::Never, false, false);
    const auto baseline = Render(request, rows, width);
    ConfigureColor(ColorMode::Always, false, false);
    const auto output = Render(request, rows, width);
    EXPECT_EQ(Plain(output), baseline);
    LinesFit(output, width);
    for (const char* type : {"app", "uid", "gid"}) {
      EXPECT_NE(output.find(Colorize(type, TextStyle::EntityType, STDOUT_FILENO)),
                std::string::npos)
          << type;
    }
    for (const auto& [operation, rate, style] :
         {std::tuple{"read", "1.00 MB/s", TextStyle::Read},
          std::tuple{"write", "2.00 MB/s", TextStyle::Write}}) {
      EXPECT_NE(output.find(Colorize(operation, style, STDOUT_FILENO)), std::string::npos)
          << operation;
      EXPECT_NE(output.find(Colorize(rate, style, STDOUT_FILENO)), std::string::npos)
          << rate;
      if (width != 300) {
        EXPECT_NE(output.find(Colorize("Assigned rate:", style, STDOUT_FILENO)),
                  std::string::npos);
      }
    }
    if (width == 300) {
      EXPECT_NE(output.find(Colorize("Assigned rate", TextStyle::Heading, STDOUT_FILENO)),
                std::string::npos);
      EXPECT_EQ(output.find(Colorize("Assigned rate", TextStyle::Read, STDOUT_FILENO)),
                std::string::npos);
      EXPECT_EQ(output.find(Colorize("Assigned rate", TextStyle::Write, STDOUT_FILENO)),
                std::string::npos);
    }
    const std::string identityLabel = width == 300 ? "Identity type" : "Identity type:";
    EXPECT_NE(output.find(Colorize(identityLabel, TextStyle::EntityType, STDOUT_FILENO)),
              std::string::npos);
    EXPECT_NE(output.find(Colorize("0.125", TextStyle::Warning, STDOUT_FILENO)),
              std::string::npos);
    EXPECT_EQ(output.find(Colorize("0.125", TextStyle::Read, STDOUT_FILENO)),
              std::string::npos);
    EXPECT_NE(output.find(Colorize("-", TextStyle::Syntax, STDOUT_FILENO)),
              std::string::npos);
    EXPECT_EQ(output.find(Colorize("-", TextStyle::Write, STDOUT_FILENO)),
              std::string::npos);
  }
}

TEST_F(IoShapingOutputTest, UnsupportedRepliesFallBackWithoutPartialRendering)
{
  IoProto_ShapingProto request;
  request.mutable_list();
  for (const char* payload :
       {"legacy server table\n", "{", "null", "{}", "[1]",
        "[{\"type\":\"new_server_type\",\"id\":\"x\"}]",
        "[{\"type\":\"app\",\"id\":\"x\",\"read_rate_bps\":\"bad\"}]"}) {
    SCOPED_TRACE(payload);
    EXPECT_FALSE(RenderIoShapingOutput(request, payload, 80, STDOUT_FILENO));
  }
  auto good = Rates("app", "valid-row-must-not-leak-before-invalid-row");
  for (const char* missing : {"id", "read_rate_bps", "write_iops"}) {
    auto invalid = good;
    invalid.removeMember(missing);
    Json::Value rows(Json::arrayValue);
    rows.append(invalid);
    EXPECT_FALSE(RenderIoShapingOutput(request, JsonText(rows), 80, STDOUT_FILENO))
        << missing;
  }
  Json::Value mixed(Json::arrayValue);
  mixed.append(good);
  mixed.append(Rates("future_schema", "invalid-row"));
  EXPECT_FALSE(RenderIoShapingOutput(request, JsonText(mixed), 80, STDOUT_FILENO));
  for (const auto& payload :
       {JsonText(mixed) + " trailing garbage", "/* comment */" + JsonText(good)}) {
    EXPECT_FALSE(RenderIoShapingOutput(request, payload, 80, STDOUT_FILENO));
  }
  request.mutable_pressure()->mutable_list();
  Json::Value badPresence(Json::arrayValue);
  Json::Value pressure(Json::objectValue);
  pressure["type"] = "app_node_pressure";
  pressure["node_id"] = "node";
  pressure["has_read_io_pressure"] = "yes";
  badPresence.append(pressure);
  EXPECT_FALSE(RenderIoShapingOutput(request, JsonText(badPresence), 80, STDOUT_FILENO));
  request.mutable_enable();
  EXPECT_FALSE(RenderIoShapingOutput(request, "[]", 80, STDOUT_FILENO));
  request.mutable_config()->mutable_list();
  EXPECT_FALSE(RenderIoShapingOutput(request, "[]", 80, STDOUT_FILENO));
  EXPECT_FALSE(
      RenderIoShapingOutput(request, R"({"enabled":"true"})", 80, STDOUT_FILENO));
}

TEST_F(IoShapingOutputTest, EmptyListsRemainUsefulAndNewScalarFieldsAreVisible)
{
  IoProto_ShapingProto request;
  for (int family = 0; family < 3; ++family) {
    if (family == 0) {
      request.mutable_list();
    }
    if (family == 1) {
      request.mutable_policy()->mutable_list();
    }
    if (family == 2) {
      request.mutable_pressure()->mutable_list();
    }
    const auto empty = Render(request, Json::Value(Json::arrayValue));
    EXPECT_FALSE(empty.empty());
  }
  request.mutable_list();
  Json::Value rows(Json::arrayValue);
  auto entry = Rates("app", "new-field-marker");
  entry["future_counter"] = Json::UInt64(12345678901234567890ULL);
  rows.append(entry);
  const auto output = Render(request, rows, 300);
  EXPECT_NE(output.find("12345678901234567890"), std::string::npos);
}
} // namespace
