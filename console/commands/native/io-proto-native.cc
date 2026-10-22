// ----------------------------------------------------------------------
// File: io-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCli.hh"
#include "console/ConsoleColor.hh"
#include "console/ConsoleHelp.hh"
#include "console/ConsoleMain.hh"
#include "console/IoShapingOutput.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <memory>
#include <sstream>

namespace {

const char* kShapingExamples = "\nEXAMPLES:\n"
                               "  # Show current application rates\n"
                               "  eos io shaping ls --apps\n"
                               "\n"
                               "  # Globally enable the traffic shaping engine\n"
                               "  eos io shaping enable\n"
                               "\n"
                               "  # Globally disable the traffic shaping engine\n"
                               "  eos io shaping disable\n"
                               "\n"
                               "  # Show reservation pressure by application and node\n"
                               "  eos io shaping pressure ls\n";

const char* kShapingPolicyExamples =
    "\nEXAMPLES:\n"
    "  # Limit 'eoscp' read rate to 10 MB/s and write rate to 50 MB/s\n"
    "  eos io shaping policy set --app eoscp --limit-read 10M --limit-write 50M\n"
    "\n"
    "  # Temporarily disable the policy for 'eoscp'\n"
    "  eos io shaping policy set --app eoscp --disable\n"
    "\n"
    "  # Remove the read limit for 'eoscp' but keep the write limit\n"
    "  eos io shaping policy set --app eoscp --limit-read 0\n"
    "\n"
    "  # Completely delete the policy for user 1001\n"
    "  eos io shaping policy rm --uid 1001\n"
    "\n"
    "  # List all configured application policies including machine limits\n"
    "  eos io shaping policy ls --apps --controller\n";

const char* kShapingConfigExamples =
    "\nEXAMPLES:\n"
    "  # Show current thread configurations\n"
    "  eos io shaping config ls\n"
    "\n"
    "  # Change the estimators update period to 200 ms\n"
    "  eos io shaping config set --estimators-period 200\n"
    "\n"
    "  # Temporarily disable limit or reservation enforcement without deleting policies\n"
    "  eos io shaping config set --limits disabled --reservations disabled\n";

bool
EnabledConfigValue(const std::string& value)
{
  return value == "enabled";
}

constexpr const char* kAutomaticDetailCardinalityPrefix = "auto-cardinality:";

//------------------------------------------------------------------------------
// Share the execution schema with every IO help entry point.
//------------------------------------------------------------------------------
void
ConfigureIoApp(CLI::App& app, eos::console::IoProto* io)
{
  app.set_help_all_flag("--help-all", "Show all IO subcommands and options");
  auto formatter = std::make_shared<CLI::Formatter>();
  formatter->enable_footer_formatting(false);
  app.formatter(formatter);
  app.require_subcommand(1);

  // stat
  auto* stat_cmd = app.add_subcommand("stat", "Print IO statistics");
  stat_cmd->description(
      "Print IO statistics for finished transfers using storage node reports");
  stat_cmd->footer("\nEXAMPLES:\n"
                   "  # Transfers finished between 180 and 120 seconds ago\n"
                   "  eos io stat -x --sa 120 --si 60\n");
  stat_cmd->add_flag("-l", "Show summary (default without -a, -t, -d or -x)");
  stat_cmd->add_flag("-a", "Break down by uid/gid");
  stat_cmd->add_flag("-m", "Monitoring format");
  stat_cmd->add_flag("-n", "Numerical uid/gids");
  stat_cmd->add_flag("-t", "Top user stats");
  stat_cmd->add_flag("-d", "Break down by domains");
  stat_cmd->add_flag("-x", "Break down by application");
  stat_cmd->add_flag("--ss", "Transfer sample statistics");
  stat_cmd->add_option("--sa", "Seconds ago")->type_name("SEC");
  stat_cmd->add_option("--si", "Time interval in seconds")->type_name("SEC");
  stat_cmd->callback([stat_cmd, io]() {
    auto* stat = io->mutable_stat();
    stat->set_summary(stat_cmd->count("-l") > 0);
    stat->set_details(stat_cmd->count("-a") > 0);
    stat->set_monitoring(stat_cmd->count("-m") > 0);
    stat->set_numerical(stat_cmd->count("-n") > 0);
    stat->set_top(stat_cmd->count("-t") > 0);
    stat->set_domain(stat_cmd->count("-d") > 0);
    stat->set_apps(stat_cmd->count("-x") > 0);
    stat->set_sample_stat(stat_cmd->count("--ss") > 0);
    if (stat_cmd->count("--sa")) {
      stat->set_time_ago(stat_cmd->get_option("--sa")->as<uint64_t>());
    }
    if (stat_cmd->count("--si")) {
      stat->set_time_interval(stat_cmd->get_option("--si")->as<uint64_t>());
    }
  });

  // ns
  auto* ns_cmd = app.add_subcommand("ns", "Show namespace IO ranking");
  ns_cmd->footer("\nEXAMPLES:\n"
                 "  # Show the top 100 paths ranked by access count\n"
                 "  eos io ns -n -100\n"
                 "\n"
                 "  # Show files with the most current opens\n"
                 "  eos io ns -f\n");
  ns_cmd->add_flag("-m", "Monitoring format");
  ns_cmd->add_flag("-b", "Rank by bytes");
  ns_cmd->add_flag("-n", "Rank by access count");
  ns_cmd->add_flag("-w", "Last 7 days");
  ns_cmd->add_flag("-f", "Hotfiles");
  ns_cmd->allow_non_standard_option_names();
  ns_cmd->add_flag("-a", "Don't limit output");
  ns_cmd->add_flag("-100", "Top 100");
  ns_cmd->add_flag("-1000", "Top 1000");
  ns_cmd->add_flag("-10000", "Top 10000");
  ns_cmd->callback([ns_cmd, io]() {
    auto* ns = io->mutable_ns();
    ns->set_monitoring(ns_cmd->count("-m") > 0);
    ns->set_rank_by_byte(ns_cmd->count("-b") > 0);
    ns->set_rank_by_access(ns_cmd->count("-n") > 0);
    ns->set_last_week(ns_cmd->count("-w") > 0);
    ns->set_hotfiles(ns_cmd->count("-f") > 0);
    if (ns_cmd->count("-a") > 0)
      ns->set_count(eos::console::IoProto_NsProto::ALL);
    else if (ns_cmd->count("-100") > 0)
      ns->set_count(eos::console::IoProto_NsProto::ONEHUNDRED);
    else if (ns_cmd->count("-1000") > 0)
      ns->set_count(eos::console::IoProto_NsProto::ONETHOUSAND);
    else if (ns_cmd->count("-10000") > 0)
      ns->set_count(eos::console::IoProto_NsProto::TENTHOUSAND);
  });

  // report
  auto* report_cmd = app.add_subcommand("report", "Show report namespace for path");
  report_cmd->add_option("path", "Path to report")->required();
  report_cmd->callback([report_cmd, io]() {
    io->mutable_report()->set_path(report_cmd->get_option("path")->as<std::string>());
  });

  // enable
  auto* enable_cmd = app.add_subcommand(
      "enable",
      "Enable IO statistics collection (no options starts the collection thread)");
  enable_cmd->add_flag("-r", "Enable reports");
  enable_cmd->add_flag("-p", "Enable popularity");
  enable_cmd->add_flag("-n", "Enable report namespace");
  enable_cmd->add_option("--udp", "UDP target address")->type_name("ADDR");
  enable_cmd->callback([enable_cmd, io]() {
    auto* en = io->mutable_enable();
    en->set_switchx(true);
    en->set_reports(enable_cmd->count("-r") > 0);
    en->set_popularity(enable_cmd->count("-p") > 0);
    en->set_namespacex(enable_cmd->count("-n") > 0);
    if (enable_cmd->count("--udp"))
      en->set_upd_address(enable_cmd->get_option("--udp")->as<std::string>());
  });

  // disable
  auto* disable_cmd = app.add_subcommand(
      "disable",
      "Disable IO statistics collection (no options stops the collection thread)");
  disable_cmd->add_flag("-r", "Disable reports");
  disable_cmd->add_flag("-p", "Disable popularity");
  disable_cmd->add_flag("-n", "Disable report namespace");
  disable_cmd->add_option("--udp", "UDP target address to remove")->type_name("ADDR");
  disable_cmd->callback([disable_cmd, io]() {
    auto* en = io->mutable_enable();
    en->set_switchx(false);
    en->set_reports(disable_cmd->count("-r") > 0);
    en->set_popularity(disable_cmd->count("-p") > 0);
    en->set_namespacex(disable_cmd->count("-n") > 0);
    if (disable_cmd->count("--udp"))
      en->set_upd_address(disable_cmd->get_option("--udp")->as<std::string>());
  });

  // shaping (nested subcommands)
  auto* shaping_cmd = app.add_subcommand("shaping", "Traffic Shaping engine");
  shaping_cmd->require_subcommand(1);
  shaping_cmd->footer(kShapingExamples);
  eos::console::IoProto_ShapingProto* shaping_proto = io->mutable_shaping();

  auto* shaping_ls = shaping_cmd->add_subcommand("ls", "View real-time IO rates");
  auto* grp = shaping_ls->add_option_group("Grouping")->require_option(0, 1);
  grp->add_flag("--apps", "Show rates by application");
  grp->add_flag("--users", "Show rates by user");
  grp->add_flag("--groups", "Show rates by group");
  grp->add_flag("--nodes", "Show rates by storage node");
  grp->add_flag("--fs", "Show rates by storage node and filesystem id");
  grp->add_flag(
      "--all", "Show rates by storage node, filesystem id, application, user, and group");
  shaping_ls->add_flag("--json", "JSON output");
  auto* resolve_ids =
      shaping_ls->add_flag("--resolve-ids", "Resolve uid/gid values to names");
  auto* no_resolve_ids =
      shaping_ls->add_flag("--no-resolve-ids", "Keep uid/gid values numeric");
  resolve_ids->excludes(no_resolve_ids);
  shaping_ls->add_flag("--sys", "Include system stats");
  shaping_ls->add_option("--window", "Time window (1|5|15|60|300)")
      ->check(CLI::IsMember({"1", "5", "15", "60", "300"}))
      ->default_val("60");
  shaping_ls->callback([shaping_ls, shaping_proto]() {
    auto* action = shaping_proto->mutable_list();
    bool su = shaping_ls->count("--users") > 0;
    bool sg = shaping_ls->count("--groups") > 0;
    bool sn = shaping_ls->count("--nodes") > 0;
    bool sf = shaping_ls->count("--fs") > 0;
    bool sa = shaping_ls->count("--all") > 0;
    bool json_output = shaping_ls->count("--json") > 0;
    bool resolve_ids = shaping_ls->count("--no-resolve-ids") == 0 &&
                       (shaping_ls->count("--resolve-ids") > 0 || !json_output);
    action->set_show_apps(shaping_ls->count("--apps") > 0 ||
                          (!su && !sg && !sn && !sf && !sa));
    action->set_show_users(su);
    action->set_show_groups(sg);
    action->set_show_nodes(sn);
    action->set_show_fs(sf);
    action->set_show_all(sa);
    action->set_json_output(json_output);
    action->set_resolve_ids(resolve_ids);
    action->set_system_stats(shaping_ls->count("--sys") > 0);
    action->set_time_window_seconds(
        static_cast<uint32_t>(std::stoul(shaping_ls->get_option("--window")->as<std::string>())));
  });

  shaping_cmd->add_subcommand("enable", "Enable traffic shaping")
      ->callback([shaping_proto]() { shaping_proto->mutable_enable(); });
  shaping_cmd->add_subcommand("disable", "Disable traffic shaping")
      ->callback([shaping_proto]() { shaping_proto->mutable_disable(); });

  auto* pressure_cmd =
      shaping_cmd->add_subcommand("pressure", "Inspect reservation IO pressure");
  pressure_cmd->require_subcommand(1);

  auto* pressure_ls =
      pressure_cmd->add_subcommand("ls", "List reserved app pressure by node");
  pressure_ls->add_flag("--apps",
                        "Deprecated no-op; pressure currently lists applications");
  pressure_ls->add_flag("--json", "Output in JSON format");
  pressure_ls->callback([pressure_ls, shaping_proto]() {
    auto* action = shaping_proto->mutable_pressure()->mutable_list();
    action->set_json_output(pressure_ls->count("--json") > 0);
  });

  auto* policy_cmd = shaping_cmd->add_subcommand("policy", "Manage shaping policies");
  policy_cmd->require_subcommand(1);
  policy_cmd->footer(kShapingPolicyExamples);

  auto* policy_ls = policy_cmd->add_subcommand("ls", "List policies");
  policy_ls->add_flag("--apps", "Filter by applications");
  policy_ls->add_flag("--users", "Filter by users (uid)");
  policy_ls->add_flag("--groups", "Filter by groups (gid)");
  policy_ls->add_flag("--controller", "Show ephemeral controller limits");
  policy_ls->add_flag("--json", "Output in JSON format");
  policy_ls->callback([policy_ls, shaping_proto]() {
    auto* a = shaping_proto->mutable_policy()->mutable_list();
    a->set_filter_apps(policy_ls->count("--apps") > 0);
    a->set_filter_users(policy_ls->count("--users") > 0);
    a->set_filter_groups(policy_ls->count("--groups") > 0);
    a->set_show_controller_limits(policy_ls->count("--controller") > 0);
    a->set_json_output(policy_ls->count("--json") > 0);
  });

  auto* policy_set = policy_cmd->add_subcommand("set", "Set policy");
  auto* tgrp = policy_set->add_option_group("Target")->require_option(1);
  tgrp->add_option("--app", "Application name")->type_name("NAME");
  tgrp->add_option("--uid", "User ID")->type_name("ID");
  tgrp->add_option("--gid", "Group ID")->type_name("ID");
  auto* pgrp = policy_set->add_option_group("Params")->require_option(1, 8);
  pgrp->add_option("--limit-read", "Read limit rate; use 0 to remove")->type_name("RATE");
  pgrp->add_option("--limit-write", "Write limit rate; use 0 to remove")
      ->type_name("RATE");
  pgrp->add_option("--reservation-read",
                   "Reserved read rate for applications only; use 0 to remove")
      ->type_name("RATE");
  pgrp->add_option("--reservation-write",
                   "Reserved write rate for applications only; use 0 to remove")
      ->type_name("RATE");
  pgrp->add_option("--controller-limit-read", "Ephemeral controller read limit")
      ->type_name("RATE");
  pgrp->add_option("--controller-limit-write", "Ephemeral controller write limit")
      ->type_name("RATE");
  auto* pe = pgrp->add_flag("--enable", "Enable the selected policy");
  auto* pd = pgrp->add_flag("--disable", "Disable the selected policy");
  pe->excludes(pd);
  policy_set->callback([policy_set, shaping_proto]() {
    auto* a = shaping_proto->mutable_policy()->mutable_set();
    if (policy_set->count("--app"))
      a->set_app(policy_set->get_option("--app")->as<std::string>());
    if (policy_set->count("--uid"))
      a->set_uid(policy_set->get_option("--uid")->as<uint32_t>());
    if (policy_set->count("--gid"))
      a->set_gid(policy_set->get_option("--gid")->as<uint32_t>());
    auto parse_rate = [](const std::string& s, const char* option) -> uint64_t {
      uint64_t n = 0;
      if (!eos::common::StringConversion::GetSizeFromString(s, n)) {
        throw CLI::ValidationError(option, "invalid rate");
      }
      return n;
    };
    if (policy_set->count("--limit-read"))
      a->set_limit_read_bytes_per_sec(parse_rate(
          policy_set->get_option("--limit-read")->as<std::string>(), "--limit-read"));
    if (policy_set->count("--limit-write"))
      a->set_limit_write_bytes_per_sec(parse_rate(
          policy_set->get_option("--limit-write")->as<std::string>(), "--limit-write"));
    if (policy_set->count("--reservation-read"))
      a->set_reservation_read_bytes_per_sec(
          parse_rate(policy_set->get_option("--reservation-read")->as<std::string>(),
                     "--reservation-read"));
    if (policy_set->count("--reservation-write"))
      a->set_reservation_write_bytes_per_sec(
          parse_rate(policy_set->get_option("--reservation-write")->as<std::string>(),
                     "--reservation-write"));
    if (policy_set->count("--controller-limit-read"))
      a->set_controller_limit_read_bytes_per_sec(
          parse_rate(policy_set->get_option("--controller-limit-read")->as<std::string>(),
                     "--controller-limit-read"));
    if (policy_set->count("--controller-limit-write"))
      a->set_controller_limit_write_bytes_per_sec(parse_rate(
          policy_set->get_option("--controller-limit-write")->as<std::string>(),
          "--controller-limit-write"));
    if (policy_set->count("--enable"))
      a->set_is_enabled(true);
    else if (policy_set->count("--disable"))
      a->set_is_enabled(false);
  });

  auto* policy_rm = policy_cmd->add_subcommand("rm", "Remove policy");
  auto* rgrp = policy_rm->add_option_group("Target")->require_option(1);
  rgrp->add_option("--app", "Application name")->type_name("NAME");
  rgrp->add_option("--uid", "User ID")->type_name("ID");
  rgrp->add_option("--gid", "Group ID")->type_name("ID");
  policy_rm->callback([policy_rm, shaping_proto]() {
    auto* a = shaping_proto->mutable_policy()->mutable_remove();
    if (policy_rm->count("--app"))
      a->set_app(policy_rm->get_option("--app")->as<std::string>());
    if (policy_rm->count("--uid"))
      a->set_uid(policy_rm->get_option("--uid")->as<uint32_t>());
    if (policy_rm->count("--gid"))
      a->set_gid(policy_rm->get_option("--gid")->as<uint32_t>());
  });

  auto* config_cmd = shaping_cmd->add_subcommand("config", "Shaping config");
  config_cmd->require_subcommand(1);
  config_cmd->footer(kShapingConfigExamples);
  auto* config_ls = config_cmd->add_subcommand("ls", "List config");
  config_ls->add_flag("--json", "Output in JSON format");
  config_ls->callback([config_ls, shaping_proto]() {
    auto* a = shaping_proto->mutable_config()->mutable_list();
    a->set_json_output(config_ls->count("--json") > 0);
  });
  auto* config_set = config_cmd->add_subcommand("set", "Set config");
  config_set->require_option(1, 14);
  config_set->add_option("--estimators-period", "Estimator update period")
      ->type_name("MS");
  config_set->add_option("--policy-period", "Policy enforcement update period")
      ->type_name("MS");
  config_set->add_option("--report-period", "FST statistics reporting period")
      ->type_name("MS");
  config_set->add_option("--system-window", "System statistics time window")
      ->type_name("SEC");
  config_set->add_option("--detail", "Shaping detail level")
      ->type_name("LEVEL")
      ->check(CLI::IsMember({"aggregate", "fs"}));
  config_set->add_option("--detail-auto", "Automatic shaping detail level toggle")
      ->type_name("STATE")
      ->check(CLI::IsMember({"enabled", "disabled"}));
  config_set
      ->add_option("--detail-auto-low-cardinality",
                   "Node-state stream cardinality for switching back to fs detail")
      ->type_name("COUNT");
  config_set
      ->add_option("--detail-auto-high-cardinality",
                   "Node-state stream cardinality for switching to aggregate detail")
      ->type_name("COUNT");
  config_set->add_option("--limits", "Limit enforcement toggle")
      ->type_name("STATE")
      ->check(CLI::IsMember({"enabled", "disabled"}));
  config_set->add_option("--reservations", "Reservation enforcement toggle")
      ->type_name("STATE")
      ->check(CLI::IsMember({"enabled", "disabled"}));
  config_set->add_option("--controller-min-limit", "Minimum ephemeral controller limit")
      ->type_name("RATE");
  config_set
      ->add_option("--active-node-rate-threshold",
                   "Per-node app rate required to treat reservation pressure as active")
      ->type_name("RATE");
  config_set
      ->add_option("--io-pressure-threshold",
                   "Disk-load threshold for reservation pressure")
      ->type_name("VALUE")
      ->check(CLI::Range(0.0, 1.0));
  config_set->add_option("--max-delay", "Maximum legacy FST delay")
      ->type_name("MS")
      ->check(CLI::Range(100, 30000));
  config_set->add_option("--gc-idle", "Garbage collection idle timeout")
      ->type_name("SEC")
      ->check(CLI::Range(1, 86400));
  config_set->callback([config_set, shaping_proto]() {
    auto* a = shaping_proto->mutable_config()->mutable_set();
    auto parse_rate = [](const std::string& s, const char* option) -> uint64_t {
      uint64_t n = 0;
      if (!eos::common::StringConversion::GetSizeFromString(s, n)) {
        throw CLI::ValidationError(option, "invalid rate");
      }
      return n;
    };
    if (config_set->count("--estimators-period"))
      a->set_update_estimators_thread_period_ms(
          config_set->get_option("--estimators-period")->as<uint32_t>());
    if (config_set->count("--policy-period"))
      a->set_fst_io_policy_update_thread_period_ms(
          config_set->get_option("--policy-period")->as<uint32_t>());
    if (config_set->count("--report-period"))
      a->set_fst_io_stats_reporting_thread_period_ms(
          config_set->get_option("--report-period")->as<uint32_t>());
    if (config_set->count("--system-window"))
      a->set_system_stats_time_window_seconds(
          config_set->get_option("--system-window")->as<uint32_t>());
    if (config_set->count("--detail-auto")) {
      const bool enabled =
          EnabledConfigValue(config_set->get_option("--detail-auto")->as<std::string>());
      a->set_detail_level(enabled ? "auto" : "manual");
    }
    if (config_set->count("--detail")) {
      a->set_detail_level(config_set->get_option("--detail")->as<std::string>());
    }
    if (config_set->count("--detail-auto-low-cardinality") ||
        config_set->count("--detail-auto-high-cardinality")) {
      std::ostringstream encoded;
      encoded << kAutomaticDetailCardinalityPrefix;

      if (config_set->count("--detail-auto-low-cardinality")) {
        encoded
            << config_set->get_option("--detail-auto-low-cardinality")->as<uint64_t>();
      }

      encoded << ":";

      if (config_set->count("--detail-auto-high-cardinality")) {
        encoded
            << config_set->get_option("--detail-auto-high-cardinality")->as<uint64_t>();
      }

      if (config_set->count("--detail-auto")) {
        encoded << ":" << config_set->get_option("--detail-auto")->as<std::string>();
      }

      a->set_detail_level(encoded.str());
    }
    if (config_set->count("--limits")) {
      a->set_limits_enabled(
          EnabledConfigValue(config_set->get_option("--limits")->as<std::string>()));
    }
    if (config_set->count("--reservations")) {
      a->set_reservations_enabled(EnabledConfigValue(
          config_set->get_option("--reservations")->as<std::string>()));
    }
    if (config_set->count("--controller-min-limit")) {
      a->set_controller_min_limit_bytes_per_sec(
          parse_rate(config_set->get_option("--controller-min-limit")->as<std::string>(),
                     "--controller-min-limit"));
    }
    if (config_set->count("--active-node-rate-threshold")) {
      a->set_active_node_rate_threshold_bytes_per_sec(parse_rate(
          config_set->get_option("--active-node-rate-threshold")->as<std::string>(),
          "--active-node-rate-threshold"));
    }
    if (config_set->count("--io-pressure-threshold")) {
      a->set_io_pressure_threshold(
          config_set->get_option("--io-pressure-threshold")->as<double>());
    }
    if (config_set->count("--max-delay")) {
      a->set_max_delay_milliseconds(
          config_set->get_option("--max-delay")->as<uint32_t>());
    }
    if (config_set->count("--gc-idle")) {
      a->set_garbage_collection_idle_seconds(
          config_set->get_option("--gc-idle")->as<uint32_t>());
    }
  });
}

void
PrintIoHelp(CLI::App& app, CLI::AppFormatMode mode, std::FILE* stream = stderr)
{
  if (mode == CLI::AppFormatMode::All) {
    // CLI11's default All formatter expands only one subcommand level.
    if (const auto page = eos::console::BuildCliHelpPage(app, mode)) {
      eos::console::PrintHelpPage(*page, stream);
      return;
    }
  }
  eos::console::PrintCliHelp(app, mode, stream);
}

class IoHelper : public ICmdHelper {
public:
  IoHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~IoHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    std::vector<std::string> args;
    return eos::console::TokenizeArguments(arg, args) && ParseArguments(args);
  }

  bool
  ParseArguments(const std::vector<std::string>& args, std::FILE* helpStream = stdout)
  {
    CLI::App app{"IO Interface", "io"};
    ConfigureIoApp(app, mReq.mutable_io());
    std::vector<std::string> reversed(args.rbegin(), args.rend());
    mParseRetc = EINVAL;
    try {
      app.parse(reversed);
    } catch (const CLI::CallForHelp&) {
      PrintIoHelp(app, CLI::AppFormatMode::Normal, helpStream);
      mParseRetc = 0;
      return false;
    } catch (const CLI::CallForAllHelp&) {
      PrintIoHelp(app, CLI::AppFormatMode::All, helpStream);
      mParseRetc = 0;
      return false;
    } catch (const CLI::ParseError& error) {
      app.exit(error);
      return false;
    }
    return true;
  }

  int
  parseRetc() const
  {
    return mParseRetc;
  }

  int
  ExecuteAndPrint()
  {
    const bool renderTable =
        eos::console::PrepareIoShapingHumanOutput(mReq, mGlobalOpts.mJsonFormat);
    const auto& shaping = mReq.io().shaping();
    const bool renderSuccess =
        !mGlobalOpts.mJsonFormat &&
        (shaping.has_enable() || shaping.has_disable() || shaping.policy().has_set() ||
         shaping.policy().has_remove() || shaping.config().has_set());
    if (!renderTable && !renderSuccess) {
      return Execute();
    }

    const int retc = ExecuteWithoutPrint();
    if (!mIsSilent && !mOutcome.result.empty()) {
      std::optional<std::string> rendered;
      if (renderTable && retc == 0) {
        const auto width = eos::console::HelpWidth(STDOUT_FILENO);
        rendered = eos::console::RenderIoShapingOutput(
            shaping, mOutcome.result, width ? width : 120, STDOUT_FILENO);
      }
      if (rendered) {
        std::cout << *rendered;
      } else if (renderSuccess && retc == 0 &&
                 mOutcome.result.compare(0, 8, "success:") == 0) {
        std::cout << eos::console::Colorize("success:", eos::console::TextStyle::Success,
                                            STDOUT_FILENO)
                  << GetResult().substr(8);
      } else {
        std::cout << GetResult();
      }
    }
    if (!mOutcome.error.empty()) {
      std::cerr << eos::console::Colorize(GetError(), eos::console::TextStyle::Error,
                                          STDERR_FILENO);
    }
    return retc;
  }

private:
  int mParseRetc = EINVAL;
};

class IoProtoCommand : public IConsoleCommand {
public:
  const char*
  name() const override
  {
    return "io";
  }
  const char*
  description() const override
  {
    return "IO Interface";
  }
  std::string
  helpText() const override
  {
    eos::console::IoProto io;
    CLI::App app{"IO Interface", "io"};
    ConfigureIoApp(app, &io);
    if (const auto page = eos::console::BuildCliHelpPage(app, CLI::AppFormatMode::All)) {
      return page->render(80);
    }
    return app.help("", CLI::AppFormatMode::All);
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return requiresMgmArguments(eos::console::TokenizeArguments(args.c_str()));
  }
  bool
  requiresMgmArguments(const std::vector<std::string>& args) const override
  {
    const auto end = std::find(args.begin(), args.end(), "--");
    return !args.empty() && std::none_of(args.begin(), end, [](const auto& arg) {
      return arg == "-h" || arg == "--help" || arg == "--help-all";
    });
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    if (args.empty()) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    IoHelper helper(*ctx.globalOpts);
    if (!helper.ParseArguments(args)) {
      global_retc = helper.parseRetc();
      return 0;
    }
    global_retc = helper.ExecuteAndPrint();
    return 0;
  }
  void
  printHelp() const override
  {
    eos::console::IoProto io;
    CLI::App app{"IO Interface", "io"};
    ConfigureIoApp(app, &io);
    PrintIoHelp(app, CLI::AppFormatMode::All);
  }
  void
  printHelp(const std::vector<std::string>& args) const override
  {
    if (args.empty()) {
      printHelp();
      return;
    }
    auto helpArgs = args;
    helpArgs.emplace_back("--help");
    IoHelper helper(GlobalOptions{});
    helper.ParseArguments(helpArgs, stderr);
  }
};
} // namespace

void
RegisterIoProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<IoProtoCommand>());
}
