// ----------------------------------------------------------------------
// File: io-proto-native.cc
// ----------------------------------------------------------------------

#include "common/StringConversion.hh"
#include "console/CommandFramework.hh"
#include "console/ConsoleCompletion.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/helpers/ICmdHelper.hh"
#include <CLI/CLI.hpp>
#include <iomanip>
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
                               "  eos io shaping disable\n";

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
    "  eos io shaping config set --estimators-period 200\n";

std::string MakeIoHelp()
{
  std::ostringstream oss;
  oss << " usage:\n"
      << std::endl
      << "io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x] [--ss] [--sa] [--si] : print io "
         "statistics\n"
      << "\t  -l : show summary information (this is the default if -a,-t,-d,-x is not "
         "selected)\n"
      << "\t  -a : break down by uid/gid\n"
      << "\t  -m : print in <key>=<val> monitoring format\n"
      << "\t  -n : print numerical uid/gids\n"
      << "\t  -t : print top user stats\n"
      << "\t  -d : break down by domains\n"
      << "\t  -x : break down by application\n"
      << "\t  --ss : show table with transfer sample statistics\n"
      << "\t  --sa : start collection of statistics given number of seconds ago\n"
      << "\t  --si : collect statistics over given interval of seconds\n"
      << "\t  Note: this tool shows data for finished transfers only (using storage node "
         "reports)\n"
      << "\t  Example: asking for data of finished transfers which were transferred "
         "during interval [now - 180s, now - 120s]:\n"
      << "\t           eos io stat -x --sa 120 --si 60\n"
      << std::endl
      << "io enable [-r] [-p] [-n] [--udp <address>] : enable collection of io "
         "statistics\n"
      << "\t         no arg. : start the collection thread\n"
      << "\t              -r : enable collection of io reports\n"
      << "\t              -p : enable popularity accounting\n"
      << "\t              -n : enable report namespace\n"
      << "\t --udp <address> : add a UDP message target for io UDP packets (the "
         "configured targets are shown by 'io stat -l')\n"
      << std::endl
      << "io disable [-r] [-p] [-n] [--udp <address>] : disable collection of io "
         "statistics\n"
      << "\t         no arg. : stop the collection thread\n"
      << "\t              -r : disable collection of io reports\n"
      << "\t              -p : disable popularity accounting\n"
      << "\t              -n : disable report namespace\n"
      << "\t --udp <address> : remove a UDP message target for io UDP packets (the "
         "configured targets are shown by 'io stat -l')\n"
      << std::endl
      << "io report <path> : show contents of report namespace for <path>\n"
      << std::endl
      << "io ns [-a] [-m] [-n] [-b] [-100|-1000|-10000] [-w] [-f] : show namespace IO "
         "ranking (popularity)\n"
      << "\t      -a :  don't limit the output list\n"
      << "\t      -m :  print in <key>=<val> monitoring format\n"
      << "\t      -n :  show ranking by number of accesses\n"
      << "\t      -b :  show ranking by number of bytes\n"
      << "\t    -100 :  show the first 100 in the ranking\n"
      << "\t   -1000 :  show the first 1000 in the ranking\n"
      << "\t  -10000 :  show the first 10000 in the ranking\n"
      << "\t      -w :  show history for the last 7 days\n"
      << "\t      -f :  show the 'hotfiles' which are the files with highest number of "
         "present file opens\n"
      << std::endl
      << "io shaping [subcommand] [options...] : interact with the Traffic Shaping "
         "engine\n"
      << std::endl
      << "   SUBCOMMANDS\n"
      << "     ls [options...] : view real-time IO rates and shaping status\n"
      << "\t   --apps   : show rates by application\n"
      << "\t   --users  : show rates by user (uid)\n"
      << "\t   --groups : show rates by group (gid)\n"
      << "\t   --nodes  : show rates by storage node (FST)\n"
      << "\t   --fs     : show rates by storage node and filesystem id\n"
      << "\t   --all    : show rates by storage node, filesystem id, application, "
         "user, and group\n"
      << "\t   --json   : output in JSON format\n"
      << "\t   --resolve-ids    : resolve uid/gid values to names\n"
      << "\t   --no-resolve-ids : keep uid/gid values numeric\n"
      << "\t   --sys    : include meta statistics about Traffic Shaping system\n"
      << "\t   --window <1|5|15|60|300> : time window in seconds for SMA (default 60)\n"
      << std::endl
      << "     enable  : globally enable traffic shaping\n"
      << "     disable : globally disable traffic shaping\n"
      << std::endl
      << "     policy [action] [options...] : manage shaping limits and reservations\n"
      << "\t   action 'ls' : list configured policies\n"
      << "\t     usage: policy ls [options...]\n"
      << "\t       --apps       : filter by applications\n"
      << "\t       --users      : filter by users (uid)\n"
      << "\t       --groups     : filter by groups (gid)\n"
      << "\t       --controller : show ephemeral controller limits\n"
      << "\t       --json       : output in JSON format\n"
      << "\t   action 'set' : configure a new policy or modify an existing one\n"
      << "\t     usage: policy set <identity> [parameters...] [--enable|--disable]\n"
      << "\t       <identity>   : --app <name> | --uid <id> | --gid <id>\n"
      << "\t       [parameters] : --limit-read <rate> | --limit-write <rate> | "
         "--reservation-read <rate> | --reservation-write <rate>\n"
      << "\t                      | --controller-limit-read <rate> | "
         "--controller-limit-write <rate>\n"
      << "\t                      (rate can use suffixes, e.g., 10M, 500K, or 0 to "
         "remove)\n"
      << "\t   action 'rm' : completely remove a configured policy\n"
      << "\t     usage: policy rm <identity>\n"
      << "\t       <identity>   : --app <name> | --uid <id> | --gid <id>\n"
      << std::endl
      << "     config [action] [options...] : manage traffic shaping thread "
         "configurations\n"
      << "\t   action 'ls' : list current thread update periods\n"
      << "\t     usage: config ls [--json]\n"
      << "\t   action 'set' : modify configuration settings such as update periods for "
         "estimators and policy enforcement\n"
      << "\t     usage: config set [--estimators-period <ms>] [--policy-period <ms>] "
         "[--report-period <ms>] [--system-window <s>] [--detail aggregate|fs]\n"
      << std::endl
      << "   EXAMPLES\n"
      << "\t   # Show current application rates\n"
      << "\t   eos io shaping ls --apps\n"
      << std::endl
      << "\t   # Globally enable the traffic shaping engine\n"
      << "\t   eos io shaping enable\n"
      << std::endl
      << "\t   # Globally disable the traffic shaping engine\n"
      << "\t   eos io shaping disable\n"
      << std::endl
      << "\t   # Limit 'eoscp' read rate to 10 MB/s and write rate to 50 MB/s\n"
      << "\t   eos io shaping policy set --app eoscp --limit-read 10M --limit-write 50M\n"
      << std::endl
      << "\t   # Temporarily disable the policy for 'eoscp'\n"
      << "\t   eos io shaping policy set --app eoscp --disable\n"
      << std::endl
      << "\t   # Remove the read limit for 'eoscp' but keep the write limit\n"
      << "\t   eos io shaping policy set --app eoscp --limit-read 0\n"
      << std::endl
      << "\t   # Completely delete the policy for user 1001\n"
      << "\t   eos io shaping policy rm --uid 1001\n"
      << std::endl
      << "\t   # List all configured application policies including machine limits\n"
      << "\t   eos io shaping policy ls --apps --controller\n"
      << std::endl
      << "\t   # Show current thread configurations\n"
      << "\t   eos io shaping config ls\n"
      << std::endl
      << "\t   # Change the estimators update period to 200 ms\n"
      << "\t   eos io shaping config set --estimators-period 200\n"
      << std::endl;
  return oss.str();
}

//------------------------------------------------------------------------------
// Build full CLI11 app with all io subcommands and populate proto
//------------------------------------------------------------------------------
bool
BuildAndParseIoApp(const std::string& input, eos::console::IoProto* io)
{
  CLI::App app{"IO Interface", "io"};
  auto formatter = std::make_shared<CLI::Formatter>();
  formatter->enable_footer_formatting(false);
  app.formatter(formatter);
  app.require_subcommand(1);

  // stat
  auto* stat_cmd = app.add_subcommand("stat", "Print IO statistics");
  stat_cmd->add_flag("-l", "Show summary");
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
  auto* enable_cmd = app.add_subcommand("enable", "Enable IO statistics collection");
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
  auto* disable_cmd = app.add_subcommand("disable", "Disable IO statistics collection");
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
  auto* pgrp = policy_set->add_option_group("Params")->require_option(1, 6);
  pgrp->add_option("--limit-read", "Read limit rate; use 0 to remove")->type_name("RATE");
  pgrp->add_option("--limit-write", "Write limit rate; use 0 to remove")
      ->type_name("RATE");
  pgrp->add_option("--reservation-read", "Reserved read rate; use 0 to remove")
      ->type_name("RATE");
  pgrp->add_option("--reservation-write", "Reserved write rate; use 0 to remove")
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
    auto parse_rate = [](const std::string& s) -> uint64_t {
      uint64_t n = 0;
      eos::common::StringConversion::GetSizeFromString(s, n);
      return n;
    };
    if (policy_set->count("--limit-read"))
      a->set_limit_read_bytes_per_sec(
          parse_rate(policy_set->get_option("--limit-read")->as<std::string>()));
    if (policy_set->count("--limit-write"))
      a->set_limit_write_bytes_per_sec(
          parse_rate(policy_set->get_option("--limit-write")->as<std::string>()));
    if (policy_set->count("--reservation-read"))
      a->set_reservation_read_bytes_per_sec(
          parse_rate(policy_set->get_option("--reservation-read")->as<std::string>()));
    if (policy_set->count("--reservation-write"))
      a->set_reservation_write_bytes_per_sec(
          parse_rate(policy_set->get_option("--reservation-write")->as<std::string>()));
    if (policy_set->count("--controller-limit-read"))
      a->set_controller_limit_read_bytes_per_sec(
          parse_rate(policy_set->get_option("--controller-limit-read")->as<std::string>()));
    if (policy_set->count("--controller-limit-write"))
      a->set_controller_limit_write_bytes_per_sec(
          parse_rate(policy_set->get_option("--controller-limit-write")->as<std::string>()));
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
  config_set->require_option(1, 5);
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
  config_set->callback([config_set, shaping_proto]() {
    auto* a = shaping_proto->mutable_config()->mutable_set();
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
    if (config_set->count("--detail")) {
      a->set_detail_level(config_set->get_option("--detail")->as<std::string>());
    }
  });

  std::string to_parse = "io " + input;
  try {
    app.parse(to_parse, true);
  } catch (const CLI::ParseError& e) {
    app.exit(e);
    return false;
  }
  return true;
}

class IoHelper : public ICmdHelper {
public:
  IoHelper(const GlobalOptions& opts) : ICmdHelper(opts) {}
  ~IoHelper() override = default;
  bool
  ParseCommand(const char* arg) override
  {
    return BuildAndParseIoApp(arg, mReq.mutable_io());
  }
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
    return MakeIoHelp();
  }
  std::vector<std::string>
  complete(const std::vector<std::string>& args) const override
  {
    return eos_help_completion_candidates(name(), helpText(), args);
  }
  bool
  requiresMgm(const std::string& args) const override
  {
    return !wants_help(args.c_str());
  }
  int
  run(const std::vector<std::string>& args, CommandContext& ctx) override
  {
    std::ostringstream oss;
    for (size_t i = 0; i < args.size(); ++i) {
      if (i)
        oss << ' ';
      if (args[i].find(' ') != std::string::npos)
        oss << std::quoted(args[i]);
      else
        oss << args[i];
    }
    std::string joined = oss.str();
    if (wants_help(joined.c_str())) {
      eos::console::IoProto io;
      BuildAndParseIoApp(joined, &io);
      global_retc = EINVAL;
      return 0;
    }
    IoHelper helper(*ctx.globalOpts);
    if (!helper.ParseCommand(joined.c_str())) {
      printHelp();
      global_retc = EINVAL;
      return 0;
    }
    global_retc = helper.Execute();
    return 0;
  }
  void
  printHelp() const override
  {
    fprintf(stderr, "%s", helpText().c_str());
  }
};
} // namespace

void
RegisterIoProtoNativeCommand()
{
  CommandRegistry::instance().reg(std::make_unique<IoProtoCommand>());
}
