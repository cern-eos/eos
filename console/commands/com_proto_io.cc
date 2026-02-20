//------------------------------------------------------------------------------
// @file: com_proto_io.cc
// @author: Fabio Luchetti - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your token) any later version.                                   *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"
#include "proto/Io.pb.h"
#include <common/CLI11.hpp>

extern int com_io(char*);

void com_io_help();

//------------------------------------------------------------------------------
//! Class IoHelper
//------------------------------------------------------------------------------
class IoHelper : public ICmdHelper {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  IoHelper(const GlobalOptions& opts)
      : ICmdHelper(opts)
  {
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~IoHelper() override = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg) override;

private:
  bool ParseMonitorCommand(eos::common::StringTokenizer& tokenizer,
                           eos::console::IoProto* io);
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
IoHelper::ParseCommand(const char* arg)
{
  eos::console::IoProto* io = mReq.mutable_io();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  std::string token;

  if (!tokenizer.NextToken(token)) {
    return false;
  }

  // one of { stat, ns, report, enable, disable }
  if (token == "stat") {
    eos::console::IoProto_StatProto* stat = io->mutable_stat();

    while (tokenizer.NextToken(token)) {
      if (token == "-a") {
        stat->set_details(true);
      } else if (token == "-m") {
        stat->set_monitoring(true);
      } else if (token == "-n") {
        stat->set_numerical(true);
      } else if (token == "-t") {
        stat->set_top(true);
      } else if (token == "-d") {
        stat->set_domain(true);
      } else if (token == "-x") {
        stat->set_apps(true);
      } else if (token == "--ss") {
        stat->set_sample_stat(true);
      } else if (token == "--sa") {
        if (!(tokenizer.NextToken(token))) {
          continue;
        }
          // Parse <time_ago>
          bool not_numeric = (token.find_first_not_of("0123456789") != std::string::npos);

          if (not_numeric) {
            std::cerr << "error: --sa value needs to be numeric (seconds ago)" << std::endl;
            return false;
          }
            try {
              uint64_t tago = std::stoull(token);
              stat->set_time_ago(tago);
            } catch (const std::exception& e) {
              std::cerr << "error: --sa value needs to be numeric (seconds ago)" << std::endl;
              return false;
            }
      } else if (token == "--si") {
        if (!(tokenizer.NextToken(token))) {
          continue;
        }
          // Parse <time_interval>
          bool not_numeric = (token.find_first_not_of("0123456789") != std::string::npos);

          if (not_numeric) {
            std::cerr << "error: --si value needs to be numeric (interval in seconds)" << std::endl;
            return false;
          } else {
            try {
              uint64_t tint = std::stoull(token);
              stat->set_time_interval(tint);
            } catch (const std::exception& e) {
              std::cerr << "error: --si value needs to be numeric (interval in seconds)" << std::endl;
              return false;
            }
          }

      } else if (token == "-l") {
        stat->set_summary(true);
      } else {
        return false;
      }
    }
    return true;
  }

  if (token == "ns") {
    eos::console::IoProto_NsProto* ns = io->mutable_ns();

    while (tokenizer.NextToken(token)) {
      if (token == "-m") {
        ns->set_monitoring(true);
      } else if (token == "-b") {
        ns->set_rank_by_byte(true);
      } else if (token == "-n") {
        ns->set_rank_by_access(true);
      } else if (token == "-w") {
        ns->set_last_week(true);
      } else if (token == "-f") {
        ns->set_hotfiles(true);
        /* (token == "-100" || token == "-1000" || token == "-10000" || token == "-a" ) */
      } else if (token == "-100") {
        ns->set_count(eos::console::IoProto_NsProto::ONEHUNDRED);
      } else if (token == "-1000") {
        ns->set_count(eos::console::IoProto_NsProto::ONETHOUSAND);
      } else if (token == "-10000") {
        ns->set_count(eos::console::IoProto_NsProto::TENTHOUSAND);
      } else if (token == "-a") {
        ns->set_count(eos::console::IoProto_NsProto::ALL);
      } else {
        return false;
      }
    }
    return true;
  }

  if (token == "report") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::IoProto_ReportProto* report = io->mutable_report();
    report->set_path(token);
    return true;
  }

  if (token == "enable" || token == "disable") {
    eos::console::IoProto_EnableProto* enable = io->mutable_enable();
    enable->set_switchx(token == "enable");

    while (tokenizer.NextToken(token)) {
      if (token == "-r") {
        enable->set_reports(true);
      } else if (token == "-p") {
        enable->set_popularity(true);
      } else if (token == "-n") {
        enable->set_namespacex(true);
      } else if (token == "--udp") {
        if (!(tokenizer.NextToken(token)) || (token.find('-') == 0)) {
          return false;
        }
        enable->set_upd_address(token);

      } else {
        return false;
      }
    }
    return true;
  }

  if (token == "monitor") {
    return ParseMonitorCommand(tokenizer, io);
  }

  return false; // Unknown command
}

//------------------------------------------------------------------------------
// io command entry point
//------------------------------------------------------------------------------
int
com_protoio(char* arg)
{
  if (wants_help(arg)) {
    com_io_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  IoHelper io(gGlobalOpts);

  if (!io.ParseCommand(arg)) {
    com_io_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = io.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void
com_io_help()
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
         "during interval [now - 180s, now - "
         "120s]:\n"
      << "\t           eos io stat -x --sa 120 --si 60\n"
      << std::endl
      << "io enable [-r] [-p] [-n] [--udp <address>] : enable collection of io "
         "statistics\n"
      << "\t         no arg. : start the colleciton thread\n"
      << "\t              -r : enable collection of io reports\n"
      << "\t              -p : enable popularity accounting\n"
      << "\t              -n : enable report namespace\n"
      << "\t --udp <address> : add a UDP message target for io UDP packtes (the "
         "configured targets are shown by 'io "
         "stat -l)\n"
      << std::endl
      << "io disable [-r] [-p] [-n] [--udp <address>] : disable collection of io "
         "statistics\n"
      << "\t         no arg. : stop the collection thread\n"
      << "\t              -r : disable collection of io reports\n"
      << "\t              -p : disable popularity accounting\n"
      << "\t              -n : disable report namespace\n"
      << "\t --udp <address> : remove a UDP message target for io UDP packtes (the "
         "configured targets are shown by 'io "
         "stat -l)\n"
      << std::endl
      << "io report <path> : show contents of report namespace for <path>\n"
      << std::endl
      << "io ns [-a] [-n] [-b] [-100|-1000|-10000] [-w] [-f] : show namespace IO ranking "
         "(popularity)\n"
      << "\t      -a :  don't limit the output list\n"
      << "\t      -n :  show ranking by number of accesses\n"
      << "\t      -b :  show ranking by number of bytes\n"
      << "\t    -100 :  show the first 100 in the ranking\n"
      << "\t   -1000 :  show the first 1000 in the ranking\n"
      << "\t  -10000 :  show the first 10000 in the ranking\n"
      << "\t      -w :  show history for the last 7 days\n"
      << "\t      -f :  show the 'hotfiles' which are the files with highest number of "
         "present file opens\n"
      << std::endl
      << "io monitor [subcommand] [options...] : interact with the IO Monitor\n"
      << std::endl
      << "   SUBCOMMANDS\n"
      << "     traffic [options...] : view real-time IO rates\n"
      << "\t   --apps   : show rates by application\n"
      << "\t   --users  : show rates by user (uid)\n"
      << "\t   --groups : show rates by group (gid)\n"
      << std::endl
      << "     policy [action] [options...] : manage shaping limits and reservations\n"
      << "\t   action 'list' : list configured policies\n"
      << "\t     usage: policy list [--apps|--users|--groups]\n"
      << "\t   action 'set' : configure a new policy or modify an existing one\n"
      << "\t     usage: policy set <identity> [parameters...] [--enable|--disable]\n"
      << "\t       <identity>   : --app <name> | --uid <id> | --gid <id>\n"
      << "\t       [parameters] : --limit-read <rate> | --limit-write <rate> | "
         "--reservation-read <rate> | --reservation-write <rate>\n"
      << "\t                      (rate can use suffixes, e.g., 10M, 500K, or 0 to "
         "remove)\n"
      << "\t   action 'delete' : completely remove a configured policy\n"
      << "\t     usage: policy delete <identity>\n"
      << "\t       <identity>   : --app <name> | --uid <id> | --gid <id>\n"
      << std::endl
      << "   EXAMPLES\n"
      << "\t   # Show current application rates\n"
      << "\t   eos io monitor traffic --apps\n"
      << std::endl
      << "\t   # Limit 'eoscp' read rate to 10 MB/s and write rate to 50 MB/s\n"
      << "\t   eos io monitor policy set --app eoscp --limit-read 10M --limit-write 50M\n"
      << std::endl
      << "\t   # Temporarily disable the policy for 'eoscp'\n"
      << "\t   eos io monitor policy set --app eoscp --disable\n"
      << std::endl
      << "\t   # Remove the read limit for 'eoscp' but keep the write limit\n"
      << "\t   eos io monitor policy set --app eoscp --limit-read 0\n"
      << std::endl
      << "\t   # Completely delete the policy for user 1001\n"
      << "\t   eos io monitor policy delete --uid 1001\n"
      << std::endl
      << "\t   # List all configured application policies\n"
      << "\t   eos io monitor policy list --apps\n"
      << std::endl;

  std::cerr << oss.str() << std::endl;
}

void
SetupTrafficCommand(CLI::App& app, eos::console::IoProto_MonitorProto* proto)
{
  auto* cmd = app.add_subcommand("traffic", "View real-time IO rates");

  auto* grp = cmd->add_option_group("Grouping")->require_option(1);
  grp->add_flag("--apps", "Show rates by application");
  grp->add_flag("--users", "Show rates by user (uid)");
  grp->add_flag("--groups", "Show rates by group (gid)");

  cmd->callback([cmd, proto]() {
    // Called ONLY if 'traffic' is matched. This sets the oneof case!
    auto* action = proto->mutable_traffic();
    action->set_show_apps(cmd->count("--apps") > 0);
    action->set_show_users(cmd->count("--users") > 0);
    action->set_show_groups(cmd->count("--groups") > 0);
  });
}

void
SetupPolicyListCommand(CLI::App* policy_cmd, eos::console::IoProto_MonitorProto* proto)
{
  auto* cmd = policy_cmd->add_subcommand("list", "Show configured policies");

  cmd->add_flag("--apps", "Show application policies");
  cmd->add_flag("--users", "Show user policies");
  cmd->add_flag("--groups", "Show group policies");

  cmd->callback([cmd, proto]() {
    // Called ONLY if 'policy list' is matched
    auto* action = proto->mutable_policy()->mutable_list();
    action->set_filter_apps(cmd->count("--apps") > 0);
    action->set_filter_users(cmd->count("--users") > 0);
    action->set_filter_groups(cmd->count("--groups") > 0);
  });
}

void
SetupPolicySetCommand(CLI::App* policy_cmd, eos::console::IoProto_MonitorProto* proto)
{
  auto* cmd = policy_cmd->add_subcommand("set", "Create or update a shaping policy");

  auto* target_grp = cmd->add_option_group("Target Identity")->require_option(1);
  target_grp->add_option("--app", "Application name");
  target_grp->add_option("--uid", "User ID");
  target_grp->add_option("--gid", "Group ID");

  auto* param_grp = cmd->add_option_group("Policy Parameters")->require_option(1, 6);
  param_grp->add_option("--limit-read", "Max read rate (e.g. 10M, 0 to remove)");
  param_grp->add_option("--limit-write", "Max write rate");
  param_grp->add_option("--reservation-read", "Guaranteed read rate");
  param_grp->add_option("--reservation-write", "Guaranteed write rate");

  auto* opt_enable = param_grp->add_flag("--enable", "Enable the policy");
  auto* opt_disable = param_grp->add_flag("--disable", "Disable the policy");
  opt_enable->excludes(opt_disable);

  cmd->callback([cmd, proto]() {
    // Called ONLY if 'policy set' is matched
    auto* action = proto->mutable_policy()->mutable_set();

    if (cmd->count("--app")) {
      action->set_app(cmd->get_option("--app")->as<std::string>());
    }
    if (cmd->count("--uid")) {
      action->set_uid(cmd->get_option("--uid")->as<uint32_t>());
    }
    if (cmd->count("--gid")) {
      action->set_gid(cmd->get_option("--gid")->as<uint32_t>());
    }

    auto parse_rate = [](const std::string& input) -> uint64_t {
      uint64_t size = 0;
      eos::common::StringConversion::GetSizeFromString(input, size);
      return size;
    };

    if (cmd->count("--limit-read")) {
      action->set_limit_read_bytes_per_sec(
          parse_rate(cmd->get_option("--limit-read")->as<std::string>()));
    }
    if (cmd->count("--limit-write")) {
      action->set_limit_write_bytes_per_sec(
          parse_rate(cmd->get_option("--limit-write")->as<std::string>()));
    }
    if (cmd->count("--reservation-read")) {
      action->set_reservation_read_bytes_per_sec(
          parse_rate(cmd->get_option("--reservation-read")->as<std::string>()));
    }
    if (cmd->count("--reservation-write")) {
      action->set_reservation_write_bytes_per_sec(
          parse_rate(cmd->get_option("--reservation-write")->as<std::string>()));
    }

    if (cmd->count("--enable") > 0) {
      action->set_is_enabled(true);
    } else if (cmd->count("--disable") > 0) {
      action->set_is_enabled(false);
    }
  });
}

void
SetupPolicyDeleteCommand(CLI::App* policy_cmd, eos::console::IoProto_MonitorProto* proto)
{
  auto* cmd = policy_cmd->add_subcommand("delete", "Delete a configured policy");

  auto* target_grp = cmd->add_option_group("Target Identity")->require_option(1);
  target_grp->add_option("--app", "Application name");
  target_grp->add_option("--uid", "User ID");
  target_grp->add_option("--gid", "Group ID");

  cmd->callback([cmd, proto]() {
    // Called ONLY if 'policy delete' is matched
    auto* action = proto->mutable_policy()->mutable_delete_();

    if (cmd->count("--app")) {
      action->set_app(cmd->get_option("--app")->as<std::string>());
    }
    if (cmd->count("--uid")) {
      action->set_uid(cmd->get_option("--uid")->as<uint32_t>());
    }
    if (cmd->count("--gid")) {
      action->set_gid(cmd->get_option("--gid")->as<uint32_t>());
    }
  });
}

bool
IoHelper::ParseMonitorCommand(eos::common::StringTokenizer& tokenizer,
                              eos::console::IoProto* io)
{
  std::string full_cmd = "eos-io-monitor"; // Dummy argv[0] for CLI11
  std::string arg_token;
  while (tokenizer.NextToken(arg_token)) {
    full_cmd += " ";
    full_cmd += arg_token;
  }

  CLI::App monitor_app{"Interact with the IO Monitor"};
  monitor_app.require_subcommand(1);

  eos::console::IoProto_MonitorProto* monitor_proto = io->mutable_monitor();

  SetupTrafficCommand(monitor_app, monitor_proto);

  auto* policy_cmd =
      monitor_app.add_subcommand("policy", "Manage shaping limits and reservations");
  policy_cmd->require_subcommand(1);

  SetupPolicyListCommand(policy_cmd, monitor_proto);
  SetupPolicySetCommand(policy_cmd, monitor_proto);
  SetupPolicyDeleteCommand(policy_cmd, monitor_proto);

  try {
    monitor_app.parse(full_cmd, true);
  } catch (const CLI::ParseError& e) {
    monitor_app.exit(e);
    return false;
  }

  return true;
}
