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
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

extern int com_io(char*);

void com_io_help();

static bool isCommand(const char* cmd);

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
        } else {
          // Parse <time_ago>
          bool not_numeric = (token.find_first_not_of("0123456789") != std::string::npos);

          if (not_numeric) {
            std::cerr << "error: --sa value needs to be numeric (seconds ago)" << std::endl;
            return false;
          } else {
            try {
              uint64_t tago = std::stoull(token);
              stat->set_time_ago(tago);
            } catch (const std::exception& e) {
              std::cerr << "error: --sa value needs to be numeric (seconds ago)" << std::endl;
              return false;
            }
          }
        }
      } else if (token == "--si") {
        if (!(tokenizer.NextToken(token))) {
          continue;
        } else {
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
        }
      } else if (token == "-l") {
        stat->set_summary(true);
      } else {
        return false;
      }
    }
  } else if (token == "ns") {
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
  } else if (token == "report") {
    if (!tokenizer.NextToken(token)) {
      return false;
    }

    eos::console::IoProto_ReportProto* report = io->mutable_report();
    report->set_path(token);
  } else if (token == "enable" || token == "disable") {
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
  } else if (token == "monitor") {
    eos::console::IoProto_MonitorProto* monitor = io->mutable_monitor();
    std::string subcmd;

    if (!tokenizer.NextToken(subcmd)) {
      return false; // missing subcommand
    }

    // eos io monitor show
    if (subcmd == "show") {
      auto* rate = monitor->mutable_show();
      std::string arg;
      while (tokenizer.NextToken(arg)) {
        if (arg == "--apps") {
          rate->set_apps_only(true);
        } else if (arg == "--users") {
          rate->set_users_only(true);
        } else if (arg == "--groups") {
          rate->set_groups_only(true);
        } else if (arg == "--json") {
          rate->set_json(true);
        } else {
          return false; // Unknown argument for rate subcommand
        }
      }
      return true;
    }
    // ---------------------------------------------------------------------------
    // Subcommand: LIMIT (The complex one)
    // ---------------------------------------------------------------------------
    if (subcmd == "throttle") {
      auto* throttle = monitor->mutable_throttle();
      std::string action;
      if (!tokenizer.NextToken(action)) {
        return false;
      }

      if (action == "show") {
        // get limits
        auto* show = throttle->mutable_show();
        std::string arg;
        while (tokenizer.NextToken(arg)) {
          if (arg == "--apps") {
            show->set_apps_only(true);
          } else if (arg == "--users") {
            show->set_users_only(true);
          } else if (arg == "--groups") {
            show->set_groups_only(true);
          } else if (arg == "--json") {
            show->set_json(true);
          } else if (arg == "--limit") {
            show->set_filter_by(eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_LIMIT);
          } else if (arg == "--reservation") {
            show->set_filter_by(eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_RESERVATION);
          } else {
            return false; // Unknown argument for show action
          }
        }

        // exit if more than one of only_apps, only_uids, only_gids is set
        {
          int count = 0;
          if (show->apps_only()) {
            count++;
          }
          if (show->users_only()) {
            count++;
          }
          if (show->groups_only()) {
            count++;
          }
          if (count > 1) {
            return false; // Only one of --app, --user, --group can be set
          }
        }

        return true;
      }

      if (action == "set") {
        auto* set = throttle->mutable_set();
        std::string key, val;
        uint read_write_count = 0;
        while (tokenizer.NextToken(key)) {
          if (key == "--app") {
            tokenizer.NextToken(val);
            set->set_app(val);
          } else if (key == "--user") {
            tokenizer.NextToken(val);
            set->set_user(std::stoi(val));
          } else if (key == "--group") {
            tokenizer.NextToken(val);
            set->set_group(std::stoi(val));
          } else if (key == "--limit") {
            set->set_type(eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_LIMIT);
          } else if (key == "--reservation") {
            set->set_type(eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_RESERVATION);
          } else if (key == "--read") {
            read_write_count++;
            set->set_is_read(true);
          } else if (key == "--write") {
            read_write_count++;
            set->set_is_read(false);
          } else if (key == "--rate") {
            tokenizer.NextToken(val);
            set->set_rate_megabytes_per_sec(std::stoull(val));
          } else if (key == "--enable") {
            set->set_enable(true);
          } else if (key == "--disable") {
            set->set_enable(false);
          }
        }

        // only exactly one of read / write must be set
        if (read_write_count != 1) {
          return false;
        }

        // only exactly one of app / user / group must be defined
        if ((set->app().empty() && set->user() == 0 && set->group() == 0) ||
            (!set->app().empty() && (!set->user() == 0 || !set->group() == 0)) ||
            (!set->user() == 0 && !set->group() == 0)) {
          return false;
        }

        return true;
      }

      if (action == "remove") {
        auto* remove = throttle->mutable_remove();
        uint read_write_count = 0;
        std::string key, val;
        while (tokenizer.NextToken(key)) {
          if (key == "--app") {
            tokenizer.NextToken(val);
            remove->set_app(val);
          } else if (key == "--user") {
            tokenizer.NextToken(val);
            remove->set_user(std::stoi(val));
          } else if (key == "--group") {
            tokenizer.NextToken(val);
            remove->set_group(std::stoi(val));
          } else if (key == "--limit") {
            remove->set_type(eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_LIMIT);
          } else if (key == "--reservation") {
            remove->set_type(eos::console::IoProto_MonitorProto_ThrottleProto_LimitOrReservation_RESERVATION);
          } else if (key == "--read") {
            remove->set_is_read(true);
            read_write_count++;
          } else if (key == "--write") {
            remove->set_is_read(false);
            read_write_count++;
          }
        }

        // only exactly one of read / write must be set
        if (read_write_count != 1) {
          return false;
        }

        // only exactly one of app / user / group must be defined
        if ((remove->app().empty() && remove->user() == 0 && remove->group() == 0) ||
            (!remove->app().empty() && (!remove->user() == 0 || !remove->group() == 0)) ||
            (!remove->user() == 0 && !remove->group() == 0)) {
          return false;
        }
        return true;
      }
      return false;
    }

    if (subcmd == "window") {
      std::cout << "Parsing window subcommand" << std::endl;
      auto* win = monitor->mutable_window();
      std::string action;
      if (!tokenizer.NextToken(action)) {
        return false;
      }

      if (action == "show") {
        win->set_ls(true);
      } else if (action == "add" || action == "rm") {
        std::string id;
        while (tokenizer.NextToken(id)) {
          // next token must be the window size (number)
          uint64_t val = std::stoull(id);
          if (action == "add") {
            win->add_add(val);
          } else if (action == "rm") {
            win->add_rm(val);
          }
        }
      } else {
        return false;
      }

      return true;
    }

    return false; // Unknown monitor subcommand
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
      << "io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x] [--ss] [--sa] [--si] : print io statistics\n"
      << "\t  -l : show summary information (this is the default if -a,-t,-d,-x is not selected)\n"
      << "\t  -a : break down by uid/gid\n"
      << "\t  -m : print in <key>=<val> monitoring format\n"
      << "\t  -n : print numerical uid/gids\n"
      << "\t  -t : print top user stats\n"
      << "\t  -d : break down by domains\n"
      << "\t  -x : break down by application\n"
      << "\t  --ss : show table with transfer sample statistics\n"
      << "\t  --sa : start collection of statistics given number of seconds ago\n"
      << "\t  --si : collect statistics over given interval of seconds\n"
      << "\t  Note: this tool shows data for finished transfers only (using storage node reports)\n"
      << "\t  Example: asking for data of finished transfers which were transferred during interval [now - 180s, now - "
         "120s]:\n"
      << "\t           eos io stat -x --sa 120 --si 60\n"
      << std::endl
      << "io enable [-r] [-p] [-n] [--udp <address>] : enable collection of io statistics\n"
      << "\t         no arg. : start the colleciton thread\n"
      << "\t              -r : enable collection of io reports\n"
      << "\t              -p : enable popularity accounting\n"
      << "\t              -n : enable report namespace\n"
      << "\t --udp <address> : add a UDP message target for io UDP packtes (the configured targets are shown by 'io "
         "stat -l)\n"
      << std::endl
      << "io disable [-r] [-p] [-n] [--udp <address>] : disable collection of io statistics\n"
      << "\t         no arg. : stop the collection thread\n"
      << "\t              -r : disable collection of io reports\n"
      << "\t              -p : disable popularity accounting\n"
      << "\t              -n : disable report namespace\n"
      << "\t --udp <address> : remove a UDP message target for io UDP packtes (the configured targets are shown by 'io "
         "stat -l)\n"
      << std::endl
      << "io report <path> : show contents of report namespace for <path>\n"
      << std::endl
      << "io ns [-a] [-n] [-b] [-100|-1000|-10000] [-w] [-f] : show namespace IO ranking (popularity)\n"
      << "\t      -a :  don't limit the output list\n"
      << "\t      -n :  show ranking by number of accesses\n"
      << "\t      -b :  show ranking by number of bytes\n"
      << "\t    -100 :  show the first 100 in the ranking\n"
      << "\t   -1000 :  show the first 1000 in the ranking\n"
      << "\t  -10000 :  show the first 10000 in the ranking\n"
      << "\t      -w :  show history for the last 7 days\n"
      << "\t      -f :  show the 'hotfiles' which are the files with highest number of present file opens\n"
      << std::endl
      << "io monitor [subcommand] [options...] : interact with the IO Monitor\n"
      << std::endl
      << "   SUBCOMMANDS\n"
      << "     show [--apps|--users|--groups] [--limit|--reservation] [--json]: show current IO rates\n"
      << "\t   --apps   : show rates by application\n"
      << "\t   --users  : show rates by user (uid)\n"
      << "\t   --groups : show rates by group (gid)\n"
      << "\t   --limit  : restrict output to configured limits\n"
      << "\t   --reservation : restrict output to configured reservations\n"
      << "\t   --json   : output in JSON format\n"
      << std::endl
      << "     throttle [action] [options...] : manage IO limits and reservations\n"
      << "\t   action 'show' : list configured throttles\n"
      << "\t     usage: throttle show [--apps|--users|--groups] [--limit|--reservation] [--json]\n"
      << "\t   action 'set' : configure a new throttle or modify an existing one\n"
      << "\t     usage: throttle set <identity> <direction> --rate <MB/s> [--limit|--reservation] "
         "[--enable|--disable]\n"
      << "\t       <identity>  : --app <name> | --user <uid> | --group <gid>\n"
      << "\t       <direction> : --read | --write\n"
      << "\t   action 'remove' : delete a configured throttle\n"
      << "\t     usage: throttle remove <identity> <direction> [--limit|--reservation]\n"
      << std::endl
      << "     window [action] [options...] : manage time windows for monitoring\n"
      << "\t   action 'show' : list available windows\n"
      << "\t   action 'add' <seconds> [<seconds>...] : add monitoring window(s)\n"
      << "\t   action 'remove'  <seconds> [<seconds>...] : remove monitoring window(s)\n"
      << std::endl
      << "   EXAMPLES\n"
      << "\t   # Show current application rates in JSON\n"
      << "\t   eos io monitor show --apps --json\n"
      << std::endl
      << "\t   # Limit 'eoscp' read rate to 10 MB/s\n"
      << "\t   eos io monitor throttle set --app eoscp --read --rate 10 --limit\n"
      << "\t   # Enable the read limit for 'eoscp'\n"
      << "\t   eos io monitor throttle set --app eoscp --read --enable\n"
      << std::endl
      << "\t   # Remove a write limit for user 1001\n"
      << "\t   eos io monitor throttle remove --user 1001 --write --limit\n"
      << std::endl
      << "\t   # List all configured application limits\n"
      << "\t   eos io monitor throttle show --apps --limit\n"
      << std::endl
      << "\t   # Add a 1-hour (3600s) monitoring window\n"
      << "\t   eos io monitor window add 3600\n"
      << std::endl;

  std::cerr << oss.str() << std::endl;
}
