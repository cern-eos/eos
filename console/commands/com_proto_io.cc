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

#include "common/StringTokenizer.hh"
#include "common/Path.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"

extern int com_io(char*);
void com_io_help();

//------------------------------------------------------------------------------
//! Class IoHelper
//------------------------------------------------------------------------------
class IoHelper : public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param opts global options
  //----------------------------------------------------------------------------
  IoHelper(const GlobalOptions& opts):
    ICmdHelper(opts)
  {}

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
bool IoHelper::ParseCommand(const char* arg)
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
        } else {
          enable->set_upd_address(token);
        }
      } else {
        return false;
      }
    }
  } else { // no proper subcommand
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// io command entry point
//------------------------------------------------------------------------------
int com_protoio(char* arg)
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

  global_retc = io.Execute(false);

  if (global_retc) {
    std::cerr << io.GetError();
  }

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_io_help()
{
  std::ostringstream oss;
  oss
      << " usage:\n"
      << std::endl
      << "io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x] : print io statistics\n"
      << "\t  -l : show summary information (this is the default if -a,-t,-d,-x is not selected)\n"
      << "\t  -a : break down by uid/gid\n"
      << "\t  -m : print in <key>=<val> monitoring format\n"
      << "\t  -n : print numerical uid/gids\n"
      << "\t  -t : print top user stats\n"
      << "\t  -d : break down by domains\n"
      << "\t  -x : break down by application\n"
      << std::endl
      << "io enable [-r] [-p] [-n] [--udp <address>] : enable collection of io statistics\n"
      << "\t              -r : enable collection of io reports\n"
      << "\t              -p : enable popularity accounting\n"
      << "\t              -n : enable report namespace\n"
      << "\t --udp <address> : add a UDP message target for io UDP packtes (the configured targets are shown by 'io stat -l)\n"
      << std::endl
      << "io disable [-r] [-p] [-n] [--udp <address>] : disable collection of io statistics\n"
      << "\t              -r : disable collection of io reports\n"
      << "\t              -p : disable popularity accounting\n"
      << "\t              -n : disable report namespace\n"
      << "\t --udp <address> : remove a UDP message target for io UDP packtes (the configured targets are shown by 'io stat -l)\n"
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
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
