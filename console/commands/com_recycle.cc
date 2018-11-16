// ----------------------------------------------------------------------
// File: com_recycle.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "common/Path.hh"
#include "common/StringTokenizer.hh"
/*----------------------------------------------------------------------------*/

/* Recycle a file/directory and configure recycling */
int
com_recycle(char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString in = "mgm.cmd=recycle&";
  std::vector<std::string> args;
  std::vector<std::string> options;
  bool monitoring = false;
  bool translateids = false;
  bool globaloption = false;
  XrdOucString subcmd = subtokenizer.GetToken();
  std::string default_route;

  if (wants_help(arg1)) {
    goto com_recycle_usage;
  }

  if ((subcmd != "") &&
      (subcmd != "config") &&
      (subcmd != "ls") &&
      (subcmd != "purge") &&
      (subcmd != "restore") &&
      (!subcmd.beginswith("-"))) {
    goto com_recycle_usage;
  }

  do {
    XrdOucString param = subtokenizer.GetToken();

    if (!param.length()) {
      if (subcmd.beginswith("-")) {
        param = subcmd;
        subcmd = "";
      } else {
        break;
      }
    }

    if (param.beginswith("-")) {
      if (param == "-m") {
        monitoring = true;
      } else {
        if (param == "-n") {
          translateids = true;
        } else {
          if (param == "-g") {
            globaloption = true;
          } else {
            options.push_back(param.c_str());
          }
        }
      }
    } else {
      args.push_back(param.c_str());
    }
  } while (1);

  if ((subcmd == "ls") && options.size()) {
    goto com_recycle_usage;
  }

  if ((subcmd == "ls") && (args.size() && globaloption)) {
    goto com_recycle_usage;
  }

  if ((subcmd == "purge") && (options.size() && !globaloption)) {
    goto com_recycle_usage;
  }

  if ((subcmd == "purge") && (args.size() && globaloption)) {
    goto com_recycle_usage;
  }

  if ((subcmd == "config") && (options.size() > 0) &&
      (options[0] != "--add-bin") &&
      (options[0] != "--remove-bin") &&
      (options[0] != "--lifetime") &&
      (options[0] != "--ratio")) {
    goto com_recycle_usage;
  }

  if ((subcmd == "config") && (options.size() == 1) && (args.size() != 1)) {
    goto com_recycle_usage;
  }

  if ((subcmd == "config") && (options.size() > 1)) {
    goto com_recycle_usage;
  }

  if ((subcmd == "restore")) {
    for (size_t i = 0; i < options.size(); i++) {
      if ((options[i] != "--force-original-name") &&
          (options[i] != "-f") &&
          (options[i] != "--restore-versions") &&
          (options[i] != "-r")) {
        goto com_recycle_usage;
      }
    }
  }

  if ((subcmd == "restore") && (args.size() != 1)) {
    goto com_recycle_usage;
  }

  for (size_t i = 0; i < options.size(); i++) {
    if ((options[i] == "-h") || (options[i] == "--help")) {
      goto com_recycle_usage;
    }
  }

  default_route = DefaultRoute().c_str();
  in += "&mgm.subcmd=";
  in += subcmd;

  if (default_route.length()) {
    in += "&eos.route=";
    in += default_route.c_str();
  }

  if (options.size()) {
    in += "&mgm.option=";
    in += options[0].c_str();
  }

  if (args.size()) {
    in += "&mgm.recycle.arg=";

    if ((options.size()) &&
        ((options[0] == "--add-bin") ||
         (options[0] == "--remove-bin"))) {
      args[0] = abspath(args[0].c_str());
    }

    in += args[0].c_str();
  }

  if (monitoring) {
    in += "&mgm.recycle.format=m";
  }

  if (translateids) {
    in += "&mgm.recycle.printid=n";
  }

  if (globaloption) {
    in += "&mgm.recycle.global=1";
  }

  global_retc = output_result(client_command(in));
  return (0);
com_recycle_usage:
  fprintf(stdout, "Usage: recycle ls|purge|restore|config ...\n");
  fprintf(stdout,
          "'[eos] recycle ..' provides recycle bin functionality to EOS.\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "recycle :\n");
  fprintf(stdout,
          "                                                  print status of recycle bin and if executed by root the recycle bin configuration settings.\n\n");
  fprintf(stdout, "recycle ls [-g|<date>]:\n");
  fprintf(stdout,
          "                                                  list files in the recycle bin\n");
  fprintf(stdout,
          "                                          -g : list files of all users (if root or admin)\n");
  fprintf(stdout,
          "                                                  [date] can be <year>,<year>/<month> or <year>/<month>/<day>\n");
  fprintf(stdout,
          "                                                  e.g.: recycle purge 2018/08/12\n\n");
  fprintf(stdout, "recycle purge [-g|<date>]:\n");
  fprintf(stdout,
          "                                                  purge files in the recycle bin\n");
  fprintf(stdout,
          "                                                 -g : empties the recycle bin of all users\n");
  fprintf(stdout,
          "                                                  [date] can be <year>, <year>/<month>  or <year>/<month>/<day>\n");
  fprintf(stdout,
          "                                                  e.g.: recycle purge 2018/03/05\n");
  fprintf(stdout,
          "                                                  -g cannot be combined with a date restriction\n\n");
  fprintf(stdout,
          "recycle restore [--force-original-name|-f] [--restore-versions|-r] <recycle-key> :\n");
  fprintf(stdout,
          "                                                  undo the deletion identified by <recycle-key>\n");
  fprintf(stdout,
          "       --force-original-name : move's deleted files/dirs back to the original location (otherwise the key entry will have a <.inode> suffix\n");
  fprintf(stdout,
          "       --restore-versions    : restore all previous versions of a file\n\n");
  fprintf(stdout, "recycle config --add-bin <sub-tree>:\n");
  fprintf(stdout,
          "                                                  configures to use the recycle bin for deletions in <sub-tree>\n");
  fprintf(stdout, "recycle config --remove-bin <sub-tree> :\n");
  fprintf(stdout,
          "                                                  disables usage of recycle bin for <sub-tree>\n");
  fprintf(stdout, "recycle config --lifetime <seconds> :\n");
  fprintf(stdout,
          "                                                  configure the FIFO lifetime of the recycle bin\n");
  fprintf(stdout, "recycle config --ratio < 0 .. 1.0 > :\n");
  fprintf(stdout,
          "                                                  configure the volume/inode keep ratio of the recycle bin e.g. 0.8 means files will only be recycled if more than 80%% of the space/inodes quota is used. The low watermark is 10%% under the given ratio by default e.g. it would cleanup volume/inodes to be around 70%%.\n");
  fprintf(stdout,
          "'ls' and 'config' support the '-m' flag to give monitoring format output!\n");
  fprintf(stdout,
          "'ls' supports the '-n' flag to give numeric user/group ids instead of names!\n");
  global_retc = EINVAL;
  return (0);
}
