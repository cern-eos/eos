//------------------------------------------------------------------------------
// File: com_config.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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

#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"

//------------------------------------------------------------------------------
// Configuration System listing, configuration, manipulation
//------------------------------------------------------------------------------
int
com_config(char* arg1)
{
  // Split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();

  if (wants_help(arg1)) {
    goto com_config_usage;
  }

  if (subcommand == "dump") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=dump";

    if (arg.length()) {
      do {
        if ((arg == "--comments") || (arg == "-c")) {
          in += "&mgm.config.comment=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--fs") || (arg == "-f")) {
          in += "&mgm.config.fs=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--global") || (arg == "-g")) {
          in += "&mgm.config.global=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--map") || (arg == "-m")) {
          in += "&mgm.config.map=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--route") || (arg == "-r")) {
          in += "&mgm.config.route=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--policy") || (arg == "-p")) {
          in += "&mgm.config.policy=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--quota") || (arg == "-q")) {
          in += "&mgm.config.quota=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--geosched") || (arg == "-s")) {
          in += "&mgm.config.geosched=1";
          arg = subtokenizer.GetToken();
        } else if ((arg == "--vid") || (arg == "-v")) {
          in += "&mgm.config.vid=1";
          arg = subtokenizer.GetToken();
        } else if (!arg.beginswith("-")) {
          in += "&mgm.config.file=";
          in += arg;
          arg = subtokenizer.GetToken();
        } else {
          goto com_config_usage;
        }
      } while (arg.length());
    }

    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "ls") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=ls";

    if ((arg == "--backup") || (arg == "-b")) {
      in += "&mgm.config.showbackup=1";
    }

    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "load") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=load&mgm.config.file=";

    if (!arg.length()) {
      goto com_config_usage;
    }

    in += arg;
    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "export") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=export";

    if (!arg.length()) {
      goto com_config_usage;
    }

    bool hasfile = false;
    bool match = false;

    do {
      match = false;

      if (arg == "-f") {
        in += "&mgm.config.force=1";
        arg = subtokenizer.GetToken();
        match = true;
      }

      if (!arg.beginswith("-")) {
        in += "&mgm.config.file=";
        in += arg;
        hasfile = true;
        arg = subtokenizer.GetToken();
        match = true;
      }

      if (!match) {
        arg = subtokenizer.GetToken();
      }
    } while (arg.length() && match);

    if (!match || !hasfile) {
      goto com_config_usage;
    }

    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "reset") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=reset";
    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "save") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=save";
    bool hascomment = false;
    bool hasfile = false;
    bool match;

    do {
      match = false;

      if (arg == "-f") {
        in += "&mgm.config.force=1";
        arg = subtokenizer.GetToken();
        match = true;
      } else if (arg == "-c") {
        std::string comment;
        XrdOucString line = arg1;
        int pos = line.find("-c");
        line.replace("-c", "--comment", pos, pos + 1);
        std::string cmd = parse_comment(line.c_str(), comment);

        if (comment.length()) {
          in += "&mgm.config.comment=";
          in += comment.c_str();
          hascomment = true;
        }

        // Skip flag and comment text
        subtokenizer.GetToken();
        arg = subtokenizer.GetToken();
        match = true;
      } else if (!arg.beginswith("-")) {
        in += "&mgm.config.file=";
        in += arg;
        hasfile = true;
        arg = subtokenizer.GetToken();
        match = true;
      }

      if (!match) {
        arg = subtokenizer.GetToken();
      }
    } while (arg.length() && match);

    if (!match || !hasfile) {
      goto com_config_usage;
    }

    // Capture global comment if -c flag not set
    if (!hascomment && global_comment.length()) {
      in += "&mgm.config.comment=";
      in += global_comment.c_str();
    }

    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "changelog") {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=changelog";

    if (arg.length()) {
      if (arg.beginswith("-")) {
        // allow -100 and 100
        arg.erase(0, 1);
      }

      in += "&mgm.config.lines=";
      in += arg;
    }

    arg = subtokenizer.GetToken();

    if (arg.length()) {
      goto com_config_usage;
    }

    global_retc = output_result(client_command(in, true));
    return (0);
  }

com_config_usage:
  std::ostringstream oss;
  oss << "Usage: config changelog|dump|export|load|ls|reset|save [OPTIONS]"
      << std::endl
      << "'[eos] config' provides the configuration interface to EOS." << std::endl
      << std::endl
      << "Subcommands:" << std::endl
      << "config changelog [#lines]" << std::endl
      << "       show the last <#> lines from the changelog - default is 10" <<
      std::endl
      << std::endl
      << "config dump [-cfgpqmsv] [<name>]" << std::endl
      << "       dump configuration with name <name> or current one by default" <<
      std::endl
      << "       -c|--comments : " << "dump only comment config" << std::endl
      << "       -f|--fs       : " << "dump only file system config" << std::endl
      << "       -g|--global   : " << "dump only global config" << std::endl
      << "       -p|--policy   : " << "dump only policy config" << std::endl
      << "       -q|--quota    : " << "dump only quota config" << std::endl
      << "       -m|--map      : " << "dump only mapping config" << std::endl
      << "       -r|--route    : " << "dump only routing config" << std::endl
      << "       -s|--geosched : " << "dump only geosched config" << std::endl
      << "       -v|--vid      : " << "dump only virtual id config" << std::endl
      << std::endl
      << "config export [-f] [<name>]" << std::endl
      << "       export a configuration stored on file to QuarkDB - you need to specify the full path"
      << std::endl
      << "       -f : " <<
      "overwrite existing config name and create a timestamped backup"
      << std::endl
      << std::endl
      << "config load <name>"  << std::endl
      << "       load config (optionally with name)" << std::endl
      << std::endl
      << "config ls [-b|--backup]" << std::endl
      << "       list existing configurations" << std::endl
      << "       --backup|-b : " << "show also backup & autosave files" << std::endl
      << std::endl
      << "config reset" << std::endl
      << "       reset all configuration to empty state" << std::endl
      << std::endl
      << "config save [-f] [<name>] [-c|--comment \"<comment>\"]" << std::endl
      << "       save config (optionally under name)" << std::endl
      << "       -f : " <<
      "overwrite existing config name and create a timestamped backup" << std::endl
      << "            " <<
      "If no name is specified the current config file is overwritten."
      << std::endl
      << "       -c : " << "add a comment entry to the config" << std::endl
      << "            " << "Extended option will also add the entry to the logbook."
      << std::endl;
  fprintf(stdout, "%s", oss.str().c_str());
  global_retc = EINVAL;
  return (0);
}
