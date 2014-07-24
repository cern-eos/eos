// ----------------------------------------------------------------------
// File: com_config.cc
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
/*----------------------------------------------------------------------------*/

/* Configuration System listing, configuration, manipulation */
int
com_config (char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();

  if (wants_help(arg1))
    goto com_config_usage;

  if (subcommand == "dump")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=dump";
    if (arg.length())
    {
      do
      {
        if ((arg == "--fs") || (arg == "-f"))
        {
          in += "&mgm.config.fs=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--vid") || (arg == "-v"))
        {
          in += "&mgm.config.vid=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--quota") || (arg == "-q"))
        {
          in += "&mgm.config.quota=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--comment") || (arg == "-c"))
        {
          in += "&mgm.config.comment=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--policy") || (arg == "-p"))
        {
          in += "&mgm.config.policy=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--global") || (arg == "-g"))
        {
          in += "&mgm.config.global=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--map") || (arg == "-m"))
        {
          in += "&mgm.config.map=1";
          arg = subtokenizer.GetToken();
        }
        else
          if ((arg == "--access") || (arg == "-a"))
        {
          in += "mgm.config.access=1";
          arg = subtokenizer.GetToken();
        }
        else
          if (!arg.beginswith("-"))
        {
          in += "&mgm.config.file=";
          in += arg;
          arg = subtokenizer.GetToken();
        }
        else
        {
          goto com_config_usage;
        }
      }
      while (arg.length());
    }

    global_retc = output_result(client_admin_command(in));
    return (0);
  }



  if (subcommand == "ls")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=ls";
    if ((arg == "--backup") || (arg == "-b"))
    {
      in += "&mgm.config.showbackup=1";
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcommand == "load")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=load&mgm.config.file=";
    if (!arg.length())
      goto com_config_usage;

    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcommand == "autosave")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=autosave&mgm.config.state=";
    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcommand == "reset")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=reset";
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcommand == "save")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=save";
    bool hasfile = false;
    bool match = false;
    do
    {
      match = false;
      if (arg == "-f")
      {
        in += "&mgm.config.force=1";
        arg = subtokenizer.GetToken();
        match = true;
      }

      if ((arg == "--comment") || (arg == "-c"))
      {
        in += "&mgm.config.comment=";
        arg = subtokenizer.GetToken();
        if ((arg.beginswith("\"") || (arg.beginswith("'"))))
        {
          arg.replace("'", "\"");
          if (arg.length())
          {
            do
            {
              in += " ";
              in += arg;
              arg = subtokenizer.GetToken();
            }
            while (arg.length() && (!arg.endswith("\"")) && (!arg.endswith("'")));

            if (arg.endswith("\"") || arg.endswith("'"))
            {
              in += " ";
              arg.replace("'", "\"");
              in += arg;
            }
            arg = subtokenizer.GetToken();
          }
        }
        match = true;
      }

      if (!arg.beginswith("-"))
      {
        in += "&mgm.config.file=";
        in += arg;
        hasfile = true;
        arg = subtokenizer.GetToken();
        match = true;
      }
      if (!match)
        arg = subtokenizer.GetToken();

    }
    while (arg.length() && match);

    if (!match) goto com_config_usage;
    if (!hasfile) goto com_config_usage;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcommand == "diff")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=diff";
    arg = subtokenizer.GetToken();
    if (arg.length())
      goto com_config_usage;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if (subcommand == "changelog")
  {
    XrdOucString in = "mgm.cmd=config&mgm.subcmd=changelog";
    if (arg.length())
    {
      if (arg.beginswith("-"))
      {
        // allow -100 and 100 
        arg.erase(0, 1);
      }
      in += "&mgm.config.lines=";
      in += arg;
    }

    arg = subtokenizer.GetToken();
    if (arg.length())
      goto com_config_usage;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

com_config_usage:
  fprintf(stdout, "Usage: config ls|dump|load|save|diff|changelog|reset|autosave [OPTIONS]\n");
  fprintf(stdout, "'[eos] config' provides the configuration interface to EOS.\n\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "config ls   [--backup|-b] :\n");
  fprintf(stdout, "                                                  list existing configurations\n");
  fprintf(stdout, "            --backup|-b : show also backup & autosave files\n");

  fprintf(stdout, "config dump [--fs|-f] [--vid|-v] [--quota|-q] [--policy|-p] [--comment|-c] [--global|-g] [--access|-a] [<name>] [--map|-m]] : \n");
  fprintf(stdout, "                                                  dump current configuration or configuration with name <name>\n");
  fprintf(stdout, "            -f : dump only file system config\n");
  fprintf(stdout, "            -v : dump only virtual id config\n");
  fprintf(stdout, "            -q : dump only quota config\n");
  fprintf(stdout, "            -p : dump only policy config\n");
  fprintf(stdout, "            -g : dump only global config\n");
  fprintf(stdout, "            -a : dump only access config\n");
  fprintf(stdout, "            -m : dump only mapping config\n");

  fprintf(stdout, "config save [-f] [<name>] [--comment|-c \"<comment>\"] ] :\n");
  fprintf(stdout, "                                                  save config (optionally under name)\n");
  fprintf(stdout, "            -f : overwrite existing config name and create a timestamped backup\n");
  fprintf(stdout, "=>   if no name is specified the current config file is overwritten\n\n");
  fprintf(stdout, "config load <name> :\n");
  fprintf(stdout, "                                                  load config (optionally with name)\n");
  fprintf(stdout, "config diff :\n");
  fprintf(stdout, "                                                  show changes since last load/save operation\n");
  fprintf(stdout, "config changelog [-#lines] :\n");
  fprintf(stdout, "                                                  show the last <#> lines from the changelog - default is -10 \n");
  fprintf(stdout, "config reset :\n");
  fprintf(stdout, "                                                  reset all configuration to empty state\n");
  fprintf(stdout, "config autosave [on|off] :\n");
  fprintf(stdout, "                                                  without on/off just prints the state otherwise set's autosave to on or off\n");

  return (0);
}
