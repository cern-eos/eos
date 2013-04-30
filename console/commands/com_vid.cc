// ----------------------------------------------------------------------
// File: com_vid.cc
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

/* VID System listing, configuration, manipulation */
int
com_vid (char* arg1)
{
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();

  if (subcommand == "ls")
  {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=ls";
    XrdOucString soption = "";
    XrdOucString option = "";
    do
    {
      option = subtokenizer.GetToken();
      if (option.beginswith("-"))
      {
        option.erase(0, 1);
        soption += option;
        if (option.beginswith("h") || option.beginswith("-h"))
          goto com_vid_usage;
      }
    }
    while (option.length());

    if (soption.length())
    {
      in += "&mgm.vid.option=";
      in += soption;
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcommand == "set")
  {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString key = subtokenizer.GetToken();
    if (!key.length())
      goto com_vid_usage;
    
    if (key.beginswith("-h") || key.beginswith("=-h"))
          goto com_vid_usage;
    
    XrdOucString vidkey = "";


    if (key == "geotag")
    {
      XrdOucString match = subtokenizer.GetToken();
      if (!match.length())
      {
        goto com_vid_usage;
      }
      
      if (match.beginswith("-h") || match.beginswith("=-h"))
          goto com_vid_usage;
      
      XrdOucString target = subtokenizer.GetToken();
      if (!target.length())
      {
        goto com_vid_usage;
      }

      vidkey = "geotag:";
      vidkey += match;
      in += "&mgm.vid.cmd=geotag";
      in += "&mgm.vid.key=";
      in += vidkey.c_str();
      in += "&mgm.vid.geotag=";
      in += target.c_str();

      global_retc = output_result(client_admin_command(in));
      return (0);
    }

    if (key == "membership")
    {

      XrdOucString uid = subtokenizer.GetToken();

      if (!uid.length())
        goto com_vid_usage;

      if (uid.beginswith("-h") || uid.beginswith("=-h"))
          goto com_vid_usage;
      
      vidkey += uid;

      XrdOucString type = subtokenizer.GetToken();

      if (!type.length())
        goto com_vid_usage;

      in += "&mgm.vid.cmd=membership";
      in += "&mgm.vid.source.uid=";
      in += uid;

      XrdOucString list = "";
      if ((type == "-uids"))
      {
        vidkey += ":uids";
        list = subtokenizer.GetToken();
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.uid=";
        in += list;
      }

      if ((type == "-gids"))
      {
        vidkey += ":gids";
        list = subtokenizer.GetToken();
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.gid=";
        in += list;
      }

      if ((type == "+sudo"))
      {
        vidkey += ":root";
        list = " "; // fake
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.sudo=true";
      }

      if ((type == "-sudo"))
      {
        vidkey += ":root";
        list = " "; // fake
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.sudo=false";
      }
      if (!list.length())
      {
        goto com_vid_usage;
      }
      global_retc = output_result(client_admin_command(in));
      return (0);
    }

    if (key == "map")
    {
      in += "&mgm.vid.cmd=map";
      XrdOucString type = subtokenizer.GetToken();

      if (!type.length())
        goto com_vid_usage;

      if (type.beginswith("-h") || type.beginswith("=-h"))
          goto com_vid_usage;
      
      bool hastype = false;
      if ((type == "-krb5"))
      {
        in += "&mgm.vid.auth=krb5";
        hastype = true;
      }
      if ((type == "-gsi"))
      {
        in += "&mgm.vid.auth=gsi";
        hastype = true;
      }
      if ((type == "-sss"))
      {
        in += "&mgm.vid.auth=sss";
        hastype = true;
      }
      if ((type == "-unix"))
      {
        in += "&mgm.vid.auth=unix";
        hastype = true;
      }
      if ((type == "-tident"))
      {
        in += "&mgm.vid.auth=tident";
        hastype = true;
      }
      if ((type == "-voms"))
      {
        in += "&mgm.vid.auth=voms";
        hastype = true;
      }

      if (!hastype)
        goto com_vid_usage;


      XrdOucString pattern = subtokenizer.GetToken();
      // deal with patterns containing spaces but inside ""
      if (pattern.beginswith("\""))
      {
        if (!pattern.endswith("\""))
          do
          {
            XrdOucString morepattern = subtokenizer.GetToken();

            if (morepattern.endswith("\""))
            {
              pattern += " ";
              pattern += morepattern;
              break;
            }
            if (!morepattern.length())
            {
              goto com_vid_usage;
            }
            pattern += " ";
            pattern += morepattern;
          }
          while (1);
      }
      if (!pattern.length())
        goto com_vid_usage;

      in += "&mgm.vid.pattern=";
      in += pattern;

      XrdOucString vid = subtokenizer.GetToken();
      if (!vid.length())
        goto com_vid_usage;

      if (vid.beginswith("vuid:"))
      {
        vid.replace("vuid:", "");
        in += "&mgm.vid.uid=";
        in += vid;

        XrdOucString vid = subtokenizer.GetToken();
        if (vid.length())
        {
          fprintf(stderr, "Got %s\n", vid.c_str());
          if (vid.beginswith("vgid:"))
          {
            vid.replace("vgid:", "");
            in += "&mgm.vid.gid=";
            in += vid;
          }
          else
          {
            goto com_vid_usage;
          }
        }
      }
      else
      {
        if (vid.beginswith("vgid:"))
        {
          vid.replace("vgid:", "");
          in += "&mgm.vid.gid=";
          in += vid;
        }
        else
        {
          goto com_vid_usage;
        }
      }

      in += "&mgm.vid.key=";
      in += "<key>";

      global_retc = output_result(client_admin_command(in));
      return (0);
    }
  }

  if ((subcommand == "enable") || (subcommand == "disable"))
  {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
    XrdOucString disableu = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString disableg = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString type = subtokenizer.GetToken();
    if (!type.length())
      goto com_vid_usage;
    
    if (type.beginswith("-h") || type.beginswith("--h"))
      goto com_vid_usage;
    
    bool hastype = false;
    if ((type == "krb5"))
    {
      in += "&mgm.vid.auth=krb5";
      disableu += "krb5:\"<pwd>\":uid";
      disableg += "krb5:\"<pwd>\":gid";
      hastype = true;
    }
    if ((type == "sss"))
    {
      in += "&mgm.vid.auth=sss";
      disableu += "sss:\"<pwd>\":uid";
      disableg += "sss:\"<pwd>\":guid";
      hastype = true;
    }
    if ((type == "gsi"))
    {
      in += "&mgm.vid.auth=gsi";
      disableu += "gsi:\"<pwd>\":uid";
      disableg += "gsi:\"<pwd>\":gid";
      hastype = true;
    }
    if ((type == "unix"))
    {
      in += "&mgm.vid.auth=unix";
      disableu += "unix\"<pwd>\":uid";
      disableg += "unix\"<pwd>\":gid";
      hastype = true;
    }
    if ((type == "tident"))
    {
      in += "&mgm.vid.auth=tident";
      disableu += "tident\"<pwd>\":uid";
      disableg += "tident\"<pwd>\":gid";
      hastype = true;
    }
    if (!hastype)
      goto com_vid_usage;

    in += "&mgm.vid.pattern=<pwd>";
    if (type != "unix")
    {
      in += "&mgm.vid.uid=0";
      in += "&mgm.vid.gid=0";
    }
    else
    {
      in += "&mgm.vid.uid=99";
      in += "&mgm.vid.gid=99";
    }

    in += "&mgm.vid.key=";
    in += "<key>";

    if ((subcommand == "enable"))
      global_retc = output_result(client_admin_command(in));

    if ((subcommand == "disable"))
    {
      global_retc = output_result(client_admin_command(disableu));
      global_retc |= output_result(client_admin_command(disableg));
    }
    return (0);
  }

  if ((subcommand == "add") || (subcommand == "remove"))
  {
    XrdOucString gw = subtokenizer.GetToken();
    if (gw != "gateway")
      goto com_vid_usage;
    XrdOucString host = subtokenizer.GetToken();
    if (!host.length())
      goto com_vid_usage;
    XrdOucString protocol = subtokenizer.GetToken();
    if (protocol.length() && ((protocol != "sss") && (protocol != "gsi") && (protocol != "krb5") && (protocol != "unix")))
      goto com_vid_usage;
    if (!protocol.length())
      protocol = "*";

    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
    XrdOucString disableu = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString disableg = "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";

    in += "&mgm.vid.auth=tident";
    in += "&mgm.vid.pattern=\"";
    in += protocol;
    in += "@";
    in += host;
    in += "\"";
    in += "&mgm.vid.uid=0";
    in += "&mgm.vid.gid=0";
    disableu += "tident:\"";
    disableu += protocol;
    disableu += "@";
    disableu += host;
    disableu += "\":uid";
    disableg += "tident:\"";
    disableg += protocol;
    disableg += "@";
    disableg += host;
    disableg += "\":gid";

    in += "&mgm.vid.key=";
    in += "<key>";

    if ((subcommand == "add"))
      global_retc = output_result(client_admin_command(in));

    if ((subcommand == "remove"))
    {
      global_retc = output_result(client_admin_command(disableu));
      global_retc |= output_result(client_admin_command(disableg));
    }
    return (0);
  }

  if (subcommand == "rm")
  {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=rm";
    XrdOucString key = subtokenizer.GetToken();
    if (key == "membership")
    {
      key = subtokenizer.GetToken();
      key.insert("vid:", 0);
      XrdOucString key1 = key;
      XrdOucString key2 = key;
      XrdOucString in1 = in;
      XrdOucString in2 = in;
      key1 += ":uids";
      key2 += ":gids";
      in1 += "&mgm.vid.key=";
      in1 += key1;
      in2 += "&mgm.vid.key=";
      in2 += key2;

      global_retc = output_result(client_admin_command(in1));
      global_retc |= output_result(client_admin_command(in2));
      return (0);
    }

    if ((!key.length()))
      goto com_vid_usage;
    
    if (key.beginswith("-h") || key.beginswith("--h"))
      goto com_vid_usage;
    
    in += "&mgm.vid.key=";
    in += key;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

com_vid_usage:
  fprintf(stdout, "usage: vid ls [-u] [-g] [-s] [-U] [-G] [-g] [-a] [-l] [-n]                                          : list configured policies\n");
  fprintf(stdout, "                                        -u : show only user role mappings\n");
  fprintf(stdout, "                                        -g : show only group role mappings\n");
  fprintf(stdout, "                                        -s : show list of sudoers\n");
  fprintf(stdout, "                                        -U : show user  alias mapping\n");
  fprintf(stdout, "                                        -G : show group alias mapping\n");
  fprintf(stdout, "                                        -y : show configured gateways\n");
  fprintf(stdout, "                                        -a : show authentication\n");
  fprintf(stdout, "                                        -l : show geo location mapping\n");
  fprintf(stdout, "                                        -n : show numerical ids instead of user/group names\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       vid set membership <uid> -uids [<uid1>,<uid2>,...]\n");
  fprintf(stdout, "       vid set membership <uid> -gids [<gid1>,<gid2>,...]\n");
  fprintf(stdout, "       vid rm membership <uid>             : delete the membership entries for <uid>.\n");
  fprintf(stdout, "       vid set membership <uid> [+|-]sudo \n");
  fprintf(stdout, "       vid set map -krb5|-gsi|-sss|-unix|-tident|-voms <pattern> [vuid:<uid>] [vgid:<gid>] \n");
  fprintf(stdout, "\n");
  fprintf(stdout, "                                                                                                      -voms <pattern>  : <pattern> is <group>:<role> e.g. to map VOMS attribute /dteam/cern/Role=NULL/Capability=NULL one should define <pattern>=/dteam/cern: \n");
  fprintf(stdout, "       vid set geotag <IP-prefix> <geotag>  : add to all IP's matching the prefix <prefix> the geo location tag <geotag>\n");
  fprintf(stdout, "                                              N.B. specify the default assumption via 'vid set geotag default <default-tag>'\n");
  fprintf(stdout, "       vid rm <key>                         : remove configured vid with name key - hint: use config dump to see the key names of vid rules\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       vid enable|disable krb5|gsi|sss|unix\n");
  fprintf(stdout, "                                            : enable/disables the default mapping via password database\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       vid add|remove gateway <hostname> [krb5|gsi|sss|unix\n");
  fprintf(stdout, "                                            : adds/removes a host as a (fuse) gateway with 'su' priviledges\n");
  fprintf(stdout, "                                              [<prot>] restricts the gateway sudo to the specified authentication method\n");

  return (0);
}
