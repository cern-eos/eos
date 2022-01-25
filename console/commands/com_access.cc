// ----------------------------------------------------------------------
// File: com_access.cc
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
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* access (deny/bounce/redirect) -  Interface */
int
com_access(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString option = "";
  XrdOucString options = "";
  bool ok = false;
  XrdOucString in = "";
  in = "mgm.cmd=access";
  XrdOucString subcmd = subtokenizer.GetToken();

  if (wants_help(arg1)) {
    goto com_access_usage;
  }

  if (subcmd == "ban") {
    ok = true;
    in += "&mgm.subcmd=ban";
  }
  if (subcmd == "unban") {
    in += "&mgm.subcmd=unban";
    ok = true;
  }
  if (subcmd == "allow") {
    in += "&mgm.subcmd=allow";
    ok = true;
  }
  if (subcmd == "unallow") {
    in += "&mgm.subcmd=unallow";
    ok = true;
  }
  if (subcmd == "ls") {
    in += "&mgm.subcmd=ls";
    ok = true;
  }
  if (subcmd == "set") {
    in += "&mgm.subcmd=set";
    ok = true;
  }
  if (subcmd == "rm") {
    in += "&mgm.subcmd=rm";
    ok = true;
  }

  if (ok) {
    ok = false;
    XrdOucString type = "";
    XrdOucString maybeoption = subtokenizer.GetToken();

    while (maybeoption.beginswith("-")) {
      if ((subcmd == "ls") && (maybeoption != "-m") && (maybeoption != "-n")) {
        goto com_access_usage;
      }

      if ((subcmd != "ls")) {
        goto com_access_usage;
      }

      maybeoption.replace("-", "");
      option += maybeoption;
      maybeoption = subtokenizer.GetToken();
    }
    if (subcmd == "ls") {
      ok = true;
    }

    if ((subcmd == "ban") || (subcmd == "unban") || (subcmd == "allow") ||
        (subcmd == "unallow")) {
      type = maybeoption;
      XrdOucString id = subtokenizer.GetToken();

      if ((!type.length()) || (!id.length())) {
        goto com_access_usage;
      }

      if (type == "host") {
        in += "&mgm.access.host=";
        in += id;
        ok = true;
      }

      if (type == "domain") {
        in += "&mgm.access.domain=";
        in += id;
        ok = true;
      }

      if (type == "user") {
        in += "&mgm.access.user=";
        in += id;
        ok = true;
      }

      if (type == "group") {
        in += "&mgm.access.group=";
        in += id;
        ok = true;
      }
    }

    if ((subcmd == "set") || (subcmd == "rm")) {
      type = maybeoption;
      XrdOucString id = subtokenizer.GetToken();

      if ((subcmd != "rm") && ((!type.length()) || (!id.length()))) {
        goto com_access_usage;
      }

      XrdOucString rtype = subtokenizer.GetToken();

      if (subcmd == "rm") {
        rtype = id;
      }

      if (!id.length()) {
        id = "dummy";
      }

      if (type == "redirect") {
        in += "&mgm.access.redirect=";
        in += id;

        if (rtype.length()) {
          if (rtype == "r") {
            in += "&mgm.access.type=r";
            ok = true;
          } else {
            if (rtype == "w") {
              in += "&mgm.access.type=w";
              ok = true;
            } else {
              if (rtype == "ENONET") {
                in += "&mgm.access.type=ENONET";
                ok = true;
              } else {
                if (rtype == "ENOENT") {
                  in += "&mgm.access.type=ENOENT";
                  ok = true;
                }
              }
            }
          }
        } else {
          ok = true;
        }
      }

      if (type == "stall") {
        in += "&mgm.access.stall=";
        in += id;

        if (rtype.length()) {
          if (rtype == "r") {
            in += "&mgm.access.type=r";
            ok = true;
          } else {
            if (rtype == "w") {
              in += "&mgm.access.type=w";
              ok = true;
            } else {
              if (rtype == "ENONET") {
                in += "&mgm.access.type=ENONET";
                ok = true;
              } else {
                if (rtype == "ENOENT") {
                  in += "&mgm.access.type=ENOENT";
                  ok = true;
                }
              }
            }
          }
        } else {
          ok = true;
        }
      }

      if (type == "limit") {
        in += "&mgm.access.stall=";
        in += id;

        if ((rtype.beginswith("rate:user:")) || (rtype.beginswith("rate:group:"))) {
          if ((rtype.find(":"), 11) != STR_NPOS) {
            in += "&mgm.access.type=";
            in += rtype;
            ok = true;
          }
        }
      }
    }

    if (!ok) {
      goto com_access_usage;
    }
  } else {
    goto com_access_usage;
  }

  if (option.length()) {
    in += "&mgm.access.option=";
    in += option;
  }

  global_retc = output_result(client_command(in, true));
  return (0);
com_access_usage:
  fprintf(stdout,
          "'[eos] access ..' provides the access interface of EOS to allow/disallow hosts/domains and/or users\n");
  fprintf(stdout, "Usage: access ban|unban|allow|unallow|set|rm|ls ...\n\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "access ban user|group|host|domain <identifier> : \n");
  fprintf(stdout,
          "                                                  ban user,group or host,DOMAIN with identifier <identifier>\n");
  fprintf(stdout,
          "                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n");
  fprintf(stdout, "access unban user|group|host|domain <identifier> :\n");
  fprintf(stdout,
          "                                                  unban user,group or host,domain with identifier <identifier>\n");
  fprintf(stdout,
          "                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n");
  fprintf(stdout, "access allow user|group|host|domain <identifier> :\n");
  fprintf(stdout,
          "                                                  allows this user,group or host,domain access\n");
  fprintf(stdout,
          "                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n");
  fprintf(stdout, "access unallow user|group|host|domain <identifier> :\n");
  fprintf(stdout,
          "                                                  unallows this user,group or host,domain access\n");
  fprintf(stdout,
          "                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP or domainname\n");
  fprintf(stdout,
          "HINT:  if you add any 'allow' the instance allows only the listed users.\nA banned identifier will still overrule an allowed identifier!\n\n");
  fprintf(stdout, "access set redirect <target-host> [r|w|ENOENT|ENONET] :\n");
  fprintf(stdout,
          "                                                  allows to set a global redirection to <target-host>\n");
  fprintf(stdout,
          "                                  <target-host> : hostname to which all requests get redirected\n");
  fprintf(stdout,
          "                                                  <taget-hosts> can be structured like <host>:<port[:<delay-in-ms>] where <delay> holds each request for a given time before redirecting\n");
  fprintf(stdout,
          "                                          [r|w] : optional set a redirect for read/write requests seperatly\n");
  fprintf(stdout,
          "                                       [ENONET] : optional set a redirect if a file is offline (ENONET) \n");
  fprintf(stdout,
          "                                       [ENOENT] : optional set a redirect if a file is not existing     \n");

  fprintf(stdout, "access rm redirect [r|w|ENOENT|ENONET]  :\n");
  fprintf(stdout,
          "                                                  removes global redirection\n");
  fprintf(stdout, "access set stall <stall-time> [r|w|ENOENT|ENONET] :\n");
  fprintf(stdout,
          "                                                  allows to set a global stall time\n");
  fprintf(stdout,
          "                                   <stall-time> : time in seconds after which clients should rebounce\n");
  fprintf(stdout,
          "                                          [r|w] : optional set stall time for read/write requests separately\n");
  fprintf(stdout,
          "                                       [ENONET] : optional set a stall if a file is offline (ENONET) \n");
  fprintf(stdout,
          "                                       [ENOENT] : optional set a stall if a file is not existing     \n");
  fprintf(stdout,
          "access set limit <frequency> rate:{user,group}:{name}:<counter>\n");
  fprintf(stdout,
          "       rate:{user:group}:{name}:<counter>       : stall the defined user group for 5s if the <counter> exceeds a frequency of <frequency> in a 5s interval\n");
  fprintf(stdout,
          "                                                  - the instantanious rate can exceed this value by 33%%\n");
  fprintf(stdout,
          "                                                  rate:user:*:<counter> : apply to all users based on user counter\n");
  fprintf(stdout,
          "                                                  rate:group:*:<counter>: apply to all groups based on group counter\n");
  fprintf(stdout,
          "                                                                          set <frequency> to 0 (zero) to continuously stall the user or group\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "access set limit <nfiles> rate:user:{name}:FindFiles\n");
  fprintf(stdout,
          "                                                : set find query limit to <nfiles> for user {name}\n");
  fprintf(stdout, "access set limit <ndirs> rate:user:{name}:FindDirs\n");
  fprintf(stdout,
          "                                                : set find query limit to <ndirs> for user {name}\n");
  fprintf(stdout, "access set limit <nfiles> rate:group:{name}:FindFiles\n");
  fprintf(stdout,
          "                                                : set find query limit to <nfiles> for group {name}\n");
  fprintf(stdout, "access set limit <ndirs> rate:group:{name}:FindDirs\n");
  fprintf(stdout,
          "                                                : set find query limit to <ndirss> for group {name}\n");
  fprintf(stdout, "access set limit <nfiles> rate:user:*:FindFiles\n");
  fprintf(stdout,
          "                                                : set default find query limit to <nfiles> for everybody\n");
  fprintf(stdout, "access set limit <ndirs> rate:user:*:FindDirs\n");
  fprintf(stdout,
          "                                                : set default find query limit to <ndirss> for everybody\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "                                                : rule strength: user-limit >> group-limit >> wildcard-limit\n");
  fprintf(stdout, "access rm  stall [r|w|ENOENT|ENONET]:\n");
  fprintf(stdout,
          "                                                  removes global stall time\n");
  fprintf(stdout,
          "                                          [r|w] : removes stall time for read or write requests\n");
  fprintf(stdout, "       rm limit rate:{user,group}:{name}:<counter\n");
  fprintf(stdout,
          "                                                : remove rate limitation\n");
  fprintf(stdout, "access ls [-m] [-n] :\n");
  fprintf(stdout,
          "                                                  print banned,unbanned user,group, hosts\n");
  fprintf(stdout,
          "                                                                  -m    : output in monitoring format with <key>=<value>\n");
  fprintf(stdout,
          "                                                                  -n    : don't translate uid/gids to names\n");
  fprintf(stdout, "Examples:\n");
  fprintf(stdout, "  access ban host foo      Ban host foo\n");
  fprintf(stdout, "  access ban domain bar    Ban domain bar\n");
  fprintf(stdout, "  access allow domain nobody@bar Allows user nobody from domain bar\n");
  fprintf(stdout, "  access allow domain -    use domain allow as whitelist - e.g. nobody@bar will additionally allow the nobody user from domain bar!");
  fprintf(stdout, "  access allow domain bar  Allow only domain bar\n");
  fprintf(stdout,
          "  access set redirect foo  Redirect all requests to host foo\n");
  fprintf(stdout,
          "  access rm redirect       Remove redirection to previously defined host foo\n");
  fprintf(stdout, "  access set stall 60      Stall all clients by 60 seconds\n");
  fprintf(stdout, "  access ls                Print all defined access rules\n");
  fprintf(stdout,
          "  access set limit 100  rate:user:*:OpenRead      Limit the rate of open for read to a frequency of 100 Hz for all users\n");
  fprintf(stdout,
          "  access set limit 0    rate:user:ab:OpenRead     Limit the open for read rate for the ab user to 0 Hz, to continuously stall it\n");
  fprintf(stdout,
          "  access set limit 2000 rate:group:zp:Stat        Limit the stat rate for the zp group to 2kHz\n");
  fprintf(stdout,
          "  access rm limit rate:user:*:OpenRead            Removes the defined limit\n");
  global_retc = EINVAL;
  return (0);
}
