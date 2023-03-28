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
#include "common/StringTokenizer.hh"
#include "common/ParseUtils.hh"
/*----------------------------------------------------------------------------*/

/* VID System listing, configuration, manipulation */
int
com_vid(char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetTokenUnquoted();

  if (wants_help(arg1)) {
    goto com_vid_usage;
  }

  if (subcommand == "ls") {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=ls";
    XrdOucString soption = "";
    XrdOucString option = "";

    do {
      option = subtokenizer.GetTokenUnquoted();

      if (option.beginswith("-")) {
        option.erase(0, 1);
        soption += option;

        if (option.beginswith("h") || option.beginswith("-h")) {
          goto com_vid_usage;
        }
      }
    } while (option.length());

    if (soption.length()) {
      in += "&mgm.vid.option=";
      in += soption;
    }

    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "set") {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString key = subtokenizer.GetTokenUnquoted();

    if (!key.length()) {
      goto com_vid_usage;
    }

    if (key.beginswith("-h") || key.beginswith("=-h")) {
      goto com_vid_usage;
    }

    XrdOucString vidkey = "";

    if (key == "geotag") {
      XrdOucString match = subtokenizer.GetTokenUnquoted();

      if (!match.length()) {
        goto com_vid_usage;
      }

      if (match.beginswith("-h") || match.beginswith("=-h")) {
        goto com_vid_usage;
      }

      XrdOucString target = subtokenizer.GetTokenUnquoted();

      if (!target.length()) {
        goto com_vid_usage;
      }

      // Check if geotag is valid
      std::string geotag = eos::common::SanitizeGeoTag(target.c_str());

      if (geotag != target.c_str()) {
        fprintf(stderr, "%s\n", geotag.c_str());
        return 0;
      }

      vidkey = "geotag:";
      vidkey += match;
      in += "&mgm.vid.cmd=geotag";
      in += "&mgm.vid.key=";
      in += vidkey.c_str();
      in += "&mgm.vid.geotag=";
      in += target.c_str();
      global_retc = output_result(client_command(in, true));
      return (0);
    }

    if (key == "membership") {
      XrdOucString uid = subtokenizer.GetTokenUnquoted();

      if (!uid.length()) {
        goto com_vid_usage;
      }

      if (uid.beginswith("-h") || uid.beginswith("=-h")) {
        goto com_vid_usage;
      }

      vidkey += uid;
      XrdOucString type = subtokenizer.GetTokenUnquoted();

      if (!type.length()) {
        goto com_vid_usage;
      }

      in += "&mgm.vid.cmd=membership";
      in += "&mgm.vid.source.uid=";
      in += uid;
      XrdOucString list = "";

      if ((type == "-uids")) {
        vidkey += ":uids";
        list = subtokenizer.GetTokenUnquoted();
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.uid=";
        in += list;
      }

      if ((type == "-gids")) {
        vidkey += ":gids";
        list = subtokenizer.GetTokenUnquoted();
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.gid=";
        in += list;
      }

      if ((type == "+sudo")) {
        vidkey += ":root";
        list = " "; // fake
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.sudo=true";
      }

      if ((type == "-sudo")) {
        vidkey += ":root";
        list = " "; // fake
        in += "&mgm.vid.key=";
        in += vidkey;
        in += "&mgm.vid.target.sudo=false";
      }

      if (!list.length()) {
        goto com_vid_usage;
      }

      global_retc = output_result(client_command(in, true));
      return (0);
    }

    if (key == "map") {
      in += "&mgm.vid.cmd=map";
      XrdOucString type = subtokenizer.GetTokenUnquoted();

      if (!type.length()) {
        goto com_vid_usage;
      }

      if ((type.beginswith("-h") || type.beginswith("=-h")) && (type != "-https")) {
        goto com_vid_usage;
      }

      bool hastype = false;

      if ((type == "-krb5")) {
        in += "&mgm.vid.auth=krb5";
        hastype = true;
      }

      if ((type == "-gsi")) {
        in += "&mgm.vid.auth=gsi";
        hastype = true;
      }

      if ((type == "-https")) {
        in += "&mgm.vid.auth=https";
        hastype = true;
      }

      if ((type == "-sss")) {
        in += "&mgm.vid.auth=sss";
        hastype = true;
      }

      if ((type == "-unix")) {
        in += "&mgm.vid.auth=unix";
        hastype = true;
      }

      if ((type == "-tident")) {
        in += "&mgm.vid.auth=tident";
        hastype = true;
      }

      if ((type == "-voms")) {
        in += "&mgm.vid.auth=voms";
        hastype = true;
      }

      if ((type == "-grpc")) {
        in += "&mgm.vid.auth=grpc";
        hastype = true;
      }

      if ((type == "-oauth2")) {
        in += "&mgm.vid.auth=oauth2";
        hastype = true;
      }

      if (!hastype) {
        goto com_vid_usage;
      }

      XrdOucString pattern = subtokenizer.GetTokenUnquoted();

      // deal with patterns containing spaces but inside ""
      if (pattern.beginswith("\"")) {
        if (!pattern.endswith("\""))
          do {
            XrdOucString morepattern = subtokenizer.GetTokenUnquoted();

            if (morepattern.endswith("\"")) {
              pattern += " ";
              pattern += morepattern;
              break;
            }

            if (!morepattern.length()) {
              goto com_vid_usage;
            }

            pattern += " ";
            pattern += morepattern;
          } while (1);
      }

      if (!pattern.length()) {
        goto com_vid_usage;
      }

      in += "&mgm.vid.pattern=";
      in += pattern;
      XrdOucString vid = subtokenizer.GetTokenUnquoted();

      if (!vid.length()) {
        goto com_vid_usage;
      }

      if (vid.beginswith("vuid:")) {
        vid.replace("vuid:", "");
        in += "&mgm.vid.uid=";
        in += vid;
        XrdOucString vid = subtokenizer.GetTokenUnquoted();

        if (vid.length()) {
          if (vid.beginswith("vgid:")) {
            vid.replace("vgid:", "");
            in += "&mgm.vid.gid=";
            in += vid;
          } else {
            goto com_vid_usage;
          }
        }
      } else {
        if (vid.beginswith("vgid:")) {
          vid.replace("vgid:", "");
          in += "&mgm.vid.gid=";
          in += vid;
        } else {
          goto com_vid_usage;
        }
      }

      in += "&mgm.vid.key=";
      in += "<key>";
      global_retc = output_result(client_command(in, true));
      return (0);
    }
  }

  if ((subcommand == "enable") || (subcommand == "disable")) {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
    XrdOucString disableu =
      "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString disableg =
      "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString type = subtokenizer.GetTokenUnquoted();

    if (!type.length()) {
      goto com_vid_usage;
    }

    if (type.beginswith("-h") || type.beginswith("--h")) {
      goto com_vid_usage;
    }

    bool hastype = false;

    if ((type == "krb5")) {
      in += "&mgm.vid.auth=krb5";
      disableu += "krb5:\"<pwd>\":uid";
      disableg += "krb5:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "sss")) {
      in += "&mgm.vid.auth=sss";
      disableu += "sss:\"<pwd>\":uid";
      disableg += "sss:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "gsi")) {
      in += "&mgm.vid.auth=gsi";
      disableu += "gsi:\"<pwd>\":uid";
      disableg += "gsi:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "https")) {
      in += "&mgm.vid.auth=https";
      disableu += "https:\"<pwd>\":uid";
      disableg += "https:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "unix")) {
      in += "&mgm.vid.auth=unix";
      disableu += "unix:\"<pwd>\":uid";
      disableg += "unix:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "grpc")) {
      in += "&mgm.vid.auth=grpc";
      disableu += "grpc:\"<pwd>\":uid";
      disableg += "grpc:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "oauth2")) {
      in += "&mgm.vid.auth=oauth2";
      disableu += "oauth2:\"<pwd>\":uid";
      disableg += "oauth2:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "tident")) {
      in += "&mgm.vid.auth=tident";
      disableu += "tident:\"<pwd>\":uid";
      disableg += "tident:\"<pwd>\":gid";
      hastype = true;
    }

    if ((type == "ztn")) {
      in += "&mgm.vid.auth=ztn";
      disableu += "ztn:\"<pwd>\":uid";
      disableg += "ztn:\"<pwd>\":gid";
      hastype = true;
    }

    if (!hastype) {
      goto com_vid_usage;
    }

    in += "&mgm.vid.pattern=<pwd>";

    if (type != "unix") {
      in += "&mgm.vid.uid=0";
      in += "&mgm.vid.gid=0";
    } else {
      in += "&mgm.vid.uid=99";
      in += "&mgm.vid.gid=99";
    }

    in += "&mgm.vid.key=";
    in += "<key>";

    if ((subcommand == "enable")) {
      global_retc = output_result(client_command(in, true));
    }

    if ((subcommand == "disable")) {
      global_retc = output_result(client_command(disableu, true));
      global_retc |= output_result(client_command(disableg, true));
    }

    return (0);
  }

  if (subcommand == "publicaccesslevel") {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString vidkey = "";
    XrdOucString level = subtokenizer.GetTokenUnquoted();

    if (!level.length()) {
      goto com_vid_usage;
    }

    if (level.beginswith("-h") || level.beginswith("=-h")) {
      goto com_vid_usage;
    }

    vidkey = "publicaccesslevel";
    in += "&mgm.vid.cmd=publicaccesslevel";
    in += "&mgm.vid.key=";
    in += vidkey.c_str();
    in += "&mgm.vid.level=";
    in += level.c_str();
    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if (subcommand == "tokensudo") {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString vidkey = "";
    XrdOucString level = subtokenizer.GetTokenUnquoted();

    if (!level.length()) {
      goto com_vid_usage;
    }

    if (level.beginswith("-h") || level.beginswith("=-h")) {
      goto com_vid_usage;
    }

    vidkey = "tokensudo";
    in += "&mgm.vid.cmd=tokensudo";
    in += "&mgm.vid.key=";
    in += vidkey.c_str();
    in += "&mgm.vid.tokensudo=";
    in += level.c_str();
    global_retc = output_result(client_command(in, true));
    return (0);
  }

  if ((subcommand == "add") || (subcommand == "remove")) {
    XrdOucString gw = subtokenizer.GetTokenUnquoted();

    if (gw != "gateway") {
      goto com_vid_usage;
    }

    XrdOucString host = subtokenizer.GetTokenUnquoted();

    if (!host.length()) {
      goto com_vid_usage;
    }

    XrdOucString protocol = subtokenizer.GetTokenUnquoted();

    if (protocol.length() &&
        ((protocol != "sss") && (protocol != "gsi") &&
         (protocol != "krb5") && (protocol != "unix") && (protocol != "https") &&
         (protocol != "grpc") && (protocol != "outh2"))) {
      goto com_vid_usage;
    }

    if (!protocol.length()) {
      protocol = "*";
    }

    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=map";
    XrdOucString disableu =
      "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
    XrdOucString disableg =
      "mgm.cmd=vid&mgm.subcmd=rm&mgm.vid.cmd=unmap&mgm.vid.key=";
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

    if ((subcommand == "add")) {
      global_retc = output_result(client_command(in, true));
    }

    if ((subcommand == "remove")) {
      global_retc = output_result(client_command(disableu, true));
      global_retc |= output_result(client_command(disableg, true));
    }

    return (0);
  }

  if (subcommand == "rm") {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=rm";
    XrdOucString key = subtokenizer.GetTokenUnquoted();

    if (key == "membership") {
      key = subtokenizer.GetTokenUnquoted();
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
      global_retc = output_result(client_command(in1, true));
      global_retc |= output_result(client_command(in2, true));
      return (0);
    }

    if ((!key.length())) {
      goto com_vid_usage;
    }

    if (key.beginswith("-h") || key.beginswith("--h")) {
      goto com_vid_usage;
    }

    in += "&mgm.vid.key=";
    in += key;
    global_retc = output_result(client_command(in, true));
    return (0);
  }

com_vid_usage:
  fprintf(stdout,
          "usage: vid ls [-u] [-g] [-s] [-U] [-G] [-g] [-a] [-l] [-n] : list configured policies\n");
  fprintf(stdout,
          "                                        -u : show only user role mappings\n");
  fprintf(stdout,
          "                                        -g : show only group role mappings\n");
  fprintf(stdout,
          "                                        -s : show list of sudoers\n");
  fprintf(stdout,
          "                                        -U : show user  alias mapping\n");
  fprintf(stdout,
          "                                        -G : show group alias mapping\n");
  fprintf(stdout,
          "                                        -y : show configured gateways\n");
  fprintf(stdout,
          "                                        -a : show authentication\n");
  fprintf(stdout,
          "                                        -N : show maximum anonymous (nobody) access level deepness - the tree deepness where unauthenticated access is possible (default is 1024)\n");
  fprintf(stdout,
          "                                        -l : show geo location mapping\n");
  fprintf(stdout,
          "                                        -n : show numerical ids instead of user/group names\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       vid set membership <uid> -uids [<uid1>,<uid2>,...]\n");
  fprintf(stdout, "       vid set membership <uid> -gids [<gid1>,<gid2>,...]\n");
  fprintf(stdout,
          "       vid rm membership <uid>             : delete the membership entries for <uid>.\n");
  fprintf(stdout, "       vid set membership <uid> [+|-]sudo \n");
  fprintf(stdout,
          "       vid set map -krb5|-gsi|-https|-sss|-unix|-tident|-voms|-grpc|-oauth2 <pattern> [vuid:<uid>] [vgid:<gid>] \n");
  fprintf(stdout,
          "           -voms <pattern>  : <pattern> is <group>:<role> e.g. to map VOMS attribute /dteam/cern/Role=NULL/Capability=NULL one should define <pattern>=/dteam/cern: \n");
  fprintf(stdout,
          "           -sss key:<key>   : <key> has to be defined on client side via 'export XrdSecsssENDORSEMENT=<key>'\n");
  fprintf(stdout,
          "           -grpc key:<key>  : <key> has to be added to the relevant GRPC request in the field 'authkey'\n");
  fprintf(stdout,
          "           -https key:<key> : <key> has to be added to the relevant HTTP(S) request as a header 'x-gateway-authorization'\n");
  fprintf(stdout,
          "           -oauth2 key:<oauth-resource> : <oauth-resource> describes the OAUTH resource endpoint to translate OAUTH tokens to user identities\n\n");
  fprintf(stdout,
          "       vid set geotag <IP-prefix> <geotag>  : add to all IP's matching the prefix <prefix> the geo location tag <geotag>\n");
  fprintf(stdout,
          "                                              N.B. specify the default assumption via 'vid set geotag default <default-tag>'\n");
  fprintf(stdout,
          "       vid rm <key>                         : remove configured vid with name key - hint: use config dump to see the key names of vid rules\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       vid enable|disable krb5|gsi|sss|unix|https|grpc|oauth2|ztn\n");
  fprintf(stdout,
          "                                            : enable/disables the default mapping via password or external database\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       vid add|remove gateway <hostname> [krb5|gsi|sss|unix|https|grpc]\n");
  fprintf(stdout,
          "                                            : adds/removes a host as a (fuse) gateway with 'su' priviledges\n");
  fprintf(stdout,
          "                                              [<prot>] restricts the gateway role change to the specified authentication method\n");
  fprintf(stdout,
          "       vid publicaccesslevel <level>\n");
  fprintf(stdout,
          "                                           : sets the deepest directory level where anonymous access (nobody) is possible\n");
  fprintf(stdout,
          "       vid tokensudo 0|1|2|3\n");
  fprintf(stdout,
          "                                           : configure sudo policy when tokens are used\n");
  fprintf(stdout,
          "                                             0 : always allow token sudo (setting uid/gid from token) [default if not set]\n");
  fprintf(stdout,
          "                                             1 : allow token sudo if transport is encrypted\n");
  fprintf(stdout,
          "                                             2 : allow token sudo for strong authentication (not unix!)\n");
  fprintf(stdout,
          "                                             3 : never allow token sudo\n");
  global_retc = EINVAL;
  return (0);
}
