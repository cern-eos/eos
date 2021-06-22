// ----------------------------------------------------------------------
// File: com_fusex.cc
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
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/

/* fusex Clients -  Interface */
int
com_fusex(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString option = "";
  XrdOucString options = "";
  XrdOucString in = "";
  XrdOucString subcmd = subtokenizer.GetToken();
  XrdOucString filter;

  if (wants_help(arg1)) {
    goto com_fusex_usage;
  }

  in = "mgm.cmd=fusex";

  if (subcmd == "ls") {
    in += "&mgm.subcmd=ls";
  } else if (subcmd == "evict") {
    XrdOucString uuid = subtokenizer.GetToken();
    XrdOucString reason = subtokenizer.GetToken();

    if (!uuid.length()) {
      goto com_fusex_usage;
    }

    in += "&mgm.subcmd=evict";
    in += "&mgm.fusex.uuid=";
    in += uuid;

    if (reason.length()) {
      XrdOucString b64;
      eos::common::SymKey::Base64(reason, b64);
      in += "&mgm.fusex.reason=";
      in += b64;
    }
  } else if (subcmd == "caps") {
    option = subtokenizer.GetToken();
    filter = subtokenizer.GetToken();

    while (option.replace("-", "")) {
    };

    in += "&mgm.subcmd=caps";

    in += "&mgm.option=";

    in += option;

    while (auto val = subtokenizer.GetToken()) {
      filter += " ";
      filter += val;
    }

    if (filter.length()) {
      in += "&mgm.filter=";
      in += eos::common::StringConversion::curl_escaped(filter.c_str()).c_str();
    }
  } else if (subcmd == "dropcaps") {
    XrdOucString uuid = subtokenizer.GetToken();

    if (!uuid.length()) {
      goto com_fusex_usage;
    }

    in += "&mgm.subcmd=dropcaps";
    in += "&mgm.fusex.uuid=";
    in += uuid;
  } else if (subcmd == "droplocks") {
    XrdOucString inode = subtokenizer.GetToken();
    XrdOucString pid = subtokenizer.GetToken();

    if (!inode.length() || !pid.length()) {
      goto com_fusex_usage;
    }

    in += "&mgm.subcmd=droplocks";
    in += "&mgm.inode=";
    in += inode;
    in += "&mgm.fusex.pid=";
    in += pid;
  } else if (subcmd == "conf") {
    XrdOucString interval = subtokenizer.GetToken();
    XrdOucString quota_interval = subtokenizer.GetToken();
    XrdOucString bc_audience = subtokenizer.GetToken();
    XrdOucString bc_audience_match = subtokenizer.GetToken();
    int i_interval = interval.length() ? atoi(interval.c_str()) : 0;
    int q_interval = quota_interval.length() ? atoi(quota_interval.c_str()) : 0;

    if ((i_interval < 0) ||
        (i_interval > 60)) {
      goto com_fusex_usage;
    }

    if ((q_interval < 0) ||
        (q_interval > 120)) {
      goto com_fusex_usage;
    }

    in += "&mgm.subcmd=conf";
    in += "&mgm.fusex.hb=";
    in += interval;

    if (quota_interval.length()) {
      in += "&mgm.fusex.qc=";
      in += quota_interval;
    }

    if (bc_audience.length()) {
      in += "&mgm.fusex.bc.max=";
      in += bc_audience;
    }

    if (bc_audience_match.length()) {
      in += "&mgm.fusex.bc.match=";
      in += bc_audience_match;
    }
  } else {
    goto com_fusex_usage;
  }

  do {
    option = subtokenizer.GetToken();

    if (!option.length()) {
      break;
    }

    if (option == "-a") {
      options += "a";
    } else {
      if (option == "-m") {
        options += "m";
      } else {
        if (option == "-s") {
          options += "s";
        } else {
          if (option == "-f") {
            options += "f";
          } else {
            if (option == "-l") {
              options += "l";
            } else {
              if (option == "-k") {
                options += "k";
              } else {
                goto com_fusex_usage;
              }
            }
          }
        }
      }
    }
  } while (true);

  if (options.length()) {
    in += "&mgm.option=";
    in += options;
  }

  global_retc = output_result(client_command(in, true));
  return (0);
com_fusex_usage:
  fprintf(stdout,
          "usage: fusex ls [-l] [-f] [-m]                     :  print statistics about eosxd fuse clients\n");
  fprintf(stdout,
          "                [no option]                                          -  break down by client host [default]\n");
  fprintf(stdout,
          "                -l                                                   -  break down by client host and show statistics \n");
  fprintf(stdout,
          "                -f                                                   -  show ongoing flush locks\n");
  fprintf(stdout,
          "                -k                                                   -  show R/W locks\n");
  fprintf(stdout,
          "                -m                                                   -  show monitoring output format\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       fuxex evict <uuid> [<reason>]                                 :  evict a fuse client\n");
  fprintf(stdout,
          "                                                              <uuid> -  uuid of the client to evict\n");
  fprintf(stdout,
          "                                                            <reason> -  optional text shown to the client why he has been evicted or an instruction for an action to the client\n");
  fprintf(stdout,
          "                                                                     - if the reason contains the keywoard 'abort' the abort handler will be called on client side (might create a stack trace/core)\n");
  fprintf(stdout,
          "                                                                     - if reason contains the keyword 'log2big' the client will effectily not be evicted, but will truncate his logfile to 0\n");
  fprintf(stdout,
          "                                                                     - if reason contains the keyword 'setlog' and 'debug','notice', 'error', 'crit', 'info', 'warning' the log level of the targeted mount is changed accordingly .e.g evict <uuid> \"setlog error\"\n");
  fprintf(stdout,
          "                                                                     - if reason contains the keyword 'stacktrace' the client will send a self-stacktrace with the next heartbeat message and it will be stored in /var/log/eos/mgm/eosxd-stacktraces.log e.g. evict <uuid> stacktrace\n");
  fprintf(stdout,
          "                                                                     - if reason contains the keyword 'sendlog' the client will send max. the last 512 lines of each log level and the log will be stored in /var/log/eos/mgm/eosxd-logtraces.log e.g. evict <uuid> sendlog\n");
  fprintf(stdout,
          "                                                                     - if reason contains the keyword 'resetbuffer' the client will reset the read-ahead and write-buffers in flight and possibly unlock a locked mount point");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       fusex evict static|autofs mem:<size-in-mb>|idle:<seconds>     :  evict all autofs or static mounts which have a resident memory footprint larger than <size-in-mb> or are idle longer than <seconds>\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       fusex dropcaps <uuid>                                         :  advice a client to drop all caps\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       fusex droplocks <inode> <pid>                                 :  advice a client to drop for a given (hexadecimal) inode and process id\n");
  fprintf(stdout, "\n");
  fprintf(stdout,
          "       fusex caps [-t | -i | -p [<regexp>] ]                         :  print caps\n");
  fprintf(stdout,
          "                -t                                                   -  sort by expiration time\n");
  fprintf(stdout,
          "                -i                                                   -  sort by inode\n");
  fprintf(stdout,
          "                -p                                                   -  display by path\n");
  fprintf(stdout,
          "                -t|i|p <regexp>>                                     -  display entries matching <regexp> for the used filter type");
  fprintf(stdout, "\n");
  fprintf(stdout, "examples:\n");
  fprintf(stdout,
          "           fusex caps -i ^0000abcd$                                  :  show caps for inode 0000abcd\n");
  fprintf(stdout,
          "           fusex caps -p ^/eos/$                                     :  show caps for path /eos\n");
  fprintf(stdout,
          "           fusex caps -p ^/eos/caps/                                 :  show all caps in subtree /eos/caps\n");
  fprintf(stdout,
          "       fusex conf [<heartbeat-in-seconds>] [quota-check-in-seconds] [max broadcast audience] [broadcast audience match]\n");
  fprintf(stdout,
          "                                                             :  show heartbeat and quota interval\n");
  fprintf(stdout,
          "                                                                     :  [ optional change heartbeat interval from [1-15] seconds ]\n");
  fprintf(stdout,
          "                                                                     :  [ optional set quota check interval from [1-16] seconds ]\n");
  fprintf(stdout, "examples:\n");
  fprintf(stdout,
          "   fusex conf                                                :  show heartbeat and quota interval\n");
  fprintf(stdout,
          "   fusex conf 10                                             :  define heartbeat interval as 10 seconds\n");
  fprintf(stdout,
          "   fusex conf 10 30                                          :  define heartbeat as 10 seconds and quota interval as 30 seconds\n");
  fprintf(stdout,
          "   fusex conf 0 0 256 @b[67]                                :  suppress broadcasts when more than 256 clients are conected and the target matches @b[67]\n");
  return (0);
}
