// ----------------------------------------------------------------------
// File: com_accounting.cc
// Author: Jozsef Makai- CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "console/ConsoleMain.hh"

inline int
com_accounting_usage() {
  fprintf(stdout,
          "usage: accounting report [-f]                          : prints accounting report in JSON, data is served from cache if possible\n");
  fprintf(stdout,
          "                                                    -f : forces a synchronous report instead of using the cache (only use this if the cached data is too old)\n");
  fprintf(stdout,
          "       accounting config -e [<expired>] -i [<invalid>] : configure caching behaviour\n");
  fprintf(stdout,
          "                                                    -e : expiry time in minutes, after this time frame asynchronous update happens\n");
  fprintf(stdout,
          "                                                    -i : invalidity time in minutes, after this time frame synchronous update happens, must be greater than expiry time, default is never\n");
  global_retc = EINVAL;
  return (0);
}

int
com_accounting(char* arg) {
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();

  XrdOucString in = "mgm.cmd=accounting";
  XrdOucString subcmd = subtokenizer.GetToken();
  XrdOucString option = "";
  bool ok = false;

  if (subcmd == "report") {
    ok = true;
    in += "&mgm.subcmd=report";
  }
  else if (subcmd == "config") {
    ok = true;
    in += "&mgm.subcmd=config";
  }

  if(!ok) {
    return com_accounting_usage();
  }

  XrdOucString maybeoption = subtokenizer.GetToken();

  if (subcmd == "report") {
    while (maybeoption.beginswith("-")) {
      if ((maybeoption != "-f")) {
        return com_accounting_usage();
      }

      maybeoption.replace("-", "");
      option += maybeoption;
      maybeoption = subtokenizer.GetToken();
    }
  }
  else if ((subcmd == "config")) {
    while (maybeoption.beginswith("-")) {
      if (maybeoption == "-e") {
        in += "&mgm.accounting.expired=";
      }
      else if (maybeoption == "-i"){
        in += "&mgm.accounting.invalid=";
      }
      else {
        return com_accounting_usage();
      }

      maybeoption = subtokenizer.GetToken();
      if(eos::common::StringTokenizer::IsUnsignedNumber(maybeoption.c_str())) {
        in += maybeoption;
      }
      else {
        return com_accounting_usage();
      }

      maybeoption = subtokenizer.GetToken();
    }
  }

  if (option.length()) {
    in += "&mgm.option=";
    in += option;
  }

  global_retc = output_result(client_user_command(in));
  return (0);
}