// ----------------------------------------------------------------------
// File: com_motd.cc
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
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/

/* Get the server version*/
int
com_motd (char *arg) {
  XrdOucString in = "mgm.cmd=motd"; 
  XrdOucString motdfile = arg;
  if (motdfile.length()) {
    int fd = open(motdfile.c_str(),O_RDONLY);
    if (fd >0) {
      char maxmotd[1024];
      memset(maxmotd,0,sizeof(maxmotd));
      size_t nread = read(fd,maxmotd,sizeof(maxmotd));
      maxmotd[1023]=0;
      XrdOucString b64out;
      if (nread>0) {
        eos::common::SymKey::Base64Encode(maxmotd, strlen(maxmotd)+1, b64out);
      }
      in += "&mgm.motd=";
      in += b64out.c_str();
    }
  }
  
  global_retc = output_result(client_user_command(in));
  return (0);
}
