// ----------------------------------------------------------------------
// File: com_reconnect.cc
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
#include "XrdClient/XrdClientConn.hh"
/*----------------------------------------------------------------------------*/

/* Force a reconnection/reauthentication */
int
com_reconnect (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString param="";
  XrdOucString option="";
  param = subtokenizer.GetToken();

  if ( (!param.length()) || 
       ( param == "gsi" ) ||
       ( param == "krb5" ) || 
       ( param == "unix" ) ||
       ( param == "sss" ) ) {
    if (param.length()) {
      fprintf(stdout,"# reconnecting to %s with <%s> authentication\n", serveruri.c_str(), param.c_str());
      setenv("XrdSecPROTOCOL",param.c_str(),1);
    } else {
      fprintf(stdout,"# reconnecting to %s\n", serveruri.c_str());
    }

    XrdOucString path = serveruri;
    path += "//proc/admin/";
    
    XrdClientAdmin admin(path.c_str());
    admin.Connect();
    if (admin.GetClientConn()) {
      admin.GetClientConn()->Disconnect(true);
    }
    
    if (debug)
      fprintf(stdout,"debug: %s\n", path.c_str());
    return (0);
  } else {
    fprintf(stdout,"usage: reconnect [gsi,krb5,unix,sss]                                    :  reconnect to the management node [using the specified protocol]\n");
    return (0);
  }
}
