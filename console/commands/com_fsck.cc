// ----------------------------------------------------------------------
// File: com_fsck.cc
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

/* Namespace Interface */
int 
com_fsck (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option;
  XrdOucString options="";
  XrdOucString path="";
  XrdOucString in ="";

  if ( ( cmd != "stat") && ( cmd != "enable" ) && ( cmd != "disable") && ( cmd != "report" ) && ( cmd != "repair" ) ) {
    goto com_fsck_usage;
  }
  
  in = "mgm.cmd=fsck&";

  if (cmd == "enable") {
    in += "mgm.subcmd=enable";
  }
  if (cmd == "disable") {
    in += "mgm.subcmd=disable";
  }

  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "report") {
    in += "mgm.subcmd=report";
    do {
      option = subtokenizer.GetToken();
      if (option.length()) {
        XrdOucString tag="";
        if (option == "--error") {
          tag = subtokenizer.GetToken();
          if (!tag.length()) {
            goto com_fsck_usage;
          } else {
            in += "&mgm.fsck.selection=";
            in += tag;
            continue;
          }
        }
        
        while (option.replace("-","")) {}
        options += option;
      }
    } while (option.length());
  }
  
  if (cmd == "repair") {
    in += "mgm.subcmd=repair";
    option = subtokenizer.GetToken();
    if ( (! option.length()) || 
         ( (option != "--checksum") &&
           (option != "--unlink-unregistered") &&
           (option != "--unlink-orphans") &&
           (option != "--adjust-replicas") &&
           (option != "--drop-missing-replicas") ) )
      goto com_fsck_usage;
    option.replace("--","");
    in += "&mgm.option=";
    in += option;
  }
  

  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  

  global_retc = output_result(client_admin_command(in));
  return (0);

 com_fsck_usage:
  fprintf(stdout,"usage: fsck stat                                                  :  print status of consistency check\n");
  fprintf(stdout,"       fsck enable                                                :  enable fsck\n");
  fprintf(stdout,"       fsck disable                                               :  disable fsck\n");
  fprintf(stdout,"       fsck report [-h] [-g] [-m] [-a] [-i] [-l] [--error <tag>]  :  report consistency check results");
  fprintf(stdout,"                                                               -g :  report global counters\n");
  fprintf(stdout,"                                                               -m :  select monitoring output format\n");
  fprintf(stdout,"                                                               -a :  break down statistics per filesystem\n");
  fprintf(stdout,"                                                               -i :  print concerned file ids\n");
  fprintf(stdout,"                                                               -l :  print concerned logical names\n");
  fprintf(stdout,"                                               --error <tag>      :  select only errors with name <tag> in the printout\n");
  fprintf(stdout,"                                                                     you get the names by doing 'fsck report -g'\n");
  fprintf(stdout,"                                                               -h :  print help explaining the individual tags!\n");

  fprintf(stdout,"       fsck repair --checksum\n");
  fprintf(stdout,"                                                                  :  issues a 'verify' operation on all files with checksum errors\n");
  fprintf(stdout,"       fsck repair --unlink-unregistered\n");
  fprintf(stdout,"                                                                  :  unlink replicas which are not connected/registered to their logical name\n");
  fprintf(stdout,"       fsck repair --unlink-orphans\n");
  fprintf(stdout,"                                                                  :  unlink replicas which don't belong to any logical name\n");
  fprintf(stdout,"       fsck repair --adjust-replicas\n");
  fprintf(stdout,"                                                                  :  try to fix all replica inconsistencies\n");
  fprintf(stdout,"       fsck repair --drop-missing-replicas\n");
  fprintf(stdout,"                                                                  :  just drop replicas from the namespace if they cannot be found on disk\n");

  

         
  return (0);
}
