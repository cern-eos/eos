// ----------------------------------------------------------------------
// File: com_geosched.cc
// Author: Geoffray Adde - CERN
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
#include "common/RWMutex.hh"
#include<set>
#include<string>
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int
com_geosched (char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();

  std::set<std::string> supportedParam = {"skipSaturatedPlct","skipSaturatedAccess",
      "skipSaturatedDrnAccess","skipSaturatedBlcAccess",
      "skipSaturatedDrnPlct","skipSaturatedBlcPlct",
      "plctDlScorePenalty","plctUlScorePenalty",
      "accessDlScorePenalty","accessUlScorePenalty",
      "fillRatioLimit","fillRatioCompTol","saturationThres",
      "timeFrameDurationMs"};

  XrdOucString in = "";

  if (wants_help(arg1))
    goto com_geosched_usage;

  if ((cmd != "show") && (cmd != "set") && (cmd != "updater"))
  {
    goto com_geosched_usage;
  }

  in = "mgm.cmd=geosched";

  if (cmd == "show")
  {
    XrdOucString subcmd = subtokenizer.GetToken();
    if ((subcmd != "tree") && (subcmd != "snapshot") && (subcmd != "state"))
    {
      goto com_geosched_usage;
    }

    if (subcmd == "state")
    {
      in += "&mgm.subcmd=showstate";
    }

    if (subcmd == "tree")
    {
      in += "&mgm.subcmd=showtree";
      in += "&mgm.schedgroup=";
      XrdOucString group = subtokenizer.GetToken();
      if(group.length())
	in += group;
    }

    if (subcmd == "snapshot")
    {
      in += "&mgm.subcmd=showsnapshot";
      in += "&mgm.schedgroup=";
      XrdOucString group = subtokenizer.GetToken();
      if(group.length())
	in += group;
      in += "&mgm.optype=";
      XrdOucString optype = subtokenizer.GetToken();
      if(optype.length())
	in += optype;
    }
  }

  if(cmd == "set")
  {
    XrdOucString parameter = subtokenizer.GetToken();
    if(!parameter.length())
    {
      fprintf(stderr, "Error: parameter name is not provided\n");
      goto com_geosched_usage;
    }
    if(supportedParam.find(parameter.c_str())==supportedParam.end())
    {
      fprintf(stderr, "Error: parameter %s not supported\n",parameter.c_str());
      return 0;
    }

    XrdOucString value = subtokenizer.GetToken();
    if(!value.length())
    {
      fprintf(stderr, "Error: value is not provided\n");
      goto com_geosched_usage;
    }
    if(!XrdOucString(value.c_str()).isdigit())
    {
      fprintf(stderr, "Error: parameter %s should have a numeric value, %s was provided\n",
	      parameter.c_str(),value.c_str());
      return 0;
    }

    in += "&mgm.subcmd=set";
    in += "&mgm.param=";
    in += parameter.c_str();
    in += "&mgm.value=";
    in += value.c_str();
  }

  if(cmd == "updater")
  {
    XrdOucString subcmd = subtokenizer.GetToken();
    if(subcmd == "pause")
    {
      in += "&mgm.subcmd=updtpause";
    }

    if(subcmd == "resume")
    {
      in += "&mgm.subcmd=updtresume";
    }
  }

  global_retc = output_result(client_admin_command(in));
  return (0);

com_geosched_usage:
  fprintf(stdout, "Usage: geosched                                                   :  Interact with the file geoscheduling engine\n");
  fprintf(stdout, "       geosched show tree [<scheduling subgroup>]                 :  show scheduling trees\n");
  fprintf(stdout, "                                                                     -  if <scheduling group> is specified only the tree for this group is shown. If it's not all, the trees are shown.\n");
  fprintf(stdout, "       geosched show snapshot [<scheduling subgroup>] [<optype>]  :  show snapshots of scheduling trees\n");
  fprintf(stdout, "                                                                     -  if <scheduling group> is specified only the snapshot(s) for this group is/are shown. If it's not all, the snapshots for all the groups are shown.\n");
  fprintf(stdout, "                                                                     -  if <optype> is specified only the snapshot for this operation is shown. If it's not, the snapshots for all the optypes are shown.\n");
  fprintf(stdout, "                                                                     -  <optype> can be one of the folowing plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc\n");
  fprintf(stdout, "       geosched show state                                        :  show internal state parameters\n");
  fprintf(stdout, "       geosched set <param name> <param value>                    :  set the value of an internal state parameter (all names can be listed with geosched show state) \n");
  fprintf(stdout, "       geosched updater {pause|resume}                            :  pause / resume the tree updater\n");
  return (0);
}
