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
      "timeFrameDurationMs","penaltyUpdateRate"};

  XrdOucString in = "";

  if (wants_help(arg1))
    goto com_geosched_usage;

  if ((cmd != "show") && (cmd != "set") && (cmd != "updater") && (cmd != "forcerefresh") && (cmd != "disabled"))
  {
    goto com_geosched_usage;
  }

  in = "mgm.cmd=geosched";

  if (cmd == "show")
  {
    XrdOucString subcmd = subtokenizer.GetToken();
    if(subcmd == "-c")
    {
      in += "&mgm.usecolors=1";
      subcmd = subtokenizer.GetToken();
    }

    if ((subcmd != "tree") && (subcmd != "snapshot") && (subcmd != "state") && (subcmd != "param"))
    {
      goto com_geosched_usage;
    }

    if (subcmd == "state")
    {
      in += "&mgm.subcmd=showstate";
    }

    if (subcmd == "param")
    {
      in += "&mgm.subcmd=showparam";
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

    XrdOucString index = subtokenizer.GetToken();
    XrdOucString value = subtokenizer.GetToken();
    if(!index.length())
    {
      fprintf(stderr, "Error: value is not provided\n");
      goto com_geosched_usage;
    }
    if(!value.length())
    {
      value=index;
      index="-1";
    }
    double didx = 0.0;
    if(!sscanf(value.c_str(),"%lf",&didx))
    {
      fprintf(stderr, "Error: parameter %s should have a numeric value, %s was provided\n",
	      parameter.c_str(),value.c_str());
      return 0;
    }
    if(!XrdOucString(index.c_str()).isdigit())
    {
      fprintf(stderr, "Error: index for parameter %s should have a numeric value, %s was provided\n",
              parameter.c_str(),index.c_str());
      return 0;
    }

    in += "&mgm.subcmd=set";
    in += "&mgm.param=";
    in += parameter.c_str();
    in += "&mgm.paramidx=";
    in += index.c_str();
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

  if(cmd == "forcerefresh")
  {
      in += "&mgm.subcmd=forcerefresh";
  }

  if (cmd == "disabled")
  {
    XrdOucString subcmd = subtokenizer.GetToken();
    XrdOucString geotag,group,optype;

    if ((subcmd != "add") && (subcmd != "rm") && (subcmd != "show"))
    {
      goto com_geosched_usage;
    }

    geotag = subtokenizer.GetToken();
    optype = subtokenizer.GetToken();
    group = subtokenizer.GetToken();

    if(!group.length() || !optype.length() || !geotag.length())
      goto com_geosched_usage;

    std::string sgroup(group.c_str()),soptype(optype.c_str()),sgeotag(geotag.c_str());
    const char fbdChars[] = "&/,;%$#@!*";
    auto fbdMatch =  sgroup.find_first_of(fbdChars);
    if(fbdMatch!=std::string::npos && !(sgroup=="*"))
    {
      fprintf(stdout, "illegal character %c detected in group name %s\n",sgroup[fbdMatch],sgroup.c_str());
      return 0;
    }
    fbdMatch =  soptype.find_first_of(fbdChars);
    if(fbdMatch!=std::string::npos && !(soptype=="*"))
    {
      fprintf(stdout, "illegal character %c detected in optype %s\n",soptype[fbdMatch],soptype.c_str());
      return 0;
    }
    fbdMatch =  sgeotag.find_first_of(fbdChars);
    if(fbdMatch!=std::string::npos && !(sgeotag=="*" && subcmd!="add") )
    {
      fprintf(stdout, "illegal character %c detected in geotag %s\n",sgeotag[fbdMatch],sgeotag.c_str());
      return 0;
    }

    in += ("&mgm.subcmd=disabled"+subcmd); // mgm.subcmd is  disabledadd or disabledrm or disabledshow

    if(geotag.length())
      in += ("&mgm.geotag="+geotag);

    in += ("&mgm.schedgroup="+group);
    in += ("&mgm.optype="+optype);

  }
  global_retc = output_result(client_admin_command(in));
  return (0);

com_geosched_usage:
  fprintf(stdout, "'[eos] geosched ..' Interact with the file geoscheduling engine in EOS.\n");
  fprintf(stdout, "Usage: geosched show|set|updater|forcerefresh|disabled ...\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "       geosched show [-c] tree [<scheduling subgroup>]                    :  show scheduling trees\n");
  fprintf(stdout, "                                                                          :  if <scheduling group> is specified only the tree for this group is shown. If it's not all, the trees are shown.\n");
  fprintf(stdout, "                                                                          :  '-c' enables color display\n");
  fprintf(stdout, "       geosched show [-c] snapshot [{<scheduling subgroup>,*}] [<optype>] :  show snapshots of scheduling trees\n");
  fprintf(stdout, "                                                                          :  if <scheduling group> is specified only the snapshot(s) for this group is/are shown. If it's not all, the snapshots for all the groups are shown.\n");
  fprintf(stdout, "                                                                          :  if <optype> is specified only the snapshot for this operation is shown. If it's not, the snapshots for all the optypes are shown.\n");
  fprintf(stdout, "                                                                          :  <optype> can be one of the folowing plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc\n");
  fprintf(stdout, "                                                                          :  '-c' enables color display\n");
  fprintf(stdout, "       geosched show param                                                :  show internal parameters\n");
  fprintf(stdout, "       geosched show state                                                :  show internal state\n");
  fprintf(stdout, "       geosched set <param name> [param index] <param value>              :  set the value of an internal state parameter (all names can be listed with geosched show state) \n");
  fprintf(stdout, "       geosched updater {pause|resume}                                    :  pause / resume the tree updater\n");
  fprintf(stdout, "       geosched forcerefresh                                              :  force a refresh of the trees/snapshots\n");
  fprintf(stdout, "       geosched disabled add <geotag> {<optype>,*} {<scheduling subgroup>,*}      :  disable a branch of a subtree for the specified group and operation\n");
  fprintf(stdout, "                                                                                  :  multiple branches can be disabled (by successive calls) as long as they have no intersection\n");
  fprintf(stdout, "       geosched disabled rm {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*}   :  re-enable a disabled branch for the specified group and operation\n");
  fprintf(stdout, "                                                                                  :  when called with <geotag> *, the whole tree(s) are re-enabled, canceling all previous disabling\n");
  fprintf(stdout, "       geosched disabled show {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*} :  show list of disabled branches for for the specified groups and operation\n");
  return (0);
}
