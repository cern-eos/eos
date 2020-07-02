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
#include "common/StringTokenizer.hh"
#include<set>
#include<string>
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int
com_geosched(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  std::set<std::string> supportedParam = {"skipSaturatedAccess",
                                          "skipSaturatedDrnAccess", "skipSaturatedBlcAccess",
                                          "plctDlScorePenalty", "plctUlScorePenalty",
                                          "accessDlScorePenalty", "accessUlScorePenalty",
                                          "fillRatioLimit", "fillRatioCompTol", "saturationThres",
                                          "timeFrameDurationMs", "penaltyUpdateRate", "proxyCloseToFs"
                                         };
  XrdOucString in = "";

  if (wants_help(arg1)) {
    goto com_geosched_usage;
  }

  if ((cmd != "show") && (cmd != "set") && (cmd != "updater") &&
      (cmd != "forcerefresh") && (cmd != "disabled") && (cmd != "access")) {
    goto com_geosched_usage;
  }

  in = "mgm.cmd=geosched";

  if (cmd == "show") {
    XrdOucString subcmd = subtokenizer.GetToken();

    if (subcmd == "-c") {
      in += "&mgm.usecolors=1";
      subcmd = subtokenizer.GetToken();
    } else if (subcmd == "-m") {
      in += "&mgm.monitoring=1";
      subcmd = subtokenizer.GetToken();
    }

    if ((subcmd != "tree") && (subcmd != "snapshot") && (subcmd != "state") &&
        (subcmd != "param")) {
      goto com_geosched_usage;
    }

    if (subcmd == "state") {
      in += "&mgm.subcmd=showstate";
      subcmd = subtokenizer.GetToken();

      if (subcmd == "-m") {
        in += "&mgm.monitoring=1";
      }
    }

    if (subcmd == "param") {
      in += "&mgm.subcmd=showparam";
    }

    if (subcmd == "tree") {
      in += "&mgm.subcmd=showtree";
      in += "&mgm.schedgroup=";
      XrdOucString group = subtokenizer.GetToken();

      if (group.length()) {
        in += group;
      }
    }

    if (subcmd == "snapshot") {
      in += "&mgm.subcmd=showsnapshot";
      in += "&mgm.schedgroup=";
      XrdOucString group = subtokenizer.GetToken();

      if (group.length()) {
        in += group;
      }

      in += "&mgm.optype=";
      XrdOucString optype = subtokenizer.GetToken();

      if (optype.length()) {
        in += optype;
      }
    }
  }

  if (cmd == "set") {
    XrdOucString parameter = subtokenizer.GetToken();

    if (!parameter.length()) {
      fprintf(stderr, "Error: parameter name is not provided\n");
      goto com_geosched_usage;
    }

    if (supportedParam.find(parameter.c_str()) == supportedParam.end()) {
      fprintf(stderr, "Error: parameter %s not supported\n", parameter.c_str());
      return 0;
    }

    XrdOucString index = subtokenizer.GetToken();
    XrdOucString value = subtokenizer.GetToken();

    if (!index.length()) {
      fprintf(stderr, "Error: value is not provided\n");
      goto com_geosched_usage;
    }

    if (!value.length()) {
      value = index;
      index = "-1";
    }

    double didx = 0.0;

    if (!sscanf(value.c_str(), "%lf", &didx)) {
      fprintf(stderr,
              "Error: parameter %s should have a numeric value, %s was provided\n",
              parameter.c_str(), value.c_str());
      return 0;
    }

    if (!XrdOucString(index.c_str()).isdigit()) {
      fprintf(stderr,
              "Error: index for parameter %s should have a numeric value, %s was provided\n",
              parameter.c_str(), index.c_str());
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

  if (cmd == "updater") {
    XrdOucString subcmd = subtokenizer.GetToken();

    if (subcmd == "pause") {
      in += "&mgm.subcmd=updtpause";
    }

    if (subcmd == "resume") {
      in += "&mgm.subcmd=updtresume";
    }
  }

  if (cmd == "forcerefresh") {
    in += "&mgm.subcmd=forcerefresh";
  }

  if (cmd == "disabled") {
    XrdOucString subcmd = subtokenizer.GetToken();
    XrdOucString geotag, group, optype;

    if ((subcmd != "add") && (subcmd != "rm") && (subcmd != "show")) {
      goto com_geosched_usage;
    }

    geotag = subtokenizer.GetToken();
    optype = subtokenizer.GetToken();
    group = subtokenizer.GetToken();

    if (!group.length() || !optype.length() || !geotag.length()) {
      goto com_geosched_usage;
    }

    std::string sgroup(group.c_str()), soptype(optype.c_str()),
        sgeotag(geotag.c_str());
    const char fbdChars[] = "&/,;%$#@!*";
    auto fbdMatch =  sgroup.find_first_of(fbdChars);

    if (fbdMatch != std::string::npos && !(sgroup == "*")) {
      fprintf(stdout, "illegal character %c detected in group name %s\n",
              sgroup[fbdMatch], sgroup.c_str());
      return 0;
    }

    fbdMatch =  soptype.find_first_of(fbdChars);

    if (fbdMatch != std::string::npos && !(soptype == "*")) {
      fprintf(stdout, "illegal character %c detected in optype %s\n",
              soptype[fbdMatch], soptype.c_str());
      return 0;
    }

    fbdMatch =  sgeotag.find_first_of(fbdChars);

    if (fbdMatch != std::string::npos && !(sgeotag == "*" && subcmd != "add")) {
      fprintf(stdout, "illegal character %c detected in geotag %s\n",
              sgeotag[fbdMatch], sgeotag.c_str());
      return 0;
    }

    in += ("&mgm.subcmd=disabled" +
           subcmd); // mgm.subcmd is  disabledadd or disabledrm or disabledshow

    if (geotag.length()) {
      in += ("&mgm.geotag=" + geotag);
    }

    in += ("&mgm.schedgroup=" + group);
    in += ("&mgm.optype=" + optype);
  }

  if (cmd == "access") {
    XrdOucString subcmd = subtokenizer.GetToken();
    XrdOucString geotag, geotag_list, optype;

    if ((subcmd != "setdirect") && (subcmd != "showdirect") &&
        (subcmd != "cleardirect") &&
        (subcmd != "setproxygroup") && (subcmd != "showproxygroup") &&
        (subcmd != "clearproxygroup")) {
      goto com_geosched_usage;
    }

    const char* token = 0;

    if ((token = subtokenizer.GetToken())) {
      geotag = token;
    }

    if ((token = subtokenizer.GetToken())) {
      geotag_list = token;
    }

    if (subcmd == "setdirect" || subcmd == "setproxygroup") {
      if (!geotag.length() || !geotag_list.length()) {
        goto com_geosched_usage;
      }
    }

    if (subcmd == "showdirect" || subcmd == "showproxygroup") {
      if (geotag.length() || geotag_list.length()) {
        if (geotag == "-m") {
          in += "&mgm.monitoring=1";
        } else {
          goto com_geosched_usage;
        }
      }
    }

    if (subcmd == "cleardirect" || subcmd == "clearproxygroup") {
      if (!geotag.length() || geotag_list.length()) {
        goto com_geosched_usage;
      }
    }

    in += ("&mgm.subcmd=access" +
           subcmd); // mgm.subcmd is accessset or accessshow or accessclear

    if (geotag.length()) {
      in += ("&mgm.geotag=" + geotag);
    }

    if (geotag.length()) {
      in += ("&mgm.geotaglist=" + geotag_list);
    }
  }

  global_retc = output_result(client_command(in, true));
  return (0);
com_geosched_usage:
  fprintf(stdout,
          "'[eos] geosched ..' Interact with the file geoscheduling engine in EOS.\n");
  fprintf(stdout,
          "Usage: geosched show|set|updater|forcerefresh|disabled|access ...\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout,
          "       geosched show [-c|-m] tree [<scheduling group>]                    :  show scheduling trees\n");
  fprintf(stdout,
          "                                                                          :  if <scheduling group> is specified only the tree for this group is shown. If it's not all, the trees are shown.\n");
  fprintf(stdout,
          "                                                                          :  '-c' enables color display\n");
  fprintf(stdout,
          "                                                                          :  '-m' list in monitoring format\n");
  fprintf(stdout,
          "       geosched show [-c|-m] snapshot [{<scheduling group>,*} [<optype>]] :  show snapshots of scheduling trees\n");
  fprintf(stdout,
          "                                                                          :  if <scheduling group> is specified only the snapshot(s) for this group is/are shown. If it's not all, the snapshots for all the groups are shown.\n");
  fprintf(stdout,
          "                                                                          :  if <optype> is specified only the snapshot for this operation is shown. If it's not, the snapshots for all the optypes are shown.\n");
  fprintf(stdout,
          "                                                                          :  <optype> can be one of the folowing plct,accsro,accsrw,accsdrain,plctdrain\n");
  fprintf(stdout,
          "                                                                          :  '-c' enables color display\n");
  fprintf(stdout,
          "                                                                          :  '-m' list in monitoring format\n");
  fprintf(stdout,
          "       geosched show param                                                :  show internal parameters\n");
  fprintf(stdout,
          "       geosched show state [-m]                                           :  show internal state\n");
  fprintf(stdout,
          "                                                                          :  '-m' list in monitoring format\n");
  fprintf(stdout,
          "       geosched set <param name> [param index] <param value>              :  set the value of an internal state parameter (all names can be listed with geosched show param) \n");
  fprintf(stdout,
          "       geosched updater {pause|resume}                                    :  pause / resume the tree updater\n");
  fprintf(stdout,
          "       geosched forcerefresh                                              :  force a refresh of the trees/snapshots\n");
  fprintf(stdout,
          "       geosched disabled add <geotag> {<optype>,*} {<scheduling subgroup>,*}      :  disable a branch of a subtree for the specified group and operation\n");
  fprintf(stdout,
          "                                                                                  :  multiple branches can be disabled (by successive calls) as long as they have no intersection\n");
  fprintf(stdout,
          "       geosched disabled rm {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*}   :  re-enable a disabled branch for the specified group and operation\n");
  fprintf(stdout,
          "                                                                                  :  when called with <geotag> *, the whole tree(s) are re-enabled, canceling all previous disabling\n");
  fprintf(stdout,
          "       geosched disabled show {<geotag>,*} {<optype>,*} {<scheduling subgroup>,*} :  show list of disabled branches for for the specified groups and operation\n");
  fprintf(stdout,
          "       geosched access setdirect <geotag> <geotag_list>                   :  set a mapping between an accesser geotag and a set of target geotags \n");
  fprintf(stdout,
          "                                                                          :  these mappings specify which geotag can be accessed from which geotag without going through a firewall entrypoint\n");
  fprintf(stdout,
          "                                                                          :  geotag_list is of the form token1::token2,token3::token4::token5,... \n");
  fprintf(stdout,
          "       geosched access showdirect [-m]                                    :  show mappings between accesser geotags and target geotags\n");
  fprintf(stdout,
          "                                                                          :  '-m' list in monitoring format\n");
  fprintf(stdout,
          "       geosched access cleardirect {<geotag>|all}                         :  clear a mapping between an accesser geotag and a set of target geotags\n");
  fprintf(stdout,
          "       geosched access setproxygroup <geotag> <proxygroup>                :  set the proxygroup acting as a firewall entrypoint for the given subtree \n");
  fprintf(stdout,
          "                                                                          :  if a client accesses a file from a geotag which does not have direct access to the subtree the replica is,\n");
  fprintf(stdout,
          "                                                                          :  it will be scheduled to access through a node from the given proxygroup \n");
  fprintf(stdout,
          "       geosched access showproxygroup [-m]                                :  show mappings between accesser geotags and target geotags\n");
  fprintf(stdout,
          "                                                                          :  '-m' list in monitoring format\n");
  fprintf(stdout,
          "       geosched access clearproxygroup {<geotag>|all}                     :  clear a mapping between an accesser geotag and a set of target geotags\n");
  return (0);
}
