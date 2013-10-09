// ----------------------------------------------------------------------
// File: com_vid.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
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

/* VID System listing, configuration, manipulation */
int
com_vid (char* arg1)
{
  XrdOucString in("");
  ConsoleCliCommand *parsedCmd, *vidCmd, *lsSubCmd, *membershiSetSubCmd,
    *membershipRmSubCmd, *rmSubCmd, *membershipSubCmd, *mapSubCmd,
    *geotagSubCmd, *gatewaySubCmd, *enableSubCmd, *disableSubCmd;

  vidCmd = new ConsoleCliCommand("vid", "virtual ID functions");

  lsSubCmd = new ConsoleCliCommand("ls", "list configured policies");
  lsSubCmd->addOptions({{"user", "show only user role mappings", "-u"},
                        {"group", "show only group role mappings", "-g"},
                        {"user-alias", "show user alias mapping", "-U"},
                        {"group-alias", "show group alias mapping", "-G"},
                        {"sudoers", "show list of sudoers", "-s"},
                        {"gateways", "show configured gateways", "-w"},
                        {"auth", "show authentication", "-a"},
                        {"geo", "show geo location mapping", "-l"},
                        {"numerical", "show numerical ids instead of "
                         "user/group names", "-n"}
                       });
  vidCmd->addSubcommand(lsSubCmd);

  membershipSubCmd = new ConsoleCliCommand("membership", "membership related "
                                           "functions");
  vidCmd->addSubcommand(membershipSubCmd);

  membershiSetSubCmd = new ConsoleCliCommand("set", "");
  membershiSetSubCmd->addOption({"uid", "", 1, 1, "<uid>", true});
  OptionsGroup *membershipGroup =
    membershiSetSubCmd->addGroupedOptions({
      {"uids", "", "--uids=", 1, "<uid1>[,<uid2>,...]", false},
      {"gids", "", "--gids=", 1, "<gid1>[,<gid2>,...]", false}
    });
  membershipGroup->addOptions({{"+sudo", "", "+sudo"}, {"-sudo", "", "-sudo"}});
  membershipGroup->setRequired(true);
  membershipSubCmd->addSubcommand(membershiSetSubCmd);

  membershipRmSubCmd = new ConsoleCliCommand("rm", "delete the membership "
                                             "entries for <uid>");
  membershipRmSubCmd->addOption({"uid", "", 1, 1, "<uid>", true});
  membershipSubCmd->addSubcommand(membershipRmSubCmd);

  mapSubCmd = new ConsoleCliCommand("map", "");
  const char *mapOptions[] = {"krb5", "gsi", "https", "sss",
                              "unix", "tident", 0};
  OptionsGroup *mapGroup = new OptionsGroup("");
  for (int i = 0; mapOptions[i]; i++)
    mapGroup->addOption({mapOptions[i], "", std::string("--") + mapOptions[i]});
  mapGroup->addOption({"voms", "<pattern> is <group>:<role> e.g. to map VOMS "
                       "attribute /dteam/cern/Role=NULL/Capability=NULL one "
                       "should define <pattern>=/dteam/cern:",
                       "--voms", "<pattern>", false});
  mapGroup->setRequired(true);
  mapSubCmd->addGroup(mapGroup);
  mapSubCmd->addOptions({{"vuid", "", "--vuid=", "<vuid:uid>", false},
                         {"vgid", "", "--vgid=", "<vgid:gid>", false},
                        });
  vidCmd->addSubcommand(mapSubCmd);

  geotagSubCmd = new ConsoleCliCommand("geotag", "add to all IP's matching the "
                                       "prefix <IP-prefix> the geo location tag "
                                       "<geotag>;\nN.B. specify the default "
                                       "assumption via 'vid geotag default "
                                       "<default-tag>'");
  geotagSubCmd->addOptions({{"ip", "", 1, 1, "<IP-prefix>", true},
                            {"tag", "", 2, 1, "<geotag>", true}
                           });
  vidCmd->addSubcommand(geotagSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "remove configured vid with name key "
                                   "- hint: use config dump to see the key "
                                   "names of vid rules");
  rmSubCmd->addOption({"key", "", 1, 1, "<key>", true});

  enableSubCmd = new ConsoleCliCommand("enable", "enable the default "
                                       "mapping via password database");

  OptionsGroup *enableDisableGroup = new OptionsGroup("");
  for (int i = 0; mapOptions[i]; i++)
    enableDisableGroup->addOption({mapOptions[i], "", mapOptions[i]});

  enableSubCmd->addGroup(enableDisableGroup);
  vidCmd->addSubcommand(enableSubCmd);

  disableSubCmd = new ConsoleCliCommand(*enableSubCmd);
  disableSubCmd->setName("disable");
  disableSubCmd->setDescription("disable the default mapping via "
                                "password database");
  vidCmd->addSubcommand(disableSubCmd);

  gatewaySubCmd = new ConsoleCliCommand("gateway", "adds/removes a host as a "
                                        "(fuse) gateway with 'su' priviledges");
  gatewaySubCmd->addGroupedOptions({{"add", "", "--add"},
                                    {"remove", "", "--remove,--rm"}
                                   })->setRequired(true);
  gatewaySubCmd->addOption({"host", "", 1, 1, "<hostname>", true});
  OptionsGroup *gatewayGroup = new OptionsGroup("");
  for (int i = 0; mapOptions[i] && strcmp(mapOptions[i], "tident") != 0; i++)
    gatewayGroup->addOption({mapOptions[i], "", mapOptions[i]});
  gatewaySubCmd->addGroup(gatewayGroup);
  vidCmd->addSubcommand(gatewaySubCmd);

  addHelpOptionRecursively(vidCmd);

  parsedCmd = vidCmd->parse(arg1);

  if (parsedCmd == vidCmd || parsedCmd == membershipSubCmd)
  {
    if (!checkHelpAndErrors(parsedCmd))
      parsedCmd->printUsage();
    goto bailout;
  }
  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == lsSubCmd)
  {
    in = "mgm.cmd=vid&mgm.subcmd=ls";
    XrdOucString soption = "";
    XrdOucString option = "";

    const char *lsOptions[] = {"user", "u",
                               "group", "g",
                               "user-alias", "U",
                               "group-alias", "G",
                               "sudoers", "s",
                               "gateways", "y",
                               "auth", "a",
                               "geo", "l",
                               "numerical", "n",
                               0
                              };

    for (int i = 0; lsOptions[i]; i += 2)
    {
      if (lsSubCmd->hasValue(lsOptions[i]))
        soption += lsOptions[i + 1];
    }

    if (soption.length())
    {
      in += "&mgm.vid.option=";
      in += soption;
    }

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }
  else if (parsedCmd == geotagSubCmd)
  {
    in += "mgm.cmd=vid&mgm.subcmd=set";
    in += "&mgm.vid.cmd=geotag";
    XrdOucString vidkey("");
    vidkey = "geotag:";
    vidkey += geotagSubCmd->getValue("ip").c_str();
    in += "&mgm.vid.key=" + vidkey;
    in += "&mgm.vid.geotag=";
    in += geotagSubCmd->getValue("tag").c_str();

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }
  else if (parsedCmd == membershiSetSubCmd)
  {
    in = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString uid = membershiSetSubCmd->getValue("uid").c_str();
    XrdOucString vidkey = uid;

    in += "&mgm.vid.cmd=membership";
    in += "&mgm.vid.source.uid=";
    in += uid;

    if (membershiSetSubCmd->hasValue("uids"))
    {
      vidkey += ":uids";
      in += "&mgm.vid.key=";
      in += vidkey;
      in += "&mgm.vid.target.uid=";
      in += membershiSetSubCmd->getValue("uids").c_str();
    }

    if (membershiSetSubCmd->hasValue("gids"))
    {
      vidkey += ":gids";
      in += "&mgm.vid.key=";
      in += vidkey;
      in += "&mgm.vid.target.gid=";
      in += membershiSetSubCmd->getValue("gids").c_str();
    }
    else
    {
      vidkey += ":root";
      in += "&mgm.vid.key= "; // fake using key=' '
      in += vidkey;
      in += "&mgm.vid.target.sudo=";

      if (membershiSetSubCmd->hasValue("+sudo"))
        in += "true";
      else
        in += "false";
    }

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }
  else if (parsedCmd == membershipRmSubCmd)
    {
      in = "mgm.cmd=vid&mgm.subcmd=rm";

      XrdOucString key = membershipRmSubCmd->getValue("uid").c_str();
      key.insert("vid:", 0);

      XrdOucString in1 = in;
      in1 += "&mgm.vid.key=" + key + ":uids";

      XrdOucString in2 = in;
      in2 += "&mgm.vid.key=" + key + ":gids";

      global_retc = output_result(client_admin_command(in1));
      global_retc |= output_result(client_admin_command(in2));
      goto bailout;
    }
  else if (parsedCmd == mapSubCmd)
  {
    in = "mgm.cmd=vid&mgm.subcmd=set";
    XrdOucString type("");
    in += "&mgm.vid.cmd=map";

    for (int i = 0; mapOptions[i]; i++)
    {
      if (mapSubCmd->hasValue(mapOptions[i]))
      {
        type = mapOptions[i];
        break;
      }
    }

    in += "&mgm.vid.auth=" + type;

    if (type == "")
    {
      in += "voms";
      in += "&mgm.vid.pattern=";
      in += mapSubCmd->getValue("voms").c_str();
    }

    bool hasUidOrGid = false;
    if (mapSubCmd->hasValue("vuid"))
    {
      in += "&mgm.vid.uid=";
      in += mapSubCmd->getValue("vuid").c_str();
      hasUidOrGid = true;
    }
    if (mapSubCmd->hasValue("vgid"))
    {
      in += "&mgm.vid.gid=";
      in += mapSubCmd->getValue("vgid").c_str();
      hasUidOrGid = true;
    }

    if (!hasUidOrGid)
    {
      fprintf(stdout, "Error: Please specify --vuid or --vgid\n");
      mapSubCmd->printUsage();
    }

    in += "&mgm.vid.key=";
    in += "<key>";

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }
  else if (parsedCmd == enableSubCmd || parsedCmd == disableSubCmd)
  {
    in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=";
    XrdOucString disableu = in + "unmap&mgm.vid.key=";
    XrdOucString disableg = disableu;
    in += "map";

    const char *protocol = 0;
    for (int i = 0; mapOptions[i]; i++)
    {
      protocol = mapOptions[i];
      in += "&mgm.vid.auth=";
      in += protocol;
      disableu += protocol;
      disableu += ":\"<pwd>\":uid";
      disableg += protocol;
      disableg += ":\"<pwd>\":gid";
    }

    in += "&mgm.vid.pattern=<pwd>";
    if (strcmp(protocol, "unix") != 0)
    {
      in += "&mgm.vid.uid=0";
      in += "&mgm.vid.gid=0";
    }
    else
    {
      in += "&mgm.vid.uid=99";
      in += "&mgm.vid.gid=99";
    }

    in += "&mgm.vid.key=";
    in += "<key>";

    if (parsedCmd == enableSubCmd)
      global_retc = output_result(client_admin_command(in));
    else if (parsedCmd == disableSubCmd)
    {
      global_retc = output_result(client_admin_command(disableu));
      global_retc |= output_result(client_admin_command(disableg));
    }
    goto bailout;
  }
  else if (parsedCmd == gatewaySubCmd)
  {
    XrdOucString host = gatewaySubCmd->getValue("host").c_str();
    XrdOucString protocol("*");

    for (int i = 0; mapOptions[i]; i++)
    {
      if (gatewaySubCmd->hasValue(mapOptions[i]))
      {
        protocol = mapOptions[i];
        break;
      }
    }

    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=set&mgm.vid.cmd=";
    XrdOucString disableu = in + "unmap&mgm.vid.key=";
    in += "map";
    XrdOucString disableg = disableu;

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

    if (gatewaySubCmd->hasValue("add"))
      global_retc = output_result(client_admin_command(in));

    if (gatewaySubCmd->hasValue("remove"))
    {
      global_retc = output_result(client_admin_command(disableu));
      global_retc |= output_result(client_admin_command(disableg));
    }
    goto bailout;
  }
  else if (parsedCmd == rmSubCmd)
  {
    XrdOucString in = "mgm.cmd=vid&mgm.subcmd=rm";
    in += "&mgm.vid.key=";
    in += rmSubCmd->getValue("key").c_str();

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }

 bailout:
  delete vidCmd;

  return (0);
}
