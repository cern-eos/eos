// ----------------------------------------------------------------------
// File: com_fs.cc
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
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/

extern int com_file (char*);
extern int com_fs (char*);
extern int com_find (char*);

using namespace eos::common;

/*----------------------------------------------------------------------------*/

static void
print_config_examples(void)
{
  fprintf(stdout, "\nExample:\n");
  fprintf(stdout, "\tfs config <fsid> configstatus=rw|wo|ro|drain|off :\n");
  fprintf(stdout, "\t\tWhere:\n");
  fprintf(stdout, "\t\t    rw : filesystem set in read write mode\n");
  fprintf(stdout, "\t\t    wo : filesystem set in write-once mode\n");
  fprintf(stdout, "\t\t    ro : filesystem set in read-only mode\n");
  fprintf(stdout, "\t\t drain : filesystem set in drain mode\n");
  fprintf(stdout, "\t\t   off : filesystem set disabled\n");
  fprintf(stdout, "\t\t empty : filesystem is set to empty - possible only "
          "if there are no files stored anymore\n\n");
  fprintf(stdout, "\tfs config <fsid> headroom=<size> : the headroom to keep "
          "per filesystem (e.g. you can write '1G' for 1 GB)\n");
  fprintf(stdout, "\t\t<size> can be (>0)[BMGT]\n\n");
  fprintf(stdout, "\tfs config <fsid> scaninterval=<seconds> : "
          "configures a scanner thread on each FST to recheck the file & block "
          "checksums of all stored files every <seconds> seconds. 0 disables "
          "the scanning.\n\n");
  fprintf(stdout, "\tfs config <fsid> graceperiod=<seconds> : grace period "
          "before a filesystem with an operation error get's automatically "
          "drained\n\n");
  fprintf(stdout, "\tfs config <fsid> drainperiod=<seconds> : drain period a "
          "drain job is waiting to finish the drain procedure\n\n");
}

/* Filesystem listing, configuration, manipulation */
int
com_fs (char* arg1)
{
  // split subcommands
  bool temp_silent = false;

  ConsoleCliCommand *parsedCmd, *fsCmd, *lsSubCmd, *addSubCmd, *mvSubCmd,
    *configSubCmd, *rmSubCmd, *dropDeletionSubCmd, *dropFilesSubCmd,
    *bootSubCmd, *dumpMdSubCmd, *statusSubCmd, *cloneSubCmd, *compareSubCmd,
    *verifySubCmd;

  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  fsCmd = new ConsoleCliCommand("fs", "provides the filesystem "
                                "interface of EOS");

  lsSubCmd = new ConsoleCliCommand("ls", "list all filesystems in default "
                                   "output format");
  lsSubCmd->addGroupedOptions(std::vector<CliOption>
			      {{"monitor", "list all filesystem parameters "
                                "in monitoring format", "-m"},
                               {"long", "display all filesystem parameters "
                                "in long format", "-l"},
                               {"error", "display all filesystems in "
                                "error state", "-e"},
                               {"io", "display all filesystems in IO "
                                "output format", "--io"},
                               {"fsck", "display filesystem check "
                                "statistics", "--fsck"},
                               {"drain", "display all filesystems in drain or "
                                "draindead status with drain progress and "
                                "statistics", "--drain,-d"}
                              });
  lsSubCmd->addOption({"silent", "run in silent mode", "-s"});
  lsSubCmd->addOption({"selection", "", 1, 1, "<selection>"});
  fsCmd->addSubcommand(lsSubCmd);

  addSubCmd = new ConsoleCliCommand("add", "add a filesystem and dynamically "
                                    "assign a filesystem id based on the "
                                    "unique identifier for the disk <uuid>");
  CliOptionWithArgs manualOption("manual", "add with user specified <fsid> and "
                                 "<schedgroup> - no automatic assignment;\n"
                                 "<fsid> is a numeric filesystem id 1..65535",
                                 "-m,--manual=", "<fsid>", false);
  std::pair<float, float> range = {0.0, 65535.0};
  manualOption.addEvalFunction(optionIsNumberInRangeEvalFunc, &range);
  addSubCmd->addOption(manualOption);
  addSubCmd->addOptions(std::vector<CliPositionalOption>
			{{"uuid", "arbitrary string unique to this particular "
                          "filesystem", 1, 1, "<uuid>", true},
                         {"host-port", "internal EOS identifier for a "
                          "node,port,mountpoint description, e.g.:\n"
                          "/eos/<host>:<port>/fst , "
                          "/eos/myhost.cern.ch:1095/fst [you should prefer the "
                          "host:port syntax]", 2, 1,
                          "<node-queue>|<host>[:<port>]", true},
                         {"mountpoint", "local path of the mounted filesystem "
                          "e.g. /data", 3, 1, "<mountpoint>", true},
                         {"schedgroup", "scheduling group where the filesystem "
                          "should be inserted;\ndefault is 'default'", 4, 1,
                          "<schedgroup>", false},
                         {"status", "file system status after the insert;\n"
                          "default is 'off', in most cases should be 'rw'",
                          5, 1, "<status>", false},
                        });
  fsCmd->addSubcommand(addSubCmd);

  mvSubCmd = new ConsoleCliCommand("mv", "move a filesystem into a different "
                                   "scheduling group");
  mvSubCmd->addOptions(std::vector<CliPositionalOption>
		       {{"src", "source system id or source space;\n"
                         "If the source is a <space>, a filesystem will be "
                         "chosen to fit into the destionation group or space",
                         1, 1, "<src-fsid>|<src-space>", true},
                        {"dst", "destination scheduling group or destination "
                         "space; If the destination is a <space>, a scheduling "
                         "group is auto-selected where the filesystem can be "
                         "placed.\n", 2, 1, "<dst-schedgroup>|<dst-space>", true}
                       });
  fsCmd->addSubcommand(mvSubCmd);

  configSubCmd = new ConsoleCliCommand("config", "configure filesystem "
                                       "parameter for a single filesystem "
                                       "identified by host:port/path, "
                                       "filesystem id or filesystem UUID");
  configSubCmd->addOptions(std::vector<CliPositionalOption>
			   {{"fsid", "", 1, 1,
                             "<host>:<port><path>|<fsid>|<uuid>", true},
                            {"key-value", "the key and value to set", 2, 1,
                             "<key>=<value>", true}
                           });
  fsCmd->addSubcommand(configSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "remove filesystem "
                                   "configuration by various identifiers");
  rmSubCmd->addOptions(std::vector<CliPositionalOption>
		       {{"fsid", "", 1, 1,
                         "<fs-id>|<node-queue>|<mount-point>|<hostname>", true},
                        {"mountpoint", "", 2, 1, "<mountpoint>", false}
                       });
  fsCmd->addSubcommand(rmSubCmd);

  dropDeletionSubCmd = new ConsoleCliCommand("dropdeletion", "drop pending "
                                             "deletions on a filesystem "
                                             "represented by <fs-id>");
  dropDeletionSubCmd->addOption({"fsid", "", 1, 1, "<fs-id>", true});
  fsCmd->addSubcommand(dropDeletionSubCmd);

  dropFilesSubCmd = new ConsoleCliCommand("dropfiles", "allows to drop all "
                                          "files on <fs-id>");
  dropFilesSubCmd->addOption({"force", "unlinks/removes files at the time from "
                              "the NS (you have to cleanup or remove the files "
                              "from disk)", "-f,--force"});
  dropFilesSubCmd->addOption({"fsid", "", 1, 1, "<fs-id>", true});
  fsCmd->addSubcommand(dropFilesSubCmd);

  bootSubCmd = new ConsoleCliCommand("boot", "boot filesystem with ID <fs-id> "
                                     "or name <node-queue> or all (*)");
  bootSubCmd->addOption({"sync-mgm", "force an MGM resynchronization during "
                         "the boot", "--syncmgm"});
  bootSubCmd->addOption({"fsid", "", 1, 1, "<fs-id>|<node-queue>|*", true});
  fsCmd->addSubcommand(bootSubCmd);

  dumpMdSubCmd = new ConsoleCliCommand("dumpmd", "dump all file meta data on "
                                       "this filesystem in query format");
  dumpMdSubCmd->addGroupedOptions(std::vector<CliOption>
				  {{"silent", "don't printout keep an "
                                    "internal reference", "-s"},
                                   {"monitor", "print the full meta data "
                                    "record in env format", "-m"}
                                  });
  dumpMdSubCmd->addOptions(std::vector<CliOption>
			   {{"fid", "dump a list of file IDs stored on this "
                             "filesystem", "--fid"},
                            {"path", "dump a list of file names stored on this "
                             "filesystem", "--path"},
                            {"size", "dump a list of file sizes stored on this "
                             "filesystem", "--size"}
                           });
  dumpMdSubCmd->addOption({"fsid", "", 1, 1, "<fs-id>", true});
  fsCmd->addSubcommand(dumpMdSubCmd);

  statusSubCmd = new ConsoleCliCommand("status", "returns all status variables "
                                       "of a filesystem and calculates the "
                                       "risk of data loss if this filesystem "
                                       "get's removed ");
  statusSubCmd->addOptions(std::vector<CliPositionalOption>
			   {{"fsid", "filesystem ID <fs-id> or mount point, in "
                             "which case the host is set as <this-host>",
                             1, 1, "<fs-id>|<mount-point>", true},
                            {"host", "specifies the host name", 2, 1,
                             "<host>", false}
                           });
  fsCmd->addSubcommand(statusSubCmd);

  cloneSubCmd = new ConsoleCliCommand("clone", "replicate all files in "
                                      "filesystem <src-fs-id> to "
                                      "filesystem <dst-fs-id>");
  cloneSubCmd->addOptions(std::vector<CliPositionalOption>
			  {{"src", "", 1, 1, "<src-fs-id>", true},
                           {"dst", "", 2, 1, "<target-fs-id>", true}
                          });
  fsCmd->addSubcommand(cloneSubCmd);

  compareSubCmd = new ConsoleCliCommand("compare", "compare filesystem "
                                        "<fs-id-1> with <fs-id-2> showing "
                                        "which files are found or not, in each "
                                        "filesystem");
  compareSubCmd->addOptions(std::vector<CliPositionalOption>
			    {{"fsid1", "", 1, 1, "<fs-id-1>", true},
                             {"fsid2", "", 2, 1, "<fs-id-2>", true}
                            });
  fsCmd->addSubcommand(compareSubCmd);

  verifySubCmd = new ConsoleCliCommand("verify", "verify all files in "
                                       "filesystem <fs-id>");
  verifySubCmd->addOptions(std::vector<CliOption>
			   {{"checksum", "trigger the checksum calculation "
                             "during the verification process", "--checksum"},
                            {"commit-checksum", "commit the computed checksum "
                             "to the MGM", "--commit-checksum"},
                            {"commit-size", "commit the file size to the MGM",
                             "--commit-size"},
                            {"commit-fmd", "commit the file metadata to the MGM",
                             "--commit-fmd"}
                           });
  CliOptionWithArgs rate("rate",
                         "restrict the verification speed to <rate> per node",
                         "-r,--rate=", 1, "<rate>", false);
  rate.addEvalFunction(optionIsFloatEvalFunc, 0);
  verifySubCmd->addOption(rate);
  verifySubCmd->addOption({"fsid", "", 1, 1, "<fs-id>", true});
  fsCmd->addSubcommand(verifySubCmd);

  addHelpOptionRecursively(fsCmd);

  parsedCmd = fsCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
  {
    if (parsedCmd == fsCmd)
      goto com_fs_usage;
    else if (parsedCmd->hasValue("help") && parsedCmd == configSubCmd)
      print_config_examples();

    goto bailout;
  }

  if (parsedCmd == addSubCmd)
  {
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=add";
    XrdOucString uuid("");
    XrdOucString manual("");
    XrdOucString fsid("");
    XrdOucString space("default");
    XrdOucString configstatus("off");

    if (addSubCmd->hasValue("manual"))
      fsid = addSubCmd->getValue("manual").c_str();

    uuid = addSubCmd->getValue("uuid").c_str();

    XrdOucString hostport = addSubCmd->getValue("host-port").c_str();
    XrdOucString mountpoint = addSubCmd->getValue("mountpoint").c_str();

    if (addSubCmd->hasValue("schedgroup"))
      space = addSubCmd->getValue("schedgroup").c_str();

    if (addSubCmd->hasValue("status"))
      configstatus = addSubCmd->getValue("status").c_str();

    if (!hostport.beginswith("/eos/"))
    {
      hostport.insert("/eos/", 0);
      hostport.append("/fst");
    }

    if (fsid.length())
    {
      in += "&mgm.fs.fsid=";
      in += fsid;
    }

    in += "&mgm.fs.uuid=";
    in += uuid;
    in += "&mgm.fs.node=";
    in += hostport;
    in += "&mgm.fs.mountpoint=";
    in += mountpoint;
    in += "&mgm.fs.space=";
    in += space;
    in += "&mgm.fs.configstatus=";
    in += configstatus;

    XrdOucEnv* result = client_admin_command(in);
    global_retc = output_result(result);

    goto bailout;
  }

  if (parsedCmd == mvSubCmd)
  {
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=mv";

    in += "&mgm.fs.id=";
    in += mvSubCmd->getValue("src").c_str();
    in += "&mgm.space=";
    in += mvSubCmd->getValue("dst").c_str();

    XrdOucEnv* result = client_admin_command(in);
    global_retc = output_result(result);
    goto bailout;
  }

  if (parsedCmd == lsSubCmd)
  {
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=ls";
    XrdOucString option = "";
    bool highlighting = true;

    if (lsSubCmd->hasValue("monitor"))
    {
      in += "&mgm.outformat=m";
      highlighting = false;
    }

    if (lsSubCmd->hasValue("long"))
      in += "&mgm.outformat=l";

    if (lsSubCmd->hasValue("io"))
      in += "&mgm.outformat=io";

    if (lsSubCmd->hasValue("fsck"))
      in += "&mgm.outformat=fsck";

    if (lsSubCmd->hasValue("drain"))
      in += "&mgm.outformat=d";

    if (lsSubCmd->hasValue("error"))
      in += "&mgm.outformat=e";

    if (lsSubCmd->hasValue("silent"))
      temp_silent = true;

    if (lsSubCmd->hasValue("selection"))
    {
      in += "&mgm.selection=";
      in += lsSubCmd->getValue("selection").c_str();
    }

    XrdOucEnv* result = client_admin_command(in);
    if (!silent && (!temp_silent))
      global_retc = output_result(result, highlighting);
    else
    {
      if (result)
        global_retc = 0;
      else
        global_retc = EINVAL;
    }

    goto bailout;
  }

  if (parsedCmd == configSubCmd)
  {
    XrdOucString identifier = configSubCmd->getValue("fsid").c_str();
    XrdOucString keyval = configSubCmd->getValue("key-value").c_str();

    if ((keyval.find("=")) == STR_NPOS)
    {
      configSubCmd->printUsage();
      print_config_examples();
    }

    std::string is = keyval.c_str();
    std::vector<std::string> token;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(is, token, delimiter);

    if (token.size() != 2)
    {
      configSubCmd->printUsage();
      print_config_examples();
    }

    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=config&mgm.fs.identifier=";
    in += identifier;
    in += "&mgm.fs.key=";
    in += token[0].c_str();
    in += "&mgm.fs.value=";
    in += token[1].c_str();

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }

  if (parsedCmd == rmSubCmd)
  {
    XrdOucString arg = rmSubCmd->getValue("fsid").c_str();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=rm";
    int fsid = atoi(arg.c_str());
    char r1fsid[128];
    sprintf(r1fsid, "%d", fsid);
    char r2fsid[128];
    sprintf(r2fsid, "%04d", fsid);
    if ((arg == r1fsid) || (arg == r2fsid))
    {
      // boot by fsid
      in += "&mgm.fs.id=";
      in += arg;
    }
    else
    {
      XrdOucString mp("");
      XrdOucString hostport = arg;

      if (rmSubCmd->hasValue("mountpoint"))
        mp = rmSubCmd->getValue("mountpoint").c_str();
      else
      {
        mp = arg;
        hostport = XrdSysDNS::getHostName();
      }
      if (!(hostport.find(":") != STR_NPOS))
      {
        in += ":1095";
        hostport += ":1095";
      }

      if (!hostport.beginswith("/eos/"))
      {
        hostport.insert("/eos/", 0);
        hostport.append("/fst");
      }
      in += "&mgm.fs.node=";
      in += hostport;

      in += "&mgm.fs.mountpoint=";

      while (mp.endswith("/"))
      {
        mp.erase(mp.length() - 1);
      }
      in += mp;

      if (access(mp.c_str(), R_OK | X_OK))
      {
        rmSubCmd->printUsage();
      }
    }

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }

  if (parsedCmd == dropDeletionSubCmd)
  {
    XrdOucString arg = dropDeletionSubCmd->getValue("fsid").c_str();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dropdeletion";
    int fsid = atoi(arg.c_str());
    char r1fsid[128];
    sprintf(r1fsid, "%d", fsid);
    char r2fsid[128];
    sprintf(r2fsid, "%04d", fsid);
    if ((arg == r1fsid) || (arg == r2fsid))
    {
      // boot by fsid
      in += "&mgm.fs.id=";
      in += arg;
    }
    else
      dropDeletionSubCmd->printUsage();

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }

  if (parsedCmd == bootSubCmd)
  {
    XrdOucString arg = bootSubCmd->getValue("fsid").c_str();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=boot";
    int fsid = atoi(arg.c_str());
    char r1fsid[128];
    sprintf(r1fsid, "%d", fsid);
    char r2fsid[128];
    sprintf(r2fsid, "%04d", fsid);
    if ((arg == r1fsid) || (arg == r2fsid))
    {
      // boot by fsid
      in += "&mgm.fs.id=";
    }
    else
      in += "&mgm.fs.node=";

    in += arg;

    if (bootSubCmd->hasValue("sync-mgm"))
      in += "&mgm.fs.forcemgmsync=1";

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }

  if (parsedCmd == statusSubCmd)
  {
    XrdOucString arg = statusSubCmd->getValue("fsid").c_str();
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=status";
    int fsid = atoi(arg.c_str());
    char r1fsid[128];
    sprintf(r1fsid, "%d", fsid);
    char r2fsid[128];
    sprintf(r2fsid, "%04d", fsid);
    if ((arg == r1fsid) || (arg == r2fsid))
    {
      // status by fsid
      in += "&mgm.fs.id=";
      in += arg;
    }
    else
    {
      XrdOucString mp = arg;
      in += "&mgm.fs.node=";
      if (statusSubCmd->hasValue("host"))
        in += statusSubCmd->getValue("host").c_str();
      else
      {
        // status by mount point
        char* HostName = XrdSysDNS::getHostName();
        in += HostName;
      }

      in += "&mgm.fs.mountpoint=" + arg;

      while (mp.endswith("/"))
      {
        mp.erase(mp.length() - 1);
      }

      if (access(mp.c_str(), R_OK | X_OK))
        statusSubCmd->printUsage();
    }

    global_retc = output_result(client_admin_command(in));
    goto bailout;
  }

  if (parsedCmd == cloneSubCmd)
  {
    XrdOucString sourceid = cloneSubCmd->getValue("src").c_str();
    XrdOucString targetid = cloneSubCmd->getValue("dst").c_str();

    XrdOucString subcmd = "dumpmd -s ";
    subcmd += sourceid;
    subcmd += " --path";

    com_fs((char*) subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size())
    {
      output_result(CommandEnv);
    }
    else
    {
      if (CommandEnv)
      {
        delete CommandEnv;
        CommandEnv = 0;
      }

      for (unsigned int i = 0; i < files_found.size(); i++)
      {
        if (!files_found[i].length())
          continue;
        XrdOucString line = files_found[i].c_str();
        if (line.beginswith("path="))
        {
          line.replace("path=", "");
          fprintf(stdout, "%06d: %s\n", i, line.c_str());
          // call the replication command here
          subcmd = "replicate ";
          subcmd += line;
          subcmd += " ";
          subcmd += sourceid;
          subcmd += " ";
          subcmd += targetid;
          com_file((char*) subcmd.c_str());
        }
      }
    }

    goto bailout;
  }

  if (parsedCmd == compareSubCmd)
  {
    XrdOucString sourceid = compareSubCmd->getValue("fsid1").c_str();
    XrdOucString targetid = compareSubCmd->getValue("fsid2").c_str();

    XrdOucString subcmd1 = "dumpmd -s ";
    subcmd1 += sourceid;
    subcmd1 += " --path";

    com_fs((char*) subcmd1.c_str());

    std::vector<std::string> files_found1;
    std::vector<std::string> files_found2;
    std::vector<std::string> files_miss1;

    files_found1.clear();
    files_found2.clear();
    files_miss1.clear();

    command_result_stdout_to_vector(files_found1);

    if (CommandEnv)
    {
      delete CommandEnv;
      CommandEnv = 0;
    }


    XrdOucString subcmd2 = "dumpmd -s ";
    subcmd2 += targetid;
    subcmd2 += " --path";

    com_fs((char*) subcmd2.c_str());

    command_result_stdout_to_vector(files_found2);

    if ((!files_found1.size()) && (!files_found2.size()))
    {
      output_result(CommandEnv);
    }

    if (CommandEnv)
    {
      delete CommandEnv;
      CommandEnv = 0;
    }

    for (unsigned int i = 0; i < files_found1.size(); i++)
    {
      bool found = false;
      std::vector<std::string>::iterator it;
      for (it = files_found2.begin(); it != files_found2.end(); ++it)
      {
        if (files_found1[i] == *it)
        {
          files_found2.erase(it);
          found = true;
          break;
        }
      }
      if (!found)
      {
        files_miss1.push_back(files_found1[i]);
      }
    }
    // files_miss1 contains the missing files in 2
    // files_found2 contains the missing files in 1

    for (unsigned int i = 0; i < files_miss1.size(); i++)
    {
      if (files_miss1[i].length())
        fprintf(stderr, "Error: %s => found in %s - missing in %s\n",
                files_miss1[i].c_str(), sourceid.c_str(), targetid.c_str());
    }

    for (unsigned int i = 0; i < files_found2.size(); i++)
    {
      if (files_found2[i].length())
        fprintf(stderr, "Error: %s => found in %s - missing in %s\n",
                files_found2[i].c_str(), targetid.c_str(), sourceid.c_str());
    }

    goto bailout;
  }

  if (parsedCmd == dropFilesSubCmd)
  {
    XrdOucString id = dropFilesSubCmd->getValue("fsid").c_str();

    XrdOucString subcmd = "dumpmd -s ";
    subcmd += id;
    subcmd += " --path";

    com_fs((char*) subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size())
    {
      output_result(CommandEnv);
    }
    else
    {
      if (CommandEnv)
      {
        delete CommandEnv;
        CommandEnv = 0;
      }

      string s;
      fprintf(stdout, "Do you really want to delete ALL %u replica's from "
                      "filesystem %s ?\n", (int) files_found.size(), id.c_str());
      fprintf(stdout, "Confirm the deletion by typing => ");

      XrdOucString confirmation = "";
      for (int i = 0; i < 10; i++)
        confirmation += (int) (9.0 * rand() / RAND_MAX);

      fprintf(stdout, "%s\n", confirmation.c_str());
      fprintf(stdout, "                               => ");
      getline(std::cin, s);
      std::string sconfirmation = confirmation.c_str();
      if (s == sconfirmation)
      {
        fprintf(stdout, "\nDeletion confirmed\n");
        for (unsigned int i = 0; i < files_found.size(); i++)
        {
          if (!files_found[i].length())
            continue;

          XrdOucString line = files_found[i].c_str();
          if (line.beginswith("path="))
          {
            line.replace("path=", "");
            fprintf(stdout, "%06d: %s\n", i, line.c_str());
            // call the replication command here
            subcmd = "drop ";
            subcmd += line;
            subcmd += " ";
            subcmd += id;

            if (dropFilesSubCmd->hasValue("force"))
              subcmd += " -f";

            com_file((char*) subcmd.c_str());
          }
        }
        fprintf(stdout, "=> Deleted %u replicas from filesystem %s\n",
                (unsigned int) files_found.size(), id.c_str());
      }
      else
      {
        fprintf(stdout, "\nDeletion aborted!\n");
      }
    }

    goto bailout;
  }

  if (parsedCmd == verifySubCmd)
  {
    XrdOucString id = verifySubCmd->getValue("fsid").c_str();
    XrdOucString options("");
    const char *fileVerifyOptions[] = {"checksum", "commit-checksum",
                                       "commit-size", "commit-fmd", NULL};

    for (int i = 0; fileVerifyOptions[i] != NULL; i++)
    {
      if (verifySubCmd->hasValue(fileVerifyOptions[i]))
      {
        options += "--";
        options += fileVerifyOptions[i];
      }
    }

    if (verifySubCmd->hasValue("rate"))
    {
      options += "--rate=";
      options += verifySubCmd->getValue("rate").c_str();
    }

    XrdOucString subcmd = "dumpmd -s ";
    subcmd += id;
    subcmd += " --path";

    com_fs((char*) subcmd.c_str());

    std::vector<std::string> files_found;
    files_found.clear();
    command_result_stdout_to_vector(files_found);
    std::vector<std::string>::const_iterator it;
    if (!files_found.size())
    {
      output_result(CommandEnv);
    }
    else
    {
      if (CommandEnv)
      {
        delete CommandEnv;
        CommandEnv = 0;
      }

      for (unsigned int i = 0; i < files_found.size(); i++)
      {
        if (!files_found[i].length())
          continue;
        XrdOucString line = files_found[i].c_str();
        if (line.beginswith("path="))
        {
          line.replace("path=", "");
          fprintf(stdout, "%06d: %s\n", i, line.c_str());
          // call the replication command here
          subcmd = "verify ";
          subcmd += line;
          subcmd += " ";
          subcmd += id;
          subcmd += " ";
          subcmd += options;
          com_file((char*) subcmd.c_str());
        }
      }
    }
    goto bailout;
  }

  if (parsedCmd == dumpMdSubCmd)
  {
    XrdOucString in = "mgm.cmd=fs&mgm.subcmd=dumpmd";
    bool silentcommand = dumpMdSubCmd->hasValue("silent");
    bool monitor = dumpMdSubCmd->hasValue("monitor");
    XrdOucString arg = dumpMdSubCmd->getValue("fsid").c_str();

    if (monitor)
      in += "&mgm.dumpmd.option=m";

    int fsid = atoi(arg.c_str());
    in += "&mgm.fsid=";
    in += (int) fsid;

    if (!monitor)
    {
      if (dumpMdSubCmd->hasValue("path"))
        in += "&mgm.dumpmd.path=1";

      if (dumpMdSubCmd->hasValue("fid"))
        in += "&mgm.dumpmd.fid=1";

      if (dumpMdSubCmd->hasValue("size"))
        in += "&mgm.dumpmd.size=1";
    }

    XrdOucEnv* result = client_admin_command(in);
    if (!silentcommand)
      global_retc = output_result(result);
    else if (result)
      global_retc = 0;
    else
      global_retc = EINVAL;

    goto bailout;
  }

com_fs_usage:

  fprintf(stdout, "\nExamples:\n");
  fprintf(stdout, "\tfs ls --io             List all filesystems with IO statistics\n\n");
  fprintf(stdout, "\tfs boot *              Send boot request to all filesystems\n\n");
  fprintf(stdout, "\tfs dumpmd 100 --path    Dump all logical path names on filesystem 100\n\n");
  fprintf(stdout, "\tfs mv spare default    Move one filesystem from the sapre space into the default space. If default has subgroups the smallest subgroup is selected.\n\n");
  fprintf(stdout, "\tfs mv 100 default.0    Move filesystem 100 into scheduling group default.0\n\n");
  fprintf(stdout, "Report bugs to eos-dev@cern.ch.\n");

 bailout:
  delete fsCmd;

  return (0);
}
