// ----------------------------------------------------------------------
// File: com_attr.cc
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

/* Attribute ls, get, set rm */
int
com_attr (char* arg1)
{
  XrdOucString option = "";
  XrdOucString in = "mgm.cmd=attr";
  XrdOucString path;
  ConsoleCliCommand *parsedCmd, *attrCmd, *lsSubCmd, *setSubCmd,
    *getSubCmd, *rmSubCmd, *resetSubCmd;

  attrCmd = new ConsoleCliCommand("attr", "provides the extended attribute "
                                  "interface for directories in EOS.");

  CliPositionalOption pathOption("path", "", 1, 1, "<path>", true);

  lsSubCmd = new ConsoleCliCommand("ls", "list attributes of path");
  CliOption recursiveOption("recursive",
                            "list recursively all directory children", "-r");
  lsSubCmd->addOption(recursiveOption);
  lsSubCmd->addOption(pathOption);
  attrCmd->addSubcommand(lsSubCmd);

  resetSubCmd = new ConsoleCliCommand("reset", "set attributes of path to the "
                                      "EOS defaults");
  resetSubCmd->addGroupedOptions(std::vector<CliOption>
                                 {{"replica", "set the attributes of <path> to "
                                   "the EOS's defaults for replicas",
                                   "--replica"},
                                  {"raiddp", "set the attributes of <path> to "
                                   "the EOS's defaults for dual-parity-raid "
                                   "(4+2)", "--raiddp"},
                                  {"raid6", "set the attributes of <path> to "
                                   "the EOS's defaults for raid-6 (4+2)",
                                   "--raid6"},
                                  {"archive", "set the attributes of <path> to "
                                   "the EOS's defaults for archive layouts "
                                   "(5+3)", "--archive"}
                                 })->setRequired(true);
  resetSubCmd->addOption(pathOption);
  recursiveOption.setDescription("reset recursively on all directory children");
  resetSubCmd->addOption(recursiveOption);
  attrCmd->addSubcommand(resetSubCmd);

  pathOption.setPosition(2);
  setSubCmd = new ConsoleCliCommand("set", "set attributes of path");
  setSubCmd->addOption({"key-value", "", 1, 1, "<key>=<value>", true});
  setSubCmd->addOption(pathOption);
  recursiveOption.setDescription("set recursively on all directory children");
  setSubCmd->addOption(recursiveOption);
  attrCmd->addSubcommand(setSubCmd);

  CliPositionalOption keyOption("key", "", 1, 1, "<key>", true);
  getSubCmd = new ConsoleCliCommand("get", "get attributes of path");
  recursiveOption.setDescription("get recursively on all directory children");
  getSubCmd->addOption(recursiveOption);
  getSubCmd->addOption(keyOption);
  getSubCmd->addOption(pathOption);
  attrCmd->addSubcommand(getSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "rm attributes of path");
  recursiveOption.setDescription("delete recursively on all directory children");
  rmSubCmd->addOption(recursiveOption);
  rmSubCmd->addOption(keyOption);
  rmSubCmd->addOption(pathOption);
  attrCmd->addSubcommand(rmSubCmd);

  addHelpOptionRecursively(attrCmd);

  parsedCmd = attrCmd->parse(arg1);

  if (parsedCmd == attrCmd)
  {
    if (!checkHelpAndErrors(attrCmd))
      parsedCmd->printUsage();

    goto com_examples;
  }
  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd->hasValue("recursive"))
    in += "&mgm.option=r";

  path = cleanPath(parsedCmd->getValue("path"));

  if (parsedCmd == lsSubCmd)
  {
    in += "&mgm.subcmd=ls";
    in += "&mgm.path=" + path;
  }
  else if (parsedCmd == setSubCmd)
  {
    XrdOucString key = setSubCmd->getValue("key-value").c_str();
    XrdOucString value = "";
    int epos = key.find("=");

    if (epos != STR_NPOS)
    {
      value = key;
      value.erase(0, epos + 1);
      key.erase(epos);
    }
    else
      value = "";

    if (!key.length() || !value.length())
    {
      fprintf(stdout, "Error: Please provide a key and a value in the "
              "following way <key>=<value>\n");
      setSubCmd->printUsage();
    }

    in += "&mgm.subcmd=set&mgm.attr.key=" + key;
    in += "&mgm.attr.value=" + value;
    in += "&mgm.path=" + path;
  }
  else if (parsedCmd == resetSubCmd)
  {
    std::string blocksize, layout, nstripes;

    if (resetSubCmd->hasValue("replica"))
    {
      blocksize = "4k";
      layout = "replica";
      nstripes = "2";
    }
    else if (resetSubCmd->hasValue("raiddp"))
    {
      blocksize = "1M";
      layout = "raiddp";
      nstripes = "6";
    }
    else if (resetSubCmd->hasValue("raid6"))
    {
      blocksize = "1M";
      layout = "raid6";
      nstripes = "6";
    }
    else if (resetSubCmd->hasValue("archive"))
    {
      blocksize = "1M";
      layout = "archive";
      nstripes = "8";
    }

    const std::string args[] = {"sys.forced.blocksize=" + blocksize,
                                "sys.forced.checksum=adler",
                                "sys.forced.layout=" + layout,
                                "sys.forced.nstripes=" + nstripes,
                                "sys.forced.space=default",
                                "sys.forced.blockchecksum=crc32c",
                                ""};

    for (int i = 0; args[i].length() > 0; i++)
    {
      std::string command = "set ";
      if (resetSubCmd->hasValue("recursive"))
        command += " -r ";
      command += args[i] + " " + path.c_str();

      global_retc = global_retc || com_attr((char *) command.c_str());
    }

    goto bailout;
  }
  else if (parsedCmd == getSubCmd || parsedCmd == rmSubCmd)
  {
    in += "&mgm.subcmd=";
    in += parsedCmd == rmSubCmd ? "rm" : "get";
    in += "&mgm.attr.key=";
    in += parsedCmd->getValue("key").c_str();
    in += "&mgm.path=" + path;
  }

  global_retc = output_result(client_user_command(in));

  goto bailout;

com_examples:

  fprintf(stdout, "\nIf <key> starts with 'sys.' you have to be member of the sudoer group to see this attributes or modify.\n\n");

  fprintf(stdout, "Administrator Variables:\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.forced.space=<space>              : enforces to use <space>    [configuration dependend]\n");
  fprintf(stdout, "         sys.forced.group=<group>              : enforces to use <group>, where <group> is the numerical index of <space>.<n>    [configuration dependend]\n");
  //  fprintf(stdout,"         sys.forced.layout=<layout>            : enforces to use <layout>   [<layout>=(plain,replica,raiddp,reeds)]\n");
  fprintf(stdout, "         sys.forced.layout=<layout>            : enforces to use <layout>   [<layout>=(plain,replica)]\n");
  fprintf(stdout, "         sys.forced.checksum=<checksum>        : enforces to use file-level checksum <checksum>\n");
  fprintf(stdout, "                                              <checksum> = adler,crc32,crc32c,md5,sha\n");
  fprintf(stdout, "         sys.forced.blockchecksum=<checksum>   : enforces to use block-level checksum <checksum>\n");
  fprintf(stdout, "                                              <checksuM> = adler,crc32,crc32c,md5,sha\n");
  fprintf(stdout, "         sys.forced.nstripes=<n>               : enforces to use <n> stripes[<n>= 1..16]\n");
  fprintf(stdout, "         sys.forced.blocksize=<w>              : enforces to use a blocksize of <w> - <w> can be 4k,64k,128k,256k or 1M \n");
  fprintf(stdout, "         sys.forced.nouserlayout=1             : disables the user settings with user.forced.<xxx>\n");
  fprintf(stdout, "         sys.forced.nofsselection=1            : disables user defined filesystem selection with environment variables for reads\n");
  fprintf(stdout, "         sys.forced.bookingsize=<bytes>        : set's the number of bytes which get for each new created replica\n");
  fprintf(stdout, "         sys.forced.minimumsize=<bytes>        : set's the minimum number of bytes a file to be stored must have\n");
  fprintf(stdout, "         sys.forced.maximumsize=<bytes>        : set's the maximum number of bytes a file to be stored can have\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.force.atime=<age>                 : enables atime tagging under that directory. <age> is the minimum age before the access time is stored as change time.\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "         sys.lru.expire.empty=<age>            : delete empty directories older than <age>\n");
  fprintf(stdout, "         sys.lru.expire.match=[match1:<age1>,match2:<age2>..]\n");
  fprintf(stdout, "                                               : defines the rule that files with a given match will be removed if \n");
  fprintf(stdout, "                                                 they havn't been accessed longer than <age> ago. <age> is defined like 3600,3600s,60min,1h,1mo,y ...\n");
  fprintf(stdout, "         sys.lru.watermark=<low>:<high>        : if the watermark reaches more than <high> %%, files will be removed \n");
  fprintf(stdout, "                                                 until the usage is reaching <low> %%.\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "         sys.lru.convert.match=[match1:<age1>,match2:<age2>...]\n");
  fprintf(stdout, "                                                 defines the rule that files with a given match will be converted to the layouts defined by sys.conversion.<match> when their access time reaches <age>.\n");
  fprintf(stdout, "\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.stall.unavailable=<sec>           : stall clients for <sec> seconds if a needed file system is unavailable\n");
  fprintf(stdout, "         sys.heal.unavailable=<tries>          : try to heal an unavailable file for atleast <tries> times - must be >= 3 !!\n");
  fprintf(stdout, "                                                     - the product <heal-tries> * <stall-time> should be bigger than the expect replication time for a given filesize!\n\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.redirect.enoent=<host[:port]>     : redirect clients opening non existing files to <host[:port]>\n");
  fprintf(stdout, "               => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eog/public \n\n");
  fprintf(stdout, "         sys.redirect.enonet=<host[:port]>     : redirect clients opening unaccessible files to <host[:port]>\n");
  fprintf(stdout, "               => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eog/public \n\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.recycle=....                      : define the recycle bin for that directory - WARNING: never modify this variables via 'attr' ... use the 'recycle' interface\n");
  fprintf(stdout, "         sys.recycle.keeptime=<seconds>        : define the time how long files stay in a recycle bin before final deletions taks place. This attribute has to defined on the recycle - WARNING: never modify this variables via 'attr' ... use the 'recycle' interface\n\n");
// ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.acl=<acllist>                     : set's an ACL which is honoured for open,rm & rmdir operations\n");
  fprintf(stdout, "               => <acllist> = <rule1>,<rule2>...<ruleN> is a comma separated list of rules\n");
  fprintf(stdout, "               => <rule> = u:<uid|username>|g:<gid|groupname>|egroup:<name>:{rwxomqc(!d)(+d)(!u)(+u)} \n\n");
  fprintf(stdout, "               e.g.: <acllist=\"u:300:rw,g:z2:rwo:egroup:eos-dev:rwx,u:500:rwm!d:u:600:rwqc\"\n\n");
  fprintf(stdout, "               => user id 300 can read + write\n");
  fprintf(stdout, "               => group z2 can read + write-once (create new files but can't delete)\n");
  fprintf(stdout, "               => members of egroup 'eos-dev' can read & write & browse\n");
  fprintf(stdout, "               => user id 500 can read + write into and chmod(m), but cannot delete the directory itself(!d)!\n");
  fprintf(stdout, "               => user id 600 can read + write and administer the quota node(q) and can change the directory ownership in child directories(c)\n");
  fprintf(stdout, "              '+d' : this tag can be used to overwrite a group rule excluding deletion via '!d' for certain users\n");
  fprintf(stdout, "              '+u' : this tag can be used to overwrite a rul excluding updates via '!u'\n");
  fprintf(stdout, "              'c'  : this tag can be used to grant chown permissions\n");
  fprintf(stdout,"               'q'  : this tag can be used to grant quota administrator permissions\n");
  fprintf(stdout, "         sys.owner.auth=<owner-auth-list>                 : set's additional owner on a directory - open/create + mkdir commands will use the owner id for operations if the client is part of the owner authentication list");
  fprintf(stdout, "               => <owner-auth-list> = <auth1>:<name1>,<auth2>:<name2  e.g. krb5:nobody,gsi:DN=...\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "User Variables:\n");
  fprintf(stdout, "         user.forced.space=<space>             : s.a.\n");
  fprintf(stdout, "         user.forced.layout=<layout>           : s.a.\n");
  fprintf(stdout, "         user.forced.checksum=<checksum>       : s.a.\n");
  fprintf(stdout, "         user.forced.blockchecksum=<checksum>  : s.a.\n");
  fprintf(stdout, "         user.forced.nstripes=<n>              : s.a.\n");
  fprintf(stdout, "         user.forced.blocksize=<w>             : s.a.\n");
  fprintf(stdout, "         user.forced.nouserlayout=1            : s.a.\n");
  fprintf(stdout, "         user.forced.nofsselection=1           : s.a.\n");
  fprintf(stdout, "         user.stall.unavailable=<sec>          : s.a.\n");
  fprintf(stdout, "         user.acl=<acllist>                    : s.a.\n");
  fprintf(stdout, "         user.tag=<tag>                        : Tag <tag> to group files for scheduling and flat file distribution. Use this tag to define datasets (if <tag> contains space use tag with quotes)\n");
  fprintf(stdout, "\n\n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"Examples:\n");
  fprintf(stdout,"...................\n");
  fprintf(stdout,"....... Layouts ...\n");
  fprintf(stdout,"...................\n");
  fprintf(stdout,"- set 2 replica as standard layout ...\n");
  fprintf(stdout,"     |eos> attr reset --replica /eos/instance/2-replica\n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- set RAID-6 4+2 as standard layout ...\n");
  fprintf(stdout,"     |eos> attr reset --raid6 /eos/instance/raid-6\n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- set ARCHIVE 5+3 as standard layout ...\n");
  fprintf(stdout,"     |eos> attr reset --archive /eos/instance/archive\n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- re-configure a layout for different number of stripes (e.g. 10) ...\n");
  fprintf(stdout,"     |eos> attr set sys.forced.stripes=10 /eos/instance/archive\n");
  fprintf(stdout, "\n");
  fprintf(stdout,"................\n");
  fprintf(stdout,"....... ACLs ...\n");
  fprintf(stdout,"................\n");
  fprintf(stdout,"- forbid deletion and updates for group xx in a directory ...\n");
  fprintf(stdout,"     |eos> attr set sys.acl=g:xx::!d!u /eos/instance/no-update-deletion\n");
  fprintf(stdout, "\n");
  fprintf(stdout,".....................\n");
  fprintf(stdout,"....... LRU Cache ...\n");
  fprintf(stdout,".....................\n");
  fprintf(stdout,"- configure a volume based LRU cache with a low/high watermark \n");
  fprintf(stdout,"  e.g. when the cache reaches the high watermark it cleans the oldest files untile low-watermark is reached ...\n");
  fprintf(stdout,"     |eos> quota set -g 99 -v 1T /eos/instance/cache/                           # define project quota on the cache\n");
  fprintf(stdout,"     |eos> attr set sys.lru.watermark=90:95  /eos/instance/cache/               # define 90 as low and 95 as high watermark\n");
  fprintf(stdout,"     |eos> attr set sys.force.atime=300 /eos/dev/instance/cache/                # track atime with a time resolution of 5 minutes\n");
  fprintf(stdout, "\n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- configure clean-up of empty directories ...\n");
  fprintf(stdout,"     |eos> attr set sys.lru.expire.empty=\"1h\" /eos/dev/instance/empty/          # remove automatically empty directories if they are older than 1 hour\n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- configure a time based LRU cache with an expiration time ...\n");
  fprintf(stdout,"     |eos> attr set sys.lru.expire.match=\"*.root:1mo,*.tgz:1w\"  /eos/dev/instance/scratch/\n");
  fprintf(stdout,"                                                                                # files with suffix *.root get removed after a month, files with *.tgz after one week\n");
  fprintf(stdout,"     |eos> attr set sys.lru.expire.match=\"*:1d\" /eos/dev/instance/scratch/      # all files older than a day are automatically removed\n"); 
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- configure automatic layout conversion if a file has reached a defined age ...\n");
  fprintf(stdout,"     |eos> attr set sys.lru.convert.match=\"*:1mo\" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next\n");
  fprintf(stdout,"     |eos> attr set sys.conversion.*=20640542                                     # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 \n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,"- configure automatic layout conversion if a file has not been used during the last 6 month ...\n");
  fprintf(stdout,"     |eos> attr set sys.force.atime=1w /eos/dev/instance/cache/                   # track atime with a time resolution of one week\n");
  fprintf(stdout,"     |eos> attr set sys.lru.convert.match=\"*:6mo\" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next\n");
  fprintf(stdout,"     |eos> attr set sys.conversion.*=20640542                                     # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 \n");
  fprintf(stdout,"--------------------------------------------------------------------------------\n");
  fprintf(stdout,".......................\n");
  fprintf(stdout,"....... Recycle Bin ...\n");
  fprintf(stdout,".......................\n");
  fprintf(stdout,"- configure a recycle bin with 1 week garbage collection and 100 TB space ...\n");
  fprintf(stdout,"     |eos> recycle config --lifetime 604800                                     # set the lifetime to 1 week\n");
  fprintf(stdout,"     |eos> recycle config --size 100T                                           # set the size of 100T\n");
  fprintf(stdout,"     |eos> recycle config --add-bin /eos/dev/instance/                          # add's the recycle bin to the subtree /eos/dev/instance\n");

 bailout:
  delete attrCmd;

  return (0);
}
