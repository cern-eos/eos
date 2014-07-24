// ----------------------------------------------------------------------
// File: com_attr.cc
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

/* Attribute ls, get, set rm */
int
com_attr (char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString option = "";
  XrdOucString optionstring = "";
  XrdOucString in = "mgm.cmd=attr";
  XrdOucString arg = "";

  if (wants_help(arg1))
    goto com_attr_usage;

  if (subcommand.beginswith("-"))
  {
    option = subcommand;
    option.erase(0, 1);
    optionstring += subcommand;
    optionstring += " ";
    subcommand = subtokenizer.GetToken();
    arg = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  }
  else
  {
    arg = subtokenizer.GetToken();
  }

  if ((!subcommand.length()) || (!arg.length()) ||
      ((subcommand != "ls") && (subcommand != "set") && (subcommand != "get") && (subcommand != "rm")))
    goto com_attr_usage;

  if (subcommand == "ls")
  {
    XrdOucString path = arg;
    if (!path.length())
      goto com_attr_usage;

    path = abspath(path.c_str());

    in += "&mgm.subcmd=ls";
    in += "&mgm.path=";
    in += path;
  }

  if (subcommand == "set")
  {
    XrdOucString key = arg;
    XrdOucString value = "";
    int epos = key.find("=");
    if (epos != STR_NPOS)
    {
      value = key;
      value.erase(0, epos + 1);
      key.erase(epos);
    }
    else
    {
      value = "";
    }

    if (!value.length())
      goto com_attr_usage;

    if (value.beginswith("\""))
    {
      if (!value.endswith("\""))
      {
        do
        {
          XrdOucString morevalue = subtokenizer.GetToken();

          if (morevalue.endswith("\""))
          {
            value += " ";
            value += morevalue;
            break;
          }
          if (!morevalue.length())
          {
            goto com_attr_usage;
          }
          value += " ";
          value += morevalue;
        }
        while (1);
      }
    }

    XrdOucString path = subtokenizer.GetToken();
    if (!key.length() || !value.length() || !path.length())
      goto com_attr_usage;

    path = abspath(path.c_str());

    in += "&mgm.subcmd=set&mgm.attr.key=";
    in += key;
    in += "&mgm.attr.value=";
    in += value;
    in += "&mgm.path=";
    in += path;

    if (key == "default")
    {
      if (value == "replica")
      {
        XrdOucString d1 = optionstring;
        d1 += "set ";
        d1 += "sys.forced.blocksize=4k ";
        d1 += path;
        XrdOucString d2 = optionstring;
        d2 += "set ";
        d2 += "sys.forced.checksum=adler ";
        d2 += path;
        XrdOucString d3 = optionstring;
        d3 += "set ";
        d3 += "sys.forced.layout=replica ";
        d3 += path;
        XrdOucString d4 = optionstring;
        d4 += "set ";
        d4 += "sys.forced.nstripes=2 ";
        d4 += path;
        XrdOucString d5 = optionstring;
        d5 += "set ";
        d5 += "sys.forced.space=default ";
        d5 += path;
        XrdOucString d6 = optionstring;
        d6 += "set ";
        d6 += "sys.forced.blockchecksum=crc32c ";
        d6 += path;
        global_retc = com_attr((char*) d1.c_str()) || com_attr((char*) d2.c_str()) || com_attr((char*) d3.c_str()) || com_attr((char*) d4.c_str()) || com_attr((char*) d5.c_str()) || com_attr((char*) d6.c_str());
        return (0);
      }
      if (value == "raiddp")
      {
        XrdOucString d1 = optionstring;
        d1 += "set ";
        d1 += "sys.forced.blocksize=1M ";
        d1 += path;
        XrdOucString d2 = optionstring;
        d2 += "set ";
        d2 += "sys.forced.checksum=adler ";
        d2 += path;
        XrdOucString d3 = optionstring;
        d3 += "set ";
        d3 += "sys.forced.layout=raiddp ";
        d3 += path;
        XrdOucString d4 = optionstring;
        d4 += "set ";
        d4 += "sys.forced.nstripes=6 ";
        d4 += path;
        XrdOucString d5 = optionstring;
        d5 += "set ";
        d5 += "sys.forced.space=default ";
        d5 += path;
        XrdOucString d6 = optionstring;
        d6 += "set ";
        d6 += "sys.forced.blockchecksum=crc32c ";
        d6 += path;
        global_retc = com_attr((char*) d1.c_str()) || com_attr((char*) d2.c_str()) || com_attr((char*) d3.c_str()) || com_attr((char*) d4.c_str()) || com_attr((char*) d5.c_str()) || com_attr((char*) d6.c_str());
        return (0);
      }
      if (value == "raid6")
      {
        XrdOucString d1 = optionstring;
        d1 += "set ";
        d1 += "sys.forced.blocksize=1M ";
        d1 += path;
        XrdOucString d2 = optionstring;
        d2 += "set ";
        d2 += "sys.forced.checksum=adler ";
        d2 += path;
        XrdOucString d3 = optionstring;
        d3 += "set ";
        d3 += "sys.forced.layout=raid6 ";
        d3 += path;
        XrdOucString d4 = optionstring;
        d4 += "set ";
        d4 += "sys.forced.nstripes=6 ";
        d4 += path;
        XrdOucString d5 = optionstring;
        d5 += "set ";
        d5 += "sys.forced.space=default ";
        d5 += path;
        XrdOucString d6 = optionstring;
        d6 += "set ";
        d6 += "sys.forced.blockchecksum=crc32c ";
        d6 += path;
        global_retc = com_attr((char*) d1.c_str()) || com_attr((char*) d2.c_str()) || com_attr((char*) d3.c_str()) || com_attr((char*) d4.c_str()) || com_attr((char*) d5.c_str()) || com_attr((char*) d6.c_str());
        return (0);
      }
      if (value == "archive")
      {
        XrdOucString d1 = optionstring;
        d1 += "set ";
        d1 += "sys.forced.blocksize=1M ";
        d1 += path;
        XrdOucString d2 = optionstring;
        d2 += "set ";
        d2 += "sys.forced.checksum=adler ";
        d2 += path;
        XrdOucString d3 = optionstring;
        d3 += "set ";
        d3 += "sys.forced.layout=archive ";
        d3 += path;
        XrdOucString d4 = optionstring;
        d4 += "set ";
        d4 += "sys.forced.nstripes=8 ";
        d4 += path;
        XrdOucString d5 = optionstring;
        d5 += "set ";
        d5 += "sys.forced.space=default ";
        d5 += path;
        XrdOucString d6 = optionstring;
        d6 += "set ";
        d6 += "sys.forced.blockchecksum=crc32c ";
        d6 += path;
        global_retc = com_attr((char*) d1.c_str()) || com_attr((char*) d2.c_str()) || com_attr((char*) d3.c_str()) || com_attr((char*) d4.c_str()) || com_attr((char*) d5.c_str()) || com_attr((char*) d6.c_str());
        return (0);
      }
      goto com_attr_usage;
    }
  }

  if (subcommand == "get")
  {
    XrdOucString key = arg;
    XrdOucString path = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    path = abspath(path.c_str());
    in += "&mgm.subcmd=get&mgm.attr.key=";
    in += key;
    in += "&mgm.path=";
    in += path;
  }

  if (subcommand == "rm")
  {
    XrdOucString key = arg;
    XrdOucString path = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    path = abspath(path.c_str());
    in += "&mgm.subcmd=rm&mgm.attr.key=";
    in += key;
    in += "&mgm.path=";
    in += path;
  }

  global_retc = output_result(client_user_command(in));
  return (0);

com_attr_usage:
  fprintf(stdout, "'[eos] attr ..' provides the extended attribute interface for directories in EOS.\n");
  fprintf(stdout, "Usage: attr [OPTIONS] ls|set|get|rm ...\n");
  fprintf(stdout, "Options:\n");

  fprintf(stdout, "attr [-r] ls <path> :\n");
  fprintf(stdout, "                                                : list attributes of path\n");
  fprintf(stdout, " -r : list recursive on all directory children\n");
  fprintf(stdout, "attr [-r] set <key>=<value> <path> :\n");
  fprintf(stdout, "                                                : set attributes of path (-r recursive)\n");
  fprintf(stdout, "attr [-r] set default=replica|raiddp|raid6|archive <path> :\n");
  fprintf(stdout, "                                                : set attributes of path (-r recursive) to the EOS defaults for replicas,dual-parity-raid (4+2), raid-6 (4+2) or archive layouts (5+3).\n");

  //  fprintf(stdout,"attr [-r] set default=raiddp <path> :\n");
  //  fprintf(stdout,"                                                : set attributes of path (-r recursive) to the EOS defaults for dual parity raid (4+2).\n");

  //  fprintf(stdout,"attr [-r] set default=reeds <path> :\n");
  //  fprintf(stdout,"                                                : set attributes of path (-r recursive) to the EOS defaults for reed solomon (4+2).\n");

  fprintf(stdout, " -r : set recursive on all directory children\n");
  fprintf(stdout, "attr [-r] get <key> <path> :\n");
  fprintf(stdout, "                                                : get attributes of path (-r recursive)\n");
  fprintf(stdout, " -r : get recursive on all directory children\n");
  fprintf(stdout, "attr [-r] rm  <key> <path> :\n");
  fprintf(stdout, "                                                : delete attributes of path (-r recursive)\n\n");
  fprintf(stdout, " -r : delete recursive on all directory children\n");

  fprintf(stdout, "If <key> starts with 'sys.' you have to be member of the sudoer group to see this attributes or modify.\n\n");

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
  fprintf(stdout, "         sys.forced.atomic=1                   : if present enforce atomic uploads e.g. files appear only when their upload is complete - during the upload they have the name <dirname>/.<basename>.<uuid>\n");
  fprintf(stdout, "         sys.mtime.propagation=1               : if present a change under this directory propagates an mtime change up to all parents until the attribute is not present anymore\n");
  fprintf(stdout, "         sys.allow.oc.sync=1                   : if present, OwnCloud clients can sync pointing to this subtree\n");
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
  fprintf(stdout, "         sys.versioning=<n>                    : keep <n> versions of a file e.g. if you upload a file <n+10> times it will keep the last <n+1> versions\n");
  // ---------------------------------------------------------------------------
  fprintf(stdout, "         sys.acl=<acllist>                     : set's an ACL which is honored for open,rm & rmdir operations\n");
  fprintf(stdout, "               => <acllist> = <rule1>,<rule2>...<ruleN> is a comma separated list of rules\n");
  fprintf(stdout, "               => <rule> = u:<uid|username>|g:<gid|groupname>|egroup:<name>|z:{irwxomqc(!d)(+d)(!u)(+u)} \n\n");
  fprintf(stdout, "               e.g.: <acllist=\"u:300:rw,g:z2:rwo:egroup:eos-dev:rwx,u:500:rwm!d:u:600:rwqc\"\n\n");
  fprintf(stdout, "               => user id 300 can read + write\n");
  fprintf(stdout, "               => group z2 can read + write-once (create new files but can't delete)\n");
  fprintf(stdout, "               => members of egroup 'eos-dev' can read & write & browse\n");
  fprintf(stdout, "               => user id 500 can read + write into and chmod(m), but cannot delete the directory itself(!d)!\n");
  fprintf(stdout, "               => user id 600 can read + write and administer the quota node(q) and can change the directory ownership in child directories(c)\n");
  fprintf(stdout, "              '+d' : this tag can be used to overwrite a group rule excluding deletion via '!d' for certain users\n");
  fprintf(stdout, "              '+u' : this tag can be used to overwrite a rul excluding updates via '!u'\n");
  fprintf(stdout, "              'c'  : this tag can be used to grant chown permissions\n");
  fprintf(stdout, "              'q'  : this tag can be used to grant quota administrator permissions\n");
  fprintf(stdout, "               e.g.: sys.acl='z:!d' => 'z' is a rule for every user besides root e.g. nobody can delete here'b\n");
  fprintf(stdout, "                     sys.acl='z:i' => directory becomes immutable");
  fprintf(stdout, "         sys.eval.useracl                      : enables the evaluation of user acls if key is defined\n");
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
  fprintf(stdout, "         user.forced.atomic=1                  : s.a.\n");
  fprintf(stdout, "         user.stall.unavailable=<sec>          : s.a.\n");
  fprintf(stdout, "         user.acl=<acllist>                    : s.a.\n");
  fprintf(stdout, "         user.versioning=<n>                   : s.a.\n");
  fprintf(stdout, "         user.tag=<tag>                        : Tag <tag> to group files for scheduling and flat file distribution. Use this tag to define datasets (if <tag> contains space use tag with quotes)\n");
  fprintf(stdout, "\n\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "Examples:\n");
  fprintf(stdout, "...................\n");
  fprintf(stdout, "....... Layouts ...\n");
  fprintf(stdout, "...................\n");
  fprintf(stdout, "- set 2 replica as standard layout ...\n");
  fprintf(stdout, "     |eos> attr set default=replicae /eos/instance/2-replica\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- set RAID-6 4+2 as standard layout ...\n");
  fprintf(stdout, "     |eos> attr set default=raid6 /eos/instance/raid-6\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- set ARCHIVE 5+3 as standard layout ...\n");
  fprintf(stdout, "     |eos> attr set default=archive /eos/instance/archive\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- re-configure a layout for different number of stripes (e.g. 10) ...\n");
  fprintf(stdout, "     |eos> attr set sys.forced.stripes=10 /eos/instance/archive\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "................\n");
  fprintf(stdout, "....... ACLs ...\n");
  fprintf(stdout, "................\n");
  fprintf(stdout, "- forbid deletion and updates for group xx in a directory ...\n");
  fprintf(stdout, "     |eos> attr set sys.acl=g:xx::!d!u /eos/instance/no-update-deletion\n");
  fprintf(stdout, "\n");
  fprintf(stdout, ".....................\n");
  fprintf(stdout, "....... LRU Cache ...\n");
  fprintf(stdout, ".....................\n");
  fprintf(stdout, "- configure a volume based LRU cache with a low/high watermark \n");
  fprintf(stdout, "  e.g. when the cache reaches the high watermark it cleans the oldest files untile low-watermark is reached ...\n");
  fprintf(stdout, "     |eos> quota set -g 99 -v 1T /eos/instance/cache/                           # define project quota on the cache\n");
  fprintf(stdout, "     |eos> attr set sys.lru.watermark=90:95  /eos/instance/cache/               # define 90 as low and 95 as high watermark\n");
  fprintf(stdout, "     |eos> attr set sys.force.atime=300 /eos/dev/instance/cache/                # track atime with a time resolution of 5 minutes\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- configure clean-up of empty directories ...\n");
  fprintf(stdout, "     |eos> attr set sys.lru.expire.empty=\"1h\" /eos/dev/instance/empty/          # remove automatically empty directories if they are older than 1 hour\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- configure a time based LRU cache with an expiration time ...\n");
  fprintf(stdout, "     |eos> attr set sys.lru.expire.match=\"*.root:1mo,*.tgz:1w\"  /eos/dev/instance/scratch/\n");
  fprintf(stdout, "                                                                                # files with suffix *.root get removed after a month, files with *.tgz after one week\n");
  fprintf(stdout, "     |eos> attr set sys.lru.expire.match=\"*:1d\" /eos/dev/instance/scratch/      # all files older than a day are automatically removed\n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- configure automatic layout conversion if a file has reached a defined age ...\n");
  fprintf(stdout, "     |eos> attr set sys.lru.convert.match=\"*:1mo\" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next\n");
  fprintf(stdout, "     |eos> attr set sys.conversion.*=20640542 /eos/dev/instance/convert/          # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 \n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, "- configure automatic layout conversion if a file has not been used during the last 6 month ...\n");
  fprintf(stdout, "     |eos> attr set sys.force.atime=1w /eos/dev/instance/cache/                   # track atime with a time resolution of one week\n");
  fprintf(stdout, "     |eos> attr set sys.lru.convert.match=\"*:6mo\" /eos/dev/instance/convert/    # convert all files older than a month to the layout defined next\n");
  fprintf(stdout, "     |eos> attr set sys.conversion.*=20640542  /eos/dev/instance/convert/         # define the conversion layout (hex) for the match rule '*' - this is RAID6 4+2 \n");
  fprintf(stdout, "--------------------------------------------------------------------------------\n");
  fprintf(stdout, ".......................\n");
  fprintf(stdout, "....... Recycle Bin ...\n");
  fprintf(stdout, ".......................\n");
  fprintf(stdout, "- configure a recycle bin with 1 week garbage collection and 100 TB space ...\n");
  fprintf(stdout, "     |eos> recycle config --lifetime 604800                                     # set the lifetime to 1 week\n");
  fprintf(stdout, "     |eos> recycle config --size 100T                                           # set the size of 100T\n");
  fprintf(stdout, "     |eos> recycle config --add-bin /eos/dev/instance/                          # add's the recycle bin to the subtree /eos/dev/instance\n");
  fprintf(stdout, ".......................\n");
  fprintf(stdout, ".... Atomic Uploads ...\n");
  fprintf(stdout, ".......................\n");
  fprintf(stdout, "     |eos> attr set sys.forced.atomic=1 /eos/dev/instance/atomic/\n");
  return (0);
}
