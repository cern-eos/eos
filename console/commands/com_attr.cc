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
com_attr (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString optionstring="";
  XrdOucString in = "mgm.cmd=attr";
  XrdOucString arg = "";

  if (subcommand.beginswith("-")) {
    option = subcommand;
    option.erase(0,1);
    optionstring += subcommand; optionstring += " ";
    subcommand = subtokenizer.GetToken();
    arg = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  } else {
    arg = subtokenizer.GetToken();
  }
    
  if ((!subcommand.length()) || (!arg.length()) ||
      ( (subcommand != "ls") && (subcommand != "set") && (subcommand != "get") && (subcommand != "rm") ))
    goto com_attr_usage;

  if ( subcommand == "ls") {
    XrdOucString path  = arg;
    if (!path.length())
      goto com_attr_usage;

    path = abspath(path.c_str());

    in += "&mgm.subcmd=ls";
    in += "&mgm.path=";in += path;
  }

  if ( subcommand == "set") {
    XrdOucString key   = arg;
    XrdOucString value = "";
    int epos = key.find("=");
    if (epos != STR_NPOS) {
      value = key;
      value.erase(0,epos+1);
      key.erase(epos);
    } else {
      value = "";
    }

    if (!value.length() )
      goto com_attr_usage;

    if (value.beginswith("\"")) {
      if (!value.endswith("\"")) {
        do {
          XrdOucString morevalue = subtokenizer.GetToken();
          
          if (morevalue.endswith("\"")) {
            value += " ";
            value += morevalue;
            break;
          }
          if (!morevalue.length()) {
            goto com_attr_usage;
          }
          value += " ";
          value += morevalue;
        } while (1);
      }
    }

    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !value.length() || !path.length()) 
      goto com_attr_usage;

    path = abspath(path.c_str());

    in += "&mgm.subcmd=set&mgm.attr.key="; in += key;
    in += "&mgm.attr.value="; in += value;
    in += "&mgm.path="; in += path;

    if (key=="default") {
      if (value == "replica") {
        XrdOucString d1 = optionstring; d1 += "set "; d1 += "sys.forced.blocksize=4k ";   d1 += path;
        XrdOucString d2 = optionstring; d2 += "set "; d2 += "sys.forced.checksum=adler "; d2 += path;
        XrdOucString d3 = optionstring; d3 += "set "; d3 += "sys.forced.layout=replica "; d3 += path;
        XrdOucString d4 = optionstring; d4 += "set "; d4 += "sys.forced.nstripes=2 ";     d4 += path;
        XrdOucString d5 = optionstring; d5 += "set "; d5 += "sys.forced.space=default ";  d5 += path;
        XrdOucString d6 = optionstring; d6 += "set "; d6 += "sys.forced.blockchecksum=crc32c ";  d6 += path;
        global_retc = com_attr((char*)d1.c_str()) || com_attr((char*)d2.c_str()) || com_attr((char*)d3.c_str()) || com_attr((char*)d4.c_str()) || com_attr((char*)d5.c_str()) || com_attr((char*)d6.c_str());
        return (0);
      } 
      if (value == "raidDP") {
        XrdOucString d1 = optionstring; d1 += "set "; d1 += "sys.forced.blocksize=4k ";   d1 += path;
        XrdOucString d2 = optionstring; d2 += "set "; d2 += "sys.forced.checksum=adler "; d2 += path;
        XrdOucString d3 = optionstring; d3 += "set "; d3 += "sys.forced.layout=raidDP "; d3 += path;
        XrdOucString d4 = optionstring; d4 += "set "; d4 += "sys.forced.nstripes=6 ";     d4 += path;
        XrdOucString d5 = optionstring; d5 += "set "; d5 += "sys.forced.space=default ";  d5 += path;
        XrdOucString d6 = optionstring; d6 += "set "; d6 += "sys.forced.blockchecksum=crc32c ";  d6 += path;
        global_retc = com_attr((char*)d1.c_str()) || com_attr((char*)d2.c_str()) || com_attr((char*)d3.c_str()) || com_attr((char*)d4.c_str()) || com_attr((char*)d5.c_str()) || com_attr((char*)d6.c_str());
        return (0);
      } 
      if (value == "reedS") {
        XrdOucString d1 = optionstring; d1 += "set "; d1 += "sys.forced.blocksize=4k ";   d1 += path;
        XrdOucString d2 = optionstring; d2 += "set "; d2 += "sys.forced.checksum=adler "; d2 += path;
        XrdOucString d3 = optionstring; d3 += "set "; d3 += "sys.forced.layout=reedS "; d3 += path;
        XrdOucString d4 = optionstring; d4 += "set "; d4 += "sys.forced.nstripes=6 ";     d4 += path;
        XrdOucString d5 = optionstring; d5 += "set "; d5 += "sys.forced.space=default ";  d5 += path;
        XrdOucString d6 = optionstring; d6 += "set "; d6 += "sys.forced.blockchecksum=crc32c ";  d6 += path;
        global_retc = com_attr((char*)d1.c_str()) || com_attr((char*)d2.c_str()) || com_attr((char*)d3.c_str()) || com_attr((char*)d4.c_str()) || com_attr((char*)d5.c_str()) || com_attr((char*)d6.c_str());
        return (0);
      } 
      goto com_attr_usage;
    }
  }

  if ( subcommand == "get") {
    XrdOucString key   = arg;
    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    path = abspath(path.c_str());
    in += "&mgm.subcmd=get&mgm.attr.key="; in += key;
    in += "&mgm.path="; in += path;
  }

  if ( subcommand == "rm") {
    XrdOucString key   = arg;
    XrdOucString path  = subtokenizer.GetToken();
    if (!key.length() || !path.length())
      goto com_attr_usage;
    path = abspath(path.c_str());
    in += "&mgm.subcmd=rm&mgm.attr.key="; in += key;
    in += "&mgm.path="; in += path;
  }
 
  global_retc = output_result(client_user_command(in));
  return (0); 

 com_attr_usage:
  printf("'[eos] attr ..' provides the extended attribute interface for directories in EOS.\n");
  printf("Usage: attr [OPTIONS] ls|set|get|rm ...\n");
  printf("Options:\n");

  printf("attr [-r] ls <path> :\n");
  printf("                                                : list attributes of path\n");  
  printf(" -r : list recursive on all directory children\n");
  printf("attr [-r] set <key>=<value> <path> :\n");
  printf("                                                : set attributes of path (-r recursive)\n");
  printf("attr [-r] set default=replica <path> :\n");
  printf("                                                : set attributes of path (-r recursive) to the EOS defaults for replicas.\n");

  printf("attr [-r] set default=raidDP <path> :\n");
  printf("                                                : set attributes of path (-r recursive) to the EOS defaults for dual parity raid (4+2).\n");

  printf("attr [-r] set default=reedS <path> :\n");
  printf("                                                : set attributes of path (-r recursive) to the EOS defaults for reed solomon (4+2).\n");

  printf(" -r : set recursive on all directory children\n");
  printf("attr [-r] get <key> <path> :\n");
  printf("                                                : get attributes of path (-r recursive)\n");
  printf(" -r : get recursive on all directory children\n");
  printf("attr [-r] rm  <key> <path> :\n");
  printf("                                                : delete attributes of path (-r recursive)\n\n");
  printf(" -r : delete recursive on all directory children\n");

  printf("If <key> starts with 'sys.' you have to be member of the sudoer group to see this attributes or modify.\n\n");

  printf("Administrator Variables:\n");
  printf("         sys.forced.space=<space>              : enforces to use <space>    [configuration dependend]\n");
  printf("         sys.forced.layout=<layout>            : enforces to use <layout>   [<layout>=(plain,replica,raidDP,reedS)]\n");
  printf("         sys.forced.checksum=<checksum>        : enforces to use file-level checksum <checksum>\n");
  printf("                                              <checksum> = adler,crc32,crc32c,md5,sha\n");
  printf("         sys.forced.blockchecksum=<checksum>   : enforces to use block-level checksum <checksum>\n");
  printf("                                              <checksuM> = adler,crc32,crc32c,md5,sha\n");
  printf("         sys.forced.nstripes=<n>               : enforces to use <n> stripes[<n>= 1..16]\n");
  printf("         sys.forced.blocksize=<w>              : enforces to use a blocksize of <w> - <w> can be 4k,64k,128k,256k or 1M \n");
  printf("         sys.forced.nouserlayout=1             : disables the user settings with user.forced.<xxx>\n");
  printf("         sys.forced.nofsselection=1            : disables user defined filesystem selection with environment variables for reads\n");
  printf("         sys.forced.bookingsize=<bytes>        : set's the number of bytes which get for each new created replica\n");
  printf("         sys.stall.unavailable=<sec>           : stall clients for <sec> seconds if a needed file system is unavailable\n");
  printf("         sys.heal.unavailable=<tries>          : try to heal an unavailable file for atleast <tries> times - must be >= 3 !!\n");
  printf("                                                     - the product <heal-tries> * <stall-time> should be bigger than the expect replication time for a given filesize!\n\n");
  printf("         sys.redirect.enoent=<host[:port]>     : redirect clients opening non existing files to <host[:port]>\n");
  printf("               => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eog/public \n\n");
  printf("         sys.redirect.enonet=<host[:port]>     : redirect clients opening unaccessible files to <host[:port]>\n");
  printf("               => hence this variable has to be set on the directory at level 2 in the eos namespace e.g. /eog/public \n\n");
  printf("         sys.acl=<acllist>                     : set's an ACL which is honoured for open,rm & rmdir operations\n");
  printf("               => <acllist> = <rule1>,<rule2>...<ruleN> is a comma separated list of rules\n");
  printf("               => <rule> = u:<uid|username>>|g:<gid|groupname>|egroup:<name>:{rwo} \n\n");
  printf("               e.g.: <acllist=\"u:300:rw,g:z2:rwo:egroup:eos-dev:rwx\"\n\n");
  printf("               => user id 300 can read + write\n");
  printf("               => group z2 can read + write-once (create new files but can't delete)\n");
  printf("               => members of egroup 'eos-dev' can read & write & browse\n");
 
  
  printf("User Variables:\n");
  printf("         user.forced.space=<space>             : s.a.\n");
  printf("         user.forced.layout=<layout>           : s.a.\n");
  printf("         user.forced.checksum=<checksum>       : s.a.\n");
  printf("         user.forced.blockchecksum=<checksum>  : s.a.\n");
  printf("         user.forced.nstripes=<n>              : s.a.\n");
  printf("         user.forced.blocksize=<w>             : s.a.\n");
  printf("         user.forced.nouserlayout=1            : s.a.\n");
  printf("         user.forced.nofsselection=1           : s.a.\n");
  printf("         user.stall.unavailable=<sec>          : s.a.\n");
  printf("         user.acl=<acllist>                    : s.a.\n");
  printf("         user.tag=<tag>                        : Tag <tag> to group files for scheduling and flat file distribution. Use this tag to define datasets (if <tag> contains space use tag with quotes)\n");
  return (0);
}
