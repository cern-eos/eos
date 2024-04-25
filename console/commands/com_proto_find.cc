// ----------------------------------------------------------------------
// @file: com_proto_find.cc
// @author: Fabio Luchetti - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "console/commands/helpers/NewfindHelper.hh"
#include "common/StringTokenizer.hh"
#include "common/StringConversion.hh"
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdOuc/XrdOucEnv.hh>
/*----------------------------------------------------------------------------*/

extern void com_find_help();

int
com_proto_find(char* arg)
{
  if (wants_help(arg)) {
    com_find_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  NewfindHelper find(gGlobalOpts);
  // Handle differently if it's an xroot, file or as3 path
  std::string argStr(arg);
  auto xrootAt = argStr.rfind("root://");
  auto fileAt = argStr.rfind("file:");
  auto as3At = argStr.rfind("as3:");

  if (xrootAt != std::string::npos) {
    auto path = argStr.substr(xrootAt);
    // remove " from the path
    path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
    global_retc = find.FindXroot(path);
    return global_retc;
  } else if (fileAt != std::string::npos) {
    auto path = argStr.substr(fileAt);
    // remove " from the path
    path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
    global_retc = find.FindXroot(path);
    return global_retc;
  } else if (as3At != std::string::npos) {
    auto path = argStr.substr(as3At);
    // remove " from the path
    path.erase(std::remove(path.begin(), path.end(), '"'), path.end());
    global_retc = find.FindAs3(path);
    return global_retc;
  }

  if (!find.ParseCommand(arg)) {
    com_find_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = find.Execute();
  return global_retc;
}

void com_find_help()
{
  std::ostringstream oss;
  oss
      << " usage\n"
      << "find/newfind [OPTIONS] <path> : find files and directories\n"
      << "OPTIONS can be filters, actions to perform, or output integrations/modifiers for the found items\n"
      << "Filters: [--maxdepth <n>] [--name <pattern>] [-d] [-f] [-0] [-g] [-uid <n>] [-nuid <n>] [-gid <n>] [-ngid <n>] [-flag <n>] [-nflag <n>] [-ctime +<n>|-<n>] [-x <key>=<val>] [--format formatlist] [--cache] [--du]\n"
//      << "\t                   -1 : \n"
      << "\t       --maxdepth <n> : descend only <n> levels\n"
      << "\t     --name <pattern> : find by name, filtering by 'egrep' style regex match\n"
      << "\t                -f,-d : find only files(-f) or directories (-d) in <path>\n"
      << "\t       -x <key>=<val> : find entries with <key>=<val>\n"
      << "\t   -uid <n>,-nuid <n> : find entries owned / not owned by a given user id number\n"
      << "\t   -gid <n>,-ngid <n> : find entries owned / not owned by a given group id number\n"
      << "\t -flag <n>,-nflag <n> : find entries with / without specified UNIX access flag, e.g. 755\n"
      << "\t    -ctime <+n>, <-n> : find files older (+n) or younger (-n) than <n> days\n"
      << "\t          --faultyacl : find files and directories with illegal ACLs\n"
      << "\t                   -0 : find 0-size files only\n"
      << "\t                   -g : find files with mixed scheduling groups\n"
      << "\t         --stripediff : find files that do not have the nominal number of stripes(replicas)\n"
      << "Actions: [-b] [--layoutstripes <n>] [--purge <n> ] [--fileinfo]"
      << "\t                   -b : query the server balance of the files found\n"
      << "\t  --layoutstripes <n> : apply new layout with <n> stripes to the files found\n"
      << "\t --purge <n> | atomic : remove versioned files keeping <n> versions (use --purge 0 to remove all old versions)\n"
      << "\t                        To apply the settings of the extended attribute definition use --purge -1\n"
      << "\t                        To remove all atomic upload left-overs older than a day use --purge atomic\n"
      << "\t         [--fileinfo] : invoke `eos fileinfo` on the entry\n"
      << "\t              --count : print aggregated number of file and directory including the search path\n"
      << "\t         --childcount : print the number of children in each directory\n"
      << "\t          --treecount : print the aggregated number of filesand directory children excluding the search path\n"
      << "\t             --format : print with the given komma separated format list, redundant switches like --uid --checksum, which can be specified via the format are automatically disabled. Possible values for format tags are: uid,gid,size,checksum,checksumtype,etag,fxid,pxid,cxid,fid,pid,cid,atime,btime,ctime,mtime,type,mode,files,directories,attr.*,attr.<name> e.g. attr.sys.acl !\n"
      << "\t              --cache : store all found entries in the in-memory namespace cache\n"
      << "\t                 --du : create du-style output\n"
      << "Output Mod: [--xurl] [-p <key>] [--nrep] [--nunlink] [--size] [--online] [--hosts] [--partition] [--fid] [--fs] [--checksum] [--ctime] [--mtime] [--uid] [--gid]\n"
//      << "                   -s :  run in silent mode"
      << "\t                      : print out the requested meta data as key value pairs\n"
      << " <path> :\n"
      << "\t path=file:...  :  do a find in the local file system (options ignored) - 'file:' is the current working directory\n"
      << "\t path=root:...  :  do a find on a plain XRootD server (options ignored) - does not work on native XRootD clusters\n"
      << "\t path=as3:...   :  do a find on an S3 bucket\n"
      << "\t path=...       :  all other paths are considered to be EOS paths!\n";
  std::cerr << oss.str() << std::endl;
}
