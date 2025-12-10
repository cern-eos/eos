//------------------------------------------------------------------------------
//! @file com_proto_file.cc
//! @author Octavian Matei - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "console/ConsoleMain.hh"
#include "console/commands/helpers/FileHelper.hh"
#include <sstream>

//! Forward declaration
void com_file_help();

//------------------------------------------------------------------------------
// File command entrypoint
//------------------------------------------------------------------------------
int com_proto_file(char* arg)
{
  if (wants_help(arg)) {
    com_file_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  FileHelper file(gGlobalOpts);

  if (!file.ParseCommand(arg)) {
    com_file_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  // Standard execution for other commands
  global_retc = file.Execute(true, true);
  return global_retc;
}


//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_file_help()
{
  std::ostringstream oss;
  oss
      << "Usage: file adjustreplica|check|convert|copy|drop|info|layout|move|purge|rename|replicate|verify|version ...\n"
      << "'[eos] file ..' provides the file management interface of EOS.\n"
      << "Options:\n"
      << "file adjustreplica [--nodrop] <path>|fid:<fid-dec>|fxid:<fid-hex> [space [subgroup]] [--exclude-fs <fsid>] :\n"
      << "                                                  tries to bring a files with replica layouts to the nominal replica level [ need to be root ]\n"
      << "       --exclude-fs <fsid>                                            :  exclude the given filesystem from being used for the replica adjustment\n"
      << "file check [<path>|fid:<fid-dec>|fxid:<fid-hex>] [%size%checksum%nrep%diskchecksum%force%output%silent] :\n"
      << "                                                  retrieves stat information from the physical replicas and verifies the correctness\n"
      << "       - %size                                                       :  return EFAULT if mismatch between the size meta data information\n"
      << "       - %checksum                                                   :  return EFAULT if mismatch between the checksum meta data information\n"
      << "       - %nrep                                                       :  return EFAULT if mismatch between the layout number of replicas and the existing replicas\n"
      << "       - %diskchecksum                                               :  return EFAULT if mismatch between the disk checksum on the FST and the reference checksum\n"
      << "       - %silent                                                     :  suppresses all information for each replica to be printed\n"
      << "       - %force                                                      :  forces to get the MD even if the node is down\n"
      << "       - %output                                                     :  prints lines with inconsistency information\n"
      << "file convert [--sync|--rewrite] [<path>|fid:<fid-dec>|fxid:<fid-hex>] [<layout>:<stripes> | <layout-id> | <sys.attribute.name>] [target-space] [placement-policy] [checksum]:\n"
      << "                                                                         convert the layout of a file\n"
      << "        <layout>:<stripes>   : specify the target layout and number of stripes\n"
      << "        <layout-id>          : specify the hexadecimal layout id \n"
      << "        <conversion-name>    : specify the name of the attribute sys.conversion.<name> in the parent directory of <path> defining the target layout\n"
      << "        <target-space>       : optional name of the target space or group e.g. default or default.3\n"
      << "        <placement-policy>   : optional placement policy valid values are 'scattered','hybrid:<some_geotag>' and 'gathered:<some_geotag>'\n"
      << "        <checksum>           : optional target checksum name. E.g.: md5, adler, etc.\n"
      << "        --sync               : run conversion in synchronous mode (by default conversions are asynchronous) - not supported yet\n"
      << "        --rewrite            : run conversion rewriting the file as is creating new copies and dropping old\n"
      << "file copy [-f] [-s] [-c] <src> <dst>                                   :  synchronous third party copy from <src> to <dst>\n"
      << "         <src>                                                         :  source can be a file or a directory (<path>|fid:<fid-dec>|fxid:<fid-hex>) \n"
      << "         <dst>                                                         :  destination can be a file (if source is a file) or a directory\n"
      << "         -f                                                            :  force overwrite\n"
      << "         -s                                                            :  don't print output\n"
      << "         -c                                                            :  clone the file (keep ctime, mtime)\n"
      << "file drop [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid> [-f] :\n"
      << "                                                  drop the file <path> from <fsid> - force removes replica without trigger/wait for deletion (used to retire a filesystem) \n"
      << "file info [<path>|fid:<fid-dec>|fxid:<fid-hex>] :\n"
      << "                                                  convenience function aliasing to 'fileinfo' command\n"
      << "file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -stripes <n> :\n"
      << "                                                  change the number of stripes of a file with replica layout to <n>\n"
      << "file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -checksum <checksum-type> :\n"
      << "                                                  change the checksum-type of a file to <checksum-type>\n"
      << "file layout <path>|fid:<fid-dec>|fxid:<fid-hex>  -type <hex-layout-type> :\n"
      << "                                                  change the layout-type of a file to <hex-layout-type> (as shown by file info)\n"
      << "file move [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid1> <fsid2> :\n"
      << "                                                  move the file <path> from  <fsid1> to <fsid2>\n"
      << "file purge <path> [purge-version] :\n"
      << "                                                  keep maximum <purge-version> versions of a file. If not specified apply the attribute definition from sys.versioning.\n"
      << "file rename [<path>|fid:<fid-dec>|fxid:<fid-hex>] <new> :\n"
      << "                                                  rename from <old> to <new> name (works for files and directories!).\n"
      << "file rename_with_symlink <source_file> <destination_dir> :\n"
      << "     rename/move source file to destination directory by doing\n"
      "     two operations in an atomic step:\n"
      "     - move file to destination directory\n"
      "     - create symlink in the source directory to the new location\n"
      << "file replicate [<path>|fid:<fid-dec>|fxid:<fid-hex>] <fsid1> <fsid2> :\n"
      << "                                                  replicate file <path> part on <fsid1> to <fsid2>\n"
      << "file symlink [-f] <name> <link-name> :\n"
      << "                                                  create a symlink with <name> pointing to <link-name>\n"
      << "         -f                                                            :  force overwrite\n"
      << "file tag <path>|fid:<fid-dec>|fxid:<fid-hex> +|-|~<fsid> :\n"
      << "                                                  add/remove/unlink a filesystem location to/from a file in the location index - attention this does not move any data!\n"
      << "                                                  unlink keeps the location in the list of deleted files e.g. the location gets a deletion request\n"
      << "file touch [-a] [-n] [-0] <path>|fid:<fid-dec>|fxid:<fid-hex> [linkpath|size [hexchecksum]] :\n"
      << "                                                  create/touch a 0-size/0-replica file if <path> does not exist or update modification time of an existing file to the present time\n"
      << "                                          - by default it uses placement logic - use [-n] to disable placement\n"
      << "                                          - use 'file touch -0 myfile' to truncate a file\n"
      << "                                          - use 'file touch -a myfile /external/path' if you want to adopt (absorb) a file which is provied by the hardlink argument - this means that the file disappears from the given hardlink path and is taken under control of an EOS FST\n"
      << "                                          - provide the optional size argument to preset the size\n"
      << "                                          - provide the optional linkpath argument to hard- or softlink the touched file to a shared filesystem\n"
      << "                                          - provide the optional checksum information for a new touched file\n"
      << "file touch -l <path>|fid:<fid-dec>|fxid:<fid-hex> [<lifetime> [<audience>=user|app]] :\n"
      << "                                          - touch a file and create an extended attribute lock with <lifetime> (default 24h)\n"
      << "                                          - with <audience> one can relax the lock owner requirements to be either same user or same app - default is both have to match\n"
      << "                                          - if the lock is already held by another caller EBUSY is returned\n"
      << "                                          - if a lock is already held by the caller a second call will extend the liftime as provided\n"
      << "                                          - use in combination with 'eos -a application' to tag a client with a given application for the lock\n"
      << "file touch -u <path|fid:<fid-dec>|fxid:<fid-hex> :\n"
      << "                                          - remove an extended attribute lock\n"
      << "                                          - if no lock was not present no error is returned - only an message\n"
      << "                                          - if the lock is held by someone else EBUSY is returned\n"
      << "                                          - use in combination with 'eos -a application' to tag a client with a given application for the lock\n"
      << "file verify <path>|fid:<fid-dec>|fxid:<fid-hex> [<fsid>] [-checksum] [-commitchecksum] [-commitsize] [-commitfmd] [-rate <rate>] : \n"
      << "                                                  verify a file against the disk images\n"
      << "file verify <path|fid:<fid-dec>|fxid:<fid-hex> -resync : \n"
      << "                                                  ask all locations to resync their file md records\n"
      << "       <fsid>          : verifies only the replica on <fsid>\n"
      << "       -checksum       : trigger the checksum calculation during the verification process\n"
      << "       -commitchecksum : commit the computed checksum to the MGM\n"
      << "       -commitsize     : commit the file size to the MGM\n"
      << "       -commitfmd      : commit the file metadata to the MGM\n"
      << "       -rate <rate>    : restrict the verification speed to <rate> per node\n"
      << "file version <path> [purge-version] :\n"
      << "                                                  create a new version of a file by cloning\n"
      << "       <purge-version> : defines the max. number of versions to keep\n"
      << "file versions [grab-version] :\n"
      << "                                                  list versions of a file\n"
      << "                                                  grab a version [grab-version] of a file\n"
      << "\n"
      << "                         if not specified it will add a new version without purging any previous version\n"
      << "file share <path> [lifetime] :\n"
      << "       <path>          : path to create a share link\n"
      << "       <lifetime>      : validity time of the share link like 1, 1s, 1d, 1w, 1mo, 1y, ... default is 28d\n"
      << "\n"
      << "file workflow <path>|fid:<fid-dec>|fxid:<fid-hex> <workflow> <event> :\n"
      << "                                                  trigger workflow <workflow> with event <event> on <path>\n"
      << "\n";
  std::cerr << oss.str() << std::endl;
}