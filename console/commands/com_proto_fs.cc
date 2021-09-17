//------------------------------------------------------------------------------
//! @file com_fs.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "console/commands/helpers/FsHelper.hh"
#include <algorithm>
#include <sstream>

void com_fs_help();

//------------------------------------------------------------------------------
// Fs command entry point
//------------------------------------------------------------------------------
int com_protofs(char* arg)
{
  if (wants_help(arg)) {
    com_fs_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  FsHelper fs(gGlobalOpts);

  if (!fs.ParseCommand(arg)) {
    com_fs_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  if (fs.NeedsConfirmation() && !fs.ConfirmOperation()) {
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = fs.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_fs_help()
{
  std::ostringstream oss;
  oss << "Usage: fs add|boot|config|dropdeletion|dropghosts|dropfiles|dumpmd|ls|mv|rm|status [OPTIONS]"
      << std::endl
      << "  Options:" << std::endl
      << "  fs add [-m|--manual <fsid>] <uuid> <node-queue>|<host>[:<port>] "
      << "<mountpoint> [<space_info>] [<status>]" << std::endl
      << "    add and assign a filesystem based on the unique identifier of the disk <uuid>"
      << std::endl
      << "    -m|--manual  : add with user specified <fsid> and <space>"
      << std::endl
      << "    <fsid>       : numeric filesystem id 1...65535" << std::endl
      << "    <uuid>       : unique string identifying current filesystem" <<
      std::endl
      << "    <node-queue> : internal EOS identifier for a node e.g /eos/<host>:<port>/fst"
      << std::endl
      << "                   it is preferable to use the host:port syntax" <<
      std::endl
      << "    <host>       : FQDN of host where filesystem is mounter" << std::endl
      << "    <port>       : FST XRootD port number [usually 1095]" << std::endl
      << "    <mountponit> : local path of the mounted filesystem e.g /data/" <<
      std::endl
      << "    <space_info> : space or space.group location where to insert the filesystem,\n"
      << "                   if nothing is specified then space \"default\" is used. E.g:\n"
      << "                   default, default.7, ssd.3, spare"
      << std::endl
      << "    <status>     : set filesystem status after insertion e.g off|rw|ro|empty etc."
      << std::endl
      << std::endl
      << "  fs boot <fsid>|<uuid>|<node-queue>|* [--syncmgm]" << std::endl
      << "    boot - filesystem identified by <fsid> or <uuid>" << std::endl
      << "         - all filesystems on a node identified by <node-queue>" <<
      std::endl
      << "         - all filesystems registered" << std::endl
      << "    --syncmgm    : for MGM resynchronization during the booting"
      << std::endl
      << std::endl
      << "  fs clone <sourceid> <targetid>" << std::endl
      << "    replicate files from the source to the target filesystem"
      << std::endl
      << "    <sourceid>   : id of the source filesystem" << std::endl
      << "    <targetid>   : id of the target filesystem" << std::endl
      << std::endl
      << std::endl
      << "  fs compare <sourceid> <targetid>" << std::endl
      << "    compares and reports which files are present on one filesystem and not on the other"
      << std::endl
      << "    <sourceid>   : id of the source filesystem" << std::endl
      << "    <targetid>   : id of the target filesystem" << std::endl
      << std::endl
      << std::endl
      << "  fs config <fsid> <key>=<value>" << std::endl
      << "    configure the filesystem parameter, where <key> and <value> can be:" <<
      std::endl
      << "    configstatus=rw|wo|ro|drain|draindead|off|empty [--comment \"<comment>\"]"
      << std::endl
      << "      rw        : set filesystem in read-write mode" << std::endl
      << "      wo        : set filesystem in write-only mode" << std::endl
      << "      ro        : set filesystem in read-only mode" << std::endl
      << "      drain     : set filesystem in drain mode" << std::endl
      << "      draindead : set filesystem in draindead mode, unusable for any read"
      << std::endl
      << "      off       : disable filesystem" << std::endl
      << "      empty     : empty filesystem, possible only if there are no"
      << std::endl
      << "                  more files stored on it" << std::endl
      << "      --comment : pass a reason for the status change" << std::endl
      << "    headroom=<size>" << std::endl
      << "      headroom to keep per filesystem. <size> can be (>0)[BMGT]"
      << std::endl
      << "    scaninterval=<seconds>\n"
      << "      entry rescan interval (default 7 days), 0 disables scanning"
      << std::endl
      << "    scanrate=<MB/s>" << std::endl
      << "      maximum IO scan rate per filesystem"
      << std::endl
      << "    scan_disk_interval=<seconds>\n"
      << "      disk consistency thread scan interval (default 4h)"
      << std::endl
      << "    scan_ns_interval=<seconds>\n"
      << "      namespace consistency thread scan interval (default 3 days)"
      << std::endl
      << "    scan_ns_rate=<entries/s>\n"
      << "      maximum scan rate of ns entries for the NS consistency. This\n"
      << "      is bound by the maxium number of IOPS per disk."
      << std::endl
      << "    fsck_refresh_interval=<sec>\n"
      << "       time interval after which fsck inconsistencies are refreshed"
      << std::endl
      << "    graceperiod=<seconds>\n"
      << "      grace period before a filesystem with an operation error gets\n"
      << "      automatically drained"
      << std::endl
      << "    drainperiod=<seconds>\n"
      << "      period a drain job is allowed to finish the drain procedure"
      << std::endl
      << "    proxygroup=<proxy_grp_name>" << std::endl
      << "      schedule a proxy for the current filesystem by taking it from"
      << std::endl
      << "      the given proxy group. The special value \"<none>\" is the"
      << std::endl
      << "      same as no value and means no proxy scheduling" << std::endl
      << "    filestickyproxydepth=<depth>" << std::endl
      << "      depth of the subtree to be considered for file-stickyness. A"
      << std::endl
      << "      negative value means no file-stickyness" << std::endl
      << "    forcegeotag=<geotag>" << std::endl
      << "      set the filesystem's geotag, overriding the host geotag value."
      << std::endl
      << "      The special value \"<none>\" is the same as no value and means"
      << std::endl
      << "      no override" << std::endl
      << "    s3credentials=<accesskey>:<secretkey>" << std::endl
      << "      the access and secret key pair used to authenticate" << std::endl
      << "      with the S3 storage endpoint" << std::endl
      << std::endl
      << "  fs dropdeletion <fsid> " << std::endl
      << "    drop all pending deletions on the filesystem" << std::endl
      << std::endl
      << "  fs dropghosts <fsid> [--fxid fid1 [fid2] ...]\n"
      << "    drop file ids (hex) without a corresponding metadata object in\n"
      << "    the namespace that are still accounted in the file system view.\n"
      << "    If no fxid is provided then all fids on the file system are checked.\n"
      << std::endl
      << "  fs dropfiles <fsid> [-f]" << std::endl
      << "    drop all files on the filesystem" << std::endl
      << "    -f : unlink/remove files from the namespace (you have to remove"
      << std::endl
      << "         the files from disk)" << std::endl
      << std::endl
      << "  fs dumpmd <fsid> [--fid] [--path] [--size] [-m|-s]" << std::endl
      << "    dump all file metadata on this filesystem in query format" << std::endl
      << "    --fid  : dump only the file ids" << std::endl
      << "    --path : dump only the file paths" << std::endl
      << "    --size : dump only the file sizes" << std::endl
      << "    -m     : print full metadata record in env format" << std::endl
      << "    -s     : silent mode (will keep an internal reference)" << std::endl
      << std::endl
      << "  fs ls [-m|-l|-e|--io|--fsck|[-d|--drain]|-D|-F] [-s] [-b|--brief] [[matchlist]]"
      << std::endl
      << "    list filesystems using the default output format" << std::endl
      << "    -m         : monitoring format" << std::endl
      << "    -b|--brief : display hostnames without domain names" << std::endl
      << "    -l         : display parameters in long format" << std::endl
      << "    -e         : display filesystems in error state" << std::endl
      << "    --io       : IO output format" << std::endl
      << "    --fsck     : display filesystem check statistics" << std::endl
      << "    -d|--drain : display filesystems in drain or draindead status"
      << std::endl
      << "                 along with drain progress and statistics" << std::endl
      << "    -D|--drain_jobs : " << std::endl
      << "                 display ongoing drain transfers, matchlist needs to be an integer"
      << std::endl
      << "                 representing the drain file system id" << std::endl
      << "    -F|--failed_drain_jobs : " << std::endl
      << "                 display failed drain transfers, matchlist needs to be an integer"
      << std::endl
      << "                 representing the drain file system id. This will only display"
      << std::endl
      << "                 information while the draining is ongoing" << std::endl
      << "    -s         : silent mode" << std::endl
      << "    [matchlist]" << std::endl
      << "       -> can be the name of a space or a comma separated list of"
      << std::endl
      << "          spaces e.g 'default,spare'" << std::endl
      << "       -> can be a grep style list to filter certain filesystems"
      << std::endl
      << "          e.g. 'fs ls -d drain,bootfailure'" << std::endl
      << "       -> can be a combination of space filter and grep e.g."
      << std::endl
      << "          'fs ls -l default,drain,bootfailure'" << std::endl
      << std::endl
      << "  fs mv [--force] <src_fsid|src_grp|src_space> <dst_grp|dst_space>" <<
      std::endl
      << "    move filesystem(s) in different scheduling group or space"
      << std::endl
      << "    --force   : force mode - allows to move non-empty filesystems bypassing group "
      << std::endl
      << "                and node constraints" << std::endl
      << "    src_fsid  : source filesystem id" << std::endl
      << "    src_grp   : all filesystems from scheduling group are moved"
      << std::endl
      << "    src_space : all filesystems from space are moved" << std::endl
      << "    dst_grp   : destination scheduling group" << std::endl
      << "    dst_space : destination space - best match scheduling group"
      << std::endl
      << "                is auto-selected" << std::endl
      << std::endl
      << "  fs rm <fsid>|<mnt>|<node-queue> <mnt>|<hostname> <mnt>" << std::endl
      << "    remove filesystem by various identifiers, where <mnt> is the "
      << std::endl
      << "    mountpoint" << std::endl
      << std::endl
      << "  fs status [-r] [-l] <identifier>" << std::endl
      << "    return all status variables of a filesystem and calculates"
      << std::endl
      << "    the risk of data loss if this filesystem is removed" << std::endl
      << "    <identifier> can be: " << std::endl
      << "       <fsid> : filesystem id" << std::endl
      << "       [<host>] <mountpoint> : if host is not specified then it's"
      << std::endl
      << "       considered localhost" << std::endl
      << "    -l : list all files which are at risk and offline files"
      << std::endl
      << "    -r : show risk analysis" << std::endl
      << std::endl
      << "  Examples: " << std::endl
      << "  fs ls --io -> list all filesystems with IO statistics" << std::endl
      << "  fs boot *  -> send boot request to all filesystems" << std::endl
      << "  fs dumpmd 100 -path -> dump all logical path names on filesystem"
      << " 100" << std::endl
      << "  fs mv 100 default.0 -> move filesystem 100 to scheduling group"
      << " default.0" << std::endl;
  std::cerr << oss.str() << std::endl;
}
