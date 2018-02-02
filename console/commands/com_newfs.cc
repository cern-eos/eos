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

#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include "console/commands/ICmdHelper.hh"
#include "common/Fs.pb.h"
#include <unistd.h>
#include <algorithm>

void com_fs_help();

//------------------------------------------------------------------------------
//! Class FsHelper
//------------------------------------------------------------------------------
class FsHelper: public ICmdHelper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FsHelper()
  {
    mIsAdmin = true;
    mHighlight = true;
  }

  //----------------------------------------------------------------------------
  //! Denstructor
  //----------------------------------------------------------------------------
  ~FsHelper() = default;

  //----------------------------------------------------------------------------
  //! Parse command line input
  //!
  //! @param arg input
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ParseCommand(const char* arg);
};

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
FsHelper::ParseCommand(const char* arg)
{
  const char* option;
  std::string soption;
  eos::console::FsProto* fs = mReq.mutable_fs();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  option = tokenizer.GetToken();
  std::string cmd = (option ? option : "");

  if (cmd == "add") {
    eos::console::FsProto_AddProto* add = fs->mutable_add();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      if (soption == "-m") {
        add->set_manual(true);

        // Parse fsid
        if (!(option = tokenizer.GetToken())) {
          std::cerr << "error: manual flag needs to be followed by fsid"
                    << std::endl;
          return false;
        }

        soption = option;

        try {
          uint64_t fsid = std::stoull(soption);
          add->set_fsid(fsid);
        } catch (const std::exception& e) {
          std::cerr << "error: fsid needs to be numeric" << std::endl;
          return false;
        }
      }

      // Parse uuid, but only if the manual was not set
      if (add->manual() && !(option = tokenizer.GetToken())) {
        std::cerr << "error: missing uuid" << std::endl;
        return false;
      }

      add->set_uuid(option);

      // Parse node queue or host:port
      if (!(option = tokenizer.GetToken())) {
        std::cerr << "error: missing node-queue or host" << std::endl;
        return false;
      }

      soption = option;

      if (!soption.empty() && (soption[0] == '/')) {
        add->set_nodequeue(soption);
      } else {
        add->set_hostport(soption);
      }

      // Parse mountpoint
      if (!(option = tokenizer.GetToken())) {
        std::cerr << "error: missing mountpoint" << std::endl;
        return false;
      }

      add->set_mountpoint(option);

      // Parse scheduling group
      if (!(option = tokenizer.GetToken())) {
        // Set "default" scheduling group
        add->set_schedgroup("default");
        add->set_status("off");
      } else {
        add->set_schedgroup(option);

        // Parse status
        if (!(option = tokenizer.GetToken())) {
          // Default status is "off"
          add->set_status("off");
        } else {
          add->set_status(option);
        }
      }
    }
  } else if (cmd == "boot") {
    using eos::console::FsProto_BootProto;
    FsProto_BootProto* boot = fs->mutable_boot();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      if (soption == "*") {
        boot->set_nodequeue("*");
      } else if (soption[0] == '/') {
        boot->set_nodequeue(soption);
      } else {
        try {
          uint64_t fsid = std::stoull(soption);
          boot->set_fsid(fsid);
        } catch (const std::exception& e) {
          std::cerr << "error: fsid needs to be numeric" << std::endl;
          return false;
        }
      }

      if ((option = tokenizer.GetToken())) {
        soption = option;

        if (soption != "--syncmgm") {
          std::cerr << "error: unknown option: " << soption << std::endl;
          return false;
        } else {
          boot->set_syncmgm(true);
        }
      }
    }
  } else if (cmd == "compare") {
    auto* compare = fs->mutable_compare();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      try {
        auto sourceid = std::stoull(soption);
        compare->set_sourceid(sourceid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid needs to be numeric" << std::endl;
        return false;
      }
    }

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      try {
        auto targetid = std::stoull(soption);
        compare->set_targetid(targetid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid needs to be numeric" << std::endl;
        return false;
      }
    }
  } else if (cmd == "config") {
    using eos::console::FsProto_ConfigProto;
    FsProto_ConfigProto* config = fs->mutable_config();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      // Parse <host>:<port><path> identifier
      if ((soption.find(':') != std::string::npos) &&
          (soption.find('/') != std::string::npos)) {
        config->set_hostportpath(soption);
      } else {
        try {
          uint64_t fsid = std::stoull(soption);
          config->set_fsid(fsid);
        } catch (const std::exception& e) {
          config->set_uuid(soption);
        }
      }

      // Parse key=value
      if (!(option = tokenizer.GetToken())) {
        std::cerr << "error: configuration must be specified in <key>=<value"
                  " format" << std::endl;
        return false;
      }

      soption = option;
      auto pos = soption.find('=');

      if (pos == std::string::npos) {
        std::cerr << "error: configuration must be specified in <key>=<value"
                  " format" << std::endl;
        return false;
      }

      config->set_key(soption.substr(0, pos));
      config->set_value(soption.substr(pos + 1));
    }
  } else if (cmd == "dropdeletion") {
    using eos::console::FsProto_DropDeletionProto;
    FsProto_DropDeletionProto* dropdel = fs->mutable_dropdel();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      try {
        uint64_t fsid = std::stoull(soption);
        dropdel->set_fsid(fsid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid needs to be numeric" << std::endl;
        return false;
      }
    }
  } else if (cmd == "dropfiles") {
    using eos::console::FsProto_DropFilesProto;
    FsProto_DropFilesProto* dropfiles = fs->mutable_dropfiles();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      try {
        uint64_t fsid = std::stoull(soption);
        dropfiles->set_fsid(fsid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid needs to be numeric" << std::endl;
        return false;
      }

      // Parse -f optional flag
      if ((option = tokenizer.GetToken())) {
        soption = option;

        if (soption != "-f") {
          std::cerr << "error: unknown option: " << soption << std::endl;
          return false;
        }

        dropfiles->set_force(true);
      }

      mNeedsConfirmation = true;
    }
  } else if (cmd == "dumpmd") {
    using eos::console::FsProto_DumpMdProto;
    mReq.set_format(eos::console::RequestProto::FUSE);
    FsProto_DumpMdProto* dumpmd = fs->mutable_dumpmd();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      try {
        uint64_t fsid = std::stoull(soption);
        dumpmd->set_fsid(fsid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid needs to be numeric" << std::endl;
        return false;
      }

      // Parse any optional flags
      if ((option = tokenizer.GetToken())) {
        while (true) {
          soption = option;

          if (soption == "--fid") {
            dumpmd->set_showfid(true);
          } else if (soption == "--path") {
            dumpmd->set_showpath(true);
          } else if (soption == "--size") {
            dumpmd->set_showsize(true);
          } else if (soption == "-s") {
            mIsSilent = true;
          } else if (soption == "-m") {
            dumpmd->set_display(FsProto_DumpMdProto::MONITOR);
          }

          if (!(option = tokenizer.GetToken())) {
            break;
          }
        }
      }
    }
  } else if (cmd == "mv") {
    using eos::console::FsProto_MvProto;
    FsProto_MvProto* mv = fs->mutable_mv();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;
      mv->set_src(soption);

      if (!(option = tokenizer.GetToken())) {
        return false;
      }

      soption = option;
      mv->set_dst(soption);
    }
  } else if (cmd == "ls") {
    using eos::console::FsProto_LsProto;
    FsProto_LsProto* ls = fs->mutable_ls();

    if ((option = tokenizer.GetToken())) {
      while (true) {
        soption = option;

        if (soption == "-m") {
          ls->set_display(FsProto_LsProto::MONITOR);
        } else if (soption == "-l") {
          ls->set_display(FsProto_LsProto::LONG);
        } else if (soption == "-e") {
          ls->set_display(FsProto_LsProto::ERROR);
        } else if (soption == "--io") {
          ls->set_display(FsProto_LsProto::IO);
        } else if (soption == "--fsck") {
          ls->set_display(FsProto_LsProto::FSCK);
        } else if ((soption == "-d") || (soption == "--drain")) {
          ls->set_display(FsProto_LsProto::DRAIN);
        } else if (soption == "-s") {
          mIsSilent = true;
        } else if ((soption == "-b") || (soption == "--brief")) {
          ls->set_brief(true);
        } else {
          // This needs to be the matchlist
          ls->set_matchlist(soption);
        }

        if (!(option = tokenizer.GetToken())) {
          break;
        }
      }
    }
  } else if (cmd == "rm") {
    using eos::console::FsProto_RmProto;
    FsProto_RmProto* rm = fs->mutable_rm();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      // Parse nodequeue specification
      if ((soption.find("/eos/") == 0) &&
          (soption.find(':') != std::string::npos) &&
          (soption.find('.') != std::string::npos)) {
        // Check if it ends in /fst, if not append it
        std::string search = "/fst";

        if (soption.rfind(search) != soption.length() - search.length()) {
          soption += "/fst";
        }

        // Parse the mountpoint and remove any ending /
        std::string mountpoint;

        if (!(option = tokenizer.GetToken())) {
          std::cerr << "error: no mountpoint specified" << std::endl;
          return false;
        }

        mountpoint = option;

        if (*mountpoint.rbegin() == '/') {
          mountpoint.pop_back();
        }

        soption += mountpoint;
        rm->set_nodequeue(soption);
      } else {
        // Parse mountpoint and append any required info
        if (soption[0] == '/') {
          char hostname[255];

          if (gethostname(hostname, sizeof(hostname)) == -1) {
            std::cerr << "error: failed to get local hostname" << std::endl;
            return false;
          }

          std::ostringstream oss;
          oss << "/eos/" << hostname << ":1095/fst" << soption;
          rm->set_nodequeue(oss.str());
        } else if (std::find_if(soption.begin(), soption.end(),
        [](char c) {
        return std::isalpha(c);
        })
        != soption.end()) {
          // This contains at least one alphabetic char therefore it must be
          // a hostname, parse the mountpoint and construct the node-queue
          std::string mountpoint;

          if (!(option = tokenizer.GetToken())) {
            std::cerr << "error: mountpoint missing" << std::endl;
            return false;
          }

          mountpoint = option;

          if (mountpoint.empty() || mountpoint[0] != '/') {
            std::cerr << "error: invalid mountpoint" << std::endl;
            return false;
          }

          if (*mountpoint.rbegin() == '/') {
            mountpoint.pop_back();
          }

          bool has_port = false;
          auto pos = soption.find(':');

          if ((pos != std::string::npos) && (pos < soption.length())) {
            has_port = true;
          }

          std::ostringstream oss;
          oss << "/eos/" << soption;

          if (!has_port) {
            oss << ":1095";
          }

          oss << "/fst" << mountpoint;
          rm->set_nodequeue(oss.str());
        }
        else {
          // This needs to be an fsid
          try {
            uint64_t fsid = std::stoull(soption);
            rm->set_fsid(fsid);
          } catch (const std::exception& e) {
            return false;
          }
        }
      }
    }
  } else if (cmd == "status") {
    using eos::console::FsProto_StatusProto;
    FsProto_StatusProto* status = fs->mutable_status();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      while (true) {
        soption = option;
        std::ostringstream oss;

        if (soption == "-l") {
          status->set_longformat(true);
        } else if (soption == "-r") {
          status->set_riskassesment(true);
        } else {
          // This is a hostname specification
          if ((soption.find('.') != std::string::npos) &&
              (soption.find('/') == std::string::npos)) {
            // Check for mountpoint
            if (!(option = tokenizer.GetToken()) || (option[0] != '/')) {
              std::cerr << "error: no mountpoint specified" << std::endl;
              return false;
            }

            oss << "/eos/" << soption << "/fst" << option;
            status->set_nodequeue(oss.str());
          } else if (soption[0] == '/') {
            // This is a mountpoint append the local hostname
            char hostname[255];

            if (gethostname(hostname, sizeof(hostname)) == -1) {
              std::cerr << "error: failed to get local hostname" << std::endl;
              return false;
            }

            oss << "/eos/" << hostname << ":1095/fst" << soption;
            status->set_nodequeue(oss.str());
          } else if (std::isalpha(soption[0])) {
            // This is a hostname specification, check for mountpoint
            if (!(option = tokenizer.GetToken())) {
              std::cerr << "error: no mountpoint specified" << std::endl;
              return false;
            }

            oss << "/eos/" << soption << "/fst" << option;
            status->set_nodequeue(oss.str());
          } else {
            // This needs to be a fsid
            try {
              uint64_t fsid = std::stoull(soption);
              status->set_fsid(fsid);
            } catch (const std::exception& e) {
              return false;
            }
          }
        }

        if (!(option = tokenizer.GetToken())) {
          break;
        }
      }

      if ((status->fsid() == 0) && (status->nodequeue().empty())) {
        std::cerr << "error: fsid or host/mountponint needs to be specified"
                  << std::endl;
        return false;
      }
    }
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Fs command entry point
//------------------------------------------------------------------------------
int com_newfs(char* arg)
{
  if (wants_help(arg)) {
    com_fs_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  FsHelper fs;

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
  oss << "Usage: fs add|boot|config|dropdeletion|dropfiles|dumpmd|ls|mv|rm|status [OPTIONS]"
      << std::endl
      << "  Options:" << std::endl
      << "  fs add [-m|--manual <fsid>] <uuid> <node-queue>|<host>[:<port>] "
      << "<mountpoint> [<schedgroup>] [<status>]" << std::endl
      << "    add and assign a filesystem based on the unique identifier of the disk <uuid>"
      << std::endl
      << "    -m|--manual  : add with user specified <fsid> and <schedgroup>"
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
      << "    <schedgroup> : scheduling group in which to insert filesystem, if nothing "
      << std::endl
      << "                   is specified then \"default\" scheduling group is used"
      << std::endl
      << "    <status>     : set filesystem status after insertion e.g off|rw|ro etc."
      << std::endl
      << std::endl
      << "  fs boot <fsid>|<node-queue>|* [--syncmgm]" << std::endl
      << "    boot filesystem identified by <fsid> or all filesystems on a node"
      << std::endl
      << "    identified by <node-queue> or all filesystems registered"
      << std::endl
      << "    --syncmgm    : for MGM resynchronization during the booting" <<
      std::endl
      << std::endl
      << "  fs config <fsid>|<uuid>|<host>:<port> <key>=<value>" << std::endl
      << "    configure the filesystem parameter, where <key> and <value> can be:" <<
      std::endl
      << "    configstatus=rw|wo|ro|drain|draindead|off|empty" << std::endl
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
      << "    headroom=<size>" << std::endl
      << "      headroom to keep per filesystem. <size> can be (>0)[BMGT]"
      << std::endl
      << "    scaninterval=<seconds>" << std::endl
      << "      configure a scanner thread to recheck the file & block checksum"
      << std::endl
      << "      of all stored files every <seconds>. 0 disables scanning"
      << std::endl
      << "    graceperiod=<seconds>" << std::endl
      << "      grace period before a filesystem with an operation error gets"
      << std::endl
      << "      automatically drained" << std::endl
      << "    drainperiod=<seconds>" << std::endl
      << "      period a drain job is allowed to finish the drain procedure"
      << std::endl
      << "     proxygroup=<proxy_grp_name>" << std::endl
      << "      schedule a proxy for the current filesystem by taking it from"
      << std::endl
      << "      the given proxy group. The special value \"<none>\" is the"
      << std::endl
      << "      same as no value and means no proxy scheduling" << std::endl
      << "    filestickyproxydepth=<depth>" << std::endl
      << "       depth of the subtree to be considered for file-stickyness. A"
      << std::endl
      << "      negative value means no file-stickyness" << std::endl
      << "    forcegeotag=<geotag>" << std::endl
      << "      set the filesystem's geotag, overriding the host geotag value."
      << std::endl
      << "      The special value \"<none>\" is the same as no value and means"
      << std::endl
      << "      no override" << std::endl
      << std::endl
      << "  fs dropdeletion <fsid> " << std::endl
      << "    drop all pending deletions on the filesystem" << std::endl
      << std::endl
      << "  fs dropfiles <fsid> [-f]" << std::endl
      << "    drop all files on the filesystem" << std::endl
      << "    -f : unlink/remove files from the namespace (you have to remove"
      << std::endl
      <<  "        the files from disk)" << std::endl
      << std::endl
      << "  fs dumpmd <fsid> [--fid] [--path] [-s|-m]"  << std::endl
      << "    dump all file metadata on this filesystem in query format" << std::endl
      << "    --fid  : dump only the list of file ids" << std::endl
      << "    --path : dump only the paths of the files" << std::endl
      << "    -s     : don't display, but keep an internal reference" << std::endl
      << "    -m     : print full metadata record in env format" << std::endl
      << std::endl
      << "  fs ls [-m|-l|-e|--io|--fsck|-d|--drain] [-s] [-b|--brief] [[matchlist]]"
      << std::endl
      << "    list filesystems using the default output format" << std::endl
      << "    -m         : monitoring format" << std::endl
      << "    -b|--brief : display hostnames without domain names" << std::endl
      << "    -l         : display parameters in long format" << std::endl
      << "    -e         : dispaly filesystems in error state" << std::endl
      << "    --io       : IO output format" << std::endl
      << "    --fsck     : display filesystem check statistics" << std::endl
      << "    -d|--drain : display filesystems in drain or draindead status"
      << std::endl
      << "                 along with drain progress and statistics" << std::endl
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
      << "  fs mv <src_fsid|src_grp|src_space> <dst_grp|dst_space>" << std::endl
      << "    move filesystem(s) in different scheduling group or space"
      << std::endl
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
      << " defalut.0" << std::endl;
  std::cerr << oss.str() << std::endl;
}
