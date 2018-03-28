//------------------------------------------------------------------------------
// File: FsHelper.cc
// Author: Jozsef Makai - CERN
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


#include "FsHelper.hh"
#include "common/StringTokenizer.hh"
#include <unistd.h>
#include <algorithm>

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
  } else if (cmd == "clone") {
    auto* clone = fs->mutable_clone();

    if (!(option = tokenizer.GetToken())) {
      return false;
    } else {
      soption = option;

      try {
        auto sourceid = std::stoull(soption);
        clone->set_sourceid(sourceid);
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
        clone->set_targetid(targetid);
      } catch (const std::exception& e) {
        std::cerr << "error: fsid needs to be numeric" << std::endl;
        return false;
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
