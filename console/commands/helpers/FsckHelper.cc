//------------------------------------------------------------------------------
//! @file FsckHelper.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "console/commands/helpers/FsckHelper.hh"
#include "common/FileSystem.hh"
#include "common/StringTokenizer.hh"
#include "common/FileId.hh"

//------------------------------------------------------------------------------
// Parse command line input
//------------------------------------------------------------------------------
bool
FsckHelper::ParseCommand(const char* arg)
{
  const char* option;
  std::string soption;
  eos::console::FsckProto* fsck = mReq.mutable_fsck();
  eos::common::StringTokenizer tokenizer(arg);
  tokenizer.GetLine();
  option = tokenizer.GetToken();
  std::string cmd = (option ? option : "");

  if (cmd == "stat") {
    fsck->set_stat(true);
  } else if (cmd == "config") {
    if ((option = tokenizer.GetToken()) == nullptr) {
      return false;
    }

    std::string key = option;
    std::string value {""};

    if ((option = tokenizer.GetToken()) != nullptr) {
      value = option;
    }

    eos::console::FsckProto::ConfigProto* config = fsck->mutable_config();
    config->set_key(key);
    config->set_value(value);
  } else if (cmd == "report") {
    eos::console::FsckProto::ReportProto* report = fsck->mutable_report();

    while (true) {
      if ((option = tokenizer.GetToken()) == nullptr) {
        break;
      }

      soption = option;

      if (soption == "-a") {
        report->set_display_per_fs(true);
      } else if (soption == "-i") {
        report->set_display_fxid(true);
      } else if (soption == "-l") {
        report->set_display_lfn(true);
      } else if ((soption == "-j") || (soption == "--json")) {
        report->set_display_json(true);
      } else if (soption == "--error") {
        // Now parse the tags until end of line
        while ((option = tokenizer.GetToken())) {
          std::string* tag = report->add_tags();
          tag->assign(option);
        }

        break;
      }
    }
  } else if (cmd == "repair") {
    eos::console::FsckProto::RepairProto* repair = fsck->mutable_repair();

    while (tokenizer.NextToken(soption)) {
      if (soption == "--fxid") {
        if ((option = tokenizer.GetToken()) == nullptr) {
          std::cerr << "error: fxid option needs a value\n\n";
          return false;
        }

        uint64_t fid = eos::common::FileId::Hex2Fid(option);

        if (fid == 0ull) {
          std::cerr << "error: fid option needs to be non-zero\n\n";
          return false;
        }

        repair->set_fid(fid);
      } else if (soption == "--fsid") {
        if ((option = tokenizer.GetToken()) == nullptr) {
          std::cerr << "error: fsid option needs a value\n\n";
          return false;
        }

        soption = option;
        uint64_t fsid {0ull};

        try {
          fsid = std::stoull(soption);
        } catch (...) {
          std::cerr << "error: fsid option needs to be numeric\n\n";
          return false;
        }

        if (fsid == 0ull) {
          std::cerr << "error: fsid option needs to be non-zero\n";
          return false;
        }

        repair->set_fsid_err(fsid);
      } else if (soption == "--error") {
        if ((option = tokenizer.GetToken()) == nullptr) {
          std::cerr << "error: the error flag needs an option\n\n";
          return false;
        }

        repair->set_error(option);
      } else if (soption == "--async") {
        repair->set_async(true);
      } else {
        std::cerr << "error: unknown option \"" << soption << "\"\n\n";
        return false;
      }
    }
  } else if (cmd == "clean_orphans") {
    eos::console::FsckProto::CleanOrphansProto* clean =
      fsck->mutable_clean_orphans();

    if (tokenizer.NextToken(soption)) {
      if (soption != "--fsid") {
        std::cerr << "error: unknown option \"" << soption << "\"\n\n";
        return false;
      }

      if (!tokenizer.NextToken(soption)) {
        std::cerr << "error: missing file system id value\n\n";
        return false;
      }

      eos::common::FileSystem::fsid_t fsid = 0ul;

      try {
        size_t pos = 0;
        fsid = std::stoul(soption.c_str(), &pos);

        if (pos != soption.length()) {
          throw std::invalid_argument("fsid not numeric");
        }
      } catch (...) {
        std::cerr << "error: file system id must be numeric\n\n";
        return false;
      }

      clean->set_fsid(fsid);
    } else {
      // Clean orphans for all file systems i.e. fsid=0;
      clean->set_fsid(0ull);
    }
  } else {
    return false;
  }

  return true;
}
