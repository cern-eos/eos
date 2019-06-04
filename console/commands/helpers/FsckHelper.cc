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
#include "common/StringTokenizer.hh"

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

  if (cmd == "enable") {
    eos::console::FsckProto::EnableProto* enable = fsck->mutable_enable();
    enable->set_interval(0ul);

    // There is an interval specified
    if ((option = tokenizer.GetToken())) {
      try {
        enable->set_interval(std::stoul(option));
      } catch (...) {
        // set by default to 0
      }
    }
  } else if (cmd == "disable") {
    fsck->set_disable(true);
  } else if (cmd == "stat") {
    fsck->set_stat(true);
  } else if (cmd == "config") {
    if ((option = tokenizer.GetToken()) == nullptr) {
      return false;
    }

    std::string key = option;
    std::string value {""};

    if ((option = tokenizer.GetToken()) == nullptr) {
      return false;
    }

    value = option;
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
        report->set_display_fid(true);
      } else if (soption == "-l") {
        report->set_display_lfn(true);
      } else if ((soption == "-j") || (soption == "--json")) {
        report->set_display_json(true);
      } else if (soption == "-h") {
        report->set_display_help(true);
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
    std::set<std::string> allowed_types {"--checksum", "--checksum-commit",
                                         "--resync", "--unlink-unregistered", "--unlink-orphans",
                                         "--adjust-replicas", "--adjust-replicas-nodrop",
                                         "--drop-missing-replicas", "--unlink-zero-replicas",
                                         "--replace-damaged-replicas", "--all"};
    eos::console::FsckProto::RepairProto* repair = fsck->mutable_repair();

    while ((option = tokenizer.GetToken())) {
      soption = option;

      if (allowed_types.find(soption) == allowed_types.end()) {
        std::cerr << "error: unknown type of error " << soption << std::endl;
        return false;
      }

      std::string* type = repair->add_types();
      type->assign(soption);
    }
  } else if (cmd == "search") {
    if ((option = tokenizer.GetToken()) == nullptr) {
      return false;
    }

    size_t nrep = 0;
    std::string path = option;

    if ((option = tokenizer.GetToken())) {
      try {
        nrep = std::stoul(option);
      } catch (...) {
        // ignore
      }
    }

    filesystems fs;
    fs.Load();
    fs.Connect();
    files f;
    f.Find(path.c_str());
    std::cout << "# found " << f.Size() << " files" << std::endl;
    f.Lookup(fs);
    f.Report(nrep);
    mIsLocal = true;
  } else {
    return false;
  }

  return true;
}
