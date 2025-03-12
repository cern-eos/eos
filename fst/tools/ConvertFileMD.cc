// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2022 CERN/Switzerland                                  *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************

#include "fst/filemd/FmdAttr.hh"
#include "fst/utils/FSPathHandler.hh"
#include "common/StringUtils.hh"
#include "common/Logging.hh"
#include "common/CLI11.hpp"
#include <iostream>
#include <memory>

bool configureLogger(FILE* fp)
{
  if (fp == nullptr) {
    return false;
  }

  // Redirect stdout and stderr to the log file
  int ret = dup2(fileno(fp), fileno(stdout));

  if (ret != -1) {
    ret = dup2(fileno(fp), fileno(stderr));
  }

  return ret != -1;
}

int
main(int argc, char* argv[])
{
  std::string log_file {""};
  std::string log_level {"err"}; // accepts info, debug, err, crit, warning etc.
  CLI::App app("Tool to inspect filemd metadata");
  app.add_option("--log-level", log_level, "Logging level", true);
  app.require_subcommand();
  std::string file_path;
  auto inspect_subcmd = app.add_subcommand("inspect",
                        "inspect filemd attributes");
  inspect_subcmd->add_option("--path", file_path, "full path to file")
  ->required();
  inspect_subcmd->add_option("--log-file", log_file,
                             "Log file for operations", true);
  uint64_t fid, cid, size, disksize, mgmsize;
  uint32_t fsid, ctime, ctime_ns, mtime, mtime_ns, atime, atime_ns, checktime,
           lid, uid, gid;
  std::string checksum, diskchecksum, mgmchecksum, unitchecksum;
  auto set_subcmd = app.add_subcommand("set",
                                       "set filemd attributes (N.B. may corrupt the file)");
  set_subcmd->add_option("--path", file_path, "full path to file")->required();
  auto fid_opt = set_subcmd->add_option("--fid", fid, "fileid");
  auto cid_opt = set_subcmd->add_option("--cid", cid, "container id");
  auto fsid_opt = set_subcmd->add_option("--fsid", fsid, "filesystem id");
  auto ctime_opt = set_subcmd->add_option("--ctime", ctime, "creation time");
  auto ctime_ns_opt = set_subcmd->add_option("--ctime-ns", ctime_ns,
                      "ns od creation time");
  auto mtime_opt = set_subcmd->add_option("--mtime", mtime, "modification time");
  auto mtime_ns_opt = set_subcmd->add_option("--mtime-ns", mtime_ns,
                      "ns of modification time");
  auto atime_opt = set_subcmd->add_option("--atime", atime, "access time");
  auto atime_ns_opt = set_subcmd->add_option("--atime-ns", atime,
                      "ns of access time");
  auto checktime_opt = set_subcmd->add_option("--checktime", checktime,
                       "time of last checksum scan");
  auto size_opt = set_subcmd->add_option("--size", size, "size");
  auto disksize_opt = set_subcmd->add_option("--disksize", disksize,
                      "size on disk");
  auto mgmsize_opt = set_subcmd->add_option("--mgmsize", mgmsize,
                     "size on the MGM");
  auto checksum_opt = set_subcmd->add_option("--checksum", checksum,
                      "checksum in hex representation");
  auto diskchecksum_opt = set_subcmd->add_option("--diskchecksum", diskchecksum,
                          "checksum in hex representation");
  auto mgmchecksum_opt = set_subcmd->add_option("--mgmchecksum", mgmchecksum,
                         "checksum in hex representation");
  auto unitchecksum_opt = set_subcmd->add_option("--unitchecksum", unitchecksum,
                          "checksum in hex representation");

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  auto& g_logger = eos::common::Logging::GetInstance();
  g_logger.SetLogPriority(g_logger.GetPriorityByString(log_level.c_str()));
  g_logger.SetUnit("EOSFileMD");
  std::unique_ptr<FILE, int(*)(FILE*)> fptr {fopen(log_file.c_str(), "a+"), &fclose};

  if (fptr.get() && !configureLogger(fptr.get())) {
    std::cerr << "error: failed to setup logging using log_file: " << log_file
              << std::endl;
    return -1;
  }

  auto attr_handler = std::make_unique<eos::fst::FmdAttrHandler>(
                        eos::fst::makeFSPathHandler(""));

  if (app.got_subcommand("inspect")) {
    auto [status, fmd] = attr_handler->LocalRetrieveFmd(file_path);

    if (!status) {
      std::cerr << "error: failed to retreive filemd for path="
                << file_path << std::endl;
    }

    std::cout << fmd.mProtoFmd.DebugString() << std::endl;
  } else if (app.got_subcommand("set")) {
    auto [status, fmd] = attr_handler->LocalRetrieveFmd(file_path);

    if (!status) {
      std::cerr << "error: failed to retreive filemd for path="
                << file_path << std::endl;
    }

    bool updated = false;

    if (!fid_opt->empty()) {
      fmd.mProtoFmd.set_fid(fid);
      updated = true;
    }

    if (!cid_opt->empty()) {
      fmd.mProtoFmd.set_cid(cid);
      updated = true;
    }

    if (!fsid_opt->empty()) {
      fmd.mProtoFmd.set_fsid(fsid);
      updated = true;
    }

    if (!ctime_opt->empty()) {
      fmd.mProtoFmd.set_ctime(ctime);
      updated = true;
    }

    if (!ctime_ns_opt->empty()) {
      fmd.mProtoFmd.set_ctime_ns(ctime_ns);
      updated = true;
    }

    if (!mtime_opt->empty()) {
      fmd.mProtoFmd.set_mtime(mtime);
      updated = true;
    }

    if (!mtime_ns_opt->empty()) {
      fmd.mProtoFmd.set_mtime_ns(mtime_ns);
      updated = true;
    }

    if (!atime_opt->empty()) {
      fmd.mProtoFmd.set_atime(atime);
      updated = true;
    }

    if (!atime_ns_opt->empty()) {
      fmd.mProtoFmd.set_atime_ns(atime_ns);
      updated = true;
    }

    if (!checktime_opt->empty()) {
      fmd.mProtoFmd.set_checktime(checktime);
      updated = true;
    }

    if (!size_opt->empty()) {
      fmd.mProtoFmd.set_size(size);
      updated = true;
    }

    if (!disksize_opt->empty()) {
      fmd.mProtoFmd.set_disksize(disksize);
      updated = true;
    }

    if (!mgmsize_opt->empty()) {
      fmd.mProtoFmd.set_mgmsize(mgmsize);
      updated = true;
    }

    if (!checksum_opt->empty()) {
      fmd.mProtoFmd.set_checksum(checksum);
      updated = true;
    }

    if (!diskchecksum_opt->empty()) {
      fmd.mProtoFmd.set_diskchecksum(diskchecksum);
      updated = true;
    }

    if (!mgmchecksum_opt->empty()) {
      fmd.mProtoFmd.set_mgmchecksum(mgmchecksum);
      updated = true;
    }

    if (!unitchecksum_opt->empty()) {
      fmd.mProtoFmd.set_unitchecksum(unitchecksum);
      updated = true;
    }

    if (updated) {
      if (!attr_handler->Commit(&fmd, true, &file_path)) {
        std::cerr << "error: failed to setting filemd for path="
                  << file_path << std::endl;
      }
    } else {
      std::cerr << "nothing to update" << std::endl;
    }
  }

  return 0;
}
