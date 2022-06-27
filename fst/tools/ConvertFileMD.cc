// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2022 CERN/Switzerland                           *
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
//

#include "fst/filemd/FmdAttr.hh"
#include "fst/filemd/FmdConverter.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/utils/FSPathHandler.hh"
#include "common/StringUtils.hh"
#include "common/Logging.hh"
#include <iostream>
#include <memory>

void usage()
{
  std::cerr << "eos-filemd-convert <fst-metadir> <fs-mount-path> [log-file] [num-threads]\n"
            << "eg: eos-filemd-convert /var/eos/md /data23 ConvertFileMD.log 8\n"
            << std::endl;

}

int
main(int argc, char* argv[])
{
  if (argc < 3) {
    usage();
    return (-1);
  }

  auto& g_logger = eos::common::Logging::GetInstance();
  g_logger.SetLogPriority(LOG_INFO);
  g_logger.SetUnit("ConvertFileMD");

  std::string log_file = "ConvertFileMD.log";
  if (argc > 2) {
    log_file = argv[3];
  }

  std::unique_ptr<FILE, decltype(&fclose)> fptr {fopen(log_file.c_str(), "a+"),
                                                 &fclose};

  if (fptr) {
    // Redirect stdout and stderr to the log file
    dup2(fileno(fptr.get()), fileno(stdout));
    dup2(fileno(fptr.get()), fileno(stderr));
  } else {
    std::cerr << "Failed to open logging file: "<< log_file << std::endl;
  }


  std::string fst_metadir = argv[1];
  std::string fst_path = argv[2];
  size_t num_threads {8};

  if (argc > 3) {
    eos::common::StringToNumeric(std::string_view(argv[4]), num_threads, num_threads);
  }

  auto db_handler = std::make_unique<eos::fst::FmdDbMapHandler>();
  auto attr_handler = std::make_unique<eos::fst::FmdAttrHandler>(
      eos::fst::makeFSPathHandler(fst_path));

  auto fsid = eos::fst::FSPathHandler::GetFsid(fst_path);
  eos_static_info("msg=\"Got FSID from .eosfsid\"fsid=%u", fsid);
  db_handler->SetDBFile(fst_metadir.c_str(), fsid);

  eos::fst::FmdConverter converter(db_handler.get(), attr_handler.get(), num_threads);
  eos_static_info("msg=\"Converting with total threads\"=%ul", num_threads);
  converter.ConvertFS(fst_path);
  db_handler->ShutdownDB(fsid, true);
  eos_static_info("%s", "msg=\"Finished Conversion!\"");
  return 0;

}

