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
  std::string log_file {"EOSFileMD.log"};
  CLI::App app("Tool to translate/inspect file fsck metadata");
  app.require_subcommand();
  auto convert_subcmd = app.add_subcommand("convert",
                        "Convert from LevelDB -> Attrs");
  std::string fst_path;
  std::string fst_metadir {"/var/eos/md"};
  size_t num_threads{8};
  convert_subcmd->add_option("--fst-path", fst_path, "Mount point of FST")
  ->required();
  convert_subcmd->add_option("--fst-metadir", fst_metadir,
                             "Metadir directory of FST ")
  ->default_str("/var/eos/md");
  convert_subcmd->add_option("--num-threads", num_threads,
                             "Num of threads for conversion")
  ->default_str("8");
  convert_subcmd->add_option("--log-file", log_file, "Log file for operations")
  ->default_str("EOSFileMD.log");
  std::string file_path;
  auto inspect_subcmd = app.add_subcommand("inspect",
                        "inspect fsck filemd attributes");
  inspect_subcmd->add_option("--path", file_path, "full path to file")
  ->required();
  inspect_subcmd->add_option("--log-file", log_file, "Log file for operations")
  ->default_str("EOSFileMD.log");

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  auto& g_logger = eos::common::Logging::GetInstance();
  g_logger.SetLogPriority(LOG_INFO);
  g_logger.SetUnit("EOSFileMD");
  std::unique_ptr<FILE, decltype(&fclose)> fptr {fopen(log_file.c_str(), "a+"),
      &fclose};

  if (!configureLogger(fptr.get())) {
    std::cerr << "FAILED to setup logging with log_file: " << log_file
              << std::endl;
  }

  auto db_handler = std::make_unique<eos::fst::FmdDbMapHandler>();
  auto attr_handler = std::make_unique<eos::fst::FmdAttrHandler>(
                        eos::fst::makeFSPathHandler(fst_path));
  auto fsid = eos::fst::FSPathHandler::GetFsid(fst_path);
  eos_static_info("msg=\"Got FSID from .eosfsid\"fsid=%u", fsid);

  if (fst_metadir.empty()) {
    std::cerr << "Empty meta dir given!" << std::endl;
    return -1;
  }

  db_handler->SetDBFile(fst_metadir.c_str(), fsid);

  if (app.got_subcommand("convert")) {
    eos::fst::FmdConverter converter(db_handler.get(), attr_handler.get(),
                                     num_threads);
    eos_static_info("msg=\"Converting with total threads\"=%ul", num_threads);
    converter.ConvertFS(fst_path);
  }

  if (app.got_subcommand("inspect")) {
    auto [status, fmd] = attr_handler->LocalRetrieveFmd(file_path);

    if (!status) {
      std::cerr << "Failed to retreive FSCK file md at path=" << file_path
                << std::endl;
    }

    std::cout << fmd.mProtoFmd.DebugString() << std::endl;
  }

  eos_static_info("%s", "msg=\"Finished Conversion!\"");
  return 0;
}

