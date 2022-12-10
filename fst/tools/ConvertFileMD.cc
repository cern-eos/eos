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
  std::string log_file {""};
  std::string log_level {"err"}; // accepts info, debug, err, crit, warning etc.
  CLI::App app("Tool to translate/inspect filemd metadata");
  app.add_option("--log-level", log_level, "Logging level", true);
  app.require_subcommand();
  auto convert_subcmd = app.add_subcommand("convert",
                        "Convert from LevelDB -> Attrs");
  std::string fst_path;
  std::string fst_metadir {"/var/eos/md"};
  std::string executor_type {"folly"};
  size_t num_threads{8};
  convert_subcmd->add_option("--fst-path", fst_path, "Mount point of FST")
  ->required();
  convert_subcmd->add_option("--fst-metadir", fst_metadir,
                             "Metadir directory of FST ", true);
  convert_subcmd->add_option("--num-threads", num_threads,
                             "Num of threads for conversion", true);
  convert_subcmd->add_option("--log-file", log_file, "Log file for operations",
                             true);
  convert_subcmd->add_option("--executor", executor_type,
                             "Executor Type: folly or std", true);
  std::string file_path;
  auto inspect_subcmd = app.add_subcommand("inspect",
                        "inspect filemd attributes");
  inspect_subcmd->add_option("--path", file_path, "full path to file")
  ->required();
  inspect_subcmd->add_option("--log-file", log_file,
                             "Log file for operations", true);

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  auto& g_logger = eos::common::Logging::GetInstance();
  g_logger.SetLogPriority(g_logger.GetPriorityByString(log_level.c_str()));
  g_logger.SetUnit("EOSFileMD");
  std::unique_ptr<FILE, decltype(&fclose)> fptr {fopen(log_file.c_str(), "a+"), &fclose};

  if (fptr.get() && !configureLogger(fptr.get())) {
    std::cerr << "error: failed to setup logging using log_file: " << log_file
              << std::endl;
    return -1;
  }

  auto attr_handler = std::make_unique<eos::fst::FmdAttrHandler>(
                        eos::fst::makeFSPathHandler(fst_path));

  if (app.got_subcommand("convert")) {
    if (fst_metadir.empty()) {
      std::cerr << "error: empty meta dir given" << std::endl;
      return -1;
    }

    auto fsid = eos::fst::FSPathHandler::GetFsid(fst_path);
    eos_static_info("msg=\"got FSID from .eosfsid\" fsid=%u", fsid);
    auto db_handler = std::make_unique<eos::fst::FmdDbMapHandler>();
    db_handler->SetDBFile(fst_metadir.c_str(), fsid);
    eos::fst::FmdConverter converter(db_handler.get(), attr_handler.get(),
                                     num_threads, executor_type);
    eos_static_info("msg=\"starting conversion\" num_threads=%ul executor=%s",
                    num_threads, executor_type.c_str());
    converter.ConvertFS(fst_path);
  }

  if (app.got_subcommand("inspect")) {
    auto [status, fmd] = attr_handler->LocalRetrieveFmd(file_path);

    if (!status) {
      std::cerr << "error: failed to retreive filemd for path="
                << file_path << std::endl;
    }

    std::cout << fmd.mProtoFmd.DebugString() << std::endl;
  }

  eos_static_info("%s", "msg=\"finished conversion\"");
  return 0;
}
