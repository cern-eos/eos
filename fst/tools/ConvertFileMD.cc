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
  }

  return 0;
}
