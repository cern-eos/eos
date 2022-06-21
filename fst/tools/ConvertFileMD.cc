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
#include <iostream>
#include <memory>

void usage()
{
  std::cerr << "eos-filemd-convert <fst-metadir> <fs-mount-path>\n"
            << "eg: eos-filemd-convert /data23\n"
            << std::endl;

}

int
main(int argc, char* argv[])
{
  if (argc != 3) {
    usage();
    return (-1);
  }

  std::string fst_metadir = argv[1];
  std::string fst_path = argv[2];


  auto db_handler = std::make_unique<eos::fst::FmdDbMapHandler>();
  auto attr_handler = std::make_unique<eos::fst::FmdAttrHandler>(
      eos::fst::makeFSPathHandler(fst_path));

  auto fsid = eos::fst::FSPathHandler::GetFsid(fst_path);
  db_handler->SetDBFile(fst_metadir.c_str(), fsid);

  eos::fst::FmdConverter converter(db_handler.get(), attr_handler.get(), 8);
  std::cout << "Converting...\n";
  converter.ConvertFS(fst_path);
  std::cout << "Done\n";
  db_handler->ShutdownDB(fsid, true);

  return 0;

}

