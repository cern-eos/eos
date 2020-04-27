/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "common/CLI11.hpp"
#include "common/FileId.hh"
#include <qclient/QClient.hh>

int main(int argc, char* argv[]) {
  CLI::App app("Tool to translate fids to FST paths");

  int64_t fid;
  app.add_option("--fid", fid, "Specify the file ID")->required();

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // Convert..
  //----------------------------------------------------------------------------
  std::string hex_fid = eos::common::FileId::Fid2Hex(fid);

  std::string prefix = "/data/";
  std::string path = eos::common::FileId::FidPrefix2FullPath(hex_fid.c_str(),
                prefix.c_str());

  std::cout << "id: " << fid << std::endl;
  std::cout << "fxid: " << hex_fid << std::endl;
  std::cout << "path: " << path << std::endl;
  return 0;
}

