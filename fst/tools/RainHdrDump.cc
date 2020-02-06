//------------------------------------------------------------------------------
//! @file RainHdrDump.cc
//! @author Elvin-Alin Sindrilaru - CERN
//! @brief Tool to dump the header information of a RAIN stripe file
//------------------------------------------------------------------------------

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

#include "fst/layout/HeaderCRC.hh"
#include "fst/io/local/FsIo.hh"
#include <iostream>

int main(int argc, char* argv[])
{
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <rain_stripe_file>" << std::endl;
    return -1;
  }

  std::string stripe_path = argv[1];
  struct stat st;

  if (stat(stripe_path.c_str(), &st)) {
    std::cerr << "ERROR: No such file " << stripe_path << std::endl;
    return -1;
  }

  eos::fst::FsIo f(stripe_path);

  if (f.fileOpen(SFS_O_RDONLY)) {
    std::cerr << "ERROR: Failed to open file " << stripe_path << std::endl;
    return -1;
  }

  eos::fst::HeaderCRC hd(0, 0);

  if (hd.ReadFromFile(&f, 0)) {
    std::cout << "RAIN header info:" << std::endl
              << hd.DumpInfo() << std::endl;
  } else {
    std::cout << "ERROR: Failed to read header information!" << std::endl;
  }

  return 0;
}
