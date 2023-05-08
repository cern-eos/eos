//------------------------------------------------------------------------------
// File: DiskMeasurements.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "fst/utils/DiskMeasurements.hh"
#include "common/Logging.hh"
#include <chrono>
#include <iostream>
#include <fcntl.h>

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  if (argc < 2) {
    std::cerr << "error: path argument required" << std::endl;
    return -1;
  }

  std::string base_path = argv[1];
  auto start = std::chrono::system_clock::now();
  uint64_t fn_size = 1 << 30; // 1 GB
  // Create temporary file name given the base path
  const std::string fn_path = eos::fst::MakeTemporaryFile(base_path);

  if (fn_path.empty()) {
    std::cerr << "err: failed to create tmp file" << std::endl;
    eos_static_err("msg=\"failed to create tmp file\" base_path=%s",
                   base_path.c_str());
    return -1;
  }

  // Fill the file up to the given size with random data
  if (!eos::fst::FillFileGivenSize(fn_path, fn_size)) {
    std::cerr << "err: failed to fill file" << std::endl;
    eos_static_err("msg=\"failed to fill file\" path=%s", fn_path.c_str());
    unlink(fn_path.c_str());
    return -1;
  }

  auto end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
                  (end - start).count();
  std::cout << "File generation took: " << duration << " ms" << std::endl;
  uint64_t rd_buf_size = 4 * (1 << 20); // 4MB
  std::cout << "Path=" << fn_path << std::endl
            << "IOPS=" << eos::fst::ComputeIops(fn_path) << std::endl
            << "  BW=" << eos::fst::ComputeBandwidth(fn_path, rd_buf_size) << " MB/s"
            << std::endl;
  unlink(fn_path.c_str());
  return 0;
}
