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
#include <sys/types.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
  if (argc < 2) {
    std::cerr << "error: path argument required" << std::endl;
    return -1;
  }

  std::string input_path = argv[1];
  std::string device_path = eos::fst::GetDevicePath(input_path);
  std::string measure_path = device_path.empty() ? input_path : device_path;

  if (device_path.empty()) {
    std::cerr << "warning: could not resolve block device for " << input_path <<
              ", using path as is." << std::endl;
  } else {
    std::cout << "info: resolved " << input_path << " to device " << device_path <<
              std::endl;
  }

  // Open the file for direct access
  int fd = open(measure_path.c_str(), O_RDONLY | O_DIRECT);

  if (fd == -1) {
    std::cerr << "err: failed to open file/device " << measure_path << std::endl;
    eos_static_err("msg=\"failed to open file/device\" path=%s",
                   measure_path.c_str());
    return -1;
  }

  uint64_t rd_buf_size = 4 * (1 << 20); // 4MB
  std::cout << "Path=" << measure_path << std::endl
            << "IOPS=" << eos::fst::ComputeIops(fd) << std::endl
            << "BW=" << eos::fst::ComputeBandwidth(fd, rd_buf_size) << " MB/s"
            << std::endl;
  (void) close(fd);
  return 0;
}
