//------------------------------------------------------------------------------
// File: DiskMeasurements.cc
// Author: Elvin Sindrilaru - CERN
// ----------------------------------------------------------------------

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
#include "common/BufferManager.hh"
#include <iostream>
#include <chrono>
#include <random>
#include <sys/stat.h>
#include <fcntl.h>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Generate random data
//------------------------------------------------------------------------------
void
GenerateRandomData(char* data, size_t length)
{
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(data, length);
  urandom.close();
}

//------------------------------------------------------------------------------
// Create file path with given size
//------------------------------------------------------------------------------
bool FillFileGivenSize(int fd, size_t length)
{
  using namespace eos::common;
  int retc = 0, nwrite = 0;
  const size_t sz {4 * 1024 * 1024};
  auto buffer = GetAlignedBuffer(sz);
  GenerateRandomData(buffer.get(), sz);

  while (length > 0) {
    nwrite = (length < sz) ? length : sz;
    retc = write(fd, buffer.get(), nwrite);

    if (retc != nwrite) {
      return false;
    }

    length -= nwrite;
  }

  fsync(fd);
  return true;
}

//------------------------------------------------------------------------------
// Create random temporary file in given location
//------------------------------------------------------------------------------
std::string MakeTemporaryFile(std::string base_path)
{
  // Absolute base path  specified
  if (base_path.empty() || (*base_path.begin() != '/')) {
    eos_static_err("msg=\"base path needs to a an absolute path\" base_path=%s",
                   base_path.c_str());
    return "";
  }

  // Make sure path is / terminated
  if (*base_path.rbegin() != '/') {
    base_path += '/';
  }

  char tmp_path[1024];
  snprintf(tmp_path, sizeof(tmp_path), "%sfst.ioping.XXXXXX", base_path.c_str());
  int tmp_fd = mkstemp(tmp_path);

  if (tmp_fd == -1) {
    eos_static_crit("%s", "msg=\"failed to create temporary file!\"");
    return "";
  }

  (void) close(tmp_fd);
  return tmp_path;
}

//------------------------------------------------------------------------------
// Get IOPS measurement for the given path
//------------------------------------------------------------------------------
int ComputeIops(int fd, uint64_t rd_buf_size, std::chrono::seconds timeout)
{
  using namespace eos::common;
  using namespace std::chrono;
  int IOPS = -1;
  // Get file size
  struct stat info;

  if (fstat(fd, &info)) {
    std::cerr << "err: failed to stat file fd=" << fd << std::endl;
    eos_static_err("msg=\"failed to stat file\" fd=%i", fd);
    return IOPS;
  }

  uint64_t fn_size = info.st_size;
  auto buf = GetAlignedBuffer(rd_buf_size);
  // Get a uniform int distribution for offset generation
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, 1024);
  int iterations = 10000;
  int actual_iter = 0;
  uint64_t offset = 0ull;
  microseconds duration {0};
  time_point<high_resolution_clock> start, end;

  for (; actual_iter < iterations; ++actual_iter) {
    // Generate offset 4kB aligned inside the given file size
    offset = (((fn_size * distrib(gen)) >> 10) >> 12) << 12;
    start = high_resolution_clock::now();

    if (pread(fd, buf.get(), rd_buf_size, offset) == -1) {
      std::cerr << "error: failed to read at offset=" << offset << std::endl;
      eos_static_err("msg=\"failed read\" offset=%llu", offset);
      return IOPS;
    }

    end = high_resolution_clock::now();
    duration += duration_cast<microseconds>(end - start);

    if (actual_iter % 10 == 0) {
      if (duration.count() > timeout.count() * 1000000) {
        break;
      }
    }
  }

  IOPS = (actual_iter * 1000000.0) / duration.count();
  return IOPS;
}

//------------------------------------------------------------------------------
// Get disk bandwidth for the given path
//------------------------------------------------------------------------------
int ComputeBandwidth(int fd, uint64_t rd_buf_size, std::chrono::seconds timeout)
{
  using namespace eos::common;
  using namespace std::chrono;
  int bandwidth = -1;
  // Get file size
  struct stat info;

  if (fstat(fd, &info)) {
    std::cerr << "err: failed to stat file fd=" << fd << std::endl;
    eos_static_err("msg=\"failed to stat file\" fd=%i", fd);
    return bandwidth;
  }

  uint64_t fn_size = info.st_size;
  auto buf = GetAlignedBuffer(rd_buf_size);
  uint64_t offset = 0ull;
  uint64_t max_read = 1 << 28; // 256 MB
  time_point<high_resolution_clock> start, end;
  start = high_resolution_clock::now();

  while ((offset < fn_size)  && (offset < max_read)) {
    if (pread(fd, buf.get(), rd_buf_size, offset) == -1) {
      std::cerr << "error: failed to read at offset=" << offset << std::endl;
      eos_static_err("msg=\"failed read\" offset=%llu", offset);
      return bandwidth;
    }

    offset += rd_buf_size;

    if ((offset & (eos::common::MB - 1)) == 0) {
      if (duration_cast<seconds>(high_resolution_clock::now() - start) > timeout) {
        break;
      }
    }
  }

  end = high_resolution_clock::now();
  auto duration = duration_cast<microseconds> (end - start).count();
  bandwidth = ((offset >> 20) * 1000000.0) / duration;
  return bandwidth;
}

EOSFSTNAMESPACE_END
