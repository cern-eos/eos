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
bool FillFileGivenSize(const std::string& path, size_t length)
{
  int fd = open(path.c_str(), O_TRUNC | O_WRONLY);

  if (fd == -1) {
    return false;
  }

  int retc = 0, nwrite = 0;
  const size_t sz {4 * 1024 * 1024};
  std::unique_ptr<char[]> buffer {new char[sz]()};
  GenerateRandomData(buffer.get(), sz);

  while (length > 0) {
    nwrite = (length < sz) ? length : sz;
    retc = write(fd, buffer.get(), nwrite);

    if (retc != nwrite) {
      (void) close(fd);
      unlink(path.c_str());
      return false;
    }

    length -= nwrite;
  }

  fsync(fd);
  close(fd);
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
int ComputeIops(const std::string& fn_path, uint64_t rd_buf_size)
{
  int IOPS = -1;
  // Open the file for direct reading
  int fd = open(fn_path.c_str(), O_RDONLY | O_DIRECT | O_SYNC);

  if (fd == -1) {
    std::cerr << "err: failed to open for direct reading" << std::endl;
    eos_static_err("msg=\"failed to open file for direct reading\" path=%s",
                   fn_path.c_str());
    return IOPS;
  }

  // Get file size
  struct stat info;

  if (fstat(fd, &info)) {
    std::cerr << "err: failed to stat file " << fn_path << std::endl;
    eos_static_err("msg=\"failed to stat file\" path=%s", fn_path.c_str());
    return IOPS;
  }

  uint64_t fn_size = info.st_size;
  char* buf = nullptr;
  int retc = posix_memalign((void**)&buf, 0x1000, rd_buf_size);

  if (retc != 0) {
    std::cerr << "err: failed to allocate aligned memory" << std::endl;
    eos_static_err("msg=\"failed to allocate aligned memory\" sz=%i",
                   rd_buf_size);
    close(fd);
    return IOPS;
  }

  // Get a uniform int distribution for offset generation
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distrib(0, 1024);
  int iterations = 10000;
  uint64_t offset = 0ull;
  double duration = 0.0;
  std::chrono::time_point<std::chrono::high_resolution_clock> start, end;

  for (int i = 0; i < iterations; ++i) {
    // Generate offset 4kB aligned inside the given file size
    offset = (((fn_size * distrib(gen)) >> 10) >> 12) << 12;
    start = std::chrono::high_resolution_clock::now();

    if (pread(fd, buf, rd_buf_size, offset) == -1) {
      std::cerr << "error: failed to read at offset=" << offset << std::endl;
      eos_static_err("msg=\"failed read\" offset=%llu", offset);
      (void) close(fd);
      free(buf);
      return IOPS;
    }

    end = std::chrono::high_resolution_clock::now();
    duration += std::chrono::duration_cast<std::chrono::microseconds>
                (end - start).count();
  }

  IOPS = iterations / (duration / 1000000);
  (void) close(fd);
  free(buf);
  return IOPS;
}

//------------------------------------------------------------------------------
// Get disk bandwidth for the given path
//------------------------------------------------------------------------------
int ComputeBandwidth(const std::string& fn_path, uint64_t rd_buf_size)
{
  int bandwidth = -1;
  // Open the file for direct reading
  int fd = open(fn_path.c_str(), O_RDONLY | O_DIRECT | O_SYNC);

  if (fd == -1) {
    std::cerr << "err: failed to open for direct reading" << std::endl;
    eos_static_err("msg=\"failed to open file for direct reading\" path=%s",
                   fn_path.c_str());
    return bandwidth;
  }

  // Get file size
  struct stat info;

  if (fstat(fd, &info)) {
    std::cerr << "err: failed to stat file " << fn_path << std::endl;
    eos_static_err("msg=\"failed to stat file\" path=%s", fn_path.c_str());
    return bandwidth;
  }

  uint64_t fn_size = info.st_size;
  char* buf = nullptr;
  int retc = posix_memalign((void**)&buf, 0x1000, rd_buf_size);

  if (retc != 0) {
    std::cerr << "err: failed to allocate aligned memory" << std::endl;
    eos_static_err("msg=\"failed to allocate aligned memory\" sz=%i",
                   rd_buf_size);
    close(fd);
    return bandwidth;
  }

  uint64_t offset = 0ull;
  uint64_t max_read = 1 << 28; // 256 MB
  auto start = std::chrono::high_resolution_clock::now();

  while ((offset < fn_size)  && (offset < max_read)) {
    if (pread(fd, buf, rd_buf_size, offset) == -1) {
      std::cerr << "error: failed to read at offset=" << offset << std::endl;
      eos_static_err("msg=\"failed read\" offset=%llu", offset);
      (void) close(fd);
      free(buf);
      return bandwidth;
    }

    offset += rd_buf_size;
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>
                  (end - start).count();
  bandwidth = ((offset >> 20) * 1000000) / duration;
  (void) close(fd);
  free(buf);
  return bandwidth;
}

EOSFSTNAMESPACE_END
