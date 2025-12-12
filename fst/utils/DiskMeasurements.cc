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
#include <sys/sysmacros.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sys/ioctl.h>
#include <linux/fs.h>

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
// Get block device for a given path
//------------------------------------------------------------------------------
std::string GetDevicePath(const std::string& path)
{
  struct stat st;

  if (stat(path.c_str(), &st)) {
    return "";
  }

  dev_t dev = st.st_dev;
  FILE* file = fopen("/proc/self/mountinfo", "r");

  if (!file) {
    file = fopen("/proc/mounts", "r");
  }

  if (!file) {
    return "";
  }

  char* line = NULL;
  size_t len = 0;
  unsigned int major, minor;
  std::string device_path;

  while (getline(&line, &len, file) != -1) {
    // Try parsing mountinfo format first: "id parent major:minor ..."
    // If not, it might be /proc/mounts format, but /proc/self/mountinfo is standard on modern linux
    // Scan for major:minor
    int num_scanned = sscanf(line, "%*d %*d %u:%u", &major, &minor);

    if (num_scanned == 2) {
      if (makedev(major, minor) == dev) {
        // Found it in mountinfo format
        // The device is usually the field after " - "
        // Format: ... - <fstype> <device> <options>
        char* sep = strstr(line, " - ");

        if (sep) {
          char* fstype = strtok(sep + 3, " ");
          char* dev_str = strtok(NULL, " ");
          (void) fstype;

          if (dev_str) {
            device_path = dev_str;
            break;
          }
        }
      }
    } else {
      // Fallback for simple /proc/mounts format: <device> <mountpoint> ...
      // We need to stat the mountpoint to see if it matches our dev
      char dev_str[1024];
      char mount_str[1024];

      if (sscanf(line, "%1023s %1023s", dev_str, mount_str) == 2) {
        struct stat mp_st;

        if (stat(mount_str, &mp_st) == 0) {
          if (mp_st.st_dev == dev) {
            // This mountpoint corresponds to our device
            // But wait, st_dev of a file IS the device ID of the filesystem it is on.
            // So if st_dev matches mp_st.st_dev, then dev_str is likely our device.
            device_path = dev_str;
            break;
          }
        }
      }
    }
  }

  free(line);
  fclose(file);
  return device_path;
}

//------------------------------------------------------------------------------
// Get file/device size
//------------------------------------------------------------------------------
uint64_t GetBlkSize(int fd)
{
  struct stat st;

  if (fstat(fd, &st)) {
    return 0;
  }

  if (S_ISBLK(st.st_mode)) {
    uint64_t size = 0;

    if (ioctl(fd, BLKGETSIZE64, &size) == 0) {
      return size;
    }
  }

  return st.st_size;
}

//------------------------------------------------------------------------------
// Get IOPS measurement for the given path
//------------------------------------------------------------------------------
int ComputeIops(int fd, uint64_t rd_buf_size, std::chrono::seconds timeout)
{
  using namespace eos::common;
  using namespace std::chrono;
  int IOPS = -1;
  uint64_t fn_size = GetBlkSize(fd);

  if (fn_size == 0) {
    std::cerr << "err: failed to get file size fd=" << fd << std::endl;
    eos_static_err("msg=\"failed to get file size\" fd=%i", fd);
    return IOPS;
  }

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
  uint64_t fn_size = GetBlkSize(fd);

  if (fn_size == 0) {
    std::cerr << "err: failed to get file size fd=" << fd << std::endl;
    eos_static_err("msg=\"failed to get file size\" fd=%i", fd);
    return bandwidth;
  }

  auto buf = GetAlignedBuffer(rd_buf_size);
  uint64_t max_read = 1 << 28; // 256 MB
  // Randomize start offset if file is large enough
  uint64_t offset = 0;

  if (fn_size > max_read) {
    std::random_device rd;
    std::mt19937 gen(rd());
    // Align to rd_buf_size (4MB)
    uint64_t max_blocks = (fn_size - max_read) / rd_buf_size;
    std::uniform_int_distribution<uint64_t> distrib(0, max_blocks);
    offset = distrib(gen) * rd_buf_size;
  }

  uint64_t start_offset = offset;
  uint64_t end_offset = offset + max_read;

  if (end_offset > fn_size) {
    end_offset = fn_size;
  }

  time_point<high_resolution_clock> start, end;
  start = high_resolution_clock::now();

  while (offset < end_offset) {
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
  bandwidth = (((offset - start_offset) >> 20) * 1000000.0) / duration;
  return bandwidth;
}

EOSFSTNAMESPACE_END
