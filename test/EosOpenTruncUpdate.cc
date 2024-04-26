//------------------------------------------------------------------------------
//! @file EosOpenTrunUpdate.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <memory>
#include <iostream>

//------------------------------------------------------------------------------
// Generate random data
//------------------------------------------------------------------------------
void
GenerateRandomData(char* data, ssize_t length)
{
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(data, length);
  urandom.close();
}

XrdPosixXrootd posixXrootd;
//------------------------------------------------------------------------------
//! Open truncate a file and write into it.
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  if (argc < 2 || (strncmp("-h", argv[1], 2) == 0)) {
    std::cerr << "Usage: " << argv[0] << " <url> [<max_file_sz>]" << std::endl;
    exit(EINVAL);
  }

  size_t max_sz {64 * 1024 * 1024};

  if (argc == 3) {
    try {
      max_sz = std::stoull(std::string(argv[2]));
    } catch (...) {}
  }

  uint64_t off {0ull};
  size_t sz {4 * 1024 * 1024};
  uint64_t len = sz;
  std::unique_ptr<char[]> buffer = std::make_unique<char[]>(sz);
  GenerateRandomData(buffer.get(), sz);
  std::string surl = argv[1];
  XrdCl::URL url(surl);

  if (!url.IsValid()) {
    std::cerr << "Usage: " << argv[0] << " <url> [<max_file_sz>]" << std::endl;
    exit(EINVAL);
  }

  int fd = XrdPosixXrootd::Open(surl.c_str(), O_RDWR,
                                kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or);
  XrdPosixXrootd::Ftruncate(fd, 0);

  while (off < max_sz) {
    if (max_sz - off < sz) {
      len = max_sz - off;
    }

    // Ignore on purpose the return from pwrite
    (void) XrdPosixXrootd::Pwrite(fd, buffer.get(), len, off);
    off += len;
  }

  return 0;
}
