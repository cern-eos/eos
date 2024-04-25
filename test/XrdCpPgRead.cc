//------------------------------------------------------------------------------
//! @file XrdCpPgRead.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include <XrdCl/XrdClFile.hh>
#include <iostream>
#include <memory>
#include <random>
//------------------------------------------------------------------------------
//! This executable simulates a client that reads from a file using pgRead API.
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  constexpr uint32_t max_length = 10 * 1024 * 1024;
  int retc = 0;

  if (argc != 4) {
    std::cerr << "Usage: " << argv[0] << " <xrd_url> <offset> <length>\n"
              << "  <xrd_url> - XRootD URL of file read\n"
              << "  <offset>  - read offset\n"
              << "  <length>  - read offset\n";
    exit(EINVAL);
  }

  std::string surl = argv[1];
  XrdCl::URL url(surl);

  if (!url.IsValid()) {
    std::cerr << "error: given XRootD URL is not valid" << std::endl;
    exit(EINVAL);
  }

  uint64_t offset = 0ull;
  uint32_t length = 0ul;

  try {
    offset = std::stoull(std::string(argv[2]));
    length = std::stoul(std::string(argv[3]));
  } catch (...) {
    std::cerr << "error: failed to convert given input" << std::endl;
    exit(EINVAL);
  }

  if (length > max_length) {
    std::cerr << "error: length must be <= 10MB" << std::endl;
    exit(EINVAL);
  }

  // Allocate buffer used for transfer
  std::unique_ptr<char[]> buffer {new char[length]};
  XrdCl::File file;
  XrdCl::XRootDStatus status = file.Open(surl, XrdCl::OpenFlags::Read,
                                         XrdCl::Access::None);

  if (!status.IsOK()) {
    std::cerr << "error: unable to open file for reading, errno="
              << status.errNo << std::endl;
    exit(status.errNo);
  }

  std::vector<uint32_t> cksums;
  uint32_t bytes_read = 0ul;

  if ((offset == 0) && (length == 0)) {
    constexpr uint32_t max_buff = 4 * 1024 * 1024;
    buffer.reset(new char[max_buff]);
    std::mt19937_64 gen(12345678);
    std::uniform_int_distribution<uint64_t> dist_off(0ull,  1235676367ull);
    std::uniform_int_distribution<uint64_t> dist_len(4096ull,  max_buff);
    uint32_t samples = 10000;

    for (unsigned int i = 0; i < samples; ++i) {
      cksums.clear();
      uint64_t rand_off = dist_off(gen);
      uint32_t rand_len = dist_len(gen);
      std::cout << "index: " << i
                << " pgread: rand_off=" << rand_off
                << " rand_len=" << rand_len << std::endl;
      status = file.PgRead(rand_off, rand_len, buffer.get(), cksums, bytes_read);

      if (!status.IsOK()) {
        std::cerr << "error: failed pgread rand_off=" << rand_off
                  << " rand_len=" << rand_len << std::endl;
        exit(status.errNo);
      }
    }
  } else {
    std::cout << " pgread: offset=" << offset
              << " length=" << length << std::endl;
    status = file.PgRead(offset, length, buffer.get(), cksums, bytes_read);

    if (!status.IsOK()) {
      std::cerr << "error: failed pgread offset=" << offset
                << " length=" << length << std::endl;
      exit(status.errNo);
    }
  }

  if (!file.Close().IsOK()) {
    std::cerr << "error: failed to close file" << std::endl;
    exit(EIO);
  }

  return retc;
}
