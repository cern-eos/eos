//------------------------------------------------------------------------------
//! @file XrdCpSlowWriter.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include <thread>
#include <chrono>
#include <fstream>
#include <iostream>

//------------------------------------------------------------------------------
//! This executable simulates a client which keeps the file open for more than
//! 1 minutes are writes slowly blocks of data.
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <xrootd_url> [<transfer_time>]"
              << std::endl
              << "  <xrootd_url> - full XRootD URL where file is written"
              << std::endl
              << "  <transfer_time> - total time in seconds the transfer "
              << " should take, default 80 seconds"
              << std::endl;
    exit(EINVAL);
  }

  std::string surl = argv[1];
  XrdCl::URL url(surl);

  if (!url.IsValid()) {
    std::cerr << "error: given URL is not valid" << std::endl;
    exit(EINVAL);
  }

  uint32_t tx_time = 80;

  if (argc == 3) {
    try {
      tx_time = std::stoul(argv[2]);
    } catch (const std::exception& e) {
      tx_time = 80;
    }
  }

  // Allocate a random buffer used for writing
  // Fill buffer with random characters
  uint32_t block_size = 1024 * 1024;
  std::unique_ptr<char[]> buffer = std::make_unique<char[]>(block_size);
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buffer.get(), block_size);
  urandom.close();
  // Open file and start writing
  XrdCl::File file;
  XrdCl::XRootDStatus status =
    file.Open(surl, XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Write,
              XrdCl::Access::UR | XrdCl::Access::UW);

  if (!status.IsOK()) {
    std::cerr << "error: unable to open file for writing, errno="
              << status.errNo << std::endl;
    exit(status.errNo);
  }

  int count = 8;
  uint64_t offset = 0ull;
  int sleep_sec = (int) tx_time / count;

  while (count) {
    status = file.Write(offset, block_size, buffer.get());
    std::cout << "info: slow write at offset=" << offset << std::endl;

    if (!status.IsOK()) {
      std::cerr << "error: failed write offset=" << offset << ", lenght="
                << block_size << std::endl;
      exit(status.errNo);
    }

    --count;
    offset += block_size;
    std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));
  }

  if (!file.Close().IsOK()) {
    std::cerr << "error: failed to close file" << std::endl;
    exit(EIO);
  }
}
