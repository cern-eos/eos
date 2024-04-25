//------------------------------------------------------------------------------
//! @file XrdCpNonStreaming.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>

//------------------------------------------------------------------------------
//! This executable simulates a client that writes a file in non-streaming mode.
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <input_file> <xrd_url>\n"
              << "  <input_file> - local input file used as source of data\n"
              << "  <xrd_url> - XRootD URL where file is written\n"
              << std::endl;
    exit(EINVAL);
  }

  std::string fn_path = argv[1];
  std::string surl = argv[2];
  XrdCl::URL url(surl);

  if (!url.IsValid()) {
    std::cerr << "error: given XRootD URL is not valid" << std::endl;
    exit(EINVAL);
  }

  struct stat info;

  if (stat(fn_path.c_str(), &info)) {
    std::cerr << "error: failed to stat input file" << std::endl;
    exit(EINVAL);
  }

  // Allocate buffer used for transfer
  uint32_t block_size = 1024 * 1024;
  std::unique_ptr<char[]> buffer {new char[block_size + 3]};
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

  size_t sz;
  int64_t offset = 0ull;
  int fd = open(fn_path.c_str(), O_RDONLY);

  if (fd == -1) {
    std::cerr << "error: failed to open input file " << fn_path << std::endl;
    exit(EIO);
  }

  // Write all the odd blocks
  offset = block_size;

  while (offset < info.st_size) {
    sz = pread(fd, buffer.get(), block_size, offset);

    if (!sz) {
      break;
    }

    std::cout << "offset = " << offset << " length = " << sz << std::endl;
    status = file.Write(offset, sz, buffer.get());

    if (!status.IsOK()) {
      std::cerr << "error: failed write offset=" << offset << ", lenght="
                << block_size << std::endl;
      exit(status.errNo);
    }

    offset += (2 * sz);
  }

  offset = 0;

  // Write all the even blocks
  while (offset < info.st_size) {
    sz = pread(fd, buffer.get(), block_size + 3, offset);

    if (!sz) {
      break;
    }

    std::cout << "offset = " << offset << " length = " << sz << std::endl;
    status = file.Write(offset, sz, buffer.get());

    if (!status.IsOK()) {
      std::cerr << "error: failed write offset=" << offset << ", lenght="
                <<  block_size << std::endl;
      exit(status.errNo);
    }

    offset += (2 * block_size);
  }

  if (!file.Close().IsOK()) {
    std::cerr << "error: failed to close file" << std::endl;
    exit(EIO);
  }
}
