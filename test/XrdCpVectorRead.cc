// ----------------------------------------------------------------------
// File: XrdCpVectorRead.cc
// Author: David Smith - CERN
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

/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
#include <XrdCl/XrdClFile.hh>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

/*-----------------------------------------------------------------------------*/
// This program reads parts of a remote and local file and compares the bytes
// read. The remote read uses XrdCl VectorRead (i.e. kXR_readv). It is intended
// that the remote file is a RAIN file, with block size bs and number of data
// stripes N. The program then reads 12*(N-1) bytes, split accross the N-1
// boundaries. e.g. with N=3
//  < block 1 > | < block 2 > | < block 3>
//  < block 4 > | < block 5 > | < block 6>
// We read the last 7 bytes of block1, first 5 bytes of block2, last 7 bytes of
// block2 and first 5 bytes of block 3; using of ChunkList of 2 elements.
/*-----------------------------------------------------------------------------*/

int main (int argc, char* argv[]) {
  const uint16_t to = 30;

  if (argc != 5) {
    fprintf(stderr,"usage: xrdcpvectorread <remote_url> <local_url> "
                   "<rain block size> <number of rain data stripes>\n");
    exit(EINVAL);
  }

  size_t nd = atoi(argv[4]);
  off_t bs = atoi(argv[3]);
  const char *rurl = argv[1];
  const char *lurl = argv[2];

  XrdCl::File remote;
  auto st = remote.Open(rurl, XrdCl::OpenFlags::Read);
  if (!st.IsOK()) {
     std::cerr << "Error during remote open: " << st.ToString() << std::endl;
     exit(EXIT_FAILURE);
  }

  int lfd = open(lurl, O_RDONLY);
  if (lfd < 0) {
     int err = errno;
     std::cerr << "Error during local open: " << strerror(err) << std::endl;
     exit(EXIT_FAILURE);
  }

  XrdCl::ChunkList chunks;
  char *bufl = new char[12*(nd-1)];
  char *bufr = new char[12*(nd-1)];

  memset(bufl, 0, 12*(nd-1));
  memset(bufr, 0, 12*(nd-1));

  for(size_t i = 0;i<(nd-1);++i) {
    chunks.emplace_back(bs*(i+1)-7,12, nullptr);
    if (pread(lfd, &bufr[12*i], 12, bs*(i+1)-7) < 0) {
      int err = errno;
      std::cerr << "Error during local pread: " << strerror(err) << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  XrdCl::VectorReadInfo *vri = nullptr;
  st = remote.VectorRead(chunks, bufl, vri, to);
  if (!st.IsOK()) {
     std::cerr << "Error during VectorRead: " << st.ToString() << std::endl;
     exit(EXIT_FAILURE);
  }

  if (memcmp(bufr, bufl, 12*(nd-1))) {
    std::cerr << "Mismatch between remote and local read data" << std::endl;
    exit(EXIT_FAILURE);
  }

  close(lfd);
  st = remote.Close(to);
  if (!st.IsOK()) {
     std::cerr << "Error during remote close: " << st.ToString() << std::endl;
     exit(EXIT_FAILURE);
  }

  delete vri;
  delete [] bufl;
  delete [] bufr;

  return EXIT_SUCCESS;
}
