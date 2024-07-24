// ----------------------------------------------------------------------
// File: XrdCpAbort.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
#include <XrdOuc/XrdOucString.hh>
#include <fcntl.h>


XrdPosixXrootd posixXrootd;

int main(int argc, char* argv[])
{
  // create a 1k file but does not close it!
  XrdOucString urlFile = argv[1];

  if (!urlFile.length()) {
    fprintf(stderr, "usage: xrdcpabort <url>\n");
    exit(EINVAL);
  }

  struct stat buf;

  if (!XrdPosixXrootd::Stat(urlFile.c_str(), &buf)) {
    int fdRead = XrdPosixXrootd::Open(urlFile.c_str(), 0, 0);

    if (fdRead >= 0) {
      constexpr int bsz = 8192;
      char buffer[bsz];
      int nbytes = buf.st_size % bsz;
      long long offset = 0;

      if (buf.st_size > bsz) {
        offset = buf.st_size - (buf.st_size % bsz);
      }

      do {
        int rbytes = XrdPosixXrootd::Pread(fdRead, buffer, nbytes, offset);

        if (rbytes != nbytes) {
          fprintf(stderr, "error: read failed at offset %lld\n", offset);
          exit(-1);
        }

        nbytes = bsz;
        offset -= nbytes;
      } while (offset >= 0);

      int rc = XrdPosixXrootd::Close(fdRead);

      if (rc) {
        fprintf(stderr, "error: close failed with retc=%d errno=%i", rc, errno);
        exit(rc);
      }
    } else {
      fprintf(stderr, "error: failed to open %s\n", urlFile.c_str());
    }
  } else {
    fprintf(stderr, "error: file %s does not exist!\n", urlFile.c_str());
    exit(-1);
  }
}
