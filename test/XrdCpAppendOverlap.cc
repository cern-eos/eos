// ----------------------------------------------------------------------
// File: XrdCpAppendOverlap.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdOuc/XrdOucString.hh>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <chrono>
#include <thread>
/*-----------------------------------------------------------------------------*/

XrdPosixXrootd posixXrootd;

int main (int argc, char* argv[]) {
  // update an existing file and append a 4k buffer;
  if (argc!=3) {
    fprintf(stderr,"usage: xrdappendoverlap <url1> <url2>\n");
    exit(EINVAL);
  }
  XrdOucString urlFile1 = argv[1];
  XrdOucString urlFile2 = argv[2];
  if (!urlFile1.length()) {
    fprintf(stderr,"usage: xrdappendoverlap <url1> <url2>\n");
    exit(EINVAL);
  }
  if (!urlFile2.length()) {
    fprintf(stderr,"usage: xrdappendoverlap <url1> <url2>\n");
    exit(EINVAL);
  }


  int fdWrite1 = XrdPosixXrootd::Open(urlFile1.c_str(),
				      O_RDWR,
				      kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );
  

  char buffer1[4096];
  char buffer2[4096];
  struct stat buf;

  if (fdWrite1>=0) {
    for (size_t i=0; i< sizeof(buffer1); i++) {
      buffer1[i] = i%255;
      buffer2[i] = (i+1)%255;
    }
    if (!XrdPosixXrootd::Stat(urlFile1.c_str(), &buf)) {
      fprintf(stderr,"offset=%llu\n", (unsigned long long)buf.st_size);
      XrdPosixXrootd::Pwrite(fdWrite1, buffer1, sizeof(buffer1),buf.st_size);
      XrdPosixXrootd::Stat(urlFile1.c_str(), &buf);
      fprintf(stderr,"offset=%llu\n", (unsigned long long)buf.st_size);
    } else {
      exit(-1);
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));


  int fdWrite2 = XrdPosixXrootd::Open(urlFile2.c_str(),
				      O_RDWR,
				      kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );
  
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  if (fdWrite2>=0) {
    if (!XrdPosixXrootd::Stat(urlFile2.c_str(), &buf)) {
      fprintf(stderr,"offset=%llu\n", (unsigned long long)buf.st_size + 4096);
      XrdPosixXrootd::Pwrite(fdWrite2, buffer2, sizeof(buffer2),buf.st_size+4096);
    } else {
      exit(-1);
    }
  } else {
    exit(-1);
  }
}
