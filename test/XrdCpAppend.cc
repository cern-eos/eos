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

/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
/*-----------------------------------------------------------------------------*/
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdOuc/XrdOucString.hh>
/*-----------------------------------------------------------------------------*/

XrdPosixXrootd posixXrootd;

int main (int argc, char* argv[]) {
  // create a 1k file but does not close it!
  XrdOucString urlFile = argv[1];
  if (!urlFile.length()) {
    fprintf(stderr,"usage: xrdcpappend <url>\n");
    exit(EINVAL);
  }
  
  
  int fdWrite = XrdPosixXrootd::Open(urlFile.c_str(),
				     kXR_async | kXR_open_updt ,
				     kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );
 
  if (fdWrite>=0) {
    char buffer[4096];
    for (size_t i=0; i< sizeof(buffer); i++) {
      buffer[i] = i%255;
    }
    struct stat buf;
    if (!XrdPosixXrootd::Stat(urlFile.c_str(), &buf)) {
      fprintf(stderr,"offset=%llu\n", (unsigned long long)buf.st_size);
      XrdPosixXrootd::Pwrite(fdWrite, buffer, sizeof(buffer),buf.st_size);
    } else {
      exit(-1);
    }
  } else {
    exit(-1);
  }
}
