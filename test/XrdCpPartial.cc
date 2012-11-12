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
  // read the first part of a file
  XrdOucString urlFile = argv[1];
  if (!urlFile.length()) {
    fprintf(stderr,"usage: xrdcppartial <url>\n");
    exit(EINVAL);
  }
  
  
  int fdRead = XrdPosixXrootd::Open(urlFile.c_str(),0,0);
				     
 
  if (fdRead>=0) {
    char buffer[4096];
    for (size_t i=0; i< sizeof(buffer); i++) {
      buffer[i] = i%255;
    }
    struct stat buf;
    if (!XrdPosixXrootd::Stat(urlFile.c_str(),&buf)) {
      ssize_t size= buf.st_size;
      if (buf.st_size>1024) {
	// read 1k
	size = 1024;
      } else {
	// read half of the file
	size = size/2;
      }
      ssize_t rs = XrdPosixXrootd::Pread(fdRead, buffer, size,0);
      if (rs != size) {
	fprintf(stderr,"error: read returned rc=%lld instead of %lld\n", (long long)rs, (long long)size);
	exit(-3);
      }
      if (XrdPosixXrootd::Close(fdRead)) {
	fprintf(stderr,"error: close failed\n");
	exit(-2);
      }
    }
  } else {
    exit(-1);
  }
}
