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
    fprintf(stderr,"usage: xrdcpabort <url>\n");
    exit(EINVAL);
  }
  
  struct stat buf;

  if (!XrdPosixXrootd::Stat(urlFile.c_str(), &buf)) {
    int fdRead = XrdPosixXrootd::Open(urlFile.c_str(),0,0);
    
    if (fdRead>=0) {
      char* buffer = (char*)malloc(256 * 4096);
      
      // download 1000 random chunks
      for (int i=0 ;i< 1000; i++) {
	off_t offset = (off_t)(buf.st_size * random()/RAND_MAX);
	size_t length = (size_t) ( (buf.st_size -offset) * random()/RAND_MAX);

	while (length > sizeof(buffer)) {
	  length /= 2;
	}

	int rbytes = XrdPosixXrootd::Pread(fdRead, buffer, length, offset);
	if (rbytes != (int)length) {
	  fprintf(stderr,"error: read failed at offset %lld length %lu \n", (unsigned long long)offset,(unsigned long)length);
	  exit(-1);
	}
      }
      int rc = XrdPosixXrootd::Close(fdRead);
      if (rc) {
	fprintf(stderr,"error: close failed\nd with retc=%d", rc);
	exit(rc);
      }
    } else {
      fprintf(stderr,"error: failed to open %s\n", urlFile.c_str());
    }
  } else {
    fprintf(stderr,"error: file %s does not exist!\n", urlFile.c_str());
    exit(-1);
  }
}
