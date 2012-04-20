// ----------------------------------------------------------------------
// File: XrdCpPosixCache.cc
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
  // download a file with the posix cache enabled
  XrdOucString urlFile = argv[1];
  if (!urlFile.length()) {
    fprintf(stderr,"usage: xrdcpposixcache <url>\n");
    exit(EINVAL);
  }

  sleep(1);
  fprintf(stderr,"%s\n", getenv("XRDPOSIX_CACHE"));
  if (getenv("XRDPOSIX_CACHE")) {
    fprintf(stderr,"INFO: using Xrd Posix Cache settings: %s\n", getenv("XRDPOSIX_CACHE"));
  } else {
    fprintf(stderr,"WARNING: please set the XRDPOSIX_CACHE variable e.g. export XRDPOSIX_CACHE=\"debug=3&mode=c&optpr=1&pagesz=128k&cachesz=1g&optlg=1&aprminp=128&aprtrig=256k&max2cache=200000\"\n");
  }

  for (int k=0; k<2; k++) {
    fprintf(stderr,"# RUN   %d ----------------------------------------------------\n", k);
    int fdRead = XrdPosixXrootd::Open(urlFile.c_str(),0,0);
    
    if (fdRead>0) {
      off_t offset = 0;
      size_t nread=0;
      do {
	char buffer[32*4096];
	nread = XrdPosixXrootd::Pread(fdRead, buffer, sizeof(buffer),offset);
	if (nread>0) {
	  offset += nread;
	}
      } while (nread>0);
    } else {
      fprintf(stderr,"ERROR: couldn't open url=%s\n", urlFile.c_str());
      exit(-1);
    }
    fprintf(stderr,"# CLOSE %d ----------------------------------------------------\n", k);
    XrdPosixXrootd::Close(fdRead);
  }

  exit(0);
}
