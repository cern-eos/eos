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
#include <fstream>
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

  int fdWrite = XrdPosixXrootd::Open(urlFile.c_str(),
				     kXR_async | kXR_mkpath |
				     kXR_open_updt | kXR_new,
				     kXR_ur | kXR_uw | kXR_gw | kXR_gr
				     | kXR_or );
  
  off_t offset = 0;
  size_t sizeHeader = 4* 1024;
  size_t sizeBuffer = 1024*1024;
  char* buffer = (char*)malloc(sizeBuffer);
  if (!buffer)
    exit(-1);

  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(buffer, sizeBuffer);
  urandom.close();

  if (fdWrite >= 0) {
    offset = 0;
    XrdPosixXrootd::Pwrite(fdWrite, buffer, sizeHeader, offset);
    XrdPosixXrootd::Ftruncate(fdWrite, sizeBuffer * 4 + sizeHeader);
    
    offset = sizeHeader;
    XrdPosixXrootd::Pwrite(fdWrite, buffer, sizeBuffer, offset);
    XrdPosixXrootd::Ftruncate(fdWrite, sizeBuffer * 4 + sizeHeader);
    
    offset = sizeHeader + sizeBuffer;
    XrdPosixXrootd::Pwrite(fdWrite, buffer, sizeBuffer, offset);
    XrdPosixXrootd::Ftruncate(fdWrite, sizeBuffer * 4 + sizeHeader);
    
    offset = sizeHeader + 2 * sizeBuffer;
    XrdPosixXrootd::Pwrite(fdWrite, buffer, sizeBuffer, offset);
    XrdPosixXrootd::Ftruncate(fdWrite, sizeBuffer * 4 + sizeHeader);

    offset = sizeHeader + 3 * sizeBuffer;
    XrdPosixXrootd::Pwrite(fdWrite, buffer, sizeBuffer, offset);
    XrdPosixXrootd::Ftruncate(fdWrite, sizeBuffer * 8 + sizeHeader);

    offset = sizeHeader + 4 * sizeBuffer;
    XrdPosixXrootd::Pwrite(fdWrite, buffer, 250*1024, offset);
    XrdPosixXrootd::Ftruncate(fdWrite, sizeBuffer * 8 + sizeHeader);

    XrdPosixXrootd::Close(fdWrite);
    exit(0);
  } else {
    exit(-1);
  }
}
