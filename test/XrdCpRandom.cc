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
#include <vector>
#include <map>
#include <stdio.h>
#include <stdlib.h>
/*-----------------------------------------------------------------------------*/
#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdOuc/XrdOucString.hh>
/*-----------------------------------------------------------------------------*/

XrdPosixXrootd posixXrootd;

int main (int argc, char* argv[]) {
  // creates a 100M file written in random order
  XrdOucString urlFile = argv[1];
  if (!urlFile.length()) {
    fprintf(stderr,"usage: xrdcpabort <url>\n");
    exit(EINVAL);
  }
  
  int fdWrite = XrdPosixXrootd::Open(urlFile.c_str(),
				     kXR_async | kXR_mkpath | kXR_open_updt | kXR_new,
				     kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or );
 
  std::map<off_t, off_t> lmap;
  std::vector<off_t> voff;

  char* buffer = (char*) malloc(100000000);
  
  for (size_t i=0; i< 100000000; i++) {
    buffer[i] = i%255;
  }
  
  // create 100 pieces
  for (size_t i=0; i<100; i++) {
    off_t offset = (off_t)(100000000.0 * random()/RAND_MAX);
    voff.push_back(offset);
    lmap[offset] = 0;
    //    fprintf(stderr,"Chunk %u : %llu\n", (unsigned int) i, (unsigned long long)offset);
  }
  
  std::map<off_t, off_t>::const_iterator it1;
  std::map<off_t, off_t>::const_iterator it2;
  it2=lmap.begin();
  it2++;
  for (it1 = lmap.begin(); (it1 != lmap.end())&&(it2 != lmap.end()) ; it1++) {
    lmap[it1->first] = (it2->first-it1->first);
    //    fprintf(stderr,"%llu %llu\n", it1->first,lmap[it1->first]);
    it2++;
  }
  
  
  if (fdWrite>=0) {
    for (size_t i=0; i< 100 ;i++) {
      //      fprintf(stderr,"Writing %llu %llu\n", voff[i], lmap[voff[i]]);
      XrdPosixXrootd::Pwrite(fdWrite, buffer+voff[i], lmap[voff[i]],voff[i]);
    }
  } else {
    exit(-1);
  }
}
