// ----------------------------------------------------------------------
// File: FstDump.cc
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

// -----------------------------------------------------------------
// ! this is a tiny program dumping the contents of an FST directory
// -----------------------------------------------------------------

#include <XrdPosix/XrdPosixXrootd.hh>
#include <XrdOuc/XrdOucString.hh>
#include <stdio.h>
#include <errno.h>

XrdPosixXrootd posixsingleton;

int main(int argc, char* argv[])
{
  setenv("XrdSecPROTOCOL","sss",1);
  // argv[1] = 'root://<host>/<datadir>
  XrdOucString url = argv[1];
  if ((argc!=2) || (!url.beginswith("root://"))) {
    fprintf(stderr,"usage: eos-fst-dump root://<host>/<datadir>\n");
    exit(-EINVAL);
  }
  DIR* dir = XrdPosixXrootd::Opendir(url.c_str());
  if (dir) {
    static struct dirent*  dentry;

    while ( (dentry = XrdPosixXrootd::Readdir(dir)) ) {
      fprintf(stderr,"%s\n",dentry->d_name);
    }
    XrdPosixXrootd::Closedir(dir);
    exit(0);
  } else {
    exit(-1);
  }
}
