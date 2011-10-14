// ----------------------------------------------------------------------
// File: eosha.cc
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

#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
  if (argc !=6) {
    fprintf(stderr,"ERROR: missing arguments to run: eosha <logfile> <master1> <master2> <alias> <failovertime>\n");
    exit(-1);
  }
  pid_t m_pid=fork();
  if(m_pid<0) {
    fprintf(stderr,"ERROR: Failed to fork daemon process\n");
    exit(-1);
  }
  
  // kill the parent
  if(m_pid>0)
    exit(0);

  // reopen stdout to the logfile name
  FILE* fstdout;
  
  if ((!(fstdout=freopen(argv[1], "a+", stdout)))) {
    fprintf(stderr,"ERROR: cannot stream stdout into %s\n",argv[2]);
    exit(-1);
  }

  setvbuf(fstdout, (char *)NULL, _IONBF, 0);
  
  FILE* fstderr;
  if ((!(fstderr=freopen(argv[1], "a+", stderr)))) {
    fprintf(stderr,"ERROR: cannot stream stderr into %s\n",argv[2]);
    exit(-1);
  }

  setvbuf(fstderr, (char *)NULL, _IONBF, 0);

  pid_t sid;
  if((sid=setsid()) < 0) {
    fprintf(stderr,"ERROR: failed to create new session (setsid())\n");
    exit(-1);
  }
  execlp("/usr/bin/perl","eosha", "/usr/sbin/eoshapl",argv[1], argv[2],argv[3],argv[4],argv[5],NULL);
}
