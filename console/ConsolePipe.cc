// ----------------------------------------------------------------------
// File: ConsolePipe.cc
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

/*----------------------------------------------------------------------------*/
#include "ConsolePipe.hh"
#include "common/Path.hh"
#include "common/IoPipe.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdNet/XrdNetSocket.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
/*----------------------------------------------------------------------------*/

extern eos::common::IoPipe iopipe;

static void* StaticThreadReaderStdout(void* arg) {
  int fd = ((unsigned long long) arg)%65536;
  FILE* fin = fdopen(fd,"r");
  char * line = NULL;
  size_t len = 0;

  do {
    int nread = getline(&line, &len, fin);
    if (nread == -1) {
      fprintf(stderr, "socket read failed on fd %d\n", fd);
    } else {
      XrdOucString sline = line;
      if (sline.endswith("#__STOP__#\n")) {
	sline.replace("#__STOP__#\n", "");
	fprintf(stdout,"%s", sline.c_str());
	return 0;
      }
      fprintf(stdout,"%s", line);
    }
  } while (1);

  return 0;
}

static void* StaticThreadReaderStderr(void* arg) {
  int fd = ((unsigned long long) arg)%65536;

  FILE* fin = fdopen(fd,"r");
  char * line = NULL;
  size_t len = 0;

  do {
    int nread = getline(&line, &len, fin);
    if (nread == -1) {
      fprintf(stderr, "socket read failed on fd %d\n", fd);
    } else {
      XrdOucString sline = line;
      if (sline.endswith("#__STOP__#\n")) {
	sline.replace("#__STOP__#\n", "");
	fprintf(stderr,"%s", sline.c_str());
	return 0;
      }
      fprintf(stderr,"%s", line);
    }
  } while (1);

  return 0;
}

void pipe_exit_handler (int a) {
  fprintf(stdout,"\n");
  fprintf(stderr,"<Control-C>\n");
  iopipe.KillProducer();
  iopipe.UnLockConsumer();
  exit(-1);
}

int pipe_command(const char* cmd) {
  
  if (!cmd) {
    return -1;
  }

  XrdSysLogger* logger = new XrdSysLogger();
  XrdSysError eDest(logger);

  if (!iopipe.Init()) {
    fprintf(stderr,"error: cannot set IoPipe\n");
    return -1;
  }

  iopipe.LockConsumer();

  int stdinfd  = iopipe.AttachStdin(eDest);
  int stdoutfd = iopipe.AttachStdout(eDest);
  int stderrfd = iopipe.AttachStderr(eDest);
  int retcfd   = iopipe.AttachRetc(eDest);


  if ( (stdinfd <0) || 
       (stdoutfd < 0) ||
       (stderrfd < 0) ||
       (retcfd   < 0) ) {
    fprintf(stderr,"error: cannot attache to pipes\n");
    return -1;
  }

  pthread_t thread1;
  pthread_t thread2;

  XrdSysThread::Run(&thread1, StaticThreadReaderStdout, (void*) stdoutfd ,XRDSYSTHREAD_HOLD, "Stdout Thread");
  XrdSysThread::Run(&thread2, StaticThreadReaderStderr, (void*) stderrfd ,XRDSYSTHREAD_HOLD, "Stderr Thread");

  signal (SIGINT,  pipe_exit_handler);

  int n = write(stdinfd, cmd, strlen(cmd));     
  if (n != (int)strlen(cmd)) {
    fprintf(stderr,"error: communication error to the connector - write failed errno=%d\n", errno);
  }

  XrdSysThread::Join(thread1, NULL);
  XrdSysThread::Join(thread2, NULL);

  // read the response code
  char a[2];
  n = read(retcfd, a, 2);

  if (n != 2) {
    fprintf(stderr, "error: socket read failed on fd %d\n", retcfd);
    pipe_exit_handler(-1);
    return -1; // we don't get here anyway
  } else {
    iopipe.UnLockConsumer();
    return(a[0]);
  }
}
