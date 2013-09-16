// ----------------------------------------------------------------------
// File: XrdMqQueueInjection.cc
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

#define TRACE_debug 0xffff
#include <mq/XrdMqClient.hh>
#include <mq/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdSys/XrdSysTimer.hh>
#include <stdio.h>


int main (int argc, char* argv[]) {
  XrdMqClient mqc;
  if ( (argc < 3) || (argc > 3) ) {
    fprintf(stderr, "usage: xrdmqinjection <brokerurl>/<queue> <injection file>\n");
    exit(-1);
  }

  XrdOucString injectionfile = argv[2];

  XrdOucString broker = argv[1];
  if (!broker.beginswith("root://")) {
    fprintf(stderr,"error: <borkerurl> has to be like root://host[:port]/<queue>\n");
    exit(-1);
  }

  if (!mqc.AddBroker(broker.c_str())) {
    fprintf(stderr,"error: failed to add broker %s\n",broker.c_str());
    exit(-1);
  } 

  XrdOucString queue = broker;
  int apos = broker.find("//");
  if (apos == STR_NPOS) {
    fprintf(stderr,"error: <brokerurl> has to be like root://host[:port]/<queue>\n");
    exit(-1);
  }

  int bpos = broker.find("/",apos+2);

  if (bpos == STR_NPOS) {
    fprintf(stderr,"error: <brokerurl> has to be like root://host[:port]/<queue>\n");
    exit(-1);
  }

  queue.erase(0,bpos+1);
  fprintf(stdout, "=> feeding into %s\n",queue.c_str());

  mqc.SetDefaultReceiverQueue(queue.c_str());

  FILE* fd = fopen(injectionfile.c_str(),"r");
  if (!fd) {
    fprintf(stderr,"error: unable to open injection file <%s>\n", injectionfile.c_str());
    exit(-1);
  }

  ssize_t linein = 0;
  char* lineptr = (char*) malloc(4096);
  size_t lineptr_n = 4096;

  size_t injected=0;
  while ( (linein = getline(&lineptr, &lineptr_n, fd)) > 0) {
    fprintf(stdout,"< %s >\n", lineptr);

    XrdMqMessage message("Injection");
    message.Configure(0);
    XrdOucString body=lineptr;
    body.erase(body.length()-1);
    message.NewId();
    message.MarkAsMonitor();
    message.kMessageHeader.kDescription="Monitor Injection";
    message.SetBody(body.c_str());
    if (!(mqc << message)) {
      fprintf(stderr,"error: failed to send message\n");
    } else {
      injected ++;
    }
  }
  fprintf(stdout,"info: injected %lu messages\n", injected);
}
