// ----------------------------------------------------------------------
// File: XrdMqQueueDumper.cc
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
#include <stdio.h>
#include <XrdClient/XrdClientEnv.hh>
#include <XrdSys/XrdSysTimer.hh>

int main (int argc, char* argv[]) {
  XrdMqClient mqc;
  long long maxdumps = 0;
  long long dumped =0;
  bool debug=false;
  long long sleeper = 10000;

  // we need that to have a sys logger object
  XrdMqMessage message("");
  message.Configure(0);

  if ( (argc < 2) || (argc > 5) ) {
    fprintf(stderr, "usage: QueueDumper <brokerurl>/<queue> [n dumps] [sleep between grab] [debug]\n");
    exit(-1);
  }

  if (argc >= 3) {
    maxdumps = strtoll(argv[2],0,10);
  }

  if (argc >= 4) {
    sleeper = strtoll(argv[3],0,10);
  }

  if (argc >= 5) {
    debug = (strtoll(argv[4],0,10))?true:false;
  }

  XrdOucString broker = argv[1];
  if (!broker.beginswith("root://")) {
    fprintf(stderr,"error: <borkerurl> has to be like root://host[:port]/<queue>\n");
    exit(-1);
  }

  if (!mqc.AddBroker(broker.c_str())) {
    fprintf(stderr,"error: failed to add broker %s\n",broker.c_str());
    exit(-1);
  } 

  mqc.Subscribe();
  while(1) {
    XrdMqMessage* newmessage = mqc.RecvMessage();

    if (newmessage) {
      dumped ++;
      if (!debug) {
        fprintf(stdout,"%s\n",newmessage->GetBody());
      } else {
        fprintf(stdout,"n: %llu/%llu size: %u\n", dumped,maxdumps, (unsigned int)strlen(newmessage->GetBody()));
      }
      fflush(stdout);
      delete newmessage;
    } else {
      XrdSysTimer mySleeper;
      mySleeper.Wait(sleeper/1000);
    }
    // we exit after maxdumps messages
    if (maxdumps && (dumped >= maxdumps)) 
      exit(0);
  }
}
