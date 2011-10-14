// ----------------------------------------------------------------------
// File: XrdMqClientWorker.cc
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

//#define CRYPTO

int main (int argc, char* argv[]) {

  XrdMqMessage::Configure("");

#ifdef CRYPTO
  if (!XrdMqMessage::Configure("xrd.mqclient.cf")) {
    fprintf(stderr, "error: cannot open client configuration file xrd.mqclient.cf\n");
    exit(-1);
  }
#endif

  XrdMqClient mqc;
  if (argc != 2) 
    exit(-1);

  XrdOucString myid= "root://lxbra0301.cern.ch:1097//eos/";
  myid += argv[1];
  myid += "/worker";

  if (mqc.AddBroker(myid.c_str())) {
    printf("Added localhost ..\n");
  } else {
    printf("Adding localhost failed 1st time \n");
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/eos/*/master");
  printf("Subscribed\n");
  XrdMqMessage message("MasterMessage");
   
  message.Encode();

  XrdMqTiming mq("send");

  TIMING("START",&mq);

  while(1) {
    for (int i=0; i< 1; i++) {
      XrdMqMessage* newmessage = mqc.RecvMessage();
      if (newmessage) newmessage->Print();
      if (newmessage) 
        delete newmessage;

      while ((newmessage = mqc.RecvFromInternalBuffer())) {
        if (newmessage) newmessage->Print();
        if (newmessage) 
          delete newmessage;
      }
    }
    
    //    message.NewId();
    //    message.kMessageHeader.kDescription = "Hello Master Test";
#ifdef CRYPTO
    message.Sign();
#endif
    //    (mqc << message);
  }

  TIMING("SEND+RECV",&mq);
  mq.Print();
}
