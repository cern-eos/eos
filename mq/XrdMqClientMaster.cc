// ----------------------------------------------------------------------
// File: XrdMqClientMaster.cc
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
#include <XrdSys/XrdSysError.hh>
#include <stdio.h>

//#define CRYPTO

int main (int argc, char* argv[]) {
  printf("Starting up ...\n");

  XrdMqMessage::Configure("");

#ifdef CRYPTO
  if (!XrdMqMessage::Configure("xrd.mqclient.cf")) {
    fprintf(stderr, "error: cannot open client configuration file xrd.mqclient.cf\n");
    exit(-1);
  }
#endif

  XrdMqClient mqc;
  
  if (mqc.AddBroker("root://lxbra0301.cern.ch:1097//eos/lxbra0301.cern.ch/master", true, true)) {
    //  if (mqc.AddBroker("root://localhost//xmessage/localhost/master", false, false)) {
    printf("Added localhost ..\n");
  } else {
    printf("Adding localhost failed 1st time \n");
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/eos/*/worker");


  XrdMqMessage message("HelloWorker");
#ifdef CRYPTO
  message.Sign();   
#else
  message.Encode();
#endif
  message.Print();

  
  XrdMqTiming mq("send");
  
  TIMING("START",&mq);
  
  
  int n = 1000;
  if (argc==2) {
    printf("%s %s\n",argv[0],argv[1]);
    n = atoi(argv[1]);
    printf("n is %d\n",n);
  }

  do {
    for (int i=0; i< n; i++) {
      message.NewId();
      message.kMessageHeader.kDescription = "Hello Worker Test";
      message.kMessageHeader.kDescription += i;
      (mqc << message);
      
      for (int j=0; j< 10; j++) {
        XrdMqMessage* newmessage = mqc.RecvMessage();
        if (!newmessage) 
          continue;
      
        if ( (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) || (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage) ) {
          XrdAdvisoryMqMessage* advisorymessage = XrdAdvisoryMqMessage::Create(newmessage->GetMessageBuffer());
          delete advisorymessage;
          //      advisorymessage->Print();
        } else {
          //      newmessage->Print();
        }
        if (newmessage) 
          delete newmessage;
        
        while ((newmessage = mqc.RecvFromInternalBuffer())) {
          if ( (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kStatusMessage) || (newmessage->kMessageHeader.kType == XrdMqMessageHeader::kQueryMessage) ) {
            XrdAdvisoryMqMessage* advisorymessage = XrdAdvisoryMqMessage::Create(newmessage->GetMessageBuffer());
            //  advisorymessage->Print();
            delete advisorymessage;
          } else {
            //      newmessage->Print();
          }
          
          if (newmessage) 
            delete newmessage;
        }
      }
    }
  } while(1);

  TIMING("SEND+RECV",&mq);
  mq.Print();
}
