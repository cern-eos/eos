// ----------------------------------------------------------------------
// File: XrdMqClientTest.cc
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

int main (int argc, char* argv[]) {
  printf("Starting up ...\n");

  XrdMqClient mqc;
  
  printf("Created broker ...\n");

  if (mqc.AddBroker("root://localhost//xmessage/")) {
    printf("Added localhost ..\n");
  } else {
    printf("Adding localhost failed 1st time \n");
  }

  if (mqc.AddBroker("root://localhost//xmessage/")) {
    printf("Added localhsot 2nd time \n");
  } else {
    printf("Adding localhost failed 2nd time as expected\n");
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/xmessage/*");
  XrdMqMessage message("TestMessage");
   
  message.Print();
  printf("Encode %d \n",message.Encode());
  message.Print();
  printf("Decode %d \n",message.Decode());
  message.Print();

  XrdMqTiming mq("send");

  TIMING("START",&mq);


#ifdef __BLA__
  for (int i=0; i< 1000; i++) {
    message.NewId();
    message.kMessageHeader.kDescription = "Test";
    message.kMessageHeader.kDescription += i;
    
    bool ret = (mqc << message);
    //    printf("Message send gave %d\n",ret);
  }

  TIMING("SEND",&mq);
  for (int i=0; i< 1000; i++) {
    XrdMqMessage* newmessage = mqc.RecvMessage();
    if (i==0) {
      if (newmessage) newmessage->Print();
    }
    if (newmessage) 
      delete newmessage;
  }
  TIMING("RECV",&mq);
  mq.Print();


#else 
  int n = 1000;
  if (argc==2) {
    printf("%s %s\n",argv[0],argv[1]);
    n = atoi(argv[1]);
    printf("n is %d\n",n);
  }
  
  for (int i=0; i< n; i++) {
    message.NewId();
    message.kMessageHeader.kDescription = "Test";
    message.kMessageHeader.kDescription += i;
    
    (mqc << message);

    XrdMqMessage* newmessage = mqc.RecvMessage();
    if (i==0) 
      newmessage->Print();
    if (newmessage) 
      delete newmessage;
  }
  TIMING("SEND+RECV",&mq);
  mq.Print();
#endif
}
