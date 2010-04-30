#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>

#define CRYPTO

int main (int argc, char* argv[]) {

#ifdef CRYPTO
  if (!XrdMqMessage::Configure("xrd.mqclient.cf")) {
    fprintf(stderr, "error: cannot open client configuration file xrd.mqclient.cf\n");
    exit(-1);
  }
#endif

  XrdMqClient mqc;
  if (argc != 2) 
    exit(-1);

  XrdOucString myid= "root://lxbra0301.cern.ch//xmessage/";
  myid += argv[1];
  myid += "/worker";

  if (mqc.AddBroker(myid.c_str())) {
    printf("Added localhost ..\n");
  } else {
    printf("Adding localhost failed 1st time \n");
  }

  mqc.Subscribe();
  mqc.SetDefaultReceiverQueue("/xmessage/*/master");
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
    
    message.NewId();
    message.kMessageHeader.kDescription = "Hello Master Test";
#ifdef CRYPTO
    message.Sign();
#endif
    bool ret = (mqc << message);
  }

  TIMING("SEND+RECV",&mq);
  mq.Print();
}
