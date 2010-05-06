#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
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
    if (i==0) 
      newmessage->Print();
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
