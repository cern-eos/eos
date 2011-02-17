#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdMqOfs/XrdMqMessaging.hh>
#include <XrdMqOfs/XrdMqSharedObject.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>

int main (int argc, char* argv[]) {
  int nhash = 1;

  XrdMqMessage::Configure("");

  if (argc != 2) 
    exit(-1);

  std::string hostname = argv[1];

  XrdOucString myid= "root://lxbra0301.cern.ch:1097//eos/";
  myid += argv[1];
  myid += "/worker";


  XrdMqSharedObjectManager ObjectManager;
  ObjectManager.SetDebug(true);

  XrdMqMessage message("MasterMessage");

  XrdMqMessaging messaging(myid.c_str(), "/eos/*/worker", false, false, &ObjectManager);

  messaging.StartListenerThread();

   
  XrdMqTiming mq("send");


  for (int i=0; i< nhash; i++) {
    XrdOucString str = "statistics";
    str += i;
    ObjectManager.CreateSharedObject(str.c_str(),"/eos/*/worker","queue");
  }


  TIMING("START",&mq);

  for (int i=0; i< 10000; i++) {
    ObjectManager.HashMutex.LockRead();
    for (int v=0; v<nhash; v++) {
      XrdOucString str = "statistics"; str += v;
      XrdMqSharedQueue* queue =  dynamic_cast<XrdMqSharedQueue*>( ObjectManager.GetObject(str.c_str(),"queue"));
      if (!queue) {
	fprintf(stderr,"error: queue get failed\n");
	exit(-1);
      }

      if (i==0) {
	queue->BroadCastRequest("/eos/*/worker");
	sleep(3);
      }
      queue->OpenTransaction();
      
      XrdOucString var = "var"; var += v;
      
      queue->PushBack(0,var.c_str());

      queue->CloseTransaction();
      XrdOucString out;
      out += "---------------------------\n";
      out += "subject="; out += str.c_str(); out += "\n";
      queue->Dump(out);
      printf("%s", out.c_str());

      ObjectManager.HashMutex.LockRead();
      std::deque<XrdMqSharedHashEntry*>::iterator it;
      printf("QUEUE [%d]: \n", (int)queue->GetQueue()->size());
      for (it=queue->GetQueue()->begin(); it != queue->GetQueue()->end(); it++) {
	printf("%s ", ((XrdMqSharedHashEntry*)(*it))->GetKey());
      }
      printf("\n");
      ObjectManager.HashMutex.UnLockRead();
      if (!(i%=10)) {
	fprintf(stderr,"==>clearing queue\n");
	queue->Clear();
      }
    }

    ObjectManager.HashMutex.UnLockRead();
    
    usleep (1000000);
  }
  
  TIMING("SEND+RECV",&mq);
  mq.Print();
}
