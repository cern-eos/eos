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
    ObjectManager.CreateSharedHash(str.c_str(),"/eos/*/worker");
  }

  TIMING("START",&mq);

  for (int i=0; i< 10000; i++) {
    ObjectManager.HashMutex.LockRead();
    for (int v=0; v<nhash; v++) {
      XrdOucString str = "statistics"; str += v;
      XrdMqSharedHash* hash = ObjectManager.GetHash(str.c_str());

      hash->OpenTransaction();
      
      for (int j=0; j< 50; j++) {
	XrdOucString var = "var"; var += j;
	unsigned long long r= random();
	fprintf(stderr,"Set %s %s %llu\n", str.c_str(), var.c_str(),r);
	hash->SetLongLong(var.c_str(), r);
      }

      hash->Set("hostname", hostname.c_str());

      if ( !(rand()%10)) {
	//	fprintf(stderr,"Clearing Hash!\n");
	hash->Clear();
      }
      hash->CloseTransaction();
      XrdOucString out;
      out += "---------------------------\n";
      out += "subject="; out += str.c_str(); out += "\n";
      //      hash->Dump(out);
      //      printf("%s", out.c_str());
      //      sleep(1);
    }

    ObjectManager.HashMutex.UnLockRead();
    
    usleep (5000000);
  }
  
  TIMING("SEND+RECV",&mq);
  mq.Print();
}
