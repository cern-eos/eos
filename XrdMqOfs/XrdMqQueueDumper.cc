#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>


int main (int argc, char* argv[]) {
  XrdMqClient mqc;
  long long maxdumps = 0;
  long long dumped =0;

  if ( (argc != 2) && (argc != 3) ) {
    fprintf(stderr, "usage: QueueDumper <brokerurl>/<queue> [n dumps]\n");
    exit(-1);
  }

  if (argc == 3) {
    maxdumps = strtoll(argv[2],0,10);
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
      fprintf(stdout,"%s\n",newmessage->GetBody());
      fflush(stdout);
      delete newmessage;
    } else {
      sleep(1);
    }
    // we exit after maxdumps messages
    if (maxdumps && (dumped >= maxdumps)) 
      exit(0);
  }
}
