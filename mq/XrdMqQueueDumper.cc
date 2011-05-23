#define TRACE_debug 0xffff
#include <mq/XrdMqClient.hh>
#include <mq/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>
#include <xrootd/XrdClient/XrdClientEnv.hh>

int main (int argc, char* argv[]) {
  XrdMqClient mqc;
  long long maxdumps = 0;
  long long dumped =0;
  bool debug=false;
  long long sleeper = 10000;

  // we need that to have a sys logger object
  XrdMqMessage message("");
  message.Configure(0);

  EnvPutInt( NAME_DEBUG, 3);
  
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
      usleep(sleeper);
    }
    // we exit after maxdumps messages
    if (maxdumps && (dumped >= maxdumps)) 
      exit(0);
  }
}
