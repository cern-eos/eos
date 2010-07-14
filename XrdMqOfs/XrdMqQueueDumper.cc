#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>


int main (int argc, char* argv[]) {
  XrdMqClient mqc;

  if (argc != 2) {
    fprintf(stderr, "usage: QueueDumper <brokerurl>/<queue>\n");
    exit(-1);
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
      fprintf(stdout,"%s\n",newmessage->GetBody());
      delete newmessage;
    }
  }
}
