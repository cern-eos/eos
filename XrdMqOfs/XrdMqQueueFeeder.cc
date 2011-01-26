#define TRACE_debug 0xffff
#include <XrdMqOfs/XrdMqClient.hh>
#include <XrdMqOfs/XrdMqTiming.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <stdio.h>


int main (int argc, char* argv[]) {
  XrdMqClient mqc;
  long long maxfeeds = 0;
  long long feeded =0;
  long long sleeper = 0;

  if ( (argc < 2) || (argc > 4) ) {
    fprintf(stderr, "usage: QueueDumper <brokerurl>/<queue> [n feed] [sleep in mus after feed]\n");
    exit(-1);
  }

  if (argc == 3) {
    maxfeeds = strtoll(argv[2],0,10);
  }

  if (argc == 4) {
    sleeper = strtoll(argv[3],0,10);
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

  XrdOucString queue = broker;
  int apos = broker.find("//");
  if (apos == STR_NPOS) {
    fprintf(stderr,"error: <brokerurl> has to be like root://host[:port]/<queue>\n");
    exit(-1);
  }

  int bpos = broker.find("/",apos+2);

  if (bpos == STR_NPOS) {
    fprintf(stderr,"error: <brokerurl> has to be like root://host[:port]/<queue>\n");
    exit(-1);
  }

  queue.erase(0,bpos+1);
  fprintf(stdout, "=> feeding into %s\n",queue.c_str());

  mqc.SetDefaultReceiverQueue(queue.c_str());
  XrdMqMessage message("HelloDumper");
  message.Configure(0);

  while(1) {
    message.NewId();
    message.kMessageHeader.kDescription="Hello Dumper";
    message.kMessageHeader.kDescription += (int)feeded;
    message.SetBody(message.kMessageHeader.kDescription.c_str());
    feeded ++;

    if (!(mqc << message)) {
      fprintf(stderr,"error: failed to send message\n");
    }
    // we exit after maxfeeds messages
    if (maxfeeds && (feeded >= maxfeeds)) 
      exit(0);
    usleep(sleeper);
  }
}
