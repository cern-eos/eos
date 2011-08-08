/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#include <sys/types.h>
#include <sys/wait.h>


/* Run error log console &*/
int
com_console (char *arg) {
  pid_t pid=0;
  if (!(pid=fork())) {
    XrdMqClient mqc;
    XrdMqMessage message("");
    message.Configure(0);
    
    XrdOucString broker = serveruri;

    if (!broker.endswith("//")) {
      if (!broker.endswith("/")) {
	broker += ":1097//";
      } else {
	broker += ":1097//";
      }
    } else {
      broker.erase(broker.length()-3);
      broker += ":1097//";
    }

    broker += "eos/";
    broker += getenv("HOSTNAME");
    broker += ":";
    broker += (int)getpid();
    broker += ":";
    broker += (int)getppid();
    broker += "/errorreport";
    
    if (!mqc.AddBroker(broker.c_str())) {
      fprintf(stderr,"error: failed to add broker %s\n",broker.c_str());
      exit(-1);
    } 

    mqc.Subscribe();

    while(1) {
      XrdMqMessage* newmessage = mqc.RecvMessage();
      
      if (newmessage) {
        fprintf(stdout,"%s\n",newmessage->GetBody());
	fflush(stdout);
	delete newmessage;
      } else {
	usleep(100000);
      }
    }
    
    exit(0);
  }
  signal(SIGINT, SIG_IGN);
  int status=0;
  waitpid(pid,&status,0);
  signal(SIGINT, exit_handler);
  return (0);
}
