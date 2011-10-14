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
        XrdOucString line = newmessage->GetBody();
        if (global_highlighting) {
          static std::string textnormal("\033[0m");
          static std::string textblack("\033[49;30m");
          static std::string textred("\033[49;31m");
          static std::string textrederror("\033[47;31m\e[5m");
          static std::string textblueerror("\033[47;34m\e[5m");
          static std::string textgreen("\033[49;32m");
          static std::string textyellow("\033[49;33m");
          static std::string textblue("\033[49;34m");
          static std::string textbold("\033[1m");
          static std::string textunbold("\033[0m");

          static std::string cinfo    = textgreen + "INFO" + textnormal;
          static std::string cdebug   = textblack + "DEBUG" + textnormal;
          static std::string cerr     = textred + "ERROR" + textnormal;
          static std::string cnote    = textblue + "NOTE" + textnormal;
          static std::string cwarn    = textblueerror + "WARN" + textnormal;
          static std::string cemerg   = textrederror + "EMERG" + textnormal;
          static std::string ccrit    = textrederror + "CRIT" + textnormal;
          static std::string calert   = textrederror + "ALERT" + textnormal;

          line.replace("INFO",cinfo.c_str());
          line.replace("DEBUG",cdebug.c_str());
          line.replace("ERROR",cerr.c_str());
          line.replace("EMERG", cemerg.c_str());
          line.replace("CRIT", ccrit.c_str());
          line.replace("WARN", cwarn.c_str());
          line.replace("ALERT", calert.c_str());
          line.replace("NOTE", cnote.c_str());
        }

        fprintf(stdout,"%s\n",line.c_str());
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
