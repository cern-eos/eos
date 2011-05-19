#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
  if (argc !=6) {
    fprintf(stderr,"ERROR: missing arguments to run: eosha <logfile> <master1> <master2> <alias> <failovertime>\n");
    exit(-1);
  }
  pid_t m_pid=fork();
  if(m_pid<0) {
    fprintf(stderr,"ERROR: Failed to fork daemon process\n");
    exit(-1);
  }
  
  // kill the parent
  if(m_pid>0)
    exit(0);

  // reopen stdout to the logfile name
  FILE* fstdout;
  
  if ((!(fstdout=freopen(argv[1], "a+", stdout)))) {
    fprintf(stderr,"ERROR: cannot stream stdout into %s\n",argv[2]);
    exit(-1);
  }

  setvbuf(fstdout, (char *)NULL, _IONBF, 0);
  
  FILE* fstderr;
  if ((!(fstderr=freopen(argv[1], "a+", stderr)))) {
    fprintf(stderr,"ERROR: cannot stream stderr into %s\n",argv[2]);
    exit(-1);
  }

  setvbuf(fstderr, (char *)NULL, _IONBF, 0);

  pid_t sid;
  if((sid=setsid()) < 0) {
    fprintf(stderr,"ERROR: failed to create new session (setsid())\n");
    exit(-1);
  }
  execlp("/usr/bin/perl","eosha", "/usr/sbin/eoshapl",argv[1], argv[2],argv[3],argv[4],argv[5],NULL);
}
