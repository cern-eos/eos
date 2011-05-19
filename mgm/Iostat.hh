#ifndef __EOSMGM_EGROUP__HH__
#define __EOSMGM_EGROUP__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
#include "mq/XrdMqClient.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <string>
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

class Iostat {
  // -------------------------------------------------------------
  // ! subscribes to our MQ, collects and digestes report messages
  // -------------------------------------------------------------

public:
  pthread_t thread;
  bool mRunning;
  bool mInit;
  XrdMqClient mClient;

  Iostat();
  ~Iostat();

  bool Start();
  bool Stop();

  void PrintOut(XrdOucString &out, bool details, bool monitoring);

  static void* StaticReceive(void*);
  void* Receive();
};

EOSMGMNAMESPACE_END

#endif
