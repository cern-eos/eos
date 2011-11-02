#ifndef __EOSFST_RAIDDPSCAN_HH__
#define __EOSFST_RAIDDPSCAN_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "fst/Namespace.hh"
/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

class RaidDPScan {

private:
  
  char *fileName;
  pthread_t thread;
  
  bool bgThread;

public:
  
  RaidDPScan(const char *path, bool bgthread);
 
  bool RecoverFile();

  static void* StaticThreadProc(void*);
  void* ThreadProc();

  virtual ~RaidDPScan();
};

EOSFSTNAMESPACE_END

#endif
