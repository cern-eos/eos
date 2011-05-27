#ifndef __EOSMGM_BALANCER__
#define __EOSMGM_BALANCER__

/* ------------------------------------------------------------------------- */
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/FileSystem.hh"
/* ------------------------------------------------------------------------- */
#include "XrdSys/XrdSysPthread.hh"
/* ------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
/* ------------------------------------------------------------------------- */

EOSMGMNAMESPACE_BEGIN

class Balancer {
  // --------------------------------------------------------------------------------------------
  // !this class run's as singleton per space on the MGM and checks all existing groups if it is unbalanced
  // !incase there is an inbalance, it starts a balancing job on the group
  // --------------------------------------------------------------------------------------------

private:
  pthread_t thread;
  std::string mSpaceName;

public: 

  Balancer(const char* spacename);
  ~Balancer();

  static void* StaticBalance(void*);
  void* Balance();
};

EOSMGMNAMESPACE_END
#endif

