#ifndef __XRDMGMOFS_POLICY__HH__
#define __XRDMGMOFS_POLICY__HH__

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/

class XrdMgmPolicy {
public:
  XrdMgmPolicy(){};
  ~XrdMgmPolicy(){};

  static void GetLayoutAndSpace(const char* path, uid_t uid, gid_t gid, unsigned long layoutId, XrdOucString &space, XrdOucEnv &env);

};

#endif
