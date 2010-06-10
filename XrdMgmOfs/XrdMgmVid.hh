#ifndef __XRDMGMOFS_VID__HH__
#define __XRDMGMOFS_VID__HH__

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/

class XrdMgmVid {
public:
  XrdMgmVid(){};
  ~XrdMgmVid(){};

  static bool Set(const char* value);
  static bool Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static void Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static bool Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);

  static const char* Get(const char* key);
};

#endif
