#ifndef __EOSFST_VID__HH__
#define __EOSFST_VID__HH__

/*----------------------------------------------------------------------------*/
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/

class Vid {
public:
  Vid(){};
  ~Vid(){};

  static bool Set(const char* value);
  static bool Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static void Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static bool Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);

  static const char* Get(const char* key);
};


#endif
