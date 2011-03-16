#ifndef __EOSFST_POLICY__HH__
#define __EOSFST_POLICY__HH__

/*----------------------------------------------------------------------------*/
#include "common/Mapping.hh"
#include "Namespace/ContainerMD.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
/*----------------------------------------------------------------------------*/

class Policy {
public:
  Policy(){};
  ~Policy(){};

  static void GetLayoutAndSpace(const char* path, eos::ContainerMD::XAttrMap &map, const eos::common::Mapping::VirtualIdentity &vid , unsigned long &layoutId, XrdOucString &space, XrdOucEnv &env, unsigned long &forcedfsid);

  static bool Set(const char* value);
  static bool Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static void Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static bool Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);

  static const char* Get(const char* key);
};

#endif
