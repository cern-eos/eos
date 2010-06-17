#ifndef __XRDMGMOFS_POLICY__HH__
#define __XRDMGMOFS_POLICY__HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonMapping.hh"
#include "Namespace/ContainerMD.hh"
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

  static void GetLayoutAndSpace(const char* path, eos::ContainerMD::XAttrMap &map, const XrdCommonMapping::VirtualIdentity &vid , unsigned long &layoutId, XrdOucString &space, XrdOucEnv &env, unsigned long &forcedfsid);

  static bool Set(const char* value);
  static bool Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static void Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);
  static bool Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr);

  static const char* Get(const char* key);
};

#endif
