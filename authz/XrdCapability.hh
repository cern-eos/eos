#ifndef __XRDCAPABILITY_CAPABILITY_HH___
#define __XRDCAPABILITY_CAPABILITY_HH__
/*----------------------------------------------------------------------------*/
#include "mq/XrdMqMessage.hh"
#include "common/SymKeys.hh"
/*----------------------------------------------------------------------------*/
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdAcc/XrdAccPrivs.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/x509.h>
#include <openssl/err.h>
#include <openssl/pem.h>
/*----------------------------------------------------------------------------*/
class XrdOucEnv;
class XrdSecEntity;
/*----------------------------------------------------------------------------*/

extern "C" XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp,
						  const char   *cfn,
						  const char   *parm);
/*----------------------------------------------------------------------------*/

class XrdCapability
{
public:
  XrdOucEnv* opaqueCapability;

  /* Access() indicates whether or not the user/host is permitted access to the
     path for the specified operation. The default implementation that is
     statically linked determines privileges by combining user, host, user group,
     and user/host netgroup privileges. If the operation is AOP_Any, then the
     actual privileges are returned and the caller may make subsequent tests using
     Test(). Otherwise, a non-zero value is returned if access is permitted or a
     zero value is returned is access is to be denied. Other iplementations may
     use other decision making schemes but the return values must mean the same.

     Parameters: Entity    -> Authentication information
     path      -> The logical path which is the target of oper
     oper      -> The operation being attempted (see above)
     Env       -> Environmental information at the time of the
     operation as supplied by the path CGI string.
     This is optional and the pointer may be zero.
  */

  virtual XrdAccPrivs Access(const XrdSecEntity    *Entity,
                             const char            *path,
                             const Access_Operation oper,
                             XrdOucEnv       *Env=0);

  virtual int         Audit(const int              accok,
                            const XrdSecEntity    *Entity,
                            const char            *path,
                            const Access_Operation oper,
                            XrdOucEnv       *Env=0) {return 0;}

  // Test() check whether the specified operation is permitted. If permitted it
  // returns a non-zero. Otherwise, zero is returned.
  //
  virtual int         Test(const XrdAccPrivs priv,
                           const Access_Operation oper) { return 0;}

          bool        Init();
          bool        Configure(const char* ConfigFN);

  XrdCapability(){}

  static int                 Create(XrdOucEnv *inenv, XrdOucEnv* &outenv, eos::common::SymKey* symkey);

  static int                 Extract(XrdOucEnv *inenv, XrdOucEnv* &outenv);

  virtual                  ~XrdCapability();

private:
};

/*----------------------------------------------------------------------------*/
extern XrdCapability gCapabilityEngine;
/*----------------------------------------------------------------------------*/

#endif
