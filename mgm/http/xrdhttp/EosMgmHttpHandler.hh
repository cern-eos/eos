#pragma once

#include <map>
#include <string>
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdVersion.hh"

XrdVERSIONINFO(XrdHttpGetExtHandler, EOSMGMHTTP );


class XrdLink;
class XrdSecEntity;
class XrdHttpReq;
class XrdHttpProtocol;
class XrdMgmOfs;

class EosMgmHttpHandler : public XrdHttpExtHandler {
  
public:
  
  bool MatchesPath(const char *verb, const char *path);
  
  int ProcessReq(XrdHttpExtReq &);
  
  int Init(const char *cfgfile);
  
  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  
  EosMgmHttpHandler() {}
  
  //------------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------------
  
  virtual     ~EosMgmHttpHandler() {}

private:
  XrdMgmOfs* OFS;
};

/******************************************************************************/
/*                    X r d H t t p G e t E x t H a n d l e   r               */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Obtain an instance of the XrdHttpExtHandler object.
//!
//! This extern "C" function is called when a shared library plug-in containing
//! implementation of this class is loaded. It must exist in the shared library
//! and must be thread-safe.
//!
//! @param  eDest -> The error object that must be used to print any errors or
//!                  other messages (see XrdSysError.hh).
//! @param  confg -> Name of the configuration file that was used. This pointer
//!                  may be null though that would be impossible.
//! @param  parms -> Argument string specified on the namelib directive. It may
//!                  be null or point to a null string if no parms exist.
//! @param  myEnv -> Environment variables for configuring the external handler;
//!                  it my be null.
//!
//! @return Success: A pointer to an instance of the XrdHttpSecXtractor object.
//!         Failure: A null pointer which causes initialization to fail.
//!

//------------------------------------------------------------------------------

class XrdSysError;
class XrdOucEnv;

#define XrdHttpExtHandlerArgs XrdSysError       *eDest, \
                              const char        *confg, \
                              const char        *parms, \
                              XrdOucEnv         *myEnv

extern "C" XrdHttpExtHandler *XrdHttpGetExtHandler(XrdHttpExtHandlerArgs) {
  XrdHttpExtHandler* handler = new EosMgmHttpHandler();
  handler->Init(confg);
  return handler;
}

//------------------------------------------------------------------------------
//! Declare compilation version.
//!
//! Additionally, you *should* declare the xrootd version you used to compile
//! your plug-in. While not currently required, it is highly recommended to
//! avoid execution issues should the class definition change. Declare it as:
//------------------------------------------------------------------------------


