//------------------------------------------------------------------------------
//! @file XrdMgmAuthz.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/token/EosTok.hh"
#include "mgm/XrdMgmAuthz.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdVersion.hh"

XrdMgmAuthz* gMgmAuthz {nullptr};

// Set the version information
XrdVERSIONINFO(XrdAccAuthorizeObject, EosMgmAuthz);

//------------------------------------------------------------------------------
// XrdAccAuthorizeObject() is called to obtain an instance of the auth object
// that will be used for all subsequent authorization decisions. If it returns
// a null pointer; initialization fails and the program exits. The args are:
//
// lp    -> XrdSysLogger to be tied to an XrdSysError object for messages
// cfn   -> The name of the configuration file
// parm  -> Paramexters specified on the authlib directive. If none it is zero.
//------------------------------------------------------------------------------
extern "C"
{
  XrdAccAuthorize* XrdAccAuthorizeObject(XrdSysLogger* lp, const char*   cfn,
                                         const char*   parm)
  {
    XrdSysError eroute(lp, "mgmauthz_");

    if (gMgmAuthz) {
      eroute.Say("====== XrdMgmAuthz plugin already loaded and available");
      return gMgmAuthz;
    }

    XrdOucString version = "EOS MGM Authorization (XrdMgmAuthz) ";
    version += VERSION;
    eroute.Say("++++++ (c) 2022 CERN/IT-ST ", version.c_str());
    gMgmAuthz = new XrdMgmAuthz();

    if (!gMgmAuthz) {
      eroute.Say("------ XrdMgmAuthz plugin initialization failed!");
    } else {
      eroute.Say("------ XrdMgmAuthz plugin initialization successful");
    }

    return static_cast<XrdAccAuthorize*>(gMgmAuthz);
  }


//------------------------------------------------------------------------------
//! Add an authorization object as a wrapper to the existing object.
//!
//! XrdAccAuthorizeObjAdd() is an extern "C" function that is called to obtain
//! an instance of the auth object that should wrap the existing object. The
//! wrapper becomes the actual authorization object. The wrapper must be
//! in the plug-in shared library, it is passed additional parameters.
//! All the following extern symbols must be defined at file level!
//!
//! @param lp   -> XrdSysLogger to be tied to an XrdSysError object for messages
//! @param cfn  -> The name of the configuration file
//! @param parm -> Parameters specified on the authlib directive. If none it
//!                is zero.
//! @param envP -> Environmental information and may be nil.
//! @param accP -> to the existing authorization object.
//!
//! @return Success: A pointer to the authorization object.
//!         Failure: Null pointer which causes initialization to fail.
  XrdAccAuthorize* XrdAccAuthorizeObjAdd(XrdSysLogger* log,
                                         const char*   config,
                                         const char*   params,
                                         XrdOucEnv*     /*not used*/,
                                         XrdAccAuthorize* chain_authz)
  {
    XrdSysError eroute(log, "mgmauthz_");

    if (gMgmAuthz) {
      if (chain_authz) {
        eroute.Say("====== XrdMgmAuthz does not support chaining other "
                   "authorization objects");
      }

      eroute.Say("====== XrdMgmAuthz plugin already loaded and available");
      return gMgmAuthz;
    }

    return XrdAccAuthorizeObject(log, config, params);
  }
}

//------------------------------------------------------------------------------
// Check whether or not the client is permitted specified access to a path.
//------------------------------------------------------------------------------
XrdAccPrivs
XrdMgmAuthz::Access(const XrdSecEntity* Entity, const char* path,
                    const Access_Operation oper, XrdOucEnv* Env)
{
  if (eos::common::EosTok::isEosToken(path)) {
    return XrdAccPriv_All;
  }

  eos_static_info("msg=\"checking access\" path=\"%s\", name=\"%s\"",
                  path, Entity->name);

  if ((Entity == nullptr) || (Entity->name == nullptr)) {
    return XrdAccPriv_None;
  }

  // When a bearer token is already supplied the token library is responsible
  // for deciding the access permissions therefore, in this case the MGM Authz
  // module will not give any additional permissions.
  if (Env && Env->Get("authz")) {
    return XrdAccPriv_None;
  }

  return XrdAccPriv_All;
}
