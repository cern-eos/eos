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

#include "mgm/XrdMgmAuthz.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdVersion.hh"

XrdSysError Eroute(0, "capability");
XrdOucTrace Trace(&Eroute);
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
extern "C" XrdAccAuthorize* XrdAccAuthorizeObject(XrdSysLogger* lp,
    const char*   cfn,
    const char*   parm)
{
  if (gMgmAuthz) {
    return gMgmAuthz;
  }

  Eroute.SetPrefix("mgmauthz__");
  Eroute.logger(lp);
  XrdOucString version = "EOS MGM Authorization ";
  version += VERSION;
  Eroute.Say("++++++ (c) 2020 CERN/IT-ST ", version.c_str());
  gMgmAuthz = new XrdMgmAuthz();

  if (!gMgmAuthz) {
    Eroute.Say("------ XrdMgmAuthz allocation failed!");
  } else {
    Eroute.Say("------ XrdMgmAuthz initialization completed");
  }

  return static_cast<XrdAccAuthorize*>(gMgmAuthz);
}

//------------------------------------------------------------------------------
// Check whether or not the client is permitted specified access to a path.
//------------------------------------------------------------------------------
XrdAccPrivs
XrdMgmAuthz::Access(const XrdSecEntity* Entity, const char* path,
                    const Access_Operation oper, XrdOucEnv* Env)
{
  eos_static_info("msg=\"checking access\" path=\"%s\", name=\"%s\"",
                  path, Entity->name);
  return XrdAccPriv_None;
}
