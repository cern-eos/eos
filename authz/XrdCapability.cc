// ----------------------------------------------------------------------
// File: XrdCapability.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include "authz/XrdCapability.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSec/XrdSecInterface.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysError.hh"
/*----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef __APPLE__
#define ENOKEY 126
#define EKEYREJECTED 129
#endif

/*----------------------------------------------------------------------------*/
XrdSysError TkEroute(0,"capability");
XrdOucTrace TkTrace(&TkEroute);
/*----------------------------------------------------------------------------*/
XrdCapability gCapabilityEngine;

/*----------------------------------------------------------------------------*/
XrdAccPrivs
XrdCapability::Access(const XrdSecEntity    *Entity,
                      const char            *path,
                      const Access_Operation oper,
                      XrdOucEnv       *Env)
{
  return XrdAccPriv_All;
}

/*----------------------------------------------------------------------------*/
bool
XrdCapability::Configure(const char* ConfigFN) 
{
  char *var;
  int  cfgFD, NoGo = 0;
  XrdOucStream Config(&TkEroute, getenv("XRDINSTANCE"));

  if (!ConfigFN || !*ConfigFN) {
    TkEroute.Emsg("Config","Configuration file not specified.");
  } else {
    // Try to open the configuration file
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return TkEroute.Emsg("Config", errno, "open config file", ConfigFN);
    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    while ((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "capability.",10)) {
        var+= 10;
      }
    }
    
    Config.Close();
  }
  
  return ( (bool) (!NoGo) );
}
  
/*----------------------------------------------------------------------------*/
bool 
XrdCapability::Init() 
{
  
  return true;
}

/*----------------------------------------------------------------------------*/
int
XrdCapability::Create(XrdOucEnv *inenv, XrdOucEnv* &outenv, eos::common::SymKey* key) 
{
  outenv = 0;

  if (!key)
    return ENOKEY;

  if (!inenv) 
    return EINVAL;

  int envlen;
  XrdOucString toencrypt = inenv->Env(envlen);
  toencrypt += "&cap.valid=";
  char validity[32];
  snprintf(validity,32,"%u", (unsigned int) time(NULL) + 3600); // one hour validity
  toencrypt += validity;

  XrdOucString encrypted="";
  
  if (!XrdMqMessage::SymmetricStringEncrypt(toencrypt, encrypted, (char*)key->GetKey())) {
    return EKEYREJECTED;
  } 
  
  XrdOucString encenv = "";
  encenv += "cap.sym="; encenv+= key->GetDigest64();
  encenv += "&cap.msg="; encenv += encrypted;
  while (encenv.replace('\n','#')) {};
  outenv = new XrdOucEnv(encenv.c_str());
  return 0;
}

/*----------------------------------------------------------------------------*/
int 
XrdCapability::Extract(XrdOucEnv *inenv, XrdOucEnv* &outenv)
{
  outenv = 0;
  if (!inenv)
    return EINVAL;

  int envlen;
  XrdOucString instring = inenv->Env(envlen);
  while (instring.replace('#','\n')) {};
  XrdOucEnv fixedenv(instring.c_str());

  //  fprintf(stderr,"Extracting: %s\n", fixedenv.Env(envlen));
  const char* symkey = fixedenv.Get("cap.sym");
  const char* symmsg = fixedenv.Get("cap.msg");

  //  fprintf(stderr,"%s\n%s\n", symkey, symmsg);
  if ( (!symkey) || (!symmsg) ) 
    return EINVAL;
  
  eos::common::SymKey* key = 0;
  if (!(key = eos::common::gSymKeyStore.GetKey(symkey))) {
    return ENOKEY;
  }
  
  XrdOucString todecrypt = symmsg;
  XrdOucString decrypted ="";
  if (!XrdMqMessage::SymmetricStringDecrypt(todecrypt, decrypted, (char*)key->GetKey())) {
    return EKEYREJECTED;
  } 
  outenv = new XrdOucEnv(decrypted.c_str());

  if (!outenv->Get("cap.valid")) {
    return EINVAL;
  } else {
    time_t now = time(NULL);
    time_t capnow = atoi(outenv->Get("cap.valid"));
    // capability expired!

    if (capnow < now) 
      return ETIME;
  }

  return 0;
}

/*----------------------------------------------------------------------------*/
XrdCapability::~XrdCapability() {}
/* XrdAccAuthorizeObject() is called to obtain an instance of the auth object
   that will be used for all subsequent authorization decisions. If it returns
   a null pointer; initialization fails and the program exits. The args are:

   lp    -> XrdSysLogger to be tied to an XrdSysError object for messages
   cfn   -> The name of the configuration file
   parm  -> Parameters specified on the authlib directive. If none it is zero.
*/

extern "C" XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp,
                                                  const char   *cfn,
                                                  const char   *parm)
{
  TkEroute.SetPrefix("capability_");
  TkEroute.logger(lp);
  XrdOucString version = "Capability (authorization) ";
  version += VERSION;

  TkEroute.Say("++++++ (c) 2010 CERN/IT-DSS ", version.c_str());

  XrdAccAuthorize* acc = (XrdAccAuthorize*) new XrdCapability();
  if (!acc) {
    TkEroute.Say("------ XrdCapability Allocation Failed!");
    return 0;
  }

  if (!((XrdCapability*)acc)->Configure(cfn) || (!((XrdCapability*)acc)->Init())) {
    TkEroute.Say("------ XrdCapability Initialization Failed!");
    delete acc;
    return 0;
  } else {
    TkEroute.Say("------ XrdCapability Initialization completed");
    return acc;
  }
}

