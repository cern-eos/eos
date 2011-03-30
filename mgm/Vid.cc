/*----------------------------------------------------------------------------*/
#include "common/Logging.hh"
#include "common/LayoutId.hh"
#include "common/Mapping.hh"
#include "mgm/Vid.hh"
#include "mgm/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool 
Vid::Set(const char* value) 
{
  XrdOucEnv env(value);
  XrdOucString skey=env.Get("mgm.vid.key");
  
  XrdOucString vidcmd = env.Get("mgm.vid.cmd");

  if (!skey.length()) {
    return false;
  }

  bool set=false;

  if (!value) 
    return false;
  
  if (vidcmd == "membership") {
    uid_t uid=99;
    
    if (env.Get("mgm.vid.source.uid")) {
      // rule for a certain user id
      uid = (uid_t) atoi(env.Get("mgm.vid.source.uid"));
    }
    
    const char* val=0;
    
    if ((val=env.Get("mgm.vid.target.uid"))) {
      // fill uid target list
      eos::common::Mapping::gUserRoleVector[uid].clear();
      eos::common::Mapping::KommaListToUidVector(val, eos::common::Mapping::gUserRoleVector[uid]);
      set = true;
    }
    
    if ((val=env.Get("mgm.vid.target.gid"))) {
      // fill gid target list
      eos::common::Mapping::gGroupRoleVector[uid].clear();
      eos::common::Mapping::KommaListToGidVector(val, eos::common::Mapping::gGroupRoleVector[uid]);
      set = true;
    }
    
    if ((val=env.Get("mgm.vid.target.sudo"))) {
      // fill sudoer list
      XrdOucString setting = val;
      if (setting == "true") {	
	eos::common::Mapping::gSudoerMap[uid]=1;
	set = true;
      } else {
	// this in fact is deletion of the right
	eos::common::Mapping::gSudoerMap[uid]=0;
	gOFS->ConfEngine->DeleteConfigValue("vid",skey.c_str());
	return true;
      }
    }
  }

  if (vidcmd == "map") {
    XrdOucString auth = env.Get("mgm.vid.auth");
    if ( (auth != "ssl") && (auth != "krb5") && (auth != "sss") && (auth!="unix") && (auth!="tident") && (auth!="gsi") ) {
      eos_static_err("invalid auth mode");
      return false;
    }
   
    XrdOucString pattern = env.Get("mgm.vid.pattern");
    if (!pattern.length()) {
      eos_static_err("missing pattern");
      return false;
    }

    if (! pattern.beginswith("\"")) {
      pattern.insert("\"",0);
    }

    if (! pattern.endswith("\"")) {
      pattern+= "\"";
    }

    skey = auth; 
    skey += ":"; skey +=pattern;

    if ((!env.Get("mgm.vid.uid")) && (!env.Get("mgm.vid.gid"))) {
      eos_static_err("missing uid|gid");
      return false;
    }
    
    XrdOucString newuid = env.Get("mgm.vid.uid");
    XrdOucString newgid = env.Get("mgm.vid.gid");
    
    if (newuid.length()) {
      uid_t muid = (uid_t)atoi(newuid.c_str());
      XrdOucString cx = ""; cx += (int)muid;
      if (cx != newuid) {
	eos_static_err("strings differ %s %s", cx.c_str(),newuid.c_str());
	return false;
      }
      skey += ":"; skey += "uid";
      eos::common::Mapping::gVirtualUidMap[skey.c_str()] = muid;
      set = true;
      
      // no '&' are allowed here
      XrdOucString svalue = value;
      while(svalue.replace("&"," ")) {};
      gOFS->ConfEngine->SetConfigValue("vid",skey.c_str(), svalue.c_str());
    }

    skey = auth; 
    skey += ":"; skey +=pattern;

    if (newgid.length()) {
      gid_t mgid = (gid_t)atoi(newgid.c_str());
      XrdOucString cx =""; cx += (int) mgid;
      if (cx != newgid) {
	eos_static_err("strings differ %s %s", cx.c_str(),newgid.c_str());
	return false;
      }
      skey += ":"; skey += "gid";
      eos::common::Mapping::gVirtualGidMap[skey.c_str()] = mgid;
      set = true;

      // no '&' are allowed here
      XrdOucString svalue = value;
      while(svalue.replace("&"," ")) {};
      gOFS->ConfEngine->SetConfigValue("vid",skey.c_str(), svalue.c_str());
    }
  }

  // put the change into the config engine
  if (set) {
    // no '&' are allowed here
    XrdOucString svalue = value;
    while(svalue.replace("&"," ")) {};
    gOFS->ConfEngine->SetConfigValue("vid",skey.c_str(), svalue.c_str());
  }
  
  return set;
}

/*----------------------------------------------------------------------------*/
bool
Vid::Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  int envlen;
  // no '&' are allowed into stdOut !
  XrdOucString inenv = env.Env(envlen);
  while(inenv.replace("&"," ")) {};
  bool rc = Set(env.Env(envlen));
  if (rc == true) {
    stdOut += "success: set vid [ "; stdOut += inenv; stdOut += "]\n";
    errno = 0;
    retc = 0;
    return true;
  } else {
    stdErr += "error: failed to set vid [ "; stdErr += inenv ; stdErr += "]\n";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void 
Vid::Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  eos::common::Mapping::Print(stdOut, env.Get("mgm.vid.option"));
  retc = 0;
}

/*----------------------------------------------------------------------------*/
bool
Vid::Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  XrdOucString skey=env.Get("mgm.vid.key");
  XrdOucString vidcmd = env.Get("mgm.vid.cmd");
  int envlen=0;
  XrdOucString inenv = env.Env(envlen);
  while(inenv.replace("&"," ")) {};
  
  if (!skey.length()) {
    stdErr += "error: failed to rm vid [ "; stdErr += inenv ; stdErr += "] - key missing";
    errno = EINVAL;
    retc = EINVAL;    
    return false;
  }

  if (vidcmd != "unmap") {
    stdErr += "error: failed to rm vid [ "; stdErr += inenv ; stdErr += "] - wrong command to unmap";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
  
  stdOut += "success: rm vid [ "; stdOut += inenv; stdOut += "]";
  errno = 0;
  retc = 0;

  eos::common::Mapping::gVirtualUidMap.erase(skey.c_str());
  eos::common::Mapping::gVirtualGidMap.erase(skey.c_str());

  gOFS->ConfEngine->DeleteConfigValue("vid",skey.c_str());

  return true;
}

/*----------------------------------------------------------------------------*/
const char* 
Vid::Get(const char* key) {
  return 0;
}

EOSMGMNAMESPACE_END
