/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdCommon/XrdCommonMapping.hh"
#include "XrdMgmOfs/XrdMgmVid.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
bool 
XrdMgmVid::Set(const char* value) 
{
  XrdOucEnv env(value);
  XrdOucString skey=env.Get("mgm.vid.key");
  
  XrdOucString vidcmd = env.Get("mgm.vid.cmd");

  if (!skey.length())
    return false;

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
      XrdCommonMapping::gUserRoleVector[uid].clear();
      XrdCommonMapping::KommaListToUidVector(val, XrdCommonMapping::gUserRoleVector[uid]);
      set = true;
    }
    
    if ((val=env.Get("mgm.vid.target.gid"))) {
      // fill gid target list
      XrdCommonMapping::gGroupRoleVector[uid].clear();
      XrdCommonMapping::KommaListToGidVector(val, XrdCommonMapping::gGroupRoleVector[uid]);
      set = true;
    }
    
    if ((val=env.Get("mgm.vid.target.sudo"))) {
      // fill sudoer list
      XrdCommonMapping::gSudoerMap[uid]=1;
      set = true;
    }
  }
  // put the change into the config engine
  if (set) {
    // no '&' are allowed here
    XrdOucString svalue = value;
    while(svalue.replace("&"," ")) {};
    eos_static_info("modified mapping: %s => %s", skey.c_str(), svalue.c_str());
    gOFS->ConfigEngine->SetConfigValue("vid",skey.c_str(), svalue.c_str());
  }
  
  return set;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmVid::Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
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
XrdMgmVid::Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  XrdCommonMapping::Print(stdOut, env.Get("mgm.vid.option"));
  retc = 0;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmVid::Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  return true;
}

/*----------------------------------------------------------------------------*/
const char* 
XrdMgmVid::Get(const char* key) {
  return 0;
}
