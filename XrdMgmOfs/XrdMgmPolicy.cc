/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdCommon/XrdCommonMapping.hh"
#include "XrdMgmOfs/XrdMgmPolicy.hh"
#include "XrdMgmOfs/XrdMgmOfs.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------*/
void
XrdMgmPolicy::GetLayoutAndSpace(const char* path, uid_t uid, gid_t gid, unsigned long &layoutId, XrdOucString &space, XrdOucEnv &env, unsigned long &forcedfsid) 

{
  // this is for the moment only defaulting or manual selection
  unsigned long layout      = XrdCommonLayoutId::GetLayoutFromEnv(env);
  unsigned long xsum        = XrdCommonLayoutId::GetChecksumFromEnv(env);
  unsigned long stripes     = XrdCommonLayoutId::GetStripeNumberFromEnv(env);
  unsigned long stripewidth = XrdCommonLayoutId::GetStripeWidthFromEnv(env);

  
  const char* val=0;
  if ( (val=env.Get("eos.space"))) {
    space = val;
  } else {
    space = "default";
  }
  
  if ((val = env.Get("eos.force.fsid"))) {
    forcedfsid = strtol(val,0,10);
  } else {
    forcedfsid = 0;
  }
  layoutId = XrdCommonLayoutId::GetId(layout, xsum, stripes, stripewidth);
  return; 
}


/*----------------------------------------------------------------------------*/
bool 
XrdMgmPolicy::Set(const char* value) 
{
  XrdOucEnv env(value);
  XrdOucString policy=env.Get("mgm.policy");

  XrdOucString skey=env.Get("mgm.policy.key");
  
  XrdOucString policycmd = env.Get("mgm.policy.cmd");

  if (!skey.length())
    return false;

  bool set=false;

  if (!value) 
    return false;

  //  gOFS->ConfigEngine->SetConfigValue("policy",skey.c_str(), svalue.c_str());
  
  return set;
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmPolicy::Set(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  int envlen;
  // no '&' are allowed into stdOut !
  XrdOucString inenv = env.Env(envlen);
  while(inenv.replace("&"," ")) {};
  bool rc = Set(env.Env(envlen));
  if (rc == true) {
    stdOut += "success: set policy [ "; stdOut += inenv; stdOut += "]\n";
    errno = 0;
    retc = 0;
    return true;
  } else {
    stdErr += "error: failed to set policy [ "; stdErr += inenv ; stdErr += "]\n";
    errno = EINVAL;
    retc = EINVAL;
    return false;
  }
}

/*----------------------------------------------------------------------------*/
void 
XrdMgmPolicy::Ls(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  
}

/*----------------------------------------------------------------------------*/
bool
XrdMgmPolicy::Rm(XrdOucEnv &env, int &retc, XrdOucString &stdOut, XrdOucString &stdErr)
{
  return true;
}

/*----------------------------------------------------------------------------*/
const char* 
XrdMgmPolicy::Get(const char* key) {
  return 0;
}
