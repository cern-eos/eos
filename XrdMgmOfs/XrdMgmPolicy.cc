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
XrdMgmPolicy::GetLayoutAndSpace(const char* path, eos::ContainerMD::XAttrMap &attrmap, const XrdCommonMapping::VirtualIdentity &vid, unsigned long &layoutId, XrdOucString &space, XrdOucEnv &env, unsigned long &forcedfsid) 

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

  if (attrmap.count("sys.forced.space")) {
    // we force to use a certain space in this directory even if the user wants something else
    space = attrmap["sys.forced.space"].c_str();
    eos_static_debug("sys.forced.space in %s",path);
  }

  if (attrmap.count("sys.forced.layout")) {
    XrdOucString layoutstring = "eos.layout.type="; layoutstring += attrmap["sys.forced.layout"].c_str();
    XrdOucEnv layoutenv(layoutstring.c_str());
    // we force to use a specified layout in this directory even if the user wants something else
    layout = XrdCommonLayoutId::GetLayoutFromEnv(layoutenv);
    eos_static_debug("sys.forced.layout in %s",path);
  }

  if (attrmap.count("sys.forced.checksum")) {
    XrdOucString layoutstring = "eos.layout.checksum="; layoutstring += attrmap["sys.forced.checksum"].c_str();
    XrdOucEnv layoutenv(layoutstring.c_str());
    // we force to use a specified checksumming in this directory even if the user wants something else
    xsum = XrdCommonLayoutId::GetChecksumFromEnv(layoutenv);
    eos_static_debug("sys.forced.checksum in %s",path);
  }
  if (attrmap.count("sys.forced.nstripes")) {
    XrdOucString layoutstring = "eos.layout.nstripes="; layoutstring += attrmap["sys.forced.nstripes"].c_str();
    XrdOucEnv layoutenv(layoutstring.c_str());
    // we force to use a specified stripe number in this directory even if the user wants something else
    stripes = XrdCommonLayoutId::GetStripeNumberFromEnv(layoutenv);
    eos_static_debug("sys.forced.nstripes in %s",path);
  }

  if (attrmap.count("sys.forced.stripewidth")) {
    XrdOucString layoutstring = "eos.layout.stripewidth="; layoutstring += attrmap["sys.forced.stripewidth"].c_str();
    XrdOucEnv layoutenv(layoutstring.c_str());
    // we force to use a specified stripe width in this directory even if the user wants something else
    stripewidth = XrdCommonLayoutId::GetStripeNumberFromEnv(layoutenv);
    eos_static_debug("sys.forced.stripewidth in %s",path);
  }

  if ( ((!attrmap.count("sys.forced.nouserlayout")) || (attrmap["sys.forced.nouserlayout"] != "1")) &&
       ((!attrmap.count("user.forced.nouserlayout")) || (attrmap["user.forced.nouserlayout"] != "1")) ) {

    if (attrmap.count("user.forced.space")) {
      // we force to use a certain space in this directory even if the user wants something else
      space = attrmap["user.forced.space"].c_str();
      eos_static_debug("user.forced.space in %s",path);
    }

    if (attrmap.count("user.forced.layout")) {
      XrdOucString layoutstring = "eos.layout.type="; layoutstring += attrmap["user.forced.layout"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified layout in this directory even if the user wants something else
      layout = XrdCommonLayoutId::GetLayoutFromEnv(layoutenv);
      eos_static_debug("user.forced.layout in %s",path);
    }
    
    if (attrmap.count("user.forced.checksum")) {
      XrdOucString layoutstring = "eos.layout.checksum="; layoutstring += attrmap["user.forced.checksum"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified checksumming in this directory even if the user wants something else
      xsum = XrdCommonLayoutId::GetChecksumFromEnv(layoutenv);
      eos_static_debug("user.forced.checksum in %s",path);
    }
    if (attrmap.count("user.forced.nstripes")) {
      XrdOucString layoutstring = "eos.layout.nstripes="; layoutstring += attrmap["user.forced.nstripes"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe number in this directory even if the user wants something else
      stripes = XrdCommonLayoutId::GetStripeNumberFromEnv(layoutenv);
      eos_static_debug("user.forced.nstripes in %s",path);
    }
    
    if (attrmap.count("user.forced.stripewidth")) {
      XrdOucString layoutstring = "eos.layout.stripewidth="; layoutstring += attrmap["user.forced.stripewidth"].c_str();
      XrdOucEnv layoutenv(layoutstring.c_str());
      // we force to use a specified stripe width in this directory even if the user wants something else
      stripewidth = XrdCommonLayoutId::GetStripeNumberFromEnv(layoutenv);
      eos_static_debug("user.forced.stripewidth in %s",path);
    }
  }

  if ( (attrmap.count("sys.forced.nofsselection") && (attrmap["sys.forced.nofsselection"]=="1")) || 
       (attrmap.count("user.forced.nofsselection") && (attrmap["user.forced.nofsselection"]=="1")) ) {
    eos_static_debug("<sys|user>.forced.nofsselection in %s",path);
    forcedfsid = 0;
  } else {
    if ((val = env.Get("eos.force.fsid"))) {
      forcedfsid = strtol(val,0,10);
    } else {
      forcedfsid = 0;
    }
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
