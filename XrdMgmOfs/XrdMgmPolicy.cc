/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonLogging.hh"
#include "XrdCommon/XrdCommonLayoutId.hh"
#include "XrdMgmOfs/XrdMgmPolicy.hh"
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

