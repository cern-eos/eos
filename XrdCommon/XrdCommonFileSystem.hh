#ifndef __XRDCOMMON_FILESYSTEM_HH__
#define __XRDCOMMON_FILESYSTEM_HH__

/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/
#include <string.h>
/*----------------------------------------------------------------------------*/

class XrdCommonFileSystem {
public:

  enum eBootStatus   { kBootFailure=-1, kDown=0, kBootSent, kBooting=2, kBooted=3};
  enum eConfigStatus { kOff=0, kRO, kRW};

  static const char* GetStatusAsString(int status) {
    if (status == kDown) return "down";
    if (status == kBootFailure) return "bootfailure";
    if (status == kBootSent) return "bootsent";
    if (status == kBooting) return "booting";
    if (status == kBooted) return "booted";
  }

  static int GetStatusFromString(const char* ss) {
    if (!ss) 
      return kDown;
    
    if (!strcmp(ss,"down")) return kDown;
    if (!strcmp(ss,"bootfailure")) return kBootFailure;
    if (!strcmp(ss,"bootsent")) return kBootSent;
    if (!strcmp(ss,"booting")) return kBooting;
    if (!strcmp(ss,"booted")) return kBooted;
    return kDown;
  }

  static const char* GetBootReplyString(XrdOucString &msgbody, XrdOucEnv &config, int status, const char* failurereason=0) {
    int envlen;
    XrdOucString envstring = config.Env(envlen);
    envstring.replace("mgm.cmd=","mgm._cmd=");
    envstring.replace("mgm.subcmd=","mgm._subcmd=");
    msgbody = "mgm.cmd=fs&"; msgbody += "mgm.subcmd=set&";msgbody += envstring; msgbody += "&" ; msgbody+="mgm.fsstatus=";msgbody += GetStatusAsString(status); msgbody+="&";
    if (failurereason) { msgbody +="errmsg="; msgbody+= failurereason;}
  }

  static const char* GetBootRequestString(XrdOucString &msgbody, XrdOucEnv &config) {
    int envlen;
    XrdOucString envstring = config.Env(envlen);
    envstring.replace("mgm.cmd=","mgm._cmd=");
    envstring.replace("mgm.subcmd=","mgm._subcmd=");
    msgbody = "mgm.cmd=fs&"; msgbody += "mgm.subcmd=boot"; msgbody += envstring;  
  }

  static const char* GetAutoBootRequestString() {
    return "mgm.cmd=bootreq";
  }
}; 



#endif
