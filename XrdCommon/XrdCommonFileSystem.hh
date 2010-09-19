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

  enum eBootStatus   { kOpsError=-2, kBootFailure=-1, kDown=0, kBootSent, kBooting=2, kBooted=3};
  enum eConfigStatus { kUnknown=-1, kOff=0, kDrain, kRO, kWO, kRW};

  static const char* GetStatusAsString(int status) {
    if (status == kDown) return "down";
    if (status == kOpsError) return "opserror";
    if (status == kBootFailure) return "bootfailure";
    if (status == kBootSent) return "bootsent";
    if (status == kBooting) return "booting";
    if (status == kBooted) return "booted";
    return "unknown";
  }

  static int GetStatusFromString(const char* ss) {
    if (!ss) 
      return kDown;
    
    if (!strcmp(ss,"down")) return kDown;
    if (!strcmp(ss,"opserror")) return kOpsError;
    if (!strcmp(ss,"bootfailure")) return kBootFailure;
    if (!strcmp(ss,"bootsent")) return kBootSent;
    if (!strcmp(ss,"booting")) return kBooting;
    if (!strcmp(ss,"booted")) return kBooted;
    return kDown;
  }

  static int GetConfigStatusFromString(const char* ss) {
    if (!ss) 
      return kDown;
    
    if (!strcmp(ss,"unknown")) return kUnknown;
    if (!strcmp(ss,"off")) return kOff;
    if (!strcmp(ss,"drain")) return kDrain;
    if (!strcmp(ss,"ro")) return kRO;
    if (!strcmp(ss,"wo")) return kWO;
    if (!strcmp(ss,"rw")) return kRW;
    return kUnknown;
  }

  static const char* GetBootReplyString(XrdOucString &msgbody, XrdOucEnv &config, int status, const char* failurereason=0) {
    int envlen;
    XrdOucString envstring = config.Env(envlen);
    envstring.replace("mgm.cmd=","mgm._cmd=");
    envstring.replace("mgm.subcmd=","mgm._subcmd=");
    msgbody = "mgm.cmd=fs&"; msgbody += "mgm.subcmd=set";msgbody += envstring; msgbody += "&" ; msgbody+="mgm.fsstatus=";msgbody += GetStatusAsString(status); msgbody+="&";
    if (failurereason) { msgbody +="errmsg="; msgbody+= failurereason;}
    return msgbody.c_str();
  }

  static const char* GetBootRequestString(XrdOucString &msgbody, XrdOucEnv &config) {
    int envlen;
    XrdOucString envstring = config.Env(envlen);
    envstring.replace("mgm.cmd=","mgm._cmd=");
    envstring.replace("mgm.subcmd=","mgm._subcmd=");
    msgbody = "mgm.cmd=fs&"; msgbody += "mgm.subcmd=boot"; msgbody += envstring;  
    return msgbody.c_str();
  }
  
  static const char* GetDropTransferRequestString(XrdOucString &msgbody) {
    msgbody = "mgm.cmd=droptransfers";
    return msgbody.c_str();
  }

  static const char* GetListTransferRequestString(XrdOucString &msgbody) {
    msgbody = "mgm.cmd=listtransfers";
    return msgbody.c_str();
  }

  static const char* GetRestartRequestString(XrdOucString &msgbody) {
    msgbody = "mgm.cmd=restart"; 
    return msgbody.c_str();
  }

  static const char* GetAutoBootRequestString() {
    return "mgm.cmd=bootreq";
  }

  static const char* GetQuotaReportString(XrdOucString &msgbody) {
    msgbody = "mgm.cmd=quota&"; msgbody += "mgm.subcmd=setstatus"; msgbody += "&" ;
    return msgbody.c_str();
  }

  static void CreateQuotaReportString(const char* tag, XrdOucString &reportstring) {
    reportstring = tag; reportstring+="=";;
  }

  static void AddQuotaReportString(unsigned long long id, unsigned long long val, XrdOucString &reportstring) {
    char addit[1024];
    sprintf(addit,"%llu:%llu,", id, val);
    reportstring += addit;
  }

  static const char* GetReadableSizeString(XrdOucString& sizestring, unsigned long long insize, const char* unit = "") {
    char formsize[1024];
    if (insize > 1000) {
      if (insize > (1000*1000)) {
	if (insize > (1000l*1000l*1000l)) {
	  if (insize > (1000l*1000l*1000l*1000l)) {
	    // TB
	    sprintf(formsize,"%.02f T%s",insize*1.0 / (1000l*1000l*1000l*1000l), unit);
	  } else {
	    // GB
	    sprintf(formsize,"%.02f G%s",insize*1.0 / (1000l*1000l*1000l), unit);
	  }
	} else {
	  // MB
	  sprintf(formsize,"%.02f M%s",insize*1.0 / (1000*1000),unit);
	}
      } else {
	// kB
	sprintf(formsize,"%.02f k%s",insize*1.0 / (1000),unit);
      }
    } else {
      if (strlen(unit)) {
	sprintf(formsize,"%.02f %s",insize*1.0, unit);
      } else {
	sprintf(formsize,"%.02f",insize*1.0);
      }
    }
    sizestring = formsize;

    return sizestring.c_str();
  }

  static unsigned long long GetSizeFromString(XrdOucString sizestring) {
    errno = 0;
    unsigned long long convfactor;
    convfactor = 1ll;
    if (!sizestring.length()) {
      errno = EINVAL;
      return 0;
    }

    if (sizestring.endswith("B") || sizestring.endswith("b")) {
      sizestring.erase(sizestring.length()-1);
    }
      
    if (sizestring.endswith("T") || sizestring.endswith("t")) {
      convfactor = 1000l*1000l*1000l*1000l;
    }

    if (sizestring.endswith("G") || sizestring.endswith("g")) {
      convfactor = 1000l*1000l*1000l;
    }
    
    if (sizestring.endswith("M") || sizestring.endswith("m")) {
      convfactor = 1000l*1000l;
    }

    if (sizestring.endswith("K") || sizestring.endswith("k")) {
      convfactor = 1000l;
    }
    if (convfactor >1)
      sizestring.erase(sizestring.length()-1);
    return (strtoll(sizestring.c_str(),0,10) * convfactor);
  }

  static const char* GetSizeString(XrdOucString& sizestring, unsigned long long insize) {
    char buffer[1024];
    sprintf(buffer,"%llu", insize);
    sizestring = buffer;
    return sizestring.c_str();
  }

  static bool SplitKeyValue(XrdOucString keyval, XrdOucString &key, XrdOucString &value) {
    int equalpos = keyval.find(":");
    if (equalpos != STR_NPOS) {
      key.assign(keyval,0,equalpos-1);
      value.assign(keyval,equalpos+1);
      return true;
    } else {
      key=value="";
      return false;
    }
  }
}; 



#endif
