#ifndef __XRDFSTOFS_TRANSFER_HH__
#define __XRDFSTOFS_TRANSFER_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileId.hh"
#include "XrdCommon/XrdCommonFmd.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstTransfer {
private:
  unsigned long long fId;
  unsigned long fsIdSource;
  unsigned long fsIdTarget;
  XrdOucString localPrefixSource;
  XrdOucString localPrefixTarget;
  XrdOucString managerId;
  XrdOucString sourceHostPort;
  XrdOucString opaque;
  XrdOucString capability;
  unsigned int tried;
  time_t nexttrytime;

  
public:
  struct XrdCommonFmd::FMD fMd;

  XrdFstTransfer(const char* sourcehostport, unsigned long long fid, unsigned long fsidsource, unsigned long fsidtarget, const char* localprefixsource,const char* localprefixtarget, const char* managerid, const char* inopaque, const char* incap) {
    fId = fid; fsIdSource = fsidsource; fsIdTarget = fsidtarget; localPrefixSource = localprefixsource; localPrefixTarget = localprefixtarget; managerId = managerid; sourceHostPort = sourcehostport; opaque = inopaque; capability = incap; tried=0; nexttrytime=0;
  }
  ~XrdFstTransfer() {}

  static XrdFstTransfer* Create(XrdOucEnv* capOpaque, XrdOucString &capability) {
    // decode the opaque tags
    const char* localprefixsource=0;
    const char* localprefixtarget=0;
    const char* sourcehostport  =0;
    XrdOucString hexfid="";
    XrdOucString access="";
    const char* sfsidsource=0;
    const char* sfsidtarget=0;
    const char* smanager=0;

    unsigned long long fileid=0;
    unsigned long fsidsource=0;
    unsigned long fsidtarget=0;

    sourcehostport   = capOpaque->Get("mgm.sourcehostport");
    localprefixsource = capOpaque->Get("mgm.localprefix");
    localprefixtarget = capOpaque->Get("mgm.localprefixtarget");
    hexfid      = capOpaque->Get("mgm.fid");

    sfsidsource = capOpaque->Get("mgm.fsid");
    sfsidtarget = capOpaque->Get("mgm.fsidtarget");

    smanager    = capOpaque->Get("mgm.manager");

    access      = capOpaque->Get("mgm.access");

    // permission check
    if (access != "read") 
      return 0;

    if (!sourcehostport || !localprefixsource || !localprefixtarget || !hexfid.length() || !sfsidsource || !sfsidtarget || !smanager) {
      return 0;
    }

    fileid = XrdCommonFileId::Hex2Fid(hexfid.c_str());	
    
    fsidsource   = atoi(sfsidsource);
    fsidtarget   = atoi(sfsidtarget);
    
    int envlen = 0;
    return new XrdFstTransfer(sourcehostport, fileid, fsidsource, fsidtarget, localprefixsource, localprefixtarget, smanager, capOpaque->Env(envlen), capability.c_str());
  }

  void Show(const char* show="") {
    eos_static_info("Pull File Id=%llu on Fs=%u from Host=%s Fs=%u tried=%u reschedul=%u %s", fId, fsIdTarget, sourceHostPort.c_str(), fsIdSource, tried, nexttrytime, show);
  }

  void Debug() {
    eos_static_debug("Pull File Id=%llu on Fs=%u from Host=%s Fs=%u tried=%u reschedul=%u", fId, fsIdTarget, sourceHostPort.c_str(), fsIdSource, tried, nexttrytime);
  }

  int Do();

  void Reschedule(unsigned int aftersecs) { tried++; nexttrytime=time(NULL) + aftersecs;}
  bool ShouldRun() { 
    if (time(0)>= nexttrytime) return true; else return false;}
};


#endif
