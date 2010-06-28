#ifndef __XRDFSTOFS_TRANSFER_HH__
#define __XRDFSTOFS_TRANSFER_HH__

/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileId.hh"
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

public:
  XrdFstTransfer(const char* sourcehostport, unsigned long long fid, unsigned long fsidsource, unsigned long fsidtarget, const char* localprefixsource,const char* localprefixtarget, const char* managerid, const char* inopaque, const char* incap) {
    fId = fid; fsIdSource = fsidsource; fsIdTarget = fsidtarget; localPrefixSource = localprefixsource; localPrefixTarget = localprefixtarget; managerId = managerid; sourceHostPort = sourcehostport; opaque = inopaque; capability = incap;
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

  void Debug() {
    eos_static_debug("Pull File Id=%llu on Fs=%u from Host=%s Fs=%u", fId, fsIdTarget, sourceHostPort.c_str(), fsIdSource);
  }

  int Do();
};


#endif
