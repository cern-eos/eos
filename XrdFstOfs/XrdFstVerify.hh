#ifndef __XRDFSTOFS_VERIFY_HH__
#define __XRDFSTOFS_VERIFY_HH__
/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
/*----------------------------------------------------------------------------*/
#include <vector>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstVerify {
public:
  unsigned long long fId;
  unsigned long fsId;
  unsigned long cId;
  unsigned long lId;

  XrdOucString localPrefix;
  XrdOucString managerId;
  XrdOucString opaque;
  XrdOucString container;
  XrdOucString path;

  bool doChecksum;

  XrdFstVerify(unsigned long long fid, unsigned long fsid, const char* localprefix, const char* managerid, const char* inopaque,const char* incontainer, unsigned long incid, unsigned long inlid, const char* inpath, bool indoChecksum) {
    fId = fid;  fsId = fsid; localPrefix = localprefix; managerId = managerid; opaque = inopaque;
    container = incontainer; cId = incid; path = inpath; doChecksum = indoChecksum; lId = inlid;
  }

  static XrdFstVerify* Create(XrdOucEnv* capOpaque) {
    // decode the opaque tags
    const char* localprefix=0;
    XrdOucString hexfids="";
    XrdOucString hexfid="";
    XrdOucString access="";
    const char* container=0;
    const char* scid=0;
    const char* layout=0;
    const char* path=0;
    bool doChecksum=false;

    const char* sfsid=0;
    const char* smanager=0;
    unsigned long long fid=0;

    unsigned long fsid=0;
    unsigned long cid=0;
    unsigned long lid=0;

    localprefix = capOpaque->Get("mgm.localprefix");
    hexfid      = capOpaque->Get("mgm.fid");
    sfsid       = capOpaque->Get("mgm.fsid");
    smanager    = capOpaque->Get("mgm.manager");
    access      = capOpaque->Get("mgm.access");
    container   = capOpaque->Get("container");
    scid        = capOpaque->Get("mgm.cid");
    path        = capOpaque->Get("mgm.path");
    layout      = capOpaque->Get("mgm.lid");

    if (capOpaque->Get("mgm.verify.dochecksum")) {
      doChecksum=true;
    }

    // permission check
    if (access != "verify") 
      return 0;

    if (!localprefix || !hexfid.length() || !sfsid || !smanager || !layout || !scid) {
      return 0;
    }

    cid = strtoul(scid,0,10);
    lid = strtoul(layout,0,10);

    int envlen=0;

    fid = XrdCommonFileId::Hex2Fid(hexfid.c_str());	
    fsid   = atoi(sfsid);
    return new XrdFstVerify(fid, fsid, localprefix, smanager, capOpaque->Env(envlen), container, cid, lid, path, doChecksum);
  };

  ~XrdFstVerify() {};
};


#endif
