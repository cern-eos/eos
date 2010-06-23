#ifndef __XRDFSTOFS_DELETION_HH__
#define __XRDFSTOFS_DELETION_HH__
/*----------------------------------------------------------------------------*/
#include "XrdCommon/XrdCommonFileId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucEnv.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
class XrdFstDeletion {
private:
  unsigned long long fId;
  unsigned long fsId;
  XrdOucString localPrefix;
  XrdOucString managerId;

public:
  XrdFstDeletion(unsigned long long fid, unsigned long fsid, const char* localprefix, const char* managerid) {
    fId = fid; fsId = fsid; localPrefix = localprefix; managerId = managerid;
  }

  static XrdFstDeletion* Create(XrdOucEnv* capOpaque) {
    // decode the opaque tags
    const char* localprefix=0;
    const char* hexfid=0;
    const char* sfsid=0;
    const char* smanager=0;
    
    unsigned long long fileid=0;
    unsigned long fsid=0;

    localprefix=capOpaque->Get("mgm.localprefix");
    hexfid=capOpaque->Get("mgm.fid");
    sfsid=capOpaque->Get("mgm.fsid");
    smanager=capOpaque->Get("mgm.manager");
    if (!localprefix || !hexfid || !sfsid || !smanager) {
      return 0;
    }
    fileid = XrdCommonFileId::Hex2Fid(hexfid);
    fsid   = atoi(sfsid);
    return new XrdFstDeletion(fileid, fsid, localprefix, smanager);
  };

  ~XrdFstDeletion() {};
};


#endif
